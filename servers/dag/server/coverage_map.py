# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""The artifact coverage map, read out of the landing-zone knowledge document.

`## Coverage map` in servers/phases/landingzone/knowledge/coverage-map.md is a
markdown table assigning every migration artifact kind to exactly one owner
(landing-zone / platform-translation / workload) and one column (terraform /
k8s). That table is the authority on the generation boundary, and this module
is how the server reads it — the same arrangement as blocker_criteria.py: the
humans and the machine read one table, so the boundary cannot drift between a
document and a hardcoded copy, and moving an artifact kind stays a
documentation edit.

Unlike the blocker taxonomy, two cells of every row are a closed vocabulary
(owner, column), so this parser validates values, not just shape: an unknown
owner is far more likely a typo that would silently exempt a row from the
plan-time omission/overlap checks (landingzone_translationplan_3/coverage.py)
and the validate step's enforced omission gate
(translation_validate_3/coverage_gate.py) than a deliberate new generator,
and a new generator IS a design event that should not slip in as a table
edit.

Failure to parse is fatal at start-up, matching dag_validation and
blocker_criteria: degrading to an empty map would turn every coverage check
into a no-op.
"""

import logging
import os
import re

logger = logging.getLogger("migration-dag")

_HERE = os.path.dirname(os.path.realpath(__file__))

# servers/dag/server/ -> repository root, the same anchor dag_validation uses.
REPO_ROOT = os.path.abspath(os.path.join(_HERE, "..", "..", ".."))

KNOWLEDGE_DOC = os.path.join(
    "servers", "phases", "landingzone", "knowledge", "coverage-map.md"
)

OWNERS = frozenset({"landing-zone", "platform-translation", "workload"})
COLUMNS = frozenset({"terraform", "k8s"})

# Matched on the "Coverage map" prefix rather than the full heading text so a
# retitle does not silently drop the map; the wording after it is free to
# change. The document's own title line is level 1 and excluded.
_SECTION_RE = re.compile(r"^#{2,4}\s+Coverage\s+map\b", re.IGNORECASE)
_NEXT_SECTION_RE = re.compile(r"^#{1,4}\s+")
_SEPARATOR_RE = re.compile(r"^\|[\s:|-]+\|$")

_cache = None


class CoverageMapError(Exception):
    """Raised when the coverage map cannot be read. Fatal at start-up."""


def _split_row(line: str) -> list:
    """Splits a markdown table row into its cells."""
    return [cell.strip() for cell in line.strip().strip("|").split("|")]


def normalize_kind(kind: str) -> str:
    """Reduces an artifact kind to its comparable form.

    The table uses backticks and free spacing for readability; enforcing the
    map should enforce the words, not the punctuation around them.
    """
    return re.sub(r"\s+", " ", kind.replace("`", "")).strip().casefold()


def parse_coverage_map(text: str, source: str) -> dict:
    """Extracts {normalized kind: row} from the document's Coverage map table.

    Each row is {"kind", "column", "owner", "source", "notes"} with kind kept
    in display form and column/owner lowercased. Rows whose owner or column
    fall outside the fixed vocabulary are fatal, not skipped: a skipped row is
    a row the omission and overlap checks silently stop seeing.
    """
    lines = text.splitlines()

    start = None
    for i, line in enumerate(lines):
        if _SECTION_RE.match(line):
            start = i + 1
            break
    if start is None:
        raise CoverageMapError(
            f"{source}: no '## Coverage map' section; the coverage map has no source"
        )

    rows = {}
    header_seen = False
    for line in lines[start:]:
        if _NEXT_SECTION_RE.match(line):
            break
        if not line.strip().startswith("|"):
            continue
        if _SEPARATOR_RE.match(line.strip()):
            header_seen = True
            continue
        # Everything before the |---|---| divider is the header row.
        if not header_seen:
            continue
        cells = _split_row(line)
        if len(cells) < 3:
            raise CoverageMapError(
                f"{source}: coverage map row needs at least kind, column and owner "
                f"cells: {line.strip()!r}"
            )
        kind, column, owner = cells[0], cells[1].casefold(), cells[2].casefold()
        if not kind:
            raise CoverageMapError(
                f"{source}: coverage map row with an empty artifact kind: {line.strip()!r}"
            )
        if column not in COLUMNS:
            raise CoverageMapError(
                f"{source}: unknown column {cells[1]!r} for artifact kind {kind!r} "
                f"(expected one of: {', '.join(sorted(COLUMNS))})"
            )
        if owner not in OWNERS:
            raise CoverageMapError(
                f"{source}: unknown owner {cells[2]!r} for artifact kind {kind!r} "
                f"(expected one of: {', '.join(sorted(OWNERS))})"
            )
        key = normalize_kind(kind)
        if key in rows:
            raise CoverageMapError(
                f"{source}: artifact kind listed twice in the coverage map: {kind!r}"
            )
        rows[key] = {
            "kind": re.sub(r"\s+", " ", kind.replace("`", "")).strip(),
            "column": column,
            "owner": owner,
            "source": cells[3] if len(cells) > 3 else "",
            "notes": cells[4] if len(cells) > 4 else "",
        }

    if not rows:
        raise CoverageMapError(
            f"{source}: the 'Coverage map' section has no table rows; the map is empty"
        )

    return rows


def load_coverage_map(repo_root: str = REPO_ROOT, refresh: bool = False) -> dict:
    """Returns {normalized kind: row}, read once per process."""
    global _cache
    if _cache is not None and not refresh:
        return _cache

    path = os.path.join(repo_root, KNOWLEDGE_DOC)
    try:
        with open(path, "r", encoding="utf-8") as f:
            text = f.read()
    except OSError as e:
        raise CoverageMapError(f"cannot read the coverage map document {path}: {e}")

    rows = parse_coverage_map(text, KNOWLEDGE_DOC)
    _cache = rows
    logger.debug(f"Loaded {len(rows)} coverage map rows from {KNOWLEDGE_DOC}")
    return rows


def validate_coverage_map(repo_root: str = REPO_ROOT) -> None:
    """Start-up check that the coverage map is readable. Raises on any problem."""
    rows = load_coverage_map(repo_root, refresh=True)
    logger.debug(f"Coverage map validation passed ({len(rows)} rows)")
