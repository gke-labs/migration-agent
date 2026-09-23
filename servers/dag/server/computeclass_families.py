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

"""The AWS-family to GCP-machineFamily table, read out of the knowledge document.

`## 3. Family table (machine-read)` in
servers/phases/landingzone/knowledge/gke-compute-classes.md is a markdown
table mapping every Karpenter `instance-family` value the mapping knows to
the GCP `machineFamily` candidates a ComputeClass priority may name, per
architecture, plus one "(no constraint)" row per architecture for a NodePool
that pins no family. The compute-class worker reads the document as prose;
the validate contract (translation_validate_3/computeclass_contract.py,
check 4) reads it through this module. Same arrangement as coverage_map.py:
one table, read by the humans and the machine, so the candidate set cannot
drift between the document and a hardcoded copy.

Like coverage_map, two cells of every row are constrained (arch is a closed
vocabulary, candidates must be non-empty) and a bad row is fatal at start-up
rather than skipped: a skipped row would silently reject every priority the
worker mapped from that family.

Import-light on purpose (stdlib only): the dag server loads it at start-up
and the phase contract imports it under a plain `python3`.
"""

import logging
import os
import re

logger = logging.getLogger("migration-dag")

_HERE = os.path.dirname(os.path.realpath(__file__))

# servers/dag/server/ -> repository root, the same anchor coverage_map uses.
REPO_ROOT = os.path.abspath(os.path.join(_HERE, "..", "..", ".."))

KNOWLEDGE_DOC = os.path.join(
    "servers", "phases", "landingzone", "knowledge", "gke-compute-classes.md"
)

ARCHITECTURES = frozenset({"amd64", "arm64"})
NO_CONSTRAINT = "(no constraint)"

# Matched on the "Family table" words after the section number so a renumber
# or a retitle of the parenthetical does not drop the table. The document's
# own title line is level 1 and excluded.
_SECTION_RE = re.compile(r"^#{2,4}\s+(?:\d+\.\s+)?Family\s+table\b", re.IGNORECASE)
_NEXT_SECTION_RE = re.compile(r"^#{1,4}\s+")
_SEPARATOR_RE = re.compile(r"^\|[\s:|-]+\|$")

_cache = None


class ComputeClassFamiliesError(Exception):
    """Raised when the family table cannot be read. Fatal at start-up."""


def _split_row(line: str) -> list:
    """Splits a markdown table row into its cells."""
    return [cell.strip() for cell in line.strip().strip("|").split("|")]


def _split_list(cell: str) -> list:
    """A comma-separated cell to lowercase names, backticks dropped."""
    return [item for item in
            (part.replace("`", "").strip().casefold() for part in cell.split(","))
            if item]


def parse_family_table(text: str, source: str) -> list:
    """Extracts the rows of the document's Family table.

    Each row is {"aws": [family, ...], "gcp": [machineFamily, ...], "arch"},
    names lowercased, candidates in document order (the first is the
    document's default recommendation). A no-constraint row has an empty
    "aws" list. Every shape or vocabulary problem is fatal, not skipped.
    """
    lines = text.splitlines()

    start = None
    for i, line in enumerate(lines):
        if _SECTION_RE.match(line):
            start = i + 1
            break
    if start is None:
        raise ComputeClassFamiliesError(
            f"{source}: no '## 3. Family table (machine-read)' section; the "
            "family table has no source")

    rows = []
    seen = {}
    header_cells = None
    header_seen = False
    for line in lines[start:]:
        if _NEXT_SECTION_RE.match(line):
            break
        stripped = line.strip()
        if not stripped.startswith("|"):
            continue
        if _SEPARATOR_RE.match(stripped):
            header_seen = True
            continue
        if not header_seen:
            # Everything before the |---|---| divider is the header row.
            header_cells = len(_split_row(stripped))
            continue
        if stripped.count("|") < 2:
            raise ComputeClassFamiliesError(
                f"{source}: family table row is not a table row: {stripped!r}")
        cells = _split_row(stripped)
        if len(cells) != (header_cells or 3):
            raise ComputeClassFamiliesError(
                f"{source}: family table row has {len(cells)} cells where the "
                f"header has {header_cells or 3} (a '|' is missing or extra): "
                f"{stripped!r}")
        aws_cell, gcp_cell, arch = cells[0], cells[1], cells[2].replace("`", "").strip().casefold()
        if arch not in ARCHITECTURES:
            raise ComputeClassFamiliesError(
                f"{source}: unknown arch {cells[2]!r} in family table row "
                f"{aws_cell!r} (expected one of: {', '.join(sorted(ARCHITECTURES))})")
        gcp = _split_list(gcp_cell)
        if not gcp:
            raise ComputeClassFamiliesError(
                f"{source}: family table row {aws_cell!r} names no GCP "
                "machineFamily candidate; a row with nothing to pick from "
                "would reject every priority mapped from it")
        if aws_cell.replace("`", "").strip().casefold() == NO_CONSTRAINT:
            aws = []
            keys = [(NO_CONSTRAINT, arch)]
        else:
            aws = _split_list(aws_cell)
            if not aws:
                raise ComputeClassFamiliesError(
                    f"{source}: family table row with an empty AWS family cell: "
                    f"{stripped!r}")
            keys = [(family, None) for family in aws]
        for key in keys:
            if key in seen:
                label = key[0] if key[1] is None else f"{key[0]} for {key[1]}"
                raise ComputeClassFamiliesError(
                    f"{source}: AWS family {label!r} is listed twice in the "
                    "family table; each source family must map to exactly one row")
            seen[key] = True
        rows.append({"aws": aws, "gcp": gcp, "arch": arch})

    if not rows:
        raise ComputeClassFamiliesError(
            f"{source}: the 'Family table' section has no table rows; the "
            "table is empty")

    return rows


def load_family_table(repo_root: str = REPO_ROOT, refresh: bool = False) -> list:
    """Returns the parsed rows, read once per process."""
    global _cache
    if _cache is not None and not refresh:
        return _cache

    path = os.path.join(repo_root, KNOWLEDGE_DOC)
    try:
        with open(path, "r", encoding="utf-8") as f:
            text = f.read()
    except OSError as e:
        raise ComputeClassFamiliesError(
            f"cannot read the compute-class knowledge document {path}: {e}")

    rows = parse_family_table(text, KNOWLEDGE_DOC)
    _cache = rows
    logger.debug(f"Loaded {len(rows)} family table rows from {KNOWLEDGE_DOC}")
    return rows


def validate_family_table(repo_root: str = REPO_ROOT) -> None:
    """Start-up check that the family table is readable. Raises on any problem."""
    rows = load_family_table(repo_root, refresh=True)
    logger.debug(f"Family table validation passed ({len(rows)} rows)")


def _rows_for_arch(table: list, arch) -> list:
    if arch is None:
        return list(table)
    wanted = str(arch).strip().casefold()
    return [row for row in table if row["arch"] == wanted]


def _union(rows: list) -> list:
    """Candidates of several rows, document order, no repeats."""
    out = []
    for row in rows:
        for family in row["gcp"]:
            if family not in out:
                out.append(family)
    return out


def candidates_for(aws_family, arch=None, table=None) -> list:
    """GCP machineFamily candidates for one AWS family (lowercase match).

    `aws_family` None means the source pins no family: the union of the
    no-constraint rows. `arch`, when given, restricts to rows of that
    architecture. An unknown family yields [] — the caller decides whether
    that is an error.
    """
    rows = _rows_for_arch(table if table is not None else load_family_table(), arch)
    if aws_family is None:
        return _union([row for row in rows if not row["aws"]])
    wanted = str(aws_family).strip().casefold()
    return _union([row for row in rows if wanted in row["aws"]])


def allowed_families(instance_families, architectures, table=None) -> set:
    """The machineFamily values a priority may name for one NodePool.

    The union of candidates for every source family, over rows whose arch is
    in `architectures` when that list is non-empty; the no-constraint rows
    when `instance_families` is empty. A source family the table does not
    know contributes nothing.
    """
    table = table if table is not None else load_family_table()
    archs = [str(a).strip().casefold() for a in (architectures or []) if str(a).strip()]
    rows = [row for row in table if not archs or row["arch"] in archs]
    families = [str(f).strip().casefold() for f in (instance_families or []) if str(f).strip()]
    if not families:
        return set(_union([row for row in rows if not row["aws"]]))
    allowed = set()
    for family in families:
        allowed.update(_union([row for row in rows if family in row["aws"]]))
    return allowed


def unknown_families(instance_families, table=None) -> list:
    """The source families no row of the table lists, lowercased, in order.
    A caller routes each to an open question instead of failing every
    priority against an empty candidate set."""
    table = table if table is not None else load_family_table()
    known = set()
    for row in table:
        known.update(row["aws"])
    seen, out = set(), []
    for family in (instance_families or []):
        name = str(family).strip().casefold()
        if name and name not in known and name not in seen:
            seen.add(name)
            out.append(name)
    return out
