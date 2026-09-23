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

"""The blocker taxonomy, read out of the assessment phase knowledge document.

`### Step 4 — Identify blockers` in
servers/phases/assessment/knowledge/migration-assessment.md is a two-column
markdown table: a blocker category and its resolution path. That table is the
authority on what counts as a blocker, and this module is how the server reads
it.

Parsing the prose rather than restating it in Python is the point. A hardcoded
copy is a second source that drifts: the agent is handed the markdown and reasons
from it, so if the server validated against a divergent list the two would
disagree about what a blocker even is, and the doc would lose an argument it
should always win. Adding a category stays a documentation edit.

Failure to parse is fatal at start-up, matching dag_validation: silently
returning an empty taxonomy would turn category validation into a no-op and let
anything through under the name of a blocker.
"""

import logging
import os
import re

logger = logging.getLogger("migration-dag")

_HERE = os.path.dirname(os.path.realpath(__file__))

# servers/dag/server/ -> repository root, the same anchor dag_validation uses.
REPO_ROOT = os.path.abspath(os.path.join(_HERE, "..", "..", ".."))

KNOWLEDGE_DOC = os.path.join(
    "servers", "phases", "assessment", "knowledge", "migration-assessment.md"
)

# The heading that opens the table. Matched on the "Step 4" prefix rather than
# the full text so retitling the section does not silently drop the taxonomy;
# the em dash and wording are free to change.
_SECTION_RE = re.compile(r"^#{2,4}\s+Step\s+4\b", re.IGNORECASE)
_NEXT_SECTION_RE = re.compile(r"^#{1,4}\s+")
_SEPARATOR_RE = re.compile(r"^\|[\s:|-]+\|$")

_cache = None


class BlockerCriteriaError(Exception):
    """Raised when the blocker taxonomy cannot be read. Fatal at start-up."""


def _split_row(line: str) -> list:
    """Splits a markdown table row into its cells."""
    return [cell.strip() for cell in line.strip().strip("|").split("|")]


def parse_blocker_categories(text: str, source: str) -> dict:
    """Extracts {category: resolution path} from a knowledge document's Step 4 table."""
    lines = text.splitlines()

    start = None
    for i, line in enumerate(lines):
        if _SECTION_RE.match(line):
            start = i + 1
            break
    if start is None:
        raise BlockerCriteriaError(
            f"{source}: no '### Step 4 — Identify blockers' section; the blocker taxonomy "
            "has no source"
        )

    categories = {}
    header_seen = False
    for line in lines[start:]:
        if _NEXT_SECTION_RE.match(line):
            break
        if not line.strip().startswith("|"):
            continue
        if _SEPARATOR_RE.match(line.strip()):
            header_seen = True
            continue
        cells = _split_row(line)
        if len(cells) < 2:
            continue
        # Everything before the |---|---| divider is the header row.
        if not header_seen:
            continue
        category, resolution = cells[0], cells[1]
        if not category or not resolution:
            continue
        if category in categories:
            raise BlockerCriteriaError(
                f"{source}: blocker category listed twice in the Step 4 table: {category!r}"
            )
        categories[category] = resolution

    if not categories:
        raise BlockerCriteriaError(
            f"{source}: the 'Step 4 — Identify blockers' section has no table rows; the "
            "blocker taxonomy is empty"
        )

    return categories


def load_blocker_categories(repo_root: str = REPO_ROOT, refresh: bool = False) -> dict:
    """Returns {category: resolution path}, read once per process."""
    global _cache
    if _cache is not None and not refresh:
        return _cache

    path = os.path.join(repo_root, KNOWLEDGE_DOC)
    try:
        with open(path, "r", encoding="utf-8") as f:
            text = f.read()
    except OSError as e:
        raise BlockerCriteriaError(f"cannot read the assessment knowledge document {path}: {e}")

    categories = parse_blocker_categories(text, KNOWLEDGE_DOC)
    _cache = categories
    logger.debug(f"Loaded {len(categories)} blocker categories from {KNOWLEDGE_DOC}")
    return categories


def normalize_category(category: str) -> str:
    """Reduces a blocker category to its comparable form.

    The Step 4 table uses markdown backticks and free spacing for readability, so
    a category read straight out of it differs by formatting alone from the same
    category typed back without the markup. Enforcing the taxonomy should enforce
    the words, not the punctuation around them, so both the parsed vocabulary and a
    submitted category are compared through here: backticks removed, whitespace
    collapsed, case folded.
    """
    return re.sub(r"\s+", " ", category.replace("`", "")).strip().casefold()


def display_category(category: str) -> str:
    """The human-facing form of a category: markup stripped, case preserved."""
    return re.sub(r"\s+", " ", category.replace("`", "")).strip()


def validate_blocker_criteria(repo_root: str = REPO_ROOT) -> None:
    """Start-up check that the taxonomy is readable. Raises on any problem."""
    categories = load_blocker_categories(repo_root, refresh=True)
    logger.debug(f"Blocker criteria validation passed ({len(categories)} categories)")
