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

"""The Ingress annotation disposition table, read out of the API translation
reference document.

`### Ingress / Gateway annotations` in reference/api-translation.md is a
three-column markdown table: an annotation key, its disposition in the
workload routing unit (`wkld-routing`, which turns an Ingress into an
HTTPRoute attached to the shared platform Gateway), and the target or the
rationale the worker records. That table is the authority on what happens to
an annotation found on a scoped Ingress, and this module is how the server
reads it — the same arrangement as blocker_criteria.py and coverage_map.py:
the humans and the machine read one table, so the disposition the reviewer
was told about and the one the worker was briefed with cannot drift between
a document and a hardcoded copy, and adding an annotation stays a
documentation edit.

The disposition cell is a closed vocabulary (DISPOSITIONS), so the parser
validates values as well as shape: a fourth word would silently exempt a row
from the brief's "mapped / dropped with tradeoff / open question" contract.

A key ending in `*` is a prefix row (`alb.ingress.kubernetes.io/auth-*`
covers every auth knob). Lookup is exact key first, then the longest matching
prefix; a key no row covers is an open question BY RULE — that fallback is a
rule of the unit, not a row, so it lives with the lookup here and not in the
table.

Failure to parse is fatal at start-up, matching dag_validation,
blocker_criteria and coverage_map: degrading to an empty table would turn
every annotation into the generic open question and lose the dispositions the
document promises.
"""

import logging
import os
import re

logger = logging.getLogger("migration-dag")

_HERE = os.path.dirname(os.path.realpath(__file__))

# servers/dag/server/ -> repository root, the same anchor dag_validation uses.
REPO_ROOT = os.path.abspath(os.path.join(_HERE, "..", "..", ".."))

REFERENCE_DOC = os.path.join("reference", "api-translation.md")

MAPPED = "mapped"
DROPPED = "dropped with tradeoff"
OPEN_QUESTION = "open question"
DISPOSITIONS = frozenset({MAPPED, DROPPED, OPEN_QUESTION})

# The unknown-key rule. Not a table row on purpose: a table can only be
# closed over the keys it lists, and the rule is what makes it closed.
UNKNOWN_ROW = (
    OPEN_QUESTION,
    "not in the closed disposition table — never silently dropped. Say what "
    "the annotation did on the source Ingress and what, if anything, carries "
    "that behaviour on the target; controller-specific behaviour (rewrites, "
    "response headers, sticky routing) is real carry-over work, not noise")

# Matched on the "Ingress / Gateway annotations" prefix rather than the full
# heading so a retitle does not silently drop the table. The separator
# between the two words is free (slash, dash, "and").
_SECTION_RE = re.compile(
    r"^#{2,4}\s+Ingress\s*(?:/|-|and)?\s*Gateway\s+annotations\b", re.IGNORECASE)
_NEXT_SECTION_RE = re.compile(r"^#{1,4}\s+")
_SEPARATOR_RE = re.compile(r"^\|[\s:|-]+\|$")

# An annotation key: DNS-ish prefix, a slash, a name (Kubernetes allows
# underscores and upper case in the name). Anything else in the key cell
# (prose, two keys in one cell) is a row the lookup could never hit.
_KEY_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9.\-]*/(?:[A-Za-z0-9._\-]+\*?|\*)$")

_cache = None


class ApiTranslationError(Exception):
    """Raised when the disposition table cannot be read. Fatal at start-up."""


def _split_row(line: str) -> list:
    """Splits a markdown table row into its cells."""
    return [cell.strip() for cell in line.strip().strip("|").split("|")]


def _clean(cell: str) -> str:
    return re.sub(r"\s+", " ", cell.replace("`", "")).strip()


def parse_annotation_dispositions(text: str, source: str) -> dict:
    """Extracts {"exact": {key: (disposition, rationale)},
    "prefix": {prefix: (disposition, rationale)}} from the document's
    `### Ingress / Gateway annotations` table.

    Rows whose disposition falls outside DISPOSITIONS, whose key is not one
    annotation key, or that repeat a key are fatal, not skipped: a skipped
    row is an annotation the brief silently demotes to the unknown rule."""
    lines = text.splitlines()

    start = None
    for i, line in enumerate(lines):
        if _SECTION_RE.match(line):
            start = i + 1
            break
    if start is None:
        raise ApiTranslationError(
            f"{source}: no '### Ingress / Gateway annotations' section; the "
            "annotation disposition table has no source")

    exact, prefix = {}, {}
    header_seen = False
    for line in lines[start:]:
        if _NEXT_SECTION_RE.match(line):
            break
        if not line.strip().startswith("|"):
            continue
        if _SEPARATOR_RE.match(line.strip()):
            header_seen = True
            continue
        if not header_seen:
            continue
        cells = _split_row(line)
        if len(cells) < 3:
            raise ApiTranslationError(
                f"{source}: annotation row needs key, disposition and "
                f"rationale cells: {line.strip()!r}")
        if len(cells) > 3:
            # A literal '|' inside a rationale. Truncating to three cells
            # would brief the worker with half a sentence and no error.
            raise ApiTranslationError(
                f"{source}: annotation row has more than three cells (a "
                f"'|' inside the rationale?): {line.strip()!r}")
        key, disposition, rationale = (
            _clean(cells[0]), _clean(cells[1]).casefold(), _clean(cells[2]))
        if not _KEY_RE.match(key):
            raise ApiTranslationError(
                f"{source}: annotation row key is not one annotation key "
                f"(optionally ending in '*'): {cells[0]!r}")
        if disposition not in DISPOSITIONS:
            raise ApiTranslationError(
                f"{source}: unknown disposition {cells[1]!r} for annotation "
                f"{key!r} (expected one of: "
                f"{', '.join(sorted(DISPOSITIONS))})")
        if not rationale:
            raise ApiTranslationError(
                f"{source}: annotation {key!r} has an empty rationale; the "
                "worker records it verbatim")
        table = prefix if key.endswith("*") else exact
        name = key[:-1] if key.endswith("*") else key
        if name in table:
            raise ApiTranslationError(
                f"{source}: annotation listed twice in the disposition "
                f"table: {key!r}")
        table[name] = (disposition, rationale)

    if not exact and not prefix:
        raise ApiTranslationError(
            f"{source}: the 'Ingress / Gateway annotations' section has no "
            "table rows; the disposition table is empty")

    return {"exact": exact, "prefix": prefix}


def load_annotation_dispositions(repo_root: str = REPO_ROOT,
                                 refresh: bool = False) -> dict:
    """Returns the parsed table, read once per process."""
    global _cache
    if _cache is not None and not refresh:
        return _cache

    path = os.path.join(repo_root, REFERENCE_DOC)
    try:
        with open(path, "r", encoding="utf-8") as f:
            text = f.read()
    except OSError as e:
        raise ApiTranslationError(
            f"cannot read the API translation document {path}: {e}")

    table = parse_annotation_dispositions(text, REFERENCE_DOC)
    _cache = table
    logger.debug(
        f"Loaded {len(table['exact'])} exact and {len(table['prefix'])} "
        f"prefix annotation dispositions from {REFERENCE_DOC}")
    return table


def annotation_disposition(key: str, table: dict = None) -> tuple:
    """(disposition, rationale) for one annotation key actually present:
    the exact row, else the longest prefix row, else the unknown rule."""
    if table is None:
        table = load_annotation_dispositions()
    if key in table["exact"]:
        return table["exact"][key]
    matches = [p for p in table["prefix"] if key.startswith(p)]
    if matches:
        return table["prefix"][max(matches, key=len)]
    return UNKNOWN_ROW


def validate_api_translation(repo_root: str = REPO_ROOT) -> None:
    """Start-up check that the disposition table is readable. Raises on any
    problem."""
    table = load_annotation_dispositions(repo_root, refresh=True)
    logger.debug(
        "Annotation disposition table validation passed "
        f"({len(table['exact'])} exact, {len(table['prefix'])} prefix rows)")
