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

"""Instantiates the artifact coverage map against the inventory — v1, section granularity.

The coverage map (landingzone/knowledge/coverage-map.md, parsed by
servers/dag/server/coverage_map.py) assigns every artifact kind this migration
produces to exactly one owner. This module is the map's first machine use
beyond start-up validation: at translation-planning time it turns each map row
into a verdict against the approved inventory, and checks the plan's units
against those verdicts.

v1 works at SECTION granularity, because that is the granularity the inventory
has: today's inventory is section-summary-shaped (a `storage` dict, a
`nodegroups` list), not typed per-fact rows. A row is "instantiated" by whether
the inventory sections its `Discovered from` cell names hold facts — nothing
finer. A `[]` path segment (e.g. `clusters[].workloads.namespaces`) steps into
an array's items, fanning out over every scanned instance. Fact-level
instantiation (each discovered fact assigned to its owning row) is deferred
until the inventory schema carries typed facts; until then a row's verdict is
only as fine as the sections it names, and a parenthetical qualifier in the
cell (e.g. "`network` (ingress hosts)") is read by humans, not by this
resolver.

Everything here is pure and deterministic over its inputs, and nothing in
this module refuses anything. At the plan-review gate the check findings are
WARNING-grade material — the humans weigh them. The OMISSION half does not
stay advisory, though: the validate step re-runs it as an enforced gate
(translation_validate_3/coverage_gate.py, over the same verdicts this module
computes) after the 2026-08-15 audit, where a landing zone shipped with no
google_container_cluster and validation passed. Overlap and traceability
remain observe-only in v1.
"""

import json
import os
import re

from servers.dag.server.coverage_map import load_coverage_map

_HERE = os.path.dirname(os.path.realpath(__file__))

# landingzone_translationplan_3/ -> landingzone/ -> phases/ -> servers/ -> repo root.
_REPO_ROOT = os.path.abspath(os.path.join(_HERE, "..", "..", "..", ".."))

INVENTORY_SCHEMA_PATH = os.path.join(
    _REPO_ROOT, "servers", "dag", "server", "schema", "inventory.json"
)

GRANULARITY = "section"

# Per-row verdict statuses.
STATUS_FACTS_PRESENT = "facts-present"
STATUS_NO_FACTS = "no-facts"
STATUS_NOT_SCANNED = "not-scanned"
STATUS_UNKNOWN_SECTION = "unknown-section"

# The map's convention for "not discovery-sourced" (standing modules,
# elicitation). Only the em dash the document actually uses, plus an empty
# cell — a mistyped variant should surface as unknown-section, not pass.
_NOT_SCANNED_CELLS = frozenset({"", "—"})

# The resolver's whole grammar: backticked tokens are dotted inventory section
# paths; every other word in the cell is annotation for humans.
_SECTION_REF_RE = re.compile(r"`([^`]+)`")


def load_inventory_schema(path: str = INVENTORY_SCHEMA_PATH) -> dict:
    """Reads the inventory JSON Schema the Discovered-from cells are checked against."""
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def resolve_discovered_from(cell: str):
    """Resolves a row's free-text `Discovered from` cell to section paths.

    Returns None for a "—" (or empty) cell — the row is not discovery-sourced —
    and otherwise the list of backticked dotted paths, in cell order. A cell
    that claims discovery sourcing but contains no backticked path returns []:
    the caller reports it as unknown-section rather than guessing at prose.
    """
    stripped = (cell or "").strip()
    if stripped in _NOT_SCANNED_CELLS:
        return None
    return _SECTION_REF_RE.findall(stripped)


def section_known(schema: dict, path: str) -> bool:
    """True when the schema declares every segment of a dotted section path.

    Walks `properties` (descending into `oneOf` alternatives when a segment is
    schema'd as alternatives, like `storage`). A segment written `name[]`
    additionally steps into the array's `items` schema, so
    `clusters[].workloads.namespaces` is known when `clusters` is a declared
    array whose items declare `workloads.namespaces`. The coverage map's own
    rule is that Discovered-from cells name sections declared in
    inventory.json, so an undeclared path is a map defect to report, even
    where the schema's open objects would let an instance carry it.
    """
    node = schema
    for segment in str(path).split("."):
        name = segment[:-2] if segment.endswith("[]") else segment
        if not isinstance(node, dict) or not name:
            return False
        props = node.get("properties")
        if isinstance(props, dict) and name in props:
            node = props[name]
        else:
            for alternative in node.get("oneOf") or []:
                alt_props = alternative.get("properties") if isinstance(alternative, dict) else None
                if isinstance(alt_props, dict) and name in alt_props:
                    node = alt_props[name]
                    break
            else:
                return False
        if segment.endswith("[]"):
            items = node.get("items") if isinstance(node, dict) else None
            if not isinstance(items, dict):
                return False
            node = items
    return True


def _resolve_path(inventory: dict, path: str):
    """Resolves a dotted path against an inventory instance.

    A `[]` segment fans out over the named array's items and returns the list
    of per-item resolutions, so `clusters[].workloads.namespaces` is every
    scanned cluster's namespaces; items lacking the rest of the path resolve
    to None, which holds_facts already treats as an absence. A `[]` segment
    over a non-list value resolves to None — shape defects read as absences,
    never as facts.
    """
    def resolve(node, segments):
        if not segments:
            return node
        head, rest = segments[0], segments[1:]
        if head.endswith("[]"):
            value = node.get(head[:-2]) if isinstance(node, dict) else None
            if not isinstance(value, (list, tuple)):
                return None
            return [resolve(item, rest) for item in value]
        if not isinstance(node, dict) or head not in node:
            return None
        return resolve(node[head], rest)

    return resolve(inventory or {}, str(path).split("."))


def holds_facts(value) -> bool:
    """Whether an inventory section value records any fact, at section granularity.

    Absences — missing keys, None, False flags, empty containers and strings —
    are not facts. Recorded values are, including zero counts (a namespace
    recorded with 0 deployments was affirmatively scanned) and explicit True
    flags. Containers hold facts when anything inside them does, so a merger
    default like {"karpenter": false, "evidence": []} stays an absence.
    """
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return True
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, dict):
        return any(holds_facts(v) for v in value.values())
    if isinstance(value, (list, tuple)):
        return any(holds_facts(v) for v in value)
    return True


def instantiate_coverage_map(rows: dict, inventory: dict, schema: dict) -> list:
    """One verdict per map row, in table order.

    Each verdict is {row_key, kind, owner, column, status, sections}:
      - not-scanned: the row's Discovered-from is "—" (standing modules,
        elicitation-sourced) — there is nothing in the scan to check.
      - unknown-section: the cell names a path inventory.json does not declare
        (or claims discovery sourcing without a single backticked path). A map
        defect is a reportable verdict, not a crash — and it outranks the fact
        statuses so it cannot hide behind a healthy-looking sibling section.
      - facts-present: at least one named section holds facts.
      - no-facts: every named section is known and empty or absent.
    `sections` carries the per-path detail behind the rollup.
    """
    verdicts = []
    for row_key, row in rows.items():
        cell = (row.get("source") or "").strip()
        paths = resolve_discovered_from(cell)
        sections = []
        if paths is None:
            status = STATUS_NOT_SCANNED
        elif not paths:
            status = STATUS_UNKNOWN_SECTION
            sections.append({"path": cell, "known": False, "facts": False})
        else:
            any_unknown = False
            any_facts = False
            for path in paths:
                known = section_known(schema, path)
                facts = known and holds_facts(_resolve_path(inventory, path))
                sections.append({"path": path, "known": known, "facts": facts})
                any_unknown = any_unknown or not known
                any_facts = any_facts or facts
            if any_unknown:
                status = STATUS_UNKNOWN_SECTION
            elif any_facts:
                status = STATUS_FACTS_PRESENT
            else:
                status = STATUS_NO_FACTS
        verdicts.append({
            "row_key": row_key,
            "kind": row.get("kind"),
            "owner": row.get("owner"),
            "column": row.get("column"),
            "status": status,
            "sections": sections,
        })
    return verdicts


def run_coverage_checks(plan: dict, verdicts: list) -> dict:
    """Checks the plan's unit citations against the instantiated map. Pure.

    - omission: platform-translation rows with facts present that no unit —
      of any status, placeholders included — covers. Placeholders count
      because they are the plan's explicit claim over an empty section; an
      omission therefore means the planner has no family for the row at all.
      Advisory here; the validate step enforces the same condition (plus the
      landing-zone rows) via coverage_gate, so an omission that survives to
      validation fails it there.
    - overlap: platform-translation rows covered by more than one active
      (non-skipped) unit FAMILY (kind); landing-zone and workload rows go to
      out_of_scope before overlap is computed. Several active units of one
      kind are a single family fanned out per inventory item (one node pool
      per nodegroup), not two generators emitting the same artifact.
    - traceability: units citing zero rows, and citations naming row keys the
      map does not hold.
    - unknown_sections: rows (any owner) whose verdict is unknown-section — a
      map defect. Reported here as well as in the verdicts, because an
      unknown-section row can never reach facts-present and would otherwise
      be exempt from omission, letting a defective map read as clean at the
      gate.
    - out_of_scope: landing-zone and workload rows with their verdicts —
      not the units' to cover (the landing zone's standing modules and the
      [PLANNED] workload phase own them), listed so the report covers the
      whole boundary rather than the built half.
    """
    units = plan.get("units", []) or []
    row_keys = {v["row_key"] for v in verdicts}
    unknown_sections = [v["row_key"] for v in verdicts
                        if v["status"] == STATUS_UNKNOWN_SECTION]

    covering = {}  # row_key -> [unit, ...] in plan order, any status
    units_without_covers = []
    unknown_citations = []
    for unit in units:
        covers = unit.get("covers") or []
        if not covers:
            units_without_covers.append(unit.get("unit_id"))
        for cited in covers:
            if cited not in row_keys:
                unknown_citations.append(
                    {"unit_id": unit.get("unit_id"), "row_key": cited}
                )
            else:
                covering.setdefault(cited, []).append(unit)

    omission = []
    overlap = []
    out_of_scope = {"landing-zone": [], "workload": []}
    for verdict in verdicts:
        if verdict["owner"] != "platform-translation":
            out_of_scope[verdict["owner"]].append(
                {"row_key": verdict["row_key"], "status": verdict["status"]}
            )
            continue
        row_units = covering.get(verdict["row_key"], [])
        if verdict["status"] == STATUS_FACTS_PRESENT and not row_units:
            omission.append(verdict["row_key"])
        active = [u for u in row_units if u.get("status") != "skipped"]
        active_kinds = sorted({u.get("kind") for u in active})
        if len(active_kinds) > 1:
            overlap.append({
                "row_key": verdict["row_key"],
                "kinds": active_kinds,
                "unit_ids": sorted(u.get("unit_id") for u in active),
            })

    return {
        "omission": omission,
        "overlap": overlap,
        "traceability": {
            "units_without_covers": units_without_covers,
            "unknown_citations": unknown_citations,
        },
        "unknown_sections": unknown_sections,
        "out_of_scope": out_of_scope,
    }


def attach_coverage(plan: dict, inventory: dict, rows: dict = None, schema: dict = None) -> dict:
    """Returns the plan with a `coverage` key beside `units`: verdicts + checks.

    `rows` and `schema` default to the repository's own coverage map (already
    validated at server start-up) and inventory schema (read here, at call
    time); tests pass fixtures instead.
    """
    if rows is None:
        rows = load_coverage_map()
    if schema is None:
        schema = load_inventory_schema()
    verdicts = instantiate_coverage_map(rows, inventory, schema)
    return {
        **plan,
        "coverage": {
            "granularity": GRANULARITY,
            "map": verdicts,
            "checks": run_coverage_checks(plan, verdicts),
        },
    }


def refresh_checks(plan: dict) -> dict:
    """Recomputes the checks after unit statuses changed (plan-review skips).

    The verdicts depend only on the map and the inventory, so they stand; the
    overlap check depends on which units are active, so it must not go stale
    between update_translation_plan and the sign-off. A plan without a
    coverage key (persisted before this existed) is returned unchanged.
    """
    coverage = plan.get("coverage")
    if not coverage or "map" not in coverage:
        return plan
    return {
        **plan,
        "coverage": {
            **coverage,
            "checks": run_coverage_checks(plan, coverage["map"]),
        },
    }


def findings(checks: dict) -> list:
    """The offending names per firing check, as compact strings for prompts.

    Empty when every check is clean. Deliberately names only counts and
    offenders — the full instantiated table stays in the plan blob, never in
    an elicitation prompt.
    """
    problems = []
    if checks.get("unknown_sections"):
        problems.append("map rows naming unknown inventory sections (map defect): "
                        + "; ".join(checks["unknown_sections"]))
    if checks.get("omission"):
        problems.append("omitted rows (facts present, no covering unit): "
                        + "; ".join(checks["omission"]))
    if checks.get("overlap"):
        problems.append("overlapping rows (multiple active families): "
                        + "; ".join(o["row_key"] for o in checks["overlap"]))
    traceability = checks.get("traceability") or {}
    if traceability.get("units_without_covers"):
        problems.append("units citing no map row: "
                        + "; ".join(traceability["units_without_covers"]))
    if traceability.get("unknown_citations"):
        problems.append("citations of unknown rows: "
                        + "; ".join(f"{c['unit_id']} -> {c['row_key']}"
                                    for c in traceability["unknown_citations"]))
    return problems
