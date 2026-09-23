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

"""The enforced coverage-omission gate at the validate step.

The plan-time coverage checks (landingzone_translationplan_3/coverage.py)
are observe-only: the humans at Gate C weigh them. A pre-submit audit
showed why observation alone is not enough at ship time: an end-to-end run
shipped a landing zone with NO google_container_cluster anywhere in the
clone — the cluster reached GKE through a manual bridge — and `terraform
validate` passed, because compiling greenly and containing the migration's
artifacts are different properties. This module enforces the omission half
of the coverage checks where the artifacts actually exist: a coverage-map
row owned by landing-zone or platform-translation whose inventory sections
hold facts, with NO artifact behind it and NO explicit skip recording the
decision, is a validation FAILURE naming the row. Overlap and traceability
stay observe-only in v1.

What counts as an artifact is per-row:
- Platform-translation rows are satisfied by the plan's citations
  (planner.FAMILY_COVERS -> unit `covers`): a done unit citing the row is
  the artifact; citing units that are ALL skipped are the explicit human
  decision at Gate C — satisfied, because the omission was chosen, not
  silent. Citing units in any other status ship nothing, so they do not
  satisfy. The planner's own no-facts `placeholder` units are excluded from
  that all-skipped route: they are born `skipped` with no human involved, so
  a row whose only citation is a placeholder is a finding, not a decision.
  A plan whose units carry no `covers` at all predates citations; its
  citations are backfilled from FAMILY_COVERS by unit `kind` (reported as
  `backfilled_citations`) rather than read as a total omission, because such
  a workspace has no route back to plan_translation.
- Landing-zone rows have no units; their deliverable is HCL in the clone.
  RESOURCE_SCANS pins the Terraform resource type that proves a row, and
  the gate scans every .tf file in the materialized clone (comments
  blanked; .git/.terraform skipped) — the GKE-cluster row is the case that
  audit found.
- UNENFORCED_ROWS pins the facts-present rows deliberately outside v1
  enforcement, each with its reason. A NEW facts-present row in the map
  that has no citing family, no scan, and no pin FAILS here by default —
  the ratchet: widening the map without widening enforcement is a
  conscious edit to these pins, never silence.

An in-scope row whose `Discovered from` cell does not resolve against the
inventory schema (verdict `unknown-section`) is also a finding: the gate
cannot know whether facts exist for it, and skipping it silently would
recreate exactly the vacuous pass this gate exists to close. Fail-closed,
like the root wiring.

Verdicts are recomputed here from the live map + schema rather than read
from the plan's stored `coverage` key: plan-time attach is deliberately
log-and-continue (a defect in the observing machinery must not block
planning), so the stored key can legitimately be missing — enforcement must
not inherit that softness.
"""

import os
import re

from servers.dag.server.coverage_map import load_coverage_map
from servers.phases.landingzone.landingzone_translationplan_3 import coverage
from servers.phases.landingzone.landingzone_translationplan_3 import planner

from . import root_wiring

GRANULARITY = "section"

# The clone subdirectory materialize_units writes translation units into.
# Duplicated rather than imported: tools.py imports THIS module, and the
# value is pinned against tools.UNITS_SUBDIR by coverage_gate_test.
UNITS_SUBDIR = "translation-units"

# Landing-zone rows proven by a Terraform resource declaration in the
# materialized clone (normalized row key -> resource type). The landing-zone
# draft vendors its modules into the clone, so a resource declared inside a
# vendored module is found; a registry-sourced external module would not be —
# acceptable for v1, where the draft convention is vendored modules.
RESOURCE_SCANS = {
    "gke cluster (control plane, target shape)": "google_container_cluster",
}

# Scanned types that must appear EXACTLY once in the landing-zone-owned
# clone, not just at-least-once. The landing-zone MUST (knowledge/
# gke-landing-zone.md, Validation): the design contains exactly 1
# `google_container_cluster` under any profile, a shrunk sandbox included —
# a second declaration is a second control plane nobody decided, and the
# exports cluster derivation (DESIGN §4.6) publishes coordinates only for
# a single literal cluster, so a duplicate also silently nulls the
# published cluster. The 2026-08-16 oracle review's duplicate-object class
# (three same-name DaemonSets, two same-name StorageClasses) is the same
# defect one layer down; this pins the one resource the map row already
# scans.
SINGLETON_TYPES = frozenset({"google_container_cluster"})

# Facts-present rows deliberately outside v1 enforcement, with the reason a
# reviewer reads in the report. Bound to the live map by coverage_gate_test:
# a renamed or removed row breaks the pin loudly.
UNENFORCED_ROWS = {
    "base vpc, subnets, secondary ranges": (
        "adopting a pre-existing VPC (data-sourced, not declared) is a "
        "legitimate landing-zone shape; a resource scan cannot tell it from "
        "an omission, so this row stays observe-only in v1"),
    "artifact registry repository": (
        "optional by design: when the design declares no repository, the "
        "deployment phase provisions a default via the API — an action, not "
        "a repository artifact (see the map row's note)"),
    "filestore instances": (
        "no unit family is charged with this row yet; the storage unit may "
        "emit instances at its discretion — enforcing would fail every "
        "estate with storage facts until a family owns the row"),
}


def scan_resource_counts(clone_dir: str, resource_types) -> dict:
    """{resource_type: {relative .tf path: declaration count}} over the clone.

    Lexical, like the root wiring's variable scan and for the same reason:
    this runs before/independently of `terraform init`, so no provider or
    module resolution is available. Comments, strings' braces and heredocs
    are blanked via root_wiring._strip_hcl — a commented-out
    `resource "google_container_cluster"` proves nothing and must not
    satisfy the gate. .git and .terraform trees are never artifacts.
    Counts, not just presence: the singleton check (SINGLETON_TYPES) needs
    to tell one declaration from two in the same file.
    """
    patterns = {
        rtype: re.compile(r'resource\s+"' + re.escape(rtype) + r'"\s+"[^"]+"\s*\{')
        for rtype in resource_types
    }
    found = {rtype: {} for rtype in resource_types}
    for dirpath, dirnames, filenames in os.walk(clone_dir):
        dirnames[:] = sorted(d for d in dirnames if d not in (".git", ".terraform"))
        for name in sorted(filenames):
            if not name.endswith(".tf"):
                continue
            path = os.path.join(dirpath, name)
            try:
                with open(path, "r", encoding="utf-8") as f:
                    text = root_wiring._strip_hcl(f.read())
            except (OSError, UnicodeDecodeError):
                continue
            for rtype, pattern in patterns.items():
                hits = len(pattern.findall(text))
                if hits:
                    found[rtype][os.path.relpath(path, clone_dir)] = hits
    return found


# The one cluster FIELD the gate reads. `terraform validate` accepts a
# cluster with no dns_config (an optional block; the provider default on
# Standard is kube-dns), and RESOURCE_SCANS proves only that a cluster
# exists. The landing-zone standing default is Cloud DNS for GKE
# (gke-landing-zone.md), and the cluster-dns unit's kube-dns ConfigMap
# assumes it, so a design that drops the block ships a provider the design
# said it would not run. Lexical like the resource scan: owned .tf files
# only (a cluster inside translation-units/ is already a boundary finding),
# comments and heredocs blanked, a literal `enable_autopilot = true` cluster
# exempt (Cloud DNS is forced there; an Autopilot flag behind an expression is
# not an exemption, the block is accepted on both modes so the remedy holds),
# a `dynamic "dns_config"` block read like a plain one, and a value that
# arrives through an expression accepted with a note rather than failed —
# validate cannot see through a variable either. The note text rides the
# ship elicitation (tools.py first_notice) so an unverified value is never
# silent, and never late.
CLUSTER_RESOURCE_TYPE = "google_container_cluster"
CLUSTER_ROW_KEY = next(k for k, v in RESOURCE_SCANS.items() if v == CLUSTER_RESOURCE_TYPE)
CLUSTER_ROW_KIND = "GKE cluster (control plane, target shape)"  # pinned to the live map by test
CLUSTER_DNS_PROVIDER = "CLOUD_DNS"
_CLUSTER_BLOCK_RE = re.compile(r'resource\s+"google_container_cluster"\s+"([^"]+)"\s*\{')
_AUTOPILOT_RE = re.compile(r"\benable_autopilot\s*=\s*true\b")
_DNS_CONFIG_RE = re.compile(r'(?:\bdynamic\s+"dns_config"|\bdns_config)\s*\{')
_CLUSTER_DNS_RE = re.compile(r"\bcluster_dns\s*=(?!=)\s*(.+)")  # `==` is a comparison


def _hcl_body(stripped: str, open_at: int) -> str:
    """The text between the brace at `open_at` and its match (mask-safe:
    the caller passes _strip_hcl output, so braces in strings are gone)."""
    depth = 0
    for i in range(open_at, len(stripped)):
        if stripped[i] == "{":
            depth += 1
        elif stripped[i] == "}":
            depth -= 1
            if depth == 0:
                return stripped[open_at + 1:i]
    return stripped[open_at + 1:]


def scan_cluster_dns_config(clone_dir: str, row: dict = None) -> dict:
    """{checked: [cluster addresses], exempt: [Autopilot clusters],
    notes: [unverified values], findings: [row findings]}.

    Every owned google_container_cluster block in the clone must set
    `dns_config { cluster_dns = "CLOUD_DNS" }` unless it sets
    `enable_autopilot = true`. Findings carry the cluster row (`row`: the
    row's verdict from check(), or the row's identity when called alone) so
    the report and the review read them beside the resource scan's.
    """
    checked, exempt, notes, findings = [], [], [], []
    row = row or {"row_key": CLUSTER_ROW_KEY, "owner": "landing-zone", "kind": CLUSTER_ROW_KIND}
    for dirpath, dirnames, filenames in os.walk(clone_dir):
        dirnames[:] = sorted(d for d in dirnames if d not in (".git", ".terraform"))
        for name in sorted(filenames):
            if not name.endswith(".tf"):
                continue
            path = os.path.join(dirpath, name)
            rel = os.path.relpath(path, clone_dir)
            if _in_units(rel):
                continue
            try:
                with open(path, "r", encoding="utf-8") as f:
                    stripped = root_wiring._strip_hcl(f.read())
            except (OSError, UnicodeDecodeError):
                continue
            for match in _CLUSTER_BLOCK_RE.finditer(stripped):
                address = f"{CLUSTER_RESOURCE_TYPE}.{match.group(1)}"
                checked.append(f"{rel}: {address}")
                body = _hcl_body(stripped, match.end() - 1)
                if _AUTOPILOT_RE.search(body):
                    exempt.append(f"{rel}: {address} is Autopilot; Cloud DNS is implicit")
                    continue
                dns_block = _DNS_CONFIG_RE.search(body)
                if not dns_block:
                    findings.append(_finding(row, (
                        f"cluster field: {rel}: {address}: add dns_config {{ cluster_dns = "
                        f'"{CLUSTER_DNS_PROVIDER}" }} to the cluster resource. It declares no '
                        "dns_config block, so this scan cannot prove it runs Cloud DNS (a "
                        "Standard cluster without the block runs kube-dns); the landing-zone "
                        "standing default is Cloud DNS for GKE (gke-landing-zone.md), and "
                        "the cluster-dns unit's kube-dns ConfigMap assumes it.")))
                    continue
                inner = _hcl_body(body, dns_block.end() - 1)
                if dns_block.group(0).startswith("dynamic"):
                    # The value lives in the `content { }` body; the block's own
                    # attributes (for_each, iterator) are not it.
                    content = re.search(r"\bcontent\s*\{", inner)
                    inner = _hcl_body(inner, content.end() - 1) if content else ""
                value = _CLUSTER_DNS_RE.search(inner)
                # A quoted value with `$` or `%` is a template ("${var.x}"):
                # the stripper blanks its braces, so it would read as a
                # mangled literal. It is an expression, like a bare var.x.
                literal = re.match(r'"([^"$%]*)"\s*$', value.group(1).strip()) if value else None
                if value is None:
                    findings.append(_finding(row, (
                        f"cluster field: {rel}: {address}: set cluster_dns = "
                        f'"{CLUSTER_DNS_PROVIDER}" inside its dns_config block. The block sets '
                        "no cluster_dns, which the provider reads as the platform default "
                        "(kube-dns on Standard).")))
                elif literal is None:
                    raw = value.group(1).strip()
                    # The stripper blanked the braces of a template string;
                    # do not echo the mangled text.
                    shown = "a template string" if raw.startswith('"') else raw
                    notes.append(f"{rel}: {address} sets cluster_dns = {shown}, an "
                                 "expression this scan cannot read; accepted, not verified "
                                 f'— confirm it resolves to "{CLUSTER_DNS_PROVIDER}"')
                elif literal.group(1) != CLUSTER_DNS_PROVIDER:
                    findings.append(_finding(row, (
                        f"cluster field: {rel}: {address}: set cluster_dns = "
                        f'"{CLUSTER_DNS_PROVIDER}" (it sets "{literal.group(1)}"). The '
                        "landing-zone standing default is Cloud DNS for GKE "
                        "(gke-landing-zone.md), and the cluster-dns unit's kube-dns "
                        "ConfigMap is read by Cloud DNS only there.")))
    return {"checked": checked, "exempt": exempt, "notes": notes, "findings": findings}


def _finding(verdict, error: str) -> dict:
    return {
        "row_key": verdict["row_key"],
        "kind": verdict["kind"],
        "owner": verdict["owner"],
        "error": error,
    }


def _in_units(rel_path: str) -> bool:
    """Is this clone-relative .tf path inside a materialized translation unit?"""
    return rel_path.split(os.sep, 1)[0] == UNITS_SUBDIR


def _check_scan_row(verdict, rtype: str, counts: dict) -> dict:
    """Verdict for a RESOURCE_SCANS row: the clone either declares it or not.

    `counts` is the scan's {relative path: declaration count}. Declarations
    inside translation-units/ never satisfy the row, and never pass
    unnamed: every RESOURCE_SCANS row is landing-zone-owned and, per the
    map, never re-emitted by a unit — so a unit declaring the resource is
    an owner-boundary finding whether it masks a landing-zone omission
    (no owned declaration) or duplicates the landing zone's own (a second
    control plane riding a green report). A SINGLETON_TYPES resource
    declared more than once in the owned clone is a finding too: present
    is not the row's whole contract, exactly-one is.
    """
    files = sorted(counts)
    owned = sorted(f for f in files if not _in_units(f))
    unit_files = sorted(f for f in files if _in_units(f))
    if unit_files and owned:
        return _finding(verdict, (
            f"owner-boundary violation: {rtype} is declared inside "
            "materialized translation units (" + ", ".join(unit_files)
            + ") beside the landing zone's own declaration ("
            + ", ".join(owned) + f") — map row '{verdict['kind']}' is "
            f"{verdict['owner']}-owned and never re-emitted by a unit, and "
            "a duplicate is a second control plane nobody decided. Remove "
            f"the {rtype} from the unit."))
    if owned:
        total = sum(counts[f] for f in owned)
        if rtype in SINGLETON_TYPES and total > 1:
            listed = ", ".join(f"{f} ({counts[f]})" for f in owned)
            return _finding(verdict, (
                f"coverage conflict: the clone declares {total} {rtype} "
                f"resources ({listed}) — the landing-zone MUST "
                "(knowledge/gke-landing-zone.md, Validation) is exactly 1 "
                "under any profile, a shrunk sandbox included. A second "
                "declaration is a second control plane nobody decided, and "
                "the exports cluster derivation publishes coordinates only "
                "for a single literal cluster. Remove the extra "
                "declaration(s) from the landing-zone draft."))
        return {"row_key": verdict["row_key"],
                "via": f"clone declares {rtype} in: " + ", ".join(owned)}
    if files:
        return _finding(verdict, (
            f"coverage omission: the only {rtype} declaration(s) in the clone "
            f"are inside materialized translation units ("
            + ", ".join(sorted(files)) + f") — map row '{verdict['kind']}' is "
            f"{verdict['owner']}-owned and never re-emitted by a unit, so this "
            "is an owner-boundary violation masking the landing-zone omission. "
            f"Declare the {rtype} in the landing-zone draft and remove it from "
            "the unit."))
    return _finding(verdict, (
        f"coverage omission: the inventory holds facts for map row "
        f"'{verdict['kind']}' ({verdict['owner']}), but no .tf file in the "
        f"materialized clone declares a {rtype} resource and no explicit "
        "skip records the decision — the migration would ship without its "
        f"{rtype}. Fix the landing-zone draft (or its vendored modules) so "
        "the clone declares one."))


def _check_cited_row(verdict, row_units: list) -> dict:
    """Verdict for a unit-cited row: a done citing unit is the artifact; an
    all-skipped citation set is the explicit human decision; anything else
    (including no citing unit at all) is the omission this gate enforces."""
    done = sorted(u.get("unit_id") or "" for u in row_units
                  if u.get("status") == "done")
    if done:
        return {"row_key": verdict["row_key"],
                "via": "done unit(s): " + ", ".join(done)}
    # A `placeholder` unit is the planner's OWN no-facts marker (planner._unit),
    # born `skipped` with no human in the loop. The all-skipped route means
    # "chosen at Gate C", so placeholders must not ride it: the map's section
    # granularity is coarser than the family trigger, so a row can hold facts
    # while its only citing unit is a birth placeholder — precisely the silent
    # omission this gate exists to catch.
    decided = [u for u in row_units if not u.get("placeholder")]
    if decided and all(u.get("status") == "skipped" for u in decided):
        skipped = sorted(u.get("unit_id") or "" for u in decided)
        return {"row_key": verdict["row_key"],
                "via": "explicitly skipped unit(s): " + ", ".join(skipped)}
    if row_units and not decided:
        names = ", ".join(sorted(u.get("unit_id") or "" for u in row_units))
        return _finding(verdict, (
            f"coverage omission: the inventory holds facts for map row "
            f"'{verdict['kind']}' ({verdict['owner']}), but the only unit(s) "
            f"citing it are the planner's no-facts placeholders ({names}) — "
            "the family's trigger did not fire for facts the row's section "
            "does hold, so nothing shipped and nobody chose the omission. "
            "Widen the unit family to the facts, or skip the unit in review "
            "to record the decision."))
    if row_units:
        states = ", ".join(sorted({str(u.get("status")) for u in row_units}))
        return _finding(verdict, (
            f"coverage omission: the inventory holds facts for map row "
            f"'{verdict['kind']}' ({verdict['owner']}), and its citing "
            f"unit(s) are neither done nor skipped (status: {states}) — "
            "nothing shipped covers the row. Finish or skip those units."))
    return None  # No citing unit at all: the caller routes pin vs finding.


def check(plan: dict, inventory: dict, clone_dir: str,
          rows: dict = None, schema: dict = None) -> dict:
    """Enforces the omission check over the materialized clone. Deterministic.

    Returns {"granularity", "checked", "findings", "satisfied", "unenforced",
    "scans", "cluster_fields"}. `findings` non-empty means validation must fail; each finding
    names the row. `rows`/`schema` default to the live coverage map (already
    validated at start-up) and inventory schema; tests pass fixtures.
    """
    if rows is None:
        rows = load_coverage_map()
    if schema is None:
        schema = coverage.load_inventory_schema()
    verdicts = coverage.instantiate_coverage_map(rows, inventory or {}, schema)
    scan_counts = scan_resource_counts(
        clone_dir, sorted(set(RESOURCE_SCANS.values())))

    # A plan stored before row citations existed carries `covers` on NO unit:
    # reading that as "every cited row omitted" would wedge the workspace
    # forever, because the remedy is unreachable — plan_translation only runs
    # at STATE_LZ_TRANSLATION_PLAN and validate's failure route parks at
    # STATE_TRANSLATION_REVIEW, which has no way back. Backfill from the
    # planner's family table by unit `kind` instead: the citation is per
    # family, so the table IS what the plan would have carried. A plan that
    # does carry citations is read verbatim and enforced hard.
    units = plan.get("units") or []
    backfilled = bool(units) and not any("covers" in u for u in units)
    covering = {}  # row_key -> [unit, ...] in plan order, any status
    for unit in units:
        cites = (planner.FAMILY_COVERS.get(unit.get("kind")) or []
                 if backfilled else unit.get("covers") or [])
        for cited in cites:
            covering.setdefault(cited, []).append(unit)

    findings, satisfied, unenforced, checked = [], [], [], 0
    for verdict in verdicts:
        if verdict["owner"] not in ("landing-zone", "platform-translation"):
            continue  # workload rows are the [PLANNED] developer phase's
        if verdict["status"] == coverage.STATUS_UNKNOWN_SECTION:
            checked += 1
            findings.append(_finding(verdict, (
                f"coverage map defect: the 'Discovered from' cell of row "
                f"'{verdict['kind']}' does not resolve against the inventory "
                "schema, so this gate cannot decide whether facts exist for "
                "it — fail-closed; fix the map (coverage-map.md)")))
            continue
        if verdict["status"] != coverage.STATUS_FACTS_PRESENT:
            continue  # no facts / not-scanned: nothing was omitted
        checked += 1
        if verdict["row_key"] in RESOURCE_SCANS:
            rtype = RESOURCE_SCANS[verdict["row_key"]]
            outcome = _check_scan_row(verdict, rtype, scan_counts.get(rtype) or {})
        else:
            outcome = _check_cited_row(verdict, covering.get(verdict["row_key"], []))
            if outcome is None:
                reason = UNENFORCED_ROWS.get(verdict["row_key"])
                if reason is not None:
                    unenforced.append({"row_key": verdict["row_key"], "reason": reason})
                    continue
                outcome = _finding(verdict, (
                    f"coverage omission: the inventory holds facts for map "
                    f"row '{verdict['kind']}' ({verdict['owner']}), but no "
                    "unit cites it, no clone scan proves it, and no "
                    "UNENFORCED_ROWS pin excuses it — the map gained a row "
                    "without an owner in enforcement. Give the row a unit "
                    "family (planner.FAMILY_COVERS), a clone scan "
                    "(RESOURCE_SCANS), or a documented UNENFORCED_ROWS pin"))
        (findings if "error" in outcome else satisfied).append(outcome)

    # The field check runs over every declared owned cluster, facts or not:
    # a cluster the design declares must run the DNS provider the design
    # promises, whatever the inventory said.
    cluster_row = next((v for v in verdicts if v["row_key"] == CLUSTER_ROW_KEY), None)
    fields = scan_cluster_dns_config(clone_dir, cluster_row)
    findings.extend(fields["findings"])

    return {
        "granularity": GRANULARITY,
        "checked": checked,
        "backfilled_citations": backfilled,
        "findings": findings,
        "satisfied": satisfied,
        "unenforced": unenforced,
        "scans": {rtype: sorted(files) for rtype, files in scan_counts.items()},
        "cluster_fields": {"checked": fields["checked"], "exempt": fields["exempt"],
                           "notes": fields["notes"]},
    }
