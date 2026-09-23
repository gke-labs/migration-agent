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

"""Unit tests for the enforced coverage-omission gate. No GCS, no LLM."""

import os
import tempfile
import unittest

from servers.dag.server import coverage_map
from servers.phases.landingzone.landingzone_translationplan_3 import coverage
from servers.phases.landingzone.landingzone_translationplan_3 import planner
from servers.phases.translation.translation_validate_3 import coverage_gate

_KNOWLEDGE_DOC = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    "..", "..", "landingzone", "knowledge", "coverage-map.md",
)

CLUSTER_ROW_KEY = coverage_gate.CLUSTER_ROW_KEY

CLUSTER_TF = (
    'resource "google_container_cluster" "this" {\n'
    '  name = "images"\n'
    '  dns_config {\n'
    '    cluster_dns = "CLOUD_DNS"\n'
    '  }\n'
    "}\n"
)
# A cluster as a design that ignored the standing default would write it.
CLUSTER_TF_NO_DNS = (
    'resource "google_container_cluster" "this" {\n'
    '  name = "images"\n'
    "}\n"
)


def _real_rows():
    with open(_KNOWLEDGE_DOC, "r", encoding="utf-8") as f:
        return coverage_map.parse_coverage_map(f.read(), "coverage-map.md")


def _row(kind, owner, source, column="terraform"):
    return {"kind": kind, "column": column, "owner": owner,
            "source": source, "notes": ""}


# A synthetic map: the real cluster row key (so RESOURCE_SCANS applies), one
# platform row per satisfaction regime, and out-of-scope/no-facts rows.
ROWS = {
    CLUSTER_ROW_KEY: _row(
        "GKE cluster (control plane, target shape)", "landing-zone",
        "`triggers`, landing-zone decisions"),
    "alpha": _row("Alpha", "platform-translation", "`addons`"),
    "beta": _row("Beta", "platform-translation", "—"),
    "empty": _row("Empty", "platform-translation", "`network`"),
    "theta": _row("Theta", "workload", "`addons`", column="k8s"),
}

SCHEMA = {
    "properties": {
        "triggers": {"properties": {"karpenter": {"type": "boolean"}}},
        "addons": {"type": "array"},
        "network": {"properties": {"vpc_peering": {"type": "boolean"}}},
    }
}

# triggers + addons hold facts; network holds none.
INVENTORY = {
    "triggers": {"karpenter": True},
    "addons": [{"name": "aws-ebs-csi-driver"}],
    "network": {},
}


def _unit(unit_id, kind, covers, status="done"):
    return {"unit_id": unit_id, "kind": kind, "status": status, "covers": covers}


def _write(clone, rel, content):
    path = os.path.join(clone, rel)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)


def _check(clone, plan=None, inventory=None):
    return coverage_gate.check(plan or {"units": []},
                               INVENTORY if inventory is None else inventory,
                               clone, rows=ROWS, schema=SCHEMA)


PLAN_ALPHA_DONE = {"units": [_unit("u-alpha", "alpha-kind", ["alpha"])]}


class ClusterScanRowTest(unittest.TestCase):
    """The audit's e2e case: the clone must declare the cluster resource."""

    def test_missing_cluster_resource_is_a_finding_naming_the_row(self):
        with tempfile.TemporaryDirectory() as clone:
            _write(clone, "main.tf", 'resource "google_compute_network" "v" {}\n')
            result = _check(clone, PLAN_ALPHA_DONE)
        keys = [f["row_key"] for f in result["findings"]]
        self.assertEqual(keys, [CLUSTER_ROW_KEY])
        error = result["findings"][0]["error"]
        self.assertIn("google_container_cluster", error)
        self.assertIn("GKE cluster (control plane, target shape)", error)

    def test_cluster_resource_at_the_root_satisfies(self):
        with tempfile.TemporaryDirectory() as clone:
            _write(clone, "main.tf", CLUSTER_TF)
            result = _check(clone, PLAN_ALPHA_DONE)
        self.assertEqual(result["findings"], [])
        via = {s["row_key"]: s["via"] for s in result["satisfied"]}
        self.assertIn("main.tf", via[CLUSTER_ROW_KEY])

    def test_cluster_resource_in_a_vendored_module_satisfies(self):
        # The landing-zone draft convention: modules copied into the clone
        # (terraform/modules/gke-cluster), instantiated from a root module.
        with tempfile.TemporaryDirectory() as clone:
            _write(clone, "terraform/modules/gke-cluster/main.tf", CLUSTER_TF)
            result = _check(clone, PLAN_ALPHA_DONE)
        self.assertEqual(result["findings"], [])

    def test_commented_out_resource_does_not_satisfy(self):
        commented = "".join("# " + line + "\n" for line in CLUSTER_TF.splitlines())
        with tempfile.TemporaryDirectory() as clone:
            _write(clone, "main.tf", commented)
            result = _check(clone, PLAN_ALPHA_DONE)
        self.assertEqual([f["row_key"] for f in result["findings"]],
                         [CLUSTER_ROW_KEY])

    def test_resource_under_dot_terraform_or_git_does_not_satisfy(self):
        with tempfile.TemporaryDirectory() as clone:
            _write(clone, ".terraform/modules/x/main.tf", CLUSTER_TF)
            _write(clone, ".git/main.tf", CLUSTER_TF)
            result = _check(clone, PLAN_ALPHA_DONE)
        self.assertEqual([f["row_key"] for f in result["findings"]],
                         [CLUSTER_ROW_KEY])

    def test_two_clusters_are_a_singleton_finding_not_a_pass(self):
        # The knowledge MUST: exactly 1 google_container_cluster under any
        # profile. Two declarations are a second control plane nobody
        # decided — present is not the row's whole contract.
        with tempfile.TemporaryDirectory() as clone:
            _write(clone, "main.tf", CLUSTER_TF)
            _write(clone, "terraform/modules/gke-cluster/main.tf",
                   CLUSTER_TF.replace('"this"', '"second"'))
            result = _check(clone, PLAN_ALPHA_DONE)
        self.assertEqual([f["row_key"] for f in result["findings"]],
                         [CLUSTER_ROW_KEY])
        error = result["findings"][0]["error"]
        self.assertIn("exactly 1", error)
        self.assertIn("2 google_container_cluster", error)
        self.assertIn("main.tf (1)", error)

    def test_two_clusters_in_one_file_are_counted_not_deduplicated(self):
        with tempfile.TemporaryDirectory() as clone:
            _write(clone, "main.tf",
                   CLUSTER_TF + CLUSTER_TF.replace('"this"', '"second"'))
            result = _check(clone, PLAN_ALPHA_DONE)
        error = result["findings"][0]["error"]
        self.assertIn("exactly 1", error)
        self.assertIn("main.tf (2)", error)

    def test_a_unit_cluster_beside_the_landing_zones_is_a_boundary_finding(self):
        # One owned + one unit-declared (2026-08-16 review): before the fix
        # the owned declaration satisfied the row while the unit's duplicate
        # rode the green report — a second control plane nobody decided.
        with tempfile.TemporaryDirectory() as clone:
            _write(clone, "main.tf", CLUSTER_TF)
            _write(clone, "translation-units/np/main.tf",
                   CLUSTER_TF.replace('"this"', '"second"'))
            result = _check(clone, PLAN_ALPHA_DONE)
        self.assertEqual([f["row_key"] for f in result["findings"]],
                         [CLUSTER_ROW_KEY])
        error = result["findings"][0]["error"]
        self.assertIn("owner-boundary violation", error)
        self.assertIn("translation-units/np/main.tf", error)


class CitedRowTest(unittest.TestCase):
    """Platform rows: done citing unit = artifact; all-skipped = explicit
    decision; anything else over facts is the enforced omission."""

    def setUp(self):
        self.clone = tempfile.TemporaryDirectory()
        _write(self.clone.name, "main.tf", CLUSTER_TF)

    def tearDown(self):
        self.clone.cleanup()

    def _findings(self, plan):
        return {f["row_key"]: f["error"]
                for f in _check(self.clone.name, plan)["findings"]}

    def test_done_citing_unit_satisfies(self):
        self.assertEqual(self._findings(PLAN_ALPHA_DONE), {})

    def test_all_skipped_citing_units_satisfy_as_the_explicit_decision(self):
        plan = {"units": [
            _unit("u-alpha", "alpha-kind", ["alpha"], status="skipped"),
            _unit("u-alpha-2", "alpha-kind", ["alpha"], status="skipped"),
        ]}
        result = _check(self.clone.name, plan)
        self.assertEqual(result["findings"], [])
        via = {s["row_key"]: s["via"] for s in result["satisfied"]}
        self.assertIn("explicitly skipped", via["alpha"])

    def test_citing_units_neither_done_nor_skipped_are_a_finding(self):
        for status in ("planned", "revise", "error"):
            plan = {"units": [_unit("u-alpha", "alpha-kind", ["alpha"], status=status)]}
            findings = self._findings(plan)
            self.assertIn("alpha", findings, status)
            self.assertIn(status, findings["alpha"])

    def test_mixed_done_and_skipped_satisfies_via_the_done_unit(self):
        plan = {"units": [
            _unit("u-1", "alpha-kind", ["alpha"], status="skipped"),
            _unit("u-2", "alpha-kind", ["alpha"]),
        ]}
        self.assertEqual(self._findings(plan), {})

    def test_uncited_facts_present_row_without_a_pin_is_a_finding(self):
        findings = self._findings({"units": []})
        self.assertIn("alpha", findings)
        self.assertIn("no unit cites it", findings["alpha"])

    def test_no_facts_and_not_scanned_rows_are_outside_enforcement(self):
        # `empty` (facts absent) and `beta` ("—") never appear anywhere.
        result = _check(self.clone.name, PLAN_ALPHA_DONE)
        seen = ({f["row_key"] for f in result["findings"]}
                | {s["row_key"] for s in result["satisfied"]}
                | {u["row_key"] for u in result["unenforced"]})
        self.assertNotIn("empty", seen)
        self.assertNotIn("beta", seen)

    def test_workload_rows_are_never_enforced(self):
        # theta has facts (addons) and no citing unit, but its owner is the
        # [PLANNED] developer phase — out of this gate's scope.
        result = _check(self.clone.name, PLAN_ALPHA_DONE)
        self.assertNotIn("theta", {f["row_key"] for f in result["findings"]})

    def test_empty_inventory_checks_nothing_and_passes(self):
        # The main_test harness regime: no discovery inventory in variables.
        result = _check(self.clone.name, {"units": []}, inventory={})
        self.assertEqual(result["checked"], 0)
        self.assertEqual(result["findings"], [])

    def test_unknown_section_row_fails_closed_as_a_map_defect(self):
        rows = dict(ROWS)
        rows["gamma"] = _row("Gamma", "platform-translation", "`no_such_section`")
        result = coverage_gate.check(PLAN_ALPHA_DONE, INVENTORY,
                                     self.clone.name, rows=rows, schema=SCHEMA)
        by_key = {f["row_key"]: f["error"] for f in result["findings"]}
        self.assertIn("gamma", by_key)
        self.assertIn("map defect", by_key["gamma"])

    def test_deterministic(self):
        first = _check(self.clone.name, PLAN_ALPHA_DONE)
        second = _check(self.clone.name, PLAN_ALPHA_DONE)
        self.assertEqual(first, second)


class RealMapBindingTest(unittest.TestCase):
    """The enforcement pins are bound to the live map: a renamed row breaks
    them here, the day it happens — never as a silently skipped row."""

    def test_pins_name_real_rows_with_the_expected_owners(self):
        rows = _real_rows()
        for key in coverage_gate.RESOURCE_SCANS:
            self.assertIn(key, rows, key)
            self.assertEqual(rows[key]["owner"], "landing-zone", key)
        for key in coverage_gate.UNENFORCED_ROWS:
            self.assertIn(key, rows, key)
        self.assertFalse(
            set(coverage_gate.RESOURCE_SCANS) & set(coverage_gate.UNENFORCED_ROWS))
        # The field check names the same row the resource scan enforces.
        self.assertIn(coverage_gate.CLUSTER_ROW_KEY, coverage_gate.RESOURCE_SCANS)
        self.assertEqual(rows[coverage_gate.CLUSTER_ROW_KEY]["kind"], coverage_gate.CLUSTER_ROW_KIND)

    def test_every_sourced_in_scope_row_has_exactly_one_enforcement_route(self):
        # The partition that makes "fails by default" safe to ship: every
        # discovery-sourced landing-zone/platform-translation row is either
        # cited by a planner family, proven by a clone scan, or consciously
        # pinned unenforced — and only one of the three. A new sourced row
        # in the map lands in this test until it gets a route.
        rows = _real_rows()
        sourced = {key for key, row in rows.items()
                   if row["owner"] in ("landing-zone", "platform-translation")
                   and coverage.resolve_discovered_from(row["source"])}
        cited = {key for covers in planner.FAMILY_COVERS.values() for key in covers}
        scans = set(coverage_gate.RESOURCE_SCANS)
        pins = set(coverage_gate.UNENFORCED_ROWS)
        self.assertEqual(sourced, cited | scans | pins)
        self.assertFalse(cited & scans)
        self.assertFalse(cited & pins)


class RealMapEndToEndTest(unittest.TestCase):
    """The gate over the shipped map and schema, acme-shaped."""

    INVENTORY = {
        "clusters": [{"name": "acme-prod", "workloads": {"namespaces": [
            {"name": "acme-shop", "deployments": 3}]}}],
        "addons": [{"name": "karpenter"}],
        "autoscaling": {"karpenter": True},
        "nodegroups": [{"name": "system"}],
        "workloads": {"privileged_daemonsets": [{"name": "node-agent"}],
                      "host_network": ["acme-shop/node-agent"],
                      "irsa_bindings": ["acme-shop/orders"]},
        "network": {"vpc_peering": True, "load_balancers": ["frontend-alb"]},
        "storage": {"ebs_csi": True, "storage_classes": ["gp3-encrypted"]},
        "triggers": {"karpenter": True},
        "cluster_dns": {"sources": [{"kind": "configmap", "name": "coredns", "text": "x"}]},
    }

    def _done_plan(self):
        # One done unit per planner family: the plan a full estate produces
        # once translation finishes, statuses advanced to done.
        return {"units": [
            _unit(f"u-{kind}", kind, list(covers))
            for kind, covers in sorted(planner.FAMILY_COVERS.items())
        ]}

    def test_full_run_with_cluster_declared_is_clean(self):
        with tempfile.TemporaryDirectory() as clone:
            _write(clone, "terraform/modules/gke-cluster/main.tf", CLUSTER_TF)
            result = coverage_gate.check(
                self._done_plan(), self.INVENTORY, clone)
        self.assertEqual(result["findings"], [])
        # Filestore instances (facts in `storage`, no family) rides the pin,
        # with base VPC and Artifact Registry outside facts or pinned too.
        self.assertIn("filestore instances",
                      {u["row_key"] for u in result["unenforced"]})

    def test_full_run_without_cluster_fails_the_e2e_case(self):
        # THE audit regression: everything translated and green, but the
        # clone ships no google_container_cluster — the run must fail
        # naming the cluster row.
        with tempfile.TemporaryDirectory() as clone:
            _write(clone, "main.tf", 'resource "google_compute_network" "v" {}\n')
            result = coverage_gate.check(
                self._done_plan(), self.INVENTORY, clone)
        self.assertEqual([f["row_key"] for f in result["findings"]],
                         [CLUSTER_ROW_KEY])

    def test_all_false_triggers_still_enforces_the_cluster_row(self):
        # The common estate: none of karpenter / privileged / gpu / peering
        # fired, so every `triggers` boolean is false. The row is sourced from
        # `clusters` too precisely so the flagship enforcement is not
        # conditional on those four having fired.
        inventory = dict(self.INVENTORY, triggers={
            "karpenter": False, "privileged_daemonsets": False,
            "gpu_tpu": False, "vpc_peering": False})
        with tempfile.TemporaryDirectory() as clone:
            _write(clone, "main.tf", 'resource "google_compute_network" "v" {}\n')
            result = coverage_gate.check(self._done_plan(), inventory, clone)
        self.assertIn(CLUSTER_ROW_KEY, [f["row_key"] for f in result["findings"]])

    def test_a_unit_declaring_the_cluster_does_not_satisfy_the_row(self):
        # A worker inventing a cluster to hang node pools off: an owner-
        # boundary violation, not the landing zone's artifact.
        with tempfile.TemporaryDirectory() as clone:
            _write(clone, f"{coverage_gate.UNITS_SUBDIR}/node-pool/main.tf",
                   CLUSTER_TF)
            result = coverage_gate.check(
                self._done_plan(), self.INVENTORY, clone)
        by_key = {f["row_key"]: f["error"] for f in result["findings"]}
        self.assertIn(CLUSTER_ROW_KEY, by_key)
        self.assertIn("owner-boundary violation", by_key[CLUSTER_ROW_KEY])

    def test_placeholder_only_citation_is_a_finding_not_a_decision(self):
        # The planner's real output for a no-facts family: born `skipped`,
        # `placeholder` True. The Gateway row is sourced from `network` as a
        # whole, which is coarser than the family's load-balancer / ingress
        # trigger — so peering facts alone make the row facts-present while
        # the family emits nothing but a placeholder.
        inventory = dict(self.INVENTORY, network={"vpc_peering": True})
        plan = planner.build_translation_plan(inventory)
        gateway = next(u for u in plan["units"] if u["kind"] == "gateway")
        self.assertTrue(gateway["placeholder"] and gateway["status"] == "skipped")
        with tempfile.TemporaryDirectory() as clone:
            _write(clone, "main.tf", CLUSTER_TF)
            result = coverage_gate.check(plan, inventory, clone)
        by_key = {f["row_key"]: f["error"] for f in result["findings"]}
        self.assertIn("gateway (shared entry point)", by_key)
        self.assertIn("no-facts placeholders",
                      by_key["gateway (shared entry point)"])
        self.assertFalse([s for s in result["satisfied"]
                          if s["row_key"] == "gateway (shared entry point)"])

    def test_shipped_map_resolves_with_no_unknown_sections(self):
        # unknown-section is a finding at this gate, so the shipped map must
        # never produce one: enforced here against the real schema.
        with tempfile.TemporaryDirectory() as clone:
            _write(clone, "main.tf", CLUSTER_TF)
            result = coverage_gate.check(self._done_plan(), {}, clone)
        self.assertEqual(result["findings"], [])


class PreCitationPlanTest(unittest.TestCase):
    """A plan stored before `covers` existed: citations are backfilled from
    the family table, not read as a total omission. Such a workspace has no
    route back to plan_translation (validate fails to STATE_TRANSLATION_REVIEW,
    which cannot reach STATE_LZ_TRANSLATION_PLAN), so a hard failure there
    would wedge it forever."""

    def _stripped(self):
        plan = planner.build_translation_plan(RealMapEndToEndTest.INVENTORY)
        for unit in plan["units"]:
            unit.pop("covers", None)
            unit.pop("placeholder", None)
            unit["status"] = "done"
        return plan

    def test_covers_less_plan_is_backfilled_by_kind_and_still_enforces(self):
        plan = self._stripped()
        with tempfile.TemporaryDirectory() as clone:
            _write(clone, "main.tf", CLUSTER_TF)
            result = coverage_gate.check(
                plan, RealMapEndToEndTest.INVENTORY, clone)
        self.assertTrue(result["backfilled_citations"])
        # The families this plan really carries cover their rows…
        self.assertIn("cross-vpc connectivity and load balancers",
                      {s["row_key"] for s in result["satisfied"]})
        # …and the cluster scan half is untouched by the backfill.
        self.assertNotIn(CLUSTER_ROW_KEY, {f["row_key"] for f in result["findings"]})

    def test_a_plan_that_does_carry_citations_is_never_backfilled(self):
        plan = planner.build_translation_plan(RealMapEndToEndTest.INVENTORY)
        for unit in plan["units"]:
            unit["status"] = "done"
        plan["units"][0]["covers"] = []  # a unit deliberately citing nothing
        with tempfile.TemporaryDirectory() as clone:
            _write(clone, "main.tf", CLUSTER_TF)
            result = coverage_gate.check(
                plan, RealMapEndToEndTest.INVENTORY, clone)
        self.assertFalse(result["backfilled_citations"])
        self.assertIn(planner.FAMILY_COVERS[plan["units"][0]["kind"]][0],
                      {f["row_key"] for f in result["findings"]})

    def test_units_subdir_matches_the_materializer(self):
        from servers.phases.translation.translation_validate_3 import tools
        self.assertEqual(coverage_gate.UNITS_SUBDIR, tools.UNITS_SUBDIR)


class ClusterDnsConfigTest(unittest.TestCase):
    """The one cluster field the gate reads: dns_config.cluster_dns."""

    def _fields(self, tf, rel="main.tf"):
        with tempfile.TemporaryDirectory() as clone:
            _write(clone, rel, tf)
            return coverage_gate.scan_cluster_dns_config(clone)

    def test_the_standing_default_is_clean(self):
        result = self._fields(CLUSTER_TF)
        self.assertEqual(result["findings"], [])
        self.assertEqual(result["checked"], ["main.tf: google_container_cluster.this"])

    def test_no_dns_config_is_a_finding_on_the_cluster_row(self):
        result = self._fields(CLUSTER_TF_NO_DNS)
        [finding] = result["findings"]
        self.assertEqual(finding["row_key"], CLUSTER_ROW_KEY)
        self.assertIn("declares no dns_config block", finding["error"])
        self.assertLess(finding["error"].index("add dns_config"), 80)  # remedy survives the reply's cut
        self.assertIn("google_container_cluster.this", finding["error"])

    def test_kube_dns_and_a_missing_cluster_dns_are_findings(self):
        tf = CLUSTER_TF.replace('"CLOUD_DNS"', '"KUBE_DNS"')
        self.assertIn('(it sets "KUBE_DNS")', self._fields(tf)["findings"][0]["error"])
        tf = CLUSTER_TF.replace('    cluster_dns = "CLOUD_DNS"\n', '    cluster_dns_scope = "CLUSTER_SCOPE"\n')
        self.assertIn("sets no cluster_dns", self._fields(tf)["findings"][0]["error"])

    def test_an_autopilot_cluster_is_exempt(self):
        tf = CLUSTER_TF_NO_DNS.replace('  name = "images"\n', '  name = "images"\n  enable_autopilot = true\n')
        result = self._fields(tf)
        self.assertEqual(result["findings"], [])
        self.assertEqual(result["notes"], [])
        self.assertTrue(any("Autopilot" in n for n in result["exempt"]))

    def test_a_dynamic_dns_config_block_is_read_like_a_plain_one(self):
        tf = CLUSTER_TF.replace('  dns_config {\n    cluster_dns = "CLOUD_DNS"\n  }\n',
                                '  dynamic "dns_config" {\n    for_each = var.autopilot ? [] : [1]\n'
                                '    content {\n      cluster_dns = "CLOUD_DNS"\n    }\n  }\n')
        self.assertEqual(self._fields(tf)["findings"], [])
        self.assertIn('(it sets "KUBE_DNS")',
                      self._fields(tf.replace("CLOUD_DNS", "KUBE_DNS"))["findings"][0]["error"])
        # The block's own for_each mentions cluster_dns; the value is in content.
        tricky = tf.replace("for_each = var.autopilot ? [] : [1]",
                            "for_each = var.cluster_dns == null ? [] : [1]").replace("CLOUD_DNS", "KUBE_DNS")
        self.assertEqual(len(self._fields(tricky)["findings"]), 1)
        no_content = tf.replace("    content {\n      cluster_dns = \"CLOUD_DNS\"\n    }\n", "")
        self.assertIn("sets no cluster_dns", self._fields(no_content)["findings"][0]["error"])

    def test_autopilot_behind_an_expression_is_not_an_exemption(self):
        tf = CLUSTER_TF_NO_DNS.replace('  name = "images"\n', '  name = "images"\n  enable_autopilot = var.autopilot\n')
        self.assertEqual(len(self._fields(tf)["findings"]), 1)

    def test_a_value_through_an_expression_is_accepted_with_a_note(self):
        for expression in ("var.cluster_dns", '"${var.cluster_dns}"', '"%{ if true }CLOUD_DNS%{ endif }"'):
            tf = CLUSTER_TF.replace('"CLOUD_DNS"', expression)
            result = self._fields(tf)
            self.assertEqual(result["findings"], [], expression)
            [note] = result["notes"]
            self.assertIn("accepted, not verified", note)
            self.assertIn("var.cluster_dns" if expression.startswith("var") else "a template string", note)
            self.assertNotIn("$ var", note)  # never the stripper's mangled text

    def test_the_reference_module_is_the_clean_shape(self):
        here = os.path.dirname(os.path.abspath(__file__))
        module = os.path.join(here, "..", "..", "..", "..", "reference", "terraform", "modules", "gke-cluster")
        result = coverage_gate.scan_cluster_dns_config(os.path.normpath(module))
        self.assertEqual(result["checked"], ["main.tf: google_container_cluster.this"])
        self.assertEqual((result["findings"], result["notes"], result["exempt"]), ([], [], []))

    def test_a_comparison_above_the_value_is_not_the_value(self):
        tf = CLUSTER_TF.replace('    cluster_dns = "CLOUD_DNS"\n',
                                '    cluster_dns_scope = var.cluster_dns == "CLOUD_DNS" ? "CLUSTER_SCOPE" : null\n'
                                '    cluster_dns = "CLOUD_DNS"\n')
        result = self._fields(tf)
        self.assertEqual((result["findings"], result["notes"]), ([], []))

    def test_odd_shapes_are_read(self):
        one_line = 'resource "google_container_cluster" "this" {\n  dns_config { cluster_dns = "CLOUD_DNS" } # note {\n}\n'
        self.assertEqual(self._fields(one_line)["findings"], [])
        crlf = CLUSTER_TF.replace("\n", "\r\n").replace('"CLOUD_DNS"', '"KUBE_DNS"')
        self.assertEqual(len(self._fields(crlf)["findings"]), 1)
        with tempfile.TemporaryDirectory() as clone:
            with open(os.path.join(clone, "bad.tf"), "wb") as f:
                f.write(b"\xff\xfe not utf-8")
            _write(clone, "main.tf", CLUSTER_TF)
            self.assertEqual(coverage_gate.scan_cluster_dns_config(clone)["findings"], [])
            counts = coverage_gate.scan_resource_counts(clone, ["google_container_cluster"])
            self.assertEqual(counts["google_container_cluster"], {"main.tf": 1})

    def test_comments_units_and_dot_terraform_are_not_read(self):
        commented = "".join("# " + line + "\n" for line in CLUSTER_TF_NO_DNS.splitlines())
        self.assertEqual(self._fields(commented)["checked"], [])
        self.assertEqual(self._fields(CLUSTER_TF_NO_DNS,
                                      rel=f"{coverage_gate.UNITS_SUBDIR}/u-1/main.tf")["checked"], [])
        self.assertEqual(self._fields(CLUSTER_TF_NO_DNS, rel=".terraform/x/main.tf")["checked"], [])

    def test_the_gate_carries_the_field_finding(self):
        with tempfile.TemporaryDirectory() as clone:
            _write(clone, "main.tf", CLUSTER_TF_NO_DNS)
            result = coverage_gate.check({"units": []}, {}, clone, rows=ROWS, schema=SCHEMA)
        self.assertEqual([f["row_key"] for f in result["findings"]], [CLUSTER_ROW_KEY])
        self.assertEqual(result["findings"][0]["kind"], ROWS[CLUSTER_ROW_KEY]["kind"])
        self.assertIn("cluster field", result["findings"][0]["error"])
        self.assertEqual(result["cluster_fields"]["checked"], ["main.tf: google_container_cluster.this"])


if __name__ == "__main__":
    unittest.main()
