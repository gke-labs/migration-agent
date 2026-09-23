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

"""Unit tests for the pure v1 coverage instantiation and checks. No GCS, no LLM."""

import os
import unittest

from servers.dag.server import coverage_map
from servers.phases.landingzone.landingzone_translationplan_3 import coverage

_KNOWLEDGE_DOC = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), "..", "knowledge", "coverage-map.md"
)


def _real_rows():
    with open(_KNOWLEDGE_DOC, "r", encoding="utf-8") as f:
        return coverage_map.parse_coverage_map(f.read(), "coverage-map.md")


def _row(kind, owner, source, column="terraform"):
    return {"kind": kind, "column": column, "owner": owner, "source": source, "notes": ""}


# A synthetic map exercising every verdict, shaped like parse_coverage_map output.
ROWS = {
    "alpha": _row("Alpha", "platform-translation", "`addons`"),
    "beta": _row("Beta", "platform-translation", "—"),
    "gamma": _row("Gamma", "platform-translation", "`no_such_section`"),
    "delta": _row("Delta", "platform-translation", "prose without a backticked path"),
    "epsilon": _row("Epsilon", "platform-translation", "`addons`, `no_such_section`"),
    "zeta": _row("Zeta", "platform-translation", "`autoscaling`, `triggers.karpenter`"),
    "eta": _row("Eta", "landing-zone", "`network` (base)"),
    "theta": _row("Theta", "workload", "`workloads.irsa_bindings`", column="k8s"),
}

SCHEMA = {
    "properties": {
        "addons": {"type": "array"},
        "autoscaling": {"properties": {"karpenter": {"type": "boolean"}}},
        "triggers": {"properties": {"karpenter": {"type": "boolean"}}},
        "network": {"properties": {"vpc_peering": {"type": "boolean"}}},
        "workloads": {"properties": {"irsa_bindings": {"type": "array"}}},
    }
}

INVENTORY = {
    "addons": [{"name": "aws-ebs-csi-driver"}],
    "autoscaling": {"karpenter": False, "evidence": []},
    "triggers": {"karpenter": False},
    "network": {"vpc_peering": True},
    "workloads": {"irsa_bindings": []},
}


def _verdicts(inventory=INVENTORY):
    return coverage.instantiate_coverage_map(ROWS, inventory, SCHEMA)


def _by_key(verdicts):
    return {v["row_key"]: v for v in verdicts}


def _unit(unit_id, kind, covers, status="planned"):
    return {"unit_id": unit_id, "kind": kind, "status": status, "covers": covers}


class ResolverTest(unittest.TestCase):

    def test_em_dash_and_empty_mean_not_discovery_sourced(self):
        self.assertIsNone(coverage.resolve_discovered_from("—"))
        self.assertIsNone(coverage.resolve_discovered_from(""))
        self.assertIsNone(coverage.resolve_discovered_from("  — "))

    def test_backticked_paths_extracted_in_order_annotation_ignored(self):
        self.assertEqual(
            coverage.resolve_discovered_from("`autoscaling`, `triggers.karpenter`"),
            ["autoscaling", "triggers.karpenter"],
        )
        self.assertEqual(coverage.resolve_discovered_from("`network` (ingress hosts)"), ["network"])
        self.assertEqual(
            coverage.resolve_discovered_from("`triggers`, landing-zone decisions"), ["triggers"]
        )

    def test_prose_only_cell_resolves_to_no_paths(self):
        self.assertEqual(coverage.resolve_discovered_from("landing-zone decisions"), [])
        # An ASCII hyphen is not the map's "—" convention: unresolvable, not not-scanned.
        self.assertEqual(coverage.resolve_discovered_from("-"), [])


class SectionKnownTest(unittest.TestCase):

    def test_declared_paths_are_known(self):
        self.assertTrue(coverage.section_known(SCHEMA, "addons"))
        self.assertTrue(coverage.section_known(SCHEMA, "workloads.irsa_bindings"))
        self.assertTrue(coverage.section_known(SCHEMA, "triggers.karpenter"))

    def test_undeclared_paths_are_unknown(self):
        self.assertFalse(coverage.section_known(SCHEMA, "no_such_section"))
        self.assertFalse(coverage.section_known(SCHEMA, "workloads.namespaces"))
        self.assertFalse(coverage.section_known(SCHEMA, "addons.name.deeper"))

    def test_real_schema_declares_the_paths_the_map_uses(self):
        schema = coverage.load_inventory_schema()
        self.assertTrue(coverage.section_known(schema, "storage"))  # oneOf section
        self.assertTrue(coverage.section_known(schema, "workloads.irsa_bindings"))
        self.assertTrue(coverage.section_known(schema, "triggers.karpenter"))
        self.assertTrue(coverage.section_known(schema, "images"))
        # namespaces live under clusters[].workloads, not the top-level section.
        self.assertTrue(coverage.section_known(schema, "clusters[].workloads.namespaces"))
        self.assertFalse(coverage.section_known(schema, "workloads.namespaces"))


class ArrayPathTest(unittest.TestCase):
    """The `[]` grammar: `clusters[].workloads.namespaces` walks array items."""

    SCHEMA = {
        "properties": {
            "clusters": {
                "type": "array",
                "items": {
                    "properties": {
                        "name": {"type": "string"},
                        "workloads": {
                            "properties": {"namespaces": {"type": "array"}}
                        },
                    }
                },
            },
            "addons": {"type": "array"},  # no items schema declared
        }
    }
    ROWS = {
        "iota": {"kind": "Iota", "column": "k8s", "owner": "platform-translation",
                 "source": "`clusters[].workloads.namespaces`", "notes": ""},
    }

    def test_section_known_steps_into_array_items(self):
        self.assertTrue(coverage.section_known(self.SCHEMA, "clusters[].workloads.namespaces"))
        self.assertTrue(coverage.section_known(self.SCHEMA, "clusters[].name"))
        # [] over an array with no items schema, over a non-declared name,
        # or bare, is unknown — never a silent pass.
        self.assertFalse(coverage.section_known(self.SCHEMA, "addons[].name"))
        self.assertFalse(coverage.section_known(self.SCHEMA, "nodegroups[].name"))
        self.assertFalse(coverage.section_known(self.SCHEMA, "[]"))
        self.assertFalse(coverage.section_known(self.SCHEMA, "clusters[].no_such"))

    def test_facts_present_when_any_cluster_holds_them(self):
        # Real-shaped inventory: clusters is a list, facts live per cluster.
        inventory = {"clusters": [
            {"name": "empty-cluster", "workloads": {"namespaces": []}},
            {"name": "acme-eks", "workloads": {"namespaces": [
                {"name": "acme-shop", "deployments": 3}]}},
        ]}
        verdicts = coverage.instantiate_coverage_map(self.ROWS, inventory, self.SCHEMA)
        self.assertEqual(verdicts[0]["status"], "facts-present")
        self.assertEqual(verdicts[0]["sections"], [
            {"path": "clusters[].workloads.namespaces", "known": True, "facts": True}])

    def test_no_facts_when_clusters_are_empty_absent_or_bare(self):
        for inventory in (
            {},                                            # never scanned
            {"clusters": []},                              # scanned, none found
            {"clusters": [{"name": "bare"}]},              # cluster without workloads
            {"clusters": [{"workloads": {"namespaces": []}}]},  # empty namespaces
            {"clusters": "not-a-list"},                    # shape defect reads as absence
        ):
            verdicts = coverage.instantiate_coverage_map(self.ROWS, inventory, self.SCHEMA)
            self.assertEqual(verdicts[0]["status"], "no-facts", repr(inventory))


class HoldsFactsTest(unittest.TestCase):

    def test_absences(self):
        for value in (None, False, {}, [], "", "   ",
                      {"karpenter": False, "evidence": []}, [None, ""], {"a": {"b": []}}):
            self.assertFalse(coverage.holds_facts(value), repr(value))

    def test_facts(self):
        for value in (True, 0, 1, 0.0, "x", ["x"], [{"name": "x"}],
                      {"evidence": ["pvc.yaml requests gp3"]}, {"deployments": 0}):
            self.assertTrue(coverage.holds_facts(value), repr(value))


class InstantiateTest(unittest.TestCase):

    def test_statuses_per_row(self):
        by_key = _by_key(_verdicts())
        self.assertEqual(by_key["alpha"]["status"], "facts-present")
        self.assertEqual(by_key["beta"]["status"], "not-scanned")
        self.assertEqual(by_key["gamma"]["status"], "unknown-section")
        self.assertEqual(by_key["delta"]["status"], "unknown-section")
        self.assertEqual(by_key["zeta"]["status"], "no-facts")  # all-false flags are absences
        self.assertEqual(by_key["eta"]["status"], "facts-present")
        self.assertEqual(by_key["theta"]["status"], "no-facts")  # empty list

    def test_unknown_section_outranks_sibling_facts(self):
        epsilon = _by_key(_verdicts())["epsilon"]
        self.assertEqual(epsilon["status"], "unknown-section")
        # The healthy sibling stays visible in the per-section detail.
        self.assertEqual(
            epsilon["sections"],
            [{"path": "addons", "known": True, "facts": True},
             {"path": "no_such_section", "known": False, "facts": False}],
        )

    def test_prose_cell_reported_with_its_text(self):
        delta = _by_key(_verdicts())["delta"]
        self.assertEqual(delta["sections"],
                         [{"path": "prose without a backticked path", "known": False, "facts": False}])

    def test_verdicts_keep_table_order_and_row_identity(self):
        verdicts = _verdicts()
        self.assertEqual([v["row_key"] for v in verdicts], list(ROWS))
        alpha = verdicts[0]
        self.assertEqual((alpha["kind"], alpha["owner"], alpha["column"]),
                         ("Alpha", "platform-translation", "terraform"))

    def test_missing_sections_are_no_facts_not_unknown(self):
        # An inventory that simply lacks the section: the schema still declares
        # it, so the verdict is an absence of facts, not a map defect.
        by_key = _by_key(_verdicts(inventory={}))
        self.assertEqual(by_key["alpha"]["status"], "no-facts")
        self.assertEqual(by_key["gamma"]["status"], "unknown-section")

    def test_deterministic(self):
        self.assertEqual(_verdicts(), _verdicts())

    def test_real_map_resolves_cleanly_against_real_schema(self):
        # Binds the document to the schema: every backticked Discovered-from
        # path in the shipped map must be schema-declared, and the "—" rows
        # must be exactly the elicitation/standing-module ones.
        rows = _real_rows()
        schema = coverage.load_inventory_schema()
        verdicts = coverage.instantiate_coverage_map(rows, {}, schema)
        self.assertFalse([v for v in verdicts if v["status"] == "unknown-section"])
        not_scanned = {v["row_key"] for v in verdicts if v["status"] == "not-scanned"}
        self.assertEqual(not_scanned, {
            "organization hierarchy and org policies",
            "monitoring baseline",
            "priority class scheme",
            "namespace rbac (roles, bindings)",
            "networkpolicy (namespace isolation)",
            # Its manifests come from the source repository, not the scan.
            "workload manifests (deployments, services, config)",
        })


class ChecksTest(unittest.TestCase):

    def test_omission_fires_only_for_uncovered_facts_present_platform_rows(self):
        verdicts = _verdicts()  # alpha facts-present; eta facts-present but landing-zone
        plan = {"units": [_unit("u-zeta", "zeta-kind", ["zeta"])]}
        checks = coverage.run_coverage_checks(plan, verdicts)
        self.assertEqual(checks["omission"], ["alpha"])

    def test_placeholder_coverage_counts_against_omission(self):
        # A skipped no-facts placeholder is still the family's coverage claim:
        # omission means "no family at all", not "family currently inactive".
        verdicts = _verdicts()
        plan = {"units": [_unit("u-alpha", "alpha-kind", ["alpha"], status="skipped")]}
        self.assertEqual(coverage.run_coverage_checks(plan, verdicts)["omission"], [])

    def test_overlap_fires_for_two_active_families_on_one_row(self):
        verdicts = _verdicts()
        plan = {"units": [
            _unit("u-1", "kind-a", ["alpha"]),
            _unit("u-2", "kind-b", ["alpha"]),
        ]}
        overlap = coverage.run_coverage_checks(plan, verdicts)["overlap"]
        self.assertEqual(overlap, [{
            "row_key": "alpha",
            "kinds": ["kind-a", "kind-b"],
            "unit_ids": ["u-1", "u-2"],
        }])

    def test_overlap_ignores_same_family_fanout_and_skipped_units(self):
        verdicts = _verdicts()
        fanout = {"units": [
            _unit("u-1", "kind-a", ["alpha"]),
            _unit("u-2", "kind-a", ["alpha"]),
        ]}
        self.assertEqual(coverage.run_coverage_checks(fanout, verdicts)["overlap"], [])
        one_skipped = {"units": [
            _unit("u-1", "kind-a", ["alpha"]),
            _unit("u-2", "kind-b", ["alpha"], status="skipped"),
        ]}
        self.assertEqual(coverage.run_coverage_checks(one_skipped, verdicts)["overlap"], [])

    def test_traceability_names_uncited_units_and_unknown_citations(self):
        verdicts = _verdicts()
        plan = {"units": [
            _unit("u-mute", "kind-a", []),
            _unit("u-lost", "kind-b", ["no-such-row"]),
        ]}
        checks = coverage.run_coverage_checks(plan, verdicts)
        self.assertEqual(checks["traceability"], {
            "units_without_covers": ["u-mute"],
            "unknown_citations": [{"unit_id": "u-lost", "row_key": "no-such-row"}],
        })
        # An unknown citation covers nothing: alpha stays omitted.
        self.assertIn("alpha", checks["omission"])

    def test_unknown_section_rows_reported_as_map_defects(self):
        # An unknown-section row can never reach facts-present, so it is
        # exempt from omission by construction; without this check a typo'd
        # Discovered-from path would read as clean at the sign-off.
        checks = coverage.run_coverage_checks({"units": []}, _verdicts())
        self.assertEqual(checks["unknown_sections"], ["gamma", "delta", "epsilon"])
        self.assertNotIn("gamma", checks["omission"])
        lines = coverage.findings(checks)
        self.assertTrue(any("unknown inventory sections" in line and "gamma" in line
                            for line in lines), lines)

    def test_out_of_scope_reports_the_unbuilt_half_of_the_boundary(self):
        checks = coverage.run_coverage_checks({"units": []}, _verdicts())
        self.assertEqual(checks["out_of_scope"], {
            "landing-zone": [{"row_key": "eta", "status": "facts-present"}],
            "workload": [{"row_key": "theta", "status": "no-facts"}],
        })

    def test_checks_deterministic(self):
        verdicts = _verdicts()
        plan = {"units": [_unit("u-1", "kind-a", ["alpha"]), _unit("u-2", "kind-b", ["alpha"])]}
        self.assertEqual(coverage.run_coverage_checks(plan, verdicts),
                         coverage.run_coverage_checks(plan, verdicts))


class AttachAndRefreshTest(unittest.TestCase):

    def test_attach_coverage_adds_the_key_beside_units(self):
        plan = {"units": [_unit("u-1", "kind-a", ["alpha"])], "decisions": {}}
        attached = coverage.attach_coverage(plan, INVENTORY, rows=ROWS, schema=SCHEMA)
        self.assertNotIn("coverage", plan)  # pure: input not mutated
        self.assertEqual(attached["units"], plan["units"])
        self.assertEqual(attached["coverage"]["granularity"], "section")
        self.assertEqual(len(attached["coverage"]["map"]), len(ROWS))
        self.assertEqual(attached["coverage"]["checks"]["omission"], [])

    def test_attach_coverage_defaults_to_the_shipped_map_and_schema(self):
        attached = coverage.attach_coverage({"units": []}, {})
        keys = {v["row_key"] for v in attached["coverage"]["map"]}
        self.assertIn("gke node pools", keys)
        self.assertIn("storageclass tier menu", keys)

    def test_refresh_checks_recomputes_after_a_review_skip(self):
        plan = {"units": [
            _unit("u-1", "kind-a", ["alpha"]),
            _unit("u-2", "kind-b", ["alpha"]),
        ]}
        attached = coverage.attach_coverage(plan, INVENTORY, rows=ROWS, schema=SCHEMA)
        self.assertEqual(len(attached["coverage"]["checks"]["overlap"]), 1)
        attached["units"] = [
            attached["units"][0],
            {**attached["units"][1], "status": "skipped"},
        ]
        refreshed = coverage.refresh_checks(attached)
        self.assertEqual(refreshed["coverage"]["checks"]["overlap"], [])
        self.assertEqual(refreshed["coverage"]["map"], attached["coverage"]["map"])

    def test_refresh_checks_passes_a_pre_coverage_plan_through(self):
        plan = {"units": [_unit("u-1", "kind-a", ["alpha"])]}
        self.assertEqual(coverage.refresh_checks(plan), plan)


class FindingsTest(unittest.TestCase):

    def test_clean_checks_yield_no_findings(self):
        # A map without defects (no unknown-section rows), every facts-present
        # platform row covered: nothing to report.
        clean_rows = {k: ROWS[k] for k in ("alpha", "beta", "zeta", "eta", "theta")}
        verdicts = coverage.instantiate_coverage_map(clean_rows, INVENTORY, SCHEMA)
        checks = coverage.run_coverage_checks(
            {"units": [_unit("u-alpha", "kind-a", ["alpha"])]}, verdicts)
        self.assertEqual(coverage.findings(checks), [])

    def test_each_check_contributes_one_named_line(self):
        checks = {
            "omission": ["alpha"],
            "overlap": [{"row_key": "zeta", "kinds": ["a", "b"], "unit_ids": ["u-1", "u-2"]}],
            "traceability": {
                "units_without_covers": ["u-mute"],
                "unknown_citations": [{"unit_id": "u-lost", "row_key": "no-such-row"}],
            },
            "unknown_sections": ["gamma"],
            "out_of_scope": {"landing-zone": [], "workload": []},
        }
        lines = coverage.findings(checks)
        self.assertEqual(len(lines), 5)
        joined = "\n".join(lines)
        self.assertIn("omitted rows", joined)
        self.assertIn("alpha", joined)
        self.assertIn("overlapping rows", joined)
        self.assertIn("zeta", joined)
        self.assertIn("u-mute", joined)
        self.assertIn("u-lost -> no-such-row", joined)
        self.assertIn("unknown inventory sections", joined)
        self.assertIn("gamma", joined)


if __name__ == "__main__":
    unittest.main()
