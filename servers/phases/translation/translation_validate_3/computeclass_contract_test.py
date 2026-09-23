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

"""Unit tests for the compute-class output contract. Pure, no GCS.

Reads the real family table (gke-compute-classes.md): n4 is in the amd64
no-constraint row, which is what the acme-shaped fixture relies on.
"""

import copy
import unittest

from servers.phases.translation.translation_validate_3 import computeclass_contract as cc

CLASS = """\
apiVersion: cloud.google.com/v1
kind: ComputeClass
metadata:
  name: shop-burst
spec:
  priorities:
  - machineFamily: n4
    spot: true
  - machineFamily: n4
  nodePoolAutoCreation:
    enabled: true
  whenUnsatisfiable: DoNotScaleUp
"""

TRADEOFFS = ("The source limit cpu=64 has no ComputeClass field; a CapacityQuota "
             "(GKE 1.36.2+) or cluster NAP resource_limits can cap it.")


def nodepool(**overrides):
    pool = {"name": "shop-burst",
            "capacity_types": ["spot", "on-demand"],
            "instance_families": [],
            "architectures": ["amd64"],
            "requirements_unreduced": [],
            "limits": {"cpu": "64"},
            "taints": []}
    pool.update(overrides)
    return pool


def unit(content=CLASS, kind="compute-class", path="shop-burst.yaml", pool=None,
         choice="computeclass", tradeoffs=TRADEOFFS, open_questions=(), files=None):
    derived = {"karpenter_replacement": {
        "choice": choice,
        "reason": ("node pools are auto-created per ComputeClass" if choice == "computeclass"
                   else "GKE_STANDARD_NAP recorded under Standard")}}
    return {"unit": {"unit_id": "compute-class-shop-burst", "kind": kind,
                     "inputs": {"nodepool": pool or nodepool(),
                                "derived_decisions": derived}},
            "result": {"files": files if files is not None
                       else [{"path": path, "content": content}],
                       "tradeoffs": tradeoffs,
                       "assumptions": [],
                       "open_questions": list(open_questions)}}


def errors_of(entry):
    return cc.check_unit(entry)


def joined(entry):
    return "\n".join(errors_of(entry))


class AcmeShapedUnitTest(unittest.TestCase):

    def test_the_faithful_unit_passes(self):
        self.assertEqual(errors_of(unit()), [])

    def test_check_units_shape(self):
        report = cc.check_units([unit()])
        self.assertEqual(report, {"checked": ["compute-class-shop-burst"], "findings": []})

    def test_no_units_at_all_is_clean(self):
        self.assertEqual(cc.check_units([]), {"checked": [], "findings": []})

    def test_findings_carry_the_unit_id(self):
        report = cc.check_units([unit(choice="nap")])
        self.assertEqual(report["checked"], ["compute-class-shop-burst"])
        self.assertEqual(len(report["findings"]), 1)
        self.assertEqual(report["findings"][0]["unit_id"], "compute-class-shop-burst")


class ShapeCheckTest(unittest.TestCase):
    """Check 1."""

    def test_a_second_document_is_a_finding(self):
        text = joined(unit(CLASS + "---\n" + CLASS.replace("shop-burst", "other")))
        self.assertIn("2 YAML documents", text)

    def test_a_document_of_another_kind_names_the_kind(self):
        text = joined(unit(CLASS + "---\napiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: x\n"))
        self.assertIn("ships a ConfigMap document", text)

    def test_the_wrong_api_version_is_a_finding(self):
        text = joined(unit(CLASS.replace("cloud.google.com/v1", "cloud.google.com/v1beta1")))
        self.assertIn("apiVersion", text)
        self.assertIn("cloud.google.com/v1", text)

    def test_a_namespace_is_a_finding(self):
        text = joined(unit(CLASS.replace("  name: shop-burst\n",
                                         "  name: shop-burst\n  namespace: default\n")))
        self.assertIn("metadata.namespace", text)

    def test_a_renamed_class_is_a_finding(self):
        text = joined(unit(CLASS.replace("name: shop-burst", "name: shop-burst-cc")))
        self.assertIn("metadata.name", text)
        self.assertIn("'shop-burst'", text)

    def test_a_terraform_file_is_a_finding(self):
        files = [{"path": "shop-burst.yaml", "content": CLASS},
                 {"path": "pool.tf", "content": 'resource "google_container_node_pool" "x" {}'}]
        text = joined(unit(files=files))
        self.assertIn("pool.tf", text)
        self.assertIn("YAML only", text)

    def test_no_yaml_at_all_is_a_finding(self):
        text = joined(unit(files=[]))
        self.assertIn("no YAML document", text)

    def test_an_unparseable_file_is_named(self):
        text = joined(unit("a: [oops"))
        self.assertIn("shop-burst.yaml", text)
        self.assertIn("cannot read", text)


class SpecCheckTest(unittest.TestCase):
    """Check 2."""

    def test_empty_priorities_is_a_finding(self):
        content = CLASS[:CLASS.index("  priorities:")] + (
            "  priorities: []\n  nodePoolAutoCreation:\n    enabled: true\n"
            "  whenUnsatisfiable: DoNotScaleUp\n")
        self.assertIn("spec.priorities", joined(unit(content)))

    def test_a_priority_naming_both_is_a_finding(self):
        text = joined(unit(CLASS.replace("  - machineFamily: n4\n    spot: true\n",
                                         "  - machineFamily: n4\n    machineType: n4-standard-4\n"
                                         "    spot: true\n")))
        self.assertIn("machineFamily and machineType", text)

    def test_a_priority_naming_neither_is_a_finding(self):
        text = joined(unit(CLASS.replace("  - machineFamily: n4\n    spot: true\n",
                                         "  - spot: true\n")))
        self.assertIn("neither machineFamily nor machineType", text)

    def test_a_quoted_true_on_node_pool_auto_creation_is_a_finding(self):
        text = joined(unit(CLASS.replace("enabled: true", 'enabled: "true"')))
        self.assertIn("nodePoolAutoCreation.enabled", text)

    def test_a_missing_node_pool_auto_creation_is_a_finding(self):
        text = joined(unit(CLASS.replace("  nodePoolAutoCreation:\n    enabled: true\n", "")))
        self.assertIn("nodePoolAutoCreation.enabled is None", text)

    def test_scale_up_anyway_is_a_finding(self):
        text = joined(unit(CLASS.replace("DoNotScaleUp", "ScaleUpAnyway")))
        self.assertIn("whenUnsatisfiable", text)

    def test_a_partial_priority_score_is_a_finding(self):
        text = joined(unit(CLASS.replace("    spot: true\n", "    spot: true\n    priorityScore: 10\n")))
        self.assertIn("priorityScore is set on 1 of 2", text)

    def test_a_score_shared_by_four_is_a_finding(self):
        four = "".join(f"  - machineFamily: n4\n    priorityScore: 5\n" for _ in range(4))
        content = CLASS.replace("  - machineFamily: n4\n    spot: true\n  - machineFamily: n4\n",
                                "  - machineFamily: n4\n    spot: true\n    priorityScore: 5\n" + four)
        text = joined(unit(content))
        self.assertIn("shared by 5", text)

    def test_a_score_on_every_entry_shared_by_three_passes(self):
        content = CLASS.replace("  - machineFamily: n4\n    spot: true\n  - machineFamily: n4\n",
                                "  - machineFamily: n4\n    spot: true\n    priorityScore: 5\n"
                                "  - machineFamily: n4\n    priorityScore: 5\n"
                                "  - machineFamily: n4\n    priorityScore: 5\n")
        self.assertEqual(errors_of(unit(content)), [])


class SpotCheckTest(unittest.TestCase):
    """Check 3."""

    def test_spot_dropped_when_the_source_allowed_it_is_a_finding(self):
        text = joined(unit(CLASS.replace("    spot: true\n", "")))
        self.assertIn("no priority sets spot: true", text)

    def test_spot_introduced_when_the_source_forbade_it_is_a_finding(self):
        text = joined(unit(pool=nodepool(capacity_types=["on-demand"])))
        self.assertIn("never allowed spot", text)

    def test_no_on_demand_floor_is_a_finding(self):
        text = joined(unit(CLASS.replace("  - machineFamily: n4\n  nodePoolAutoCreation",
                                         "  - machineFamily: n4\n    spot: true\n"
                                         "  nodePoolAutoCreation")))
        self.assertIn("on-demand floor", text)

    def test_spot_only_source_needs_no_floor(self):
        content = CLASS.replace("  - machineFamily: n4\n  nodePoolAutoCreation",
                                "  nodePoolAutoCreation")
        self.assertEqual(errors_of(unit(content, pool=nodepool(capacity_types=["spot"]))), [])

    def test_a_quoted_spot_string_is_not_spot(self):
        text = joined(unit(CLASS.replace("spot: true", 'spot: "true"')))
        self.assertIn("no priority sets spot: true", text)

    def test_an_unreduced_capacity_type_replaces_the_check_with_an_open_question(self):
        pool = nodepool(requirements_unreduced=["karpenter.sh/capacity-type"])
        text = joined(unit(pool=pool))
        self.assertIn("open_questions must name karpenter.sh/capacity-type", text)
        self.assertEqual(errors_of(unit(
            pool=pool, open_questions=["Which karpenter.sh/capacity-type does the client want?"])),
            [])


class FamilyCheckTest(unittest.TestCase):
    """Check 4, against the real family table."""

    def test_a_family_outside_the_no_constraint_row_is_a_finding(self):
        text = joined(unit(CLASS.replace("machineFamily: n4\n  nodePool", "machineFamily: a3\n  nodePool")))
        self.assertIn("machineFamily 'a3'", text)
        self.assertIn("no-constraint row", text)

    def test_a_family_from_the_source_family_row_passes(self):
        content = CLASS.replace("machineFamily: n4", "machineFamily: c4")
        self.assertEqual(errors_of(unit(content, pool=nodepool(instance_families=["c6i"]))), [])

    def test_a_family_from_another_row_is_a_finding(self):
        text = joined(unit(pool=nodepool(instance_families=["c6i"])))
        self.assertIn("machineFamily 'n4'", text)
        self.assertIn("['c6i']", text)

    def test_an_arm_family_for_an_amd64_source_is_a_finding(self):
        text = joined(unit(CLASS.replace("machineFamily: n4", "machineFamily: n4a")))
        self.assertIn("machineFamily 'n4a'", text)

    def test_machine_type_entries_are_checked_by_their_series_prefix(self):
        content = CLASS.replace("  - machineFamily: n4\n  nodePool",
                                "  - machineType: a3-highgpu-1g\n  nodePool")
        text = joined(unit(content))
        self.assertIn("machineType 'a3-highgpu-1g'", text)
        ok = CLASS.replace("  - machineFamily: n4\n  nodePool",
                           "  - machineType: n4-standard-16\n  nodePool")
        self.assertEqual(errors_of(unit(ok)), [])

    def test_one_unknown_family_does_not_lift_the_check_for_the_known_ones(self):
        pool = nodepool(instance_families=["c6i", "i3"])
        text = joined(unit(pool=pool, open_questions=["i3 has no GCE family in the table"]))
        self.assertIn("machineFamily 'n4'", text)   # n4 is not a c6i candidate
        ok = CLASS.replace("machineFamily: n4", "machineFamily: c4")
        self.assertEqual(errors_of(unit(ok, pool=pool, open_questions=["i3 has no GCE family"])), [])

    def test_a_kubernetes_io_taint_key_under_node_pool_config_is_a_finding(self):
        content = CLASS + "  nodePoolConfig:\n    taints:\n    - key: node.kubernetes.io/gpu\n      effect: NoSchedule\n"
        text = joined(unit(content))
        self.assertIn("contains kubernetes.io", text)

    def test_conservation_matches_whole_tokens(self):
        pool = nodepool(limits={"cpu": "1"})
        # "10" and "v1" do not satisfy the limit "1".
        text = joined(unit(pool=pool, tradeoffs="minCores 10 for v1 workloads"))
        self.assertIn("source limit cpu=1", text)
        self.assertEqual(errors_of(unit(pool=pool, tradeoffs="the source limit cpu 1 is dropped")), [])

    def test_an_unknown_source_family_routes_to_an_open_question(self):
        pool = nodepool(instance_families=["i3"])
        text = joined(unit(pool=pool))
        self.assertIn("source family 'i3' is not in the family table", text)
        self.assertNotIn("candidates", text)
        self.assertEqual(errors_of(unit(pool=pool, open_questions=["i3 has no GCE family in the table"])), [])

    def test_a_quoted_priority_score_is_a_finding_and_equal_scores_are_one_score(self):
        content = CLASS.replace("  - machineFamily: n4\n    spot: true\n  - machineFamily: n4\n",
                                "  - machineFamily: n4\n    spot: true\n    priorityScore: \"10\"\n"
                                "  - machineFamily: n4\n    priorityScore: 10\n")
        text = joined(unit(content))
        self.assertIn("priorityScore is an integer field", text)

    def test_an_unreduced_instance_family_replaces_the_check(self):
        pool = nodepool(requirements_unreduced=["karpenter.k8s.aws/instance-family"])
        content = CLASS.replace("machineFamily: n4", "machineFamily: a3")
        text = joined(unit(content, pool=pool))
        self.assertIn("open_questions must name karpenter.k8s.aws/instance-family", text)
        self.assertNotIn("family table", text)
        self.assertEqual(errors_of(unit(
            content, pool=pool,
            open_questions=["karpenter.k8s.aws/instance-family NotIn [...] could not be typed"])),
            [])

    def test_an_unreduced_arch_lifts_only_the_architecture_filter(self):
        pool = nodepool(requirements_unreduced=["kubernetes.io/arch"], instance_families=["c6i"])
        text = joined(unit(pool=pool))
        self.assertIn("open_questions must name kubernetes.io/arch", text)
        # The family row still applies: n4 is not a c6i candidate.
        self.assertIn("machineFamily 'n4'", text)
        arm = CLASS.replace("machineFamily: n4", "machineFamily: c4")
        self.assertEqual(errors_of(unit(arm, pool=pool, open_questions=["kubernetes.io/arch NotIn"])), [])


class ForbiddenCheckTest(unittest.TestCase):
    """Check 5."""

    def test_boot_disk_size_gb_key_is_a_finding(self):
        text = joined(unit(CLASS.replace("    spot: true\n", "    spot: true\n    bootDiskSizeGb: 100\n")))
        self.assertIn("bootDiskSizeGb", text)
        self.assertIn("bootDiskSize", text)

    def test_spec_autopilot_is_a_finding(self):
        text = joined(unit(CLASS + "  autopilot:\n    enabled: true\n"))
        self.assertIn("spec.autopilot", text)

    def test_placeholder_strings_are_findings(self):
        for needle in ("EXAMPLE TEMPLATE", "<zone>", "REPLACE_ME"):
            text = joined(unit(CLASS + f"# {needle}\n"))
            self.assertIn(needle, text)

    def test_a_quoted_integer_field_is_a_finding(self):
        for key in ("bootDiskSize", "minCores", "minMemoryGb"):
            text = joined(unit(CLASS.replace("    spot: true\n", f'    spot: true\n    {key}: "4"\n')))
            self.assertIn(f"{key} is an integer field", text)

    def test_an_unquoted_integer_field_passes(self):
        content = CLASS.replace("    spot: true\n", "    spot: true\n    minCores: 4\n")
        self.assertEqual(errors_of(unit(content)), [])


class ConservationCheckTest(unittest.TestCase):
    """Check 6."""

    def test_a_dropped_limit_is_a_finding(self):
        text = joined(unit(tradeoffs="Nothing to say."))
        self.assertIn("cpu=64", text)

    def test_a_limit_stated_in_assumptions_passes(self):
        entry = unit(tradeoffs="Nothing to say.")
        entry["result"]["assumptions"] = ["The source cpu limit of 64 is capped by NAP."]
        self.assertEqual(errors_of(entry), [])

    def test_a_dropped_taint_key_is_a_finding(self):
        pool = nodepool(taints=[{"key": "acme.io/burst", "value": "true", "effect": "NoSchedule"}])
        text = joined(unit(pool=pool))
        self.assertIn("acme.io/burst", text)

    def test_a_carried_taint_passes(self):
        pool = nodepool(taints=[{"key": "acme.io/burst", "value": "true", "effect": "NoSchedule"}])
        content = CLASS + ("  nodePoolConfig:\n    taints:\n    - key: acme.io/burst\n"
                           "      value: \"true\"\n      effect: NoSchedule\n")
        self.assertEqual(errors_of(unit(content, pool=pool)), [])


class DecisionCheckTest(unittest.TestCase):
    """Check 7."""

    def test_a_nap_decision_is_a_finding_naming_the_value(self):
        text = joined(unit(choice="nap"))
        self.assertIn("karpenter_replacement.choice is 'nap'", text)
        self.assertIn("unskipped at Gate C", text)

    def test_a_missing_registry_is_a_finding(self):
        entry = unit()
        del entry["unit"]["inputs"]["derived_decisions"]
        text = joined(entry)
        self.assertIn("choice is None", text)
        self.assertIn("predates the registry", text)


class ForeignSweepTest(unittest.TestCase):

    def test_a_storage_unit_shipping_a_compute_class_is_a_finding_on_that_unit(self):
        storage = {"unit": {"unit_id": "storage", "kind": "storage"},
                   "result": {"files": [{"path": "sc.yaml", "content": CLASS}]}}
        report = cc.check_units([storage])
        self.assertEqual(report["checked"], [])
        self.assertEqual(len(report["findings"]), 1)
        self.assertEqual(report["findings"][0]["unit_id"], "storage")
        self.assertIn("ComputeClass shop-burst", report["findings"][0]["error"])

    def test_a_storage_unit_without_one_is_clean(self):
        storage = {"unit": {"unit_id": "storage", "kind": "storage"},
                   "result": {"files": [{"path": "sc.yaml",
                                         "content": "apiVersion: storage.k8s.io/v1\n"
                                                    "kind: StorageClass\nmetadata:\n  name: x\n"}]}}
        self.assertEqual(cc.check_units([storage]), {"checked": [], "findings": []})

    def test_a_legacy_blob_without_a_kind_is_swept_not_crashed_on(self):
        legacy = {"unit": {"unit_id": "old"}, "result": {"files": [{"path": "a.yaml", "content": CLASS}]}}
        report = cc.check_units([legacy, {}, None])
        self.assertEqual(report["checked"], [])
        self.assertEqual([f["unit_id"] for f in report["findings"]], ["old"])

    def test_foreign_errors_are_direct(self):
        self.assertEqual(cc.foreign_computeclass_errors(
            {"unit": {"kind": "storage"}, "result": {"files": [{"path": "x.yaml", "content": "a: [oops"}]}}),
            [])


if __name__ == "__main__":
    unittest.main()
