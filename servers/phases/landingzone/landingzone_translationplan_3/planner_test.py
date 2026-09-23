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

"""Unit tests for the pure translation-plan decomposition. No GCS, no LLM."""

import json
import os
import unittest

from servers.dag.server import coverage_map
from servers.dag.server import exports as exports_lib
from servers.phases.landingzone.landingzone_translationplan_3 import coverage
from servers.phases.landingzone.landingzone_translationplan_3 import planner

_KNOWLEDGE_DOC = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), "..", "knowledge", "coverage-map.md"
)


def _real_map_rows():
    with open(_KNOWLEDGE_DOC, "r", encoding="utf-8") as f:
        return coverage_map.parse_coverage_map(f.read(), "coverage-map.md")

# Shaped like the acme-eks-estate e2e inventory: every unit family has facts.
FULL_INVENTORY = {
    "clusters": [
        {"name": "acme-prod", "workloads": {"namespaces": [
            {"name": "acme-shop", "deployments": 3},
        ]}},
    ],
    "addons": [
        {"name": "karpenter", "version": "0.36.0"},
        {"name": "aws-ebs-csi-driver"},
    ],
    "autoscaling": {"karpenter": True, "cluster_autoscaler": False,
                    "karpenter_nodepools": [SHOP_BURST := {
                        "name": "shop-burst", "kind": "NodePool",
                        "api_version": "karpenter.sh/v1beta1",
                        "requirements": [
                            {"key": "kubernetes.io/arch", "operator": "In", "values": ["amd64"]},
                            {"key": "karpenter.sh/capacity-type", "operator": "In",
                             "values": ["spot", "on-demand"]}],
                        "capacity_types": ["spot", "on-demand"], "instance_families": [],
                        "architectures": ["amd64"], "requirements_unreduced": [],
                        "weight": None, "taints": [], "labels": {}, "limits": {"cpu": "64"},
                        "disruption": {"consolidation_policy": "WhenUnderutilized"},
                        "node_class_ref": {"name": "default"},
                        "source_files": ["k8s/karpenter-nodepool.yaml"]}]},
    "nodegroups": [{"name": "system", "instance_types": ["m5.large"]}],
    "workloads": {
        "privileged_daemonsets": [{"name": "node-agent", "namespace": "acme-shop"}],
        "host_network": ["acme-shop/node-agent"],
        "irsa_bindings": ["acme-shop/orders"],
    },
    "network": {"vpc_peering": True, "load_balancers": ["frontend-alb"]},
    "storage": {"ebs_csi": True, "efs_csi": False, "storage_classes": ["gp3-encrypted"]},
    "cluster_dns": {"sources": [
        {"kind": "configmap", "name": "coredns", "address": None, "path": "k8s/coredns.yaml",
         "directory": "k8s", "form": "manifest", "addon_version": None,
         "resolve_conflicts_on_update": None,
         "text": "corp.example.com:53 {\n    forward . 10.1.2.3\n}\n",
         "evidence": ["k8s/coredns.yaml (document 1)"]}]},
    "triggers": {"karpenter": True, "privileged_daemonsets": True, "vpc_peering": True},
}

DECISIONS = {
    "karpenter": "GKE_STANDARD_NAP",
    "privileged_daemonsets": "GKE_STANDARD",
    "gpu_tpu": "GKE_STANDARD_SPECIALIZED",
    "vpc_peering": "PUBLIC_AUTHORIZED_NETS",
}

ALL_FAMILY_PLACEHOLDER_IDS = {
    "cluster-addons",
    "autoscaling-karpenter",
    "node-pools",
    "workload-policies",
    "host-network-workloads",
    "workload-identity",
    "network",
    "gateway",
    "storage",
    "tenancy",
    "cluster-dns",
    "compute-classes",
}


def _by_id(plan):
    return {unit["unit_id"]: unit for unit in plan["units"]}


class BuildTranslationPlanTest(unittest.TestCase):

    def test_full_inventory_plans_every_family(self):
        plan = planner.build_translation_plan(FULL_INVENTORY, DECISIONS)
        by_id = _by_id(plan)
        self.assertEqual(
            set(by_id),
            {
                "cluster-addons",
                "autoscaling-karpenter",
                "node-pool-system",
                "workload-policy-node-agent",
                "host-network-workloads",
                "workload-identity",
                "network",
                "gateway",
                "storage",
                "tenancy",
                "cluster-dns",
                "compute-class-shop-burst",
            },
        )
        for unit in plan["units"]:
            if unit["unit_id"] == "compute-class-shop-burst":
                # A one-arm construct: under the NAP choice the class is a
                # non-placeholder skip (facts kept), like node pools under
                # Autopilot. ComputeClassFamilyTest covers the other arms.
                self.assertEqual(unit["status"], "skipped")
                self.assertFalse(unit["placeholder"])
                continue
            self.assertEqual(unit["status"], "planned", unit["unit_id"])

    def test_empty_inventory_emits_skipped_placeholders_not_silence(self):
        plan = planner.build_translation_plan({}, {})
        by_id = _by_id(plan)
        self.assertEqual(set(by_id), ALL_FAMILY_PLACEHOLDER_IDS)
        for unit in plan["units"]:
            self.assertEqual(unit["status"], "skipped", unit["unit_id"])
            self.assertTrue(
                any(note.startswith("Skipped by default") for note in unit["notes"]),
                f"{unit['unit_id']} lacks a skip reason: {unit['notes']}",
            )

    def test_skip_reasons_name_the_empty_inventory_section(self):
        plan = planner.build_translation_plan({}, {})
        by_id = _by_id(plan)
        expected_sections = {
            "cluster-addons": "inventory.addons",
            "node-pools": "inventory.nodegroups",
            "workload-policies": "inventory.workloads.privileged_daemonsets",
            "host-network-workloads": "inventory.workloads.host_network",
            "workload-identity": "inventory.workloads.irsa_bindings",
            "gateway": "inventory.network",
            "storage": "inventory.storage",
            "tenancy": "inventory.clusters[].workloads.namespaces",
            "cluster-dns": "inventory.cluster_dns",
        }
        for unit_id, section in expected_sections.items():
            self.assertIn(section, by_id[unit_id]["notes"][0], unit_id)

    def test_storage_gap_with_csi_addon_evidence_gets_warning(self):
        # The acme e2e defect: addons prove a StorageClass exists, but the
        # structured storage section came back empty. The placeholder must
        # escalate the contradiction, not just record the absence.
        inventory = {
            "addons": [{"name": "karpenter"}, {"name": "aws-ebs-csi-driver"}],
            "storage": {"ebs_csi": False, "efs_csi": False, "storage_classes": []},
        }
        plan = planner.build_translation_plan(inventory, {})
        storage = _by_id(plan)["storage"]
        self.assertEqual(storage["status"], "skipped")
        warnings = [note for note in storage["notes"] if note.startswith("WARNING")]
        self.assertEqual(len(warnings), 1)
        self.assertIn("aws-ebs-csi-driver", warnings[0])
        self.assertNotIn("karpenter", warnings[0])

    def test_storage_with_facts_is_planned_without_warning(self):
        plan = planner.build_translation_plan(FULL_INVENTORY, DECISIONS)
        storage = _by_id(plan)["storage"]
        self.assertEqual(storage["status"], "planned")
        self.assertFalse([note for note in storage["notes"] if note.startswith("WARNING")])

    def test_storage_briefs_one_manifest_per_discovered_class(self):
        # ebs_csi + one discovered name: the notes are the worker's whole
        # StorageClass brief — .yaml manifests per source name, the PD CSI
        # mapping with parameter hints, and the standing conventions.
        plan = planner.build_translation_plan(FULL_INVENTORY, DECISIONS)
        storage = _by_id(plan)["storage"]
        self.assertEqual(storage["status"], "planned")
        notes = storage["notes"]
        self.assertIn("one per discovered StorageClass name", notes[0])
        self.assertIn("gp3-encrypted", notes[0])
        self.assertIn("(.yaml)", notes[0])
        joined = "\n".join(notes)
        self.assertIn("pd.csi.storage.gke.io", joined)
        self.assertIn("type pd-balanced", joined)
        # No EFS facts in FULL_INVENTORY, so no Filestore brief.
        self.assertNotIn("filestore.csi.storage.gke.io", joined)
        self.assertIn("volumeBindingMode: WaitForFirstConsumer", joined)
        self.assertIn("reclaimPolicy", joined)
        self.assertIn("is-default-class", joined)
        self.assertIn("open question", joined)
        # The whole fact slice still rides in inputs, unreduced.
        self.assertEqual(storage["inputs"], {"storage": FULL_INVENTORY["storage"]})

    def test_storage_without_class_names_briefs_a_tier_menu(self):
        # CSI facts but no names: no per-name manifests to order, so the brief
        # asks for a small GKE tier menu instead — and says truthfully that
        # storage_classes named nothing.
        inventory = {"storage": {"ebs_csi": True, "efs_csi": False, "storage_classes": []}}
        plan = planner.build_translation_plan(inventory, {})
        storage = _by_id(plan)["storage"]
        self.assertEqual(storage["status"], "planned")
        self.assertIn("names no StorageClasses", storage["notes"][0])
        self.assertIn("GKE tier menu", storage["notes"][0])
        self.assertIn("pd-balanced", storage["notes"][0])
        self.assertNotIn("Filestore", storage["notes"][0])

    def test_storage_efs_facts_brief_filestore(self):
        inventory = {"storage": {"ebs_csi": False, "efs_csi": True, "storage_classes": []}}
        plan = planner.build_translation_plan(inventory, {})
        notes = _by_id(plan)["storage"]["notes"]
        self.assertIn("Filestore class", notes[0])
        joined = "\n".join(notes)
        self.assertIn("filestore.csi.storage.gke.io", joined)
        self.assertIn("open_questions", joined)
        # No EBS facts: the EBS mapping note must not appear (the not-ebs
        # note's translate-never-copy guidance may still name the driver).
        self.assertNotIn("ebs.csi.aws.com maps", joined)
        # The menu orders PD classes, so the brief must still supply the PD
        # provisioner (as a stated choice, not an evidence-backed mapping) —
        # otherwise "invent nothing" forbids the one value the manifests
        # cannot omit.
        self.assertIn("pd.csi.storage.gke.io", joined)
        self.assertIn("assumptions", joined)

    def test_storage_efs_facts_with_names_do_not_steer_to_filestore(self):
        # Reachable: extraction lists every StorageClass name while setting
        # only the CSI flags it saw evidence for. The only evidence-backed
        # mapping is Filestore, but a named (likely PD-shaped) class must
        # still get PD provisioner guidance as a stated choice.
        inventory = {"storage": {"efs_csi": True, "storage_classes": ["gp3-encrypted"]}}
        plan = planner.build_translation_plan(inventory, {})
        joined = "\n".join(_by_id(plan)["storage"]["notes"])
        self.assertIn("pd.csi.storage.gke.io", joined)
        self.assertIn("filestore.csi.storage.gke.io", joined)
        self.assertIn("assumption", joined)

    def test_storage_names_without_csi_facts_route_provisioner_to_assumption(self):
        # Names recovered without driver evidence (both flags falsy): the
        # provisioner is a choice, and the brief must say so instead of
        # asserting a mapping from facts the inventory does not hold.
        inventory = {"storage": {"storage_classes": ["data-tier"]}}
        plan = planner.build_translation_plan(inventory, {})
        storage = _by_id(plan)["storage"]
        self.assertEqual(storage["status"], "planned")
        self.assertIn("data-tier", storage["notes"][0])
        joined = "\n".join(storage["notes"])
        self.assertIn("does not record ebs_csi as true", joined)
        self.assertIn("assumptions", joined)
        self.assertNotIn("ebs.csi.aws.com maps", joined)
        self.assertNotIn("efs.csi.aws.com maps", joined)

    def test_storage_provisioner_evidence_translates_never_copies(self):
        # A worker can misfile the driver as an extra provisioner key without
        # setting the flag (the merger preserves it; _derive_storage repairs
        # flags from addons only). The brief must tell the worker to translate
        # that evidence, not copy an AWS provisioner into a GKE manifest.
        inventory = {"storage": {"storage_classes": ["gp3-encrypted"],
                                 "provisioner": "ebs.csi.aws.com"}}
        plan = planner.build_translation_plan(inventory, {})
        joined = "\n".join(_by_id(plan)["storage"]["notes"])
        self.assertIn("translate it", joined)
        self.assertIn("never copy an AWS provisioner", joined)
        self.assertIn("ebs.csi.aws.com -> pd.csi.storage.gke.io", joined)

    def test_workload_identity_brief_carries_the_output_contract(self):
        plan = planner.build_translation_plan(FULL_INVENTORY, DECISIONS)
        notes = " ".join(_by_id(plan)["workload-identity"]["notes"])
        self.assertIn('output named "ksa_annotations"', notes)
        self.assertIn("Literal quoted pairs only", notes)
        self.assertIn("machine-checked by the validate step", notes)
        # The 2026-08-14 e2e lesson: the brief must forbid placeholder emails
        # and give the undecided-project case an honest escape.
        self.assertIn("placeholder", notes)
        self.assertIn("value = {}", notes)
        # The escape must be described as loud, not as a silent pass: an
        # empty map over recorded bindings is a finding routed to review.
        self.assertIn("raises a validation finding", notes)
        self.assertIn("never ships silently", notes)
        # Every FULL_INVENTORY binding is a well-formed "namespace/sa" string,
        # so the malformed-entries note must not appear.
        self.assertNotIn("do not parse", notes)

    def test_workload_identity_brief_pins_a_recorded_target_project(self):
        """The data half of the trust pair: with the ledger's gcp_project
        threaded into inputs.target_project, the brief pins the literal
        project id and demotes the empty-map escape — a rule-following
        worker can finally emit a real email."""
        plan = planner.build_translation_plan(
            FULL_INVENTORY, DECISIONS, target_project="my-target-proj")
        unit = _by_id(plan)["workload-identity"]
        self.assertEqual(unit["inputs"]["target_project"], "my-target-proj")
        notes = " ".join(unit["notes"])
        self.assertIn("'my-target-proj'", notes)
        self.assertIn("transcription, not", notes)
        self.assertIn("Do NOT use the empty-map escape", notes)
        self.assertNotIn("emit value = {}", notes)

    def test_workload_identity_inputs_record_a_null_project_when_absent(self):
        plan = planner.build_translation_plan(FULL_INVENTORY, DECISIONS)
        unit = _by_id(plan)["workload-identity"]
        self.assertIsNone(unit["inputs"]["target_project"])
        self.assertIn("inputs.target_project is null",
                      " ".join(unit["notes"]))

    # The straddle (2026-08-15): the unit sheds the KSA convenience resource
    # and keeps the ksa_annotations output. Table-driven both ways — the
    # boundary must be stated, and no phrase may license the resource again.
    KSA_BOUNDARY_REQUIRED = (
        "Do not emit the KSA itself",
        "iam.gke.io/gcp-service-account",
        "not as a kubernetes-provider resource",
        "not behind a default-off flag or variable",
        "wkld-identity",
        '"KSA and Workload Identity annotation"',
    )
    # Substrings that appear only in an instruction to CREATE the KSA: the
    # Terraform resource type names and the provider a kubernetes resource
    # needs. The prohibition above names none of them.
    KSA_BOUNDARY_FORBIDDEN = (
        "kubernetes_service_account",
        "kubernetes_manifest",
        'provider "kubernetes"',
    )

    def test_workload_identity_brief_sheds_the_ksa_resource(self):
        plan = planner.build_translation_plan(FULL_INVENTORY, DECISIONS)
        notes = " ".join(_by_id(plan)["workload-identity"]["notes"])
        for phrase in self.KSA_BOUNDARY_REQUIRED:
            with self.subTest(required=phrase):
                self.assertIn(phrase, notes)
        for phrase in self.KSA_BOUNDARY_FORBIDDEN:
            with self.subTest(forbidden=phrase):
                self.assertNotIn(phrase, notes)
        # Shedding the resource must not shed the output contract with it.
        self.assertIn('output named "ksa_annotations"', notes)

    def test_workload_identity_brief_names_unparseable_binding_entries(self):
        inventory = dict(FULL_INVENTORY, workloads={
            **FULL_INVENTORY["workloads"],
            "irsa_bindings": ["acme-shop/orders", "roleonly", "a/b/c"],
        })
        plan = planner.build_translation_plan(inventory, DECISIONS)
        notes = " ".join(_by_id(plan)["workload-identity"]["notes"])
        self.assertIn("do not parse", notes)
        self.assertIn("roleonly", notes)
        self.assertIn("a/b/c", notes)

    def test_workload_identity_placeholder_carries_no_contract_brief(self):
        plan = planner.build_translation_plan({}, DECISIONS)
        unit = _by_id(plan)["workload-identity"]
        self.assertEqual(unit["status"], "skipped")
        self.assertNotIn("ksa_annotations", " ".join(unit["notes"]))

    def test_no_placeholders_when_facts_exist(self):
        plan = planner.build_translation_plan(FULL_INVENTORY, DECISIONS)
        by_id = _by_id(plan)
        self.assertNotIn("node-pools", by_id)
        self.assertNotIn("workload-policies", by_id)

    def test_autopilot_decision_still_skips_node_pools_with_autopilot_note(self):
        plan = planner.build_translation_plan(FULL_INVENTORY, {"karpenter": "GKE_AUTOPILOT"})
        node_pool = _by_id(plan)["node-pool-system"]
        self.assertEqual(node_pool["status"], "skipped")
        self.assertIn("Autopilot", node_pool["notes"][0])
        self.assertFalse(node_pool["notes"][0].startswith("Skipped by default"))

    def test_deterministic(self):
        first = planner.build_translation_plan(FULL_INVENTORY, DECISIONS)
        second = planner.build_translation_plan(FULL_INVENTORY, DECISIONS)
        self.assertEqual(first, second)

    def test_duplicate_nodegroup_slugs_get_unique_ids(self):
        inventory = {"nodegroups": [{"name": "gpu_pool"}, {"name": "gpu-pool"}]}
        plan = planner.build_translation_plan(inventory, {})
        ids = [unit["unit_id"] for unit in plan["units"] if unit["kind"] == "node-pool"]
        self.assertEqual(ids, ["node-pool-gpu-pool", "node-pool-gpu-pool-2"])

    def test_placeholder_can_be_unskipped_by_review(self):
        plan = planner.build_translation_plan({}, {})
        updated, notes = planner.set_unit_status(plan, ["storage"], "planned")
        self.assertEqual(_by_id(updated)["storage"]["status"], "planned")
        self.assertIn("storage -> planned", notes)

    def test_skip_claims_stay_truthful_when_section_has_unrelated_facts(self):
        # A section can hold facts and still produce a placeholder when the
        # facts are not the kind the gate looks for (cluster-autoscaler but no
        # Karpenter; private endpoints but no peering/LBs; stray storage
        # evidence but no CSI/StorageClass fields). The claim must not pretend
        # the section was empty — and the facts ride along in inputs.
        inventory = {
            "autoscaling": {"karpenter": False, "cluster_autoscaler": True, "evidence": ["cas"]},
            "network": {"private_only_endpoints": True},
            "storage": {"evidence": ["pvc.yaml requests gp3"]},
        }
        plan = planner.build_translation_plan(inventory, {})
        by_id = _by_id(plan)
        autoscaling = by_id["autoscaling-karpenter"]
        self.assertIn("no Karpenter evidence", autoscaling["notes"][0])
        self.assertNotIn("recorded no facts", autoscaling["notes"][0])
        self.assertEqual(autoscaling["inputs"]["autoscaling"]["cluster_autoscaler"], True)
        network = by_id["network"]
        self.assertIn("no cross-VPC peering or load balancers", network["notes"][0])
        self.assertNotIn("recorded no facts", network["notes"][0])
        gateway = by_id["gateway"]
        self.assertIn("no load balancers or ingress hosts", gateway["notes"][0])
        self.assertNotIn("recorded no facts", gateway["notes"][0])
        self.assertEqual(gateway["inputs"]["network"]["private_only_endpoints"], True)
        storage = by_id["storage"]
        self.assertIn("no CSI drivers or StorageClasses", storage["notes"][0])
        self.assertNotIn("recorded no facts", storage["notes"][0])
        self.assertEqual(storage["inputs"]["storage"]["evidence"], ["pvc.yaml requests gp3"])

    def test_active_units_full_and_empty(self):
        full = planner.build_translation_plan(FULL_INVENTORY, DECISIONS)
        self.assertEqual(len(planner.active_units(full)), 11)
        empty = planner.build_translation_plan({}, {})
        self.assertEqual(planner.active_units(empty), [])

    def test_placeholder_flag_marks_only_no_facts_placeholders(self):
        plan = planner.build_translation_plan(FULL_INVENTORY, {"karpenter": "GKE_AUTOPILOT"})
        by_id = _by_id(plan)
        # Autopilot-skipped node pool: real unit (facts in inputs), not a placeholder.
        self.assertEqual(by_id["node-pool-system"]["status"], "skipped")
        self.assertFalse(by_id["node-pool-system"]["placeholder"])
        for unit in planner.build_translation_plan({}, {})["units"]:
            self.assertTrue(unit["placeholder"], unit["unit_id"])

    def test_every_unit_cites_map_rows_including_placeholders(self):
        rows = _real_map_rows()
        for inventory, decisions in ((FULL_INVENTORY, DECISIONS), ({}, {})):
            plan = planner.build_translation_plan(inventory, decisions)
            for unit in plan["units"]:
                self.assertTrue(unit.get("covers"), f"{unit['unit_id']} cites no map row")
                self.assertLessEqual(
                    set(unit["covers"]), set(rows),
                    f"{unit['unit_id']} cites a row the map does not hold",
                )

    def test_placeholders_carry_their_familys_citation(self):
        for unit in planner.build_translation_plan({}, {})["units"]:
            self.assertTrue(unit["placeholder"], unit["unit_id"])
            self.assertEqual(
                unit["covers"], planner.FAMILY_COVERS[unit["kind"]], unit["unit_id"]
            )

    def test_map_and_planner_families_stay_bound(self):
        # The invariant binding the coverage map to the planner: every unit
        # family cites discovery-sourced platform-translation rows, exactly
        # one family per cited row, and the rows left uncovered are exactly
        # the ones no planner family is charged with yet — only Filestore
        # instances now, which the storage unit may emit at its discretion
        # but no family is bound to (and which the validate gate's
        # UNENFORCED_ROWS pin excuses, bound by coverage_gate_test). A new
        # map row without a planner family — or a new family without a map
        # row — lands in this test, today. Pinned, not absolute: growing a
        # family for this row means shrinking this set in the same change.
        rows = _real_map_rows()
        platform_sourced = set()
        for key, row in rows.items():
            if row["owner"] != "platform-translation":
                continue
            if coverage.resolve_discovered_from(row["source"]):
                platform_sourced.add(key)

        families_per_row = {}
        for kind, cited in planner.FAMILY_COVERS.items():
            for key in cited:
                families_per_row.setdefault(key, []).append(kind)

        self.assertLessEqual(set(families_per_row), platform_sourced)
        for key, kinds in sorted(families_per_row.items()):
            self.assertEqual(len(kinds), 1, f"{key} is cited by {kinds}")
        self.assertEqual(
            platform_sourced - set(families_per_row),
            {"filestore instances"},
        )

    def test_summarize_plan_reports_compact_coverage(self):
        plan = planner.build_translation_plan(FULL_INVENTORY, DECISIONS)
        plan = coverage.attach_coverage(plan, FULL_INVENTORY)
        summary = json.loads(planner.summarize_plan(plan))
        for line in summary["units"]:
            self.assertTrue(line.get("covers"), line["unit_id"])
        compact = summary["coverage"]
        self.assertEqual(compact["granularity"], "section")
        self.assertNotIn("unknown-section", compact["row_verdicts"])
        # The one known-uncovered platform row surfaces as an omission on a
        # full estate — WARNING material for the review, not silence (and
        # the validate gate's pin, not a failure, once enforcement runs).
        self.assertEqual(set(compact["omission"]), {"filestore instances"})
        self.assertEqual(compact["overlap"], [])
        self.assertEqual(
            compact["traceability"],
            {"units_without_covers": [], "unknown_citations": []},
        )
        self.assertEqual(compact["unknown_sections"], [])
        # 6 workload rows post-merge: the developer-phase set (manifests,
        # HTTPRoute, KSA, PVC, images) plus the workload-side NetworkPolicy
        # row the workload pipeline chain added beside the platform-side
        # namespace-isolation row.
        self.assertEqual(compact["out_of_scope_rows"], {"landing-zone": 5, "workload": 6})
        # Compact means compact: the instantiated table itself stays out.
        self.assertNotIn("map", compact)

    def test_summarize_plan_survives_corrupt_stored_coverage(self):
        # A coverage key this code did not write (tampered or half-written
        # ledger state) degrades to an error entry in the summary — the unit
        # lines still render, and no exception escapes to the tool layer.
        plan = planner.build_translation_plan(FULL_INVENTORY, DECISIONS)
        plan["coverage"] = {"map": {"x": 1}, "checks": "not-a-dict"}
        summary = json.loads(planner.summarize_plan(plan))
        self.assertTrue(summary["units"])
        self.assertIn("error", summary["coverage"])
        self.assertIn("re-run plan_translation", summary["coverage"]["error"])

    def test_all_placeholders_distinguishes_empty_inventory_from_autopilot_skips(self):
        # The round-2 review reproduction: nodegroups are the only facts and
        # the karpenter decision is GKE_AUTOPILOT — zero active units, but the
        # inventory holds facts, so this plan must NOT look like an empty one
        # (rediscovery would loop forever; review/decline is the way out).
        inventory = {"nodegroups": [{"name": "system"}]}
        plan = planner.build_translation_plan(inventory, {"karpenter": "GKE_AUTOPILOT"})
        self.assertEqual(planner.active_units(plan), [])
        self.assertFalse(planner.all_placeholders(plan))
        self.assertTrue(planner.all_placeholders(planner.build_translation_plan({}, {})))
        self.assertFalse(planner.all_placeholders({"units": []}))


class GatewayFamilyTest(unittest.TestCase):
    """The 2d shared entry-point Gateway family: fact regimes and the brief."""

    def test_planned_from_load_balancer_facts_with_full_fact_slice(self):
        plan = planner.build_translation_plan(FULL_INVENTORY, DECISIONS)
        gateway = _by_id(plan)["gateway"]
        self.assertEqual(gateway["status"], "planned")
        self.assertFalse(gateway["placeholder"])
        self.assertEqual(gateway["inputs"]["network"],
                         FULL_INVENTORY["network"])

    def test_planned_from_ingress_hosts_alone(self):
        inventory = {"network": {"ingress_hosts": ["shop.acme.example"]}}
        plan = planner.build_translation_plan(inventory, {})
        gateway = _by_id(plan)["gateway"]
        self.assertEqual(gateway["status"], "planned")
        joined = "\n".join(gateway["notes"])
        self.assertIn("shop.acme.example", joined)
        self.assertIn("verbatim", joined)

    def test_placeholder_names_the_exact_missing_facts(self):
        inventory = {"network": {"private_only_endpoints": True}}
        plan = planner.build_translation_plan(inventory, {})
        gateway = _by_id(plan)["gateway"]
        self.assertEqual(gateway["status"], "skipped")
        self.assertTrue(gateway["placeholder"])
        self.assertIn("no load balancers or ingress hosts", gateway["notes"][0])
        # The unrelated facts still ride along in inputs.
        self.assertEqual(gateway["inputs"]["network"]["private_only_endpoints"], True)

    def test_brief_requires_the_shared_marker_and_names_the_stakes(self):
        # The interlock: exports._find_gateway publishes only a marked
        # Gateway; the brief must order the marker verbatim and say why.
        plan = planner.build_translation_plan(FULL_INVENTORY, DECISIONS)
        joined = "\n".join(_by_id(plan)["gateway"]["notes"])
        self.assertIn('gkma.dev/shared-gateway: "true"', joined)
        self.assertIn("only from a Gateway carrying this marker", joined)
        self.assertIn("parks their routing units", joined)


    def test_brief_orders_one_gateway_api_manifest(self):
        plan = planner.build_translation_plan(FULL_INVENTORY, DECISIONS)
        notes = _by_id(plan)["gateway"]["notes"]
        self.assertIn("exactly ONE", notes[0])
        self.assertIn("gateway.networking.k8s.io/v1", notes[0])
        self.assertIn("(.yaml)", notes[0])
        self.assertIn("tradeoffs", notes[0])  # the namespace choice

    def test_brief_defaults_http_listener_and_routes_tls_to_open_question(self):
        plan = planner.build_translation_plan(FULL_INVENTORY, DECISIONS)
        joined = "\n".join(_by_id(plan)["gateway"]["notes"])
        self.assertIn("one HTTP listener on port 80", joined)
        self.assertIn("Do not emit certificate configuration", joined)
        self.assertIn("open question", joined)
        # FULL_INVENTORY records no cert evidence; the brief must not invent
        # one — the ACM mention is conditioned on the inputs recording it.
        self.assertIn("only if inputs.network records it", joined)

    def test_brief_without_hosts_routes_hostname_inventory_to_open_questions(self):
        plan = planner.build_translation_plan(FULL_INVENTORY, DECISIONS)
        joined = "\n".join(_by_id(plan)["gateway"]["notes"])
        self.assertIn("no ingress_hosts entries", joined)
        self.assertIn("leave the listener hostname unset", joined)
        self.assertIn("open_questions", joined)

    def test_brief_class_choice_conditioned_on_lb_facts(self):
        plan = planner.build_translation_plan(FULL_INVENTORY, DECISIONS)
        joined = "\n".join(_by_id(plan)["gateway"]["notes"])
        self.assertIn("gatewayClassName", joined)
        self.assertIn("frontend-alb", joined)
        self.assertIn("assumption to state, not a discovered fact", joined)

    def test_brief_class_choice_without_lb_facts_states_an_assumption(self):
        inventory = {"network": {"ingress_hosts": ["shop.acme.example"]}}
        plan = planner.build_translation_plan(inventory, {})
        joined = "\n".join(_by_id(plan)["gateway"]["notes"])
        self.assertIn("names no load balancers", joined)
        self.assertIn("assumption", joined)


    def test_brief_orders_the_two_legal_attach_policies_and_bans_same(self):
        # Family 1 (2026-08-15 audit): the free-form "explicit cross-
        # namespace attach policy" wording let the worker invent a Selector
        # label no Namespace carried — attachedRoutes stayed 0 behind a
        # structurally-valid report. The brief now names the only two legal
        # shapes, the product label literal, and the banned default.
        plan = planner.build_translation_plan(FULL_INVENTORY, DECISIONS)
        joined = "\n".join(_by_id(plan)["gateway"]["notes"])
        self.assertIn("allowedRoutes.namespaces", joined)
        self.assertIn("`from: All`", joined)
        self.assertIn("`from: Selector`", joined)
        self.assertIn("Never `from: Same`", joined)
        self.assertIn("never a selector label of your own devising", joined)

    def test_the_attach_label_is_the_product_constant(self):
        # One definition: the brief's literal is exports' constant — the
        # same pair the tenancy brief stamps and the validate gate reads.
        plan = planner.build_translation_plan(FULL_INVENTORY, DECISIONS)
        joined = "\n".join(_by_id(plan)["gateway"]["notes"])
        self.assertIn(f'{exports_lib.GATEWAY_ACCESS_LABEL}: '
                      f'"{exports_lib.GATEWAY_ACCESS_VALUE}"', joined)

    def test_brief_orders_the_platform_namespace_manifest(self):
        # F1-EXT-2: nothing else creates the Gateway's namespace, exports
        # publishes the attach point from files, and apply order is
        # alphabetical — the unit must ship the Namespace itself, labeled.
        plan = planner.build_translation_plan(FULL_INVENTORY, DECISIONS)
        joined = "\n".join(_by_id(plan)["gateway"]["notes"])
        self.assertIn("ONE Kubernetes Namespace manifest", joined)
        self.assertIn("byte-identical to the Gateway's", joined)
        self.assertIn('namespaces "<name>" not found', joined)
        self.assertIn("NO other object kind", joined)

    def test_brief_fences_the_namespace_choice_off_recorded_names(self):
        inventory = dict(FULL_INVENTORY, clusters=[
            {"name": "acme-prod", "workloads": {"namespaces": [
                {"name": "acme-shop", "deployments": 3}]}}])
        plan = planner.build_translation_plan(inventory, DECISIONS)
        gateway = _by_id(plan)["gateway"]
        self.assertEqual(gateway["inputs"]["recorded_namespaces"],
                         ["acme-shop"])
        joined = "\n".join(gateway["notes"])
        self.assertIn("none of the recorded workload namespaces — acme-shop",
                      joined)
        self.assertIn("tenancy unit's to issue", joined)

    def test_brief_without_recorded_names_still_orders_the_namespace(self):
        inventory = {"network": {"ingress_hosts": ["shop.acme.example"]}}
        plan = planner.build_translation_plan(inventory, {})
        gateway = _by_id(plan)["gateway"]
        self.assertEqual(gateway["inputs"]["recorded_namespaces"], [])
        joined = "\n".join(gateway["notes"])
        self.assertIn("ONE Kubernetes Namespace manifest", joined)
        self.assertNotIn("tenancy unit's to issue", joined)

    def test_scalar_ingress_hosts_is_evidence_but_never_enumerated(self):
        # The open dict can carry a scalar; iterating it would fabricate
        # per-character "hostnames". Planned, but routed to open_questions.
        inventory = {"network": {"ingress_hosts": "shop.acme.example"}}
        plan = planner.build_translation_plan(inventory, {})
        gateway = _by_id(plan)["gateway"]
        self.assertEqual(gateway["status"], "planned")
        joined = "\n".join(gateway["notes"])
        self.assertIn("does not parse as a list of hostnames", joined)
        self.assertNotIn("verbatim", joined)

    def test_scalar_load_balancers_is_evidence_but_never_enumerated(self):
        inventory = {"network": {"load_balancers": "frontend-alb"}}
        plan = planner.build_translation_plan(inventory, {})
        gateway = _by_id(plan)["gateway"]
        self.assertEqual(gateway["status"], "planned")
        joined = "\n".join(gateway["notes"])
        self.assertIn("does not parse as a list of names", joined)

    def test_placeholder_carries_no_marker_brief(self):
        plan = planner.build_translation_plan({}, {})
        gateway = _by_id(plan)["gateway"]
        self.assertEqual(gateway["status"], "skipped")
        self.assertNotIn("shared-gateway", "\n".join(gateway["notes"]))

    def test_brief_requires_a_literal_metadata_namespace(self):
        # exports.gateway.namespace is read off metadata.namespace alone, so
        # "stated in tradeoffs" or applied by an overlay publishes null.
        plan = planner.build_translation_plan(FULL_INVENTORY, DECISIONS)
        joined = "\n".join(_by_id(plan)["gateway"]["notes"])
        self.assertIn("set metadata.namespace literally in the manifest", joined)
        self.assertIn("publishes as null", joined)

    def test_brief_forbids_httproutes_and_the_network_units_resources(self):
        # The two-party split: this unit is the Gateway half only. Nothing
        # downstream detects a straddle, so the brief has to say it.
        plan = planner.build_translation_plan(FULL_INVENTORY, DECISIONS)
        joined = "\n".join(_by_id(plan)["gateway"]["notes"])
        self.assertIn("HTTPRoutes are the workload phase's half", joined)
        self.assertIn("emit none", joined)
        self.assertIn("`network` unit owns the Terraform half", joined)

    def test_the_network_unit_is_told_not_to_emit_gateway_api_objects(self):
        # The peer half of the same boundary: `network` and `gateway` are
        # planned from the same load_balancers fact and get the same input
        # slice. The 2026-08-14 e2e's network unit shipped a real Gateway
        # API Gateway AND an HTTPRoute from exactly this brief.
        plan = planner.build_translation_plan(FULL_INVENTORY, DECISIONS)
        network, gateway = _by_id(plan)["network"], _by_id(plan)["gateway"]
        self.assertEqual(network["status"], "planned")
        self.assertEqual(gateway["status"], "planned")
        joined = "\n".join(network["notes"])
        self.assertIn("emit no Gateway API objects here", joined)
        self.assertIn("no HTTPRoute", joined)
        self.assertIn("`gateway` unit", joined)

    def test_brief_names_the_proxy_only_subnet_cost_of_the_regional_classes(self):
        plan = planner.build_translation_plan(FULL_INVENTORY, DECISIONS)
        joined = "\n".join(_by_id(plan)["gateway"]["notes"])
        self.assertIn("REGIONAL_MANAGED_PROXY", joined)
        self.assertIn("landing-zone-owned artifact no translation unit", joined)
        self.assertIn("open question for the landing zone", joined)

    def test_non_string_host_entries_are_never_quoted_into_the_brief(self):
        # An Ingress rule's natural shape is {host, path}; str()-ing it would
        # order the worker to write a Python dict repr as a listener hostname.
        inventory = {"network": {"ingress_hosts": [
            {"host": "shop.acme.example", "path": "/"}]}}
        plan = planner.build_translation_plan(inventory, {})
        gateway = _by_id(plan)["gateway"]
        self.assertEqual(gateway["status"], "planned")
        joined = "\n".join(gateway["notes"])
        self.assertNotIn("{", joined)
        self.assertIn("does not parse as a list of hostnames", joined)
        self.assertNotIn("verbatim", joined)

    def test_a_mixed_host_list_enumerates_only_the_strings(self):
        inventory = {"network": {"ingress_hosts": [
            "shop.acme.example", {"host": "api.acme.example"}, ""]}}
        joined = "\n".join(
            _by_id(planner.build_translation_plan(inventory, {}))["gateway"]["notes"])
        # Exactly one hostname is enumerated (the em dash ends the list).
        self.assertIn("ingress_hosts records shop.acme.example —", joined)
        self.assertIn("do not parse as hostnames", joined)
        self.assertIn("{'host': 'api.acme.example'}", joined)

    def test_non_string_load_balancer_entries_go_to_open_questions(self):
        inventory = {"network": {"load_balancers": [{"name": "frontend"}]}}
        joined = "\n".join(
            _by_id(planner.build_translation_plan(inventory, {}))["gateway"]["notes"])
        self.assertNotIn("{", joined)
        self.assertIn("does not parse as a list of names", joined)

    def test_placeholder_warns_when_evidence_mentions_an_entry_point(self):
        # The storage placeholder's precedent: extraction files facts in the
        # wrong section often enough that silence is the worse failure. The
        # free-text mention warns; it never plans the unit.
        inventory = {"network": {"evidence": [
            "ALB Ingress annotations on frontend", "VPC peering to shared-svcs"]}}
        gateway = _by_id(planner.build_translation_plan(inventory, {}))["gateway"]
        self.assertEqual(gateway["status"], "skipped")
        self.assertTrue(gateway["placeholder"])
        joined = "\n".join(gateway["notes"])
        self.assertIn("WARNING", joined)
        self.assertIn("ALB Ingress annotations on frontend", joined)
        self.assertIn("discovery gap", joined)
        self.assertNotIn("VPC peering to shared-svcs", joined)

    def test_placeholder_stays_quiet_when_the_evidence_mentions_no_entry_point(self):
        inventory = {"network": {"evidence": ["Global VPC, albeit a small one"]}}
        gateway = _by_id(planner.build_translation_plan(inventory, {}))["gateway"]
        self.assertNotIn("WARNING", "\n".join(gateway["notes"]))


class TenancyFamilyTest(unittest.TestCase):
    """The 2c tenancy family: namespace-granularity Namespace + ResourceQuota."""

    def test_planned_from_cluster_namespaces_with_merged_inputs(self):
        plan = planner.build_translation_plan(FULL_INVENTORY, DECISIONS)
        tenancy = _by_id(plan)["tenancy"]
        self.assertEqual(tenancy["status"], "planned")
        self.assertFalse(tenancy["placeholder"])
        # Entries ride whole (open objects), not reduced to names.
        self.assertEqual(tenancy["inputs"], {"namespaces": [
            {"name": "acme-shop", "deployments": 3}]})

    def test_brief_orders_a_namespace_and_quota_pair_per_name(self):
        plan = planner.build_translation_plan(FULL_INVENTORY, DECISIONS)
        notes = _by_id(plan)["tenancy"]["notes"]
        self.assertIn("acme-shop", notes[0])
        self.assertIn("one Kubernetes Namespace manifest", notes[0])
        self.assertIn("one ResourceQuota manifest", notes[0])
        self.assertIn("(.yaml)", notes[0])
        self.assertIn("namespace-granularity", notes[0])
        self.assertIn("verbatim", notes[0])
        self.assertIn("never invent, rename, or split", notes[0])

    def test_brief_issues_zero_deployment_namespaces(self):
        inventory = {"clusters": [{"name": "c1", "workloads": {"namespaces": [
            {"name": "idle-ns", "deployments": 0}]}}]}
        tenancy = _by_id(planner.build_translation_plan(inventory, {}))["tenancy"]
        self.assertEqual(tenancy["status"], "planned")
        joined = "\n".join(tenancy["notes"])
        self.assertIn("idle-ns", joined)
        self.assertIn("zero deployments", joined)
        self.assertIn("not an issuance threshold", joined)

    def test_brief_routes_quota_values_to_assumptions_never_facts(self):
        # The inventory records a name and a deployment count per namespace;
        # quota numbers are the unit's choice and the brief must say so.
        plan = planner.build_translation_plan(FULL_INVENTORY, DECISIONS)
        joined = "\n".join(_by_id(plan)["tenancy"]["notes"])
        self.assertIn("no quota value is a discovered fact", joined)
        self.assertIn("starter quotas", joined)
        self.assertIn("assumptions as unverified", joined)
        self.assertIn("never present a quota number as discovered", joined)
        # Entries are open: recorded quota/limit keys win over invention.
        self.assertIn("honor them literally", joined)


    def test_starter_quotas_are_object_count_only_from_zero_facts(self):
        # A compute quota makes requests/limits mandatory for every pod in
        # the namespace, and the brief forbids the LimitRange that would
        # supply defaults unless an entry records limit facts — so inventing
        # a cpu/memory quota from a bare {name, deployments} entry would
        # tighten posture from zero facts. acme is exactly this regime.
        plan = planner.build_translation_plan(FULL_INVENTORY, DECISIONS)
        joined = "\n".join(_by_id(plan)["tenancy"]["notes"])
        self.assertIn("OBJECT COUNTS only", joined)
        self.assertIn("persistentvolumeclaims", joined)
        self.assertIn("never constrain compute (cpu or memory) from zero facts",
                      joined)
        self.assertIn("requests/limits mandatory", joined)
        # Recorded compute quotas are honored, but their pod-rejection edge
        # routes to open_questions when no limit facts ride along.
        self.assertIn("pods declaring no requests/limits will be rejected",
                      joined)

    def test_brief_fences_the_unit_to_its_two_object_kinds(self):
        # The gateway brief spells out its boundary; without the same
        # positive fence here, TRANSLATE_RULES' open menu (ServiceAccount,
        # HTTPRoute, ...) plus "production-quality" invites a default-deny
        # NetworkPolicy — the canonical tenancy-hygiene object — that no
        # row of the coverage map owns and no gate would catch.
        plan = planner.build_translation_plan(FULL_INVENTORY, DECISIONS)
        joined = "\n".join(_by_id(plan)["tenancy"]["notes"])
        self.assertIn("NO other Kubernetes object kind", joined)
        self.assertIn("No NetworkPolicy", joined)
        self.assertIn("no ServiceAccount", joined)
        self.assertIn("nothing in any namespace this brief does not name",
                      joined)

    def test_brief_orders_the_attach_label_on_every_namespace(self):
        # Family 1 (2026-08-15 audit): the shared Gateway's Selector reads
        # this label off every issued Namespace; a brief that never orders
        # it ships namespaces whose HTTPRoutes are accepted and never
        # attach (the acme e2e needed the KB1 hand-label bridge).
        plan = planner.build_translation_plan(FULL_INVENTORY, DECISIONS)
        joined = "\n".join(_by_id(plan)["tenancy"]["notes"])
        self.assertIn("Label every Namespace manifest this unit emits",
                      joined)
        self.assertIn(f'{exports_lib.GATEWAY_ACCESS_LABEL}: '
                      f'"{exports_lib.GATEWAY_ACCESS_VALUE}"', joined)
        self.assertIn("not a discovered fact", joined)
        self.assertIn("accepted by the API server and never attaches",
                      joined)

    def test_both_briefs_order_the_same_attach_label_literal(self):
        # One definition, two briefs, one gate: the literal in the gateway
        # brief's Selector order and in the tenancy brief's label order is
        # the same product constant the validate gate checks against.
        plan = planner.build_translation_plan(FULL_INVENTORY, DECISIONS)
        by_id = _by_id(plan)
        literal = (f'`{exports_lib.GATEWAY_ACCESS_LABEL}: '
                   f'"{exports_lib.GATEWAY_ACCESS_VALUE}"`')
        self.assertIn(literal, "\n".join(by_id["gateway"]["notes"]))
        self.assertIn(literal, "\n".join(by_id["tenancy"]["notes"]))

    def test_brief_cedes_the_platform_namespace_to_the_gateway_unit(self):
        # The reciprocal fence: the gateway brief orders exactly one
        # platform Namespace; this side must not issue a second one.
        plan = planner.build_translation_plan(FULL_INVENTORY, DECISIONS)
        joined = "\n".join(_by_id(plan)["tenancy"]["notes"])
        self.assertIn("`gateway` unit's to emit", joined)
        self.assertIn("issue ONLY the recorded names above", joined)

    def test_gateway_fence_names_come_from_the_tenancy_merge(self):
        # The same clusters[] recording feeds both sides: the tenancy unit
        # issues acme-shop, so the gateway unit's input slice carries it
        # as a name its platform namespace must not collide with.
        plan = planner.build_translation_plan(FULL_INVENTORY, DECISIONS)
        gateway = _by_id(plan)["gateway"]
        self.assertEqual(gateway["inputs"]["recorded_namespaces"],
                         ["acme-shop"])

    def test_no_usable_name_brief_carries_no_issuance_instructions(self):
        # Prohibition and issuance must not arrive together: with no usable
        # name the worker gets the open_questions route and the fences, and
        # neither a quota instruction nor a per-name issuance order beside
        # the quoted raw recordings it could guess a namespace from.
        inventory = {"clusters": [{"name": "c1", "workloads": {"namespaces": [
            {"ns": "acme-shop"}, {"name": ""}]}}]}
        tenancy = _by_id(planner.build_translation_plan(inventory, {}))["tenancy"]
        joined = "\n".join(tenancy["notes"])
        self.assertIn("emit no Namespace or ResourceQuota manifests", joined)
        self.assertIn("open_questions", joined)
        self.assertIn("Do not emit RBAC objects", joined)
        self.assertNotIn("starter quotas", joined)
        self.assertNotIn("exactly ONE Namespace", joined)
        self.assertNotIn("Per recorded namespace", joined)
        self.assertNotIn("Issue every recorded namespace", joined)
        # No issuance, so no label order to ride beside the raw reprs.
        self.assertNotIn("Label every Namespace", joined)

    def test_padded_names_fold_into_the_one_real_namespace(self):
        # Whitespace is never part of a DNS-1123 name: keying the dedupe on
        # the padded recording would issue two pairs for one namespace, one
        # of them with a name no cluster accepts — and the manifest gate
        # only checks that metadata.name is non-blank.
        inventory = {"clusters": [
            {"name": "c1", "workloads": {"namespaces": [
                {"name": "acme-shop ", "deployments": 3}]}},
            {"name": "c2", "workloads": {"namespaces": [
                {"name": "acme-shop", "deployments": 3}]}},
        ]}
        tenancy = _by_id(planner.build_translation_plan(inventory, {}))["tenancy"]
        self.assertEqual(tenancy["inputs"], {"namespaces": [
            {"name": "acme-shop", "deployments": 3}]})
        joined = "\n".join(tenancy["notes"])
        self.assertNotIn("differing entries", joined)
        self.assertNotIn("acme-shop ,", joined)

    def test_zero_deployment_claim_appears_only_when_recorded(self):
        # FULL_INVENTORY records deployments: 3 — the brief must not assert
        # that some entry records zero when none does.
        plan = planner.build_translation_plan(FULL_INVENTORY, DECISIONS)
        joined = "\n".join(_by_id(plan)["tenancy"]["notes"])
        self.assertIn("not an issuance threshold", joined)
        self.assertNotIn("zero deployments", joined)

    def test_brief_issues_per_namespace_never_per_team(self):
        # Resolution 6: multi-team shared namespaces (acme: 3 teams in
        # acme-shop) still get ONE quota; splitting by team is forbidden.
        plan = planner.build_translation_plan(FULL_INVENTORY, DECISIONS)
        joined = "\n".join(_by_id(plan)["tenancy"]["notes"])
        self.assertIn("per namespace, never per team", joined)
        self.assertIn("never split a namespace by team", joined)
        self.assertIn("never equate a namespace with a team", joined)
        self.assertIn("tradeoff", joined)

    def test_brief_forbids_rbac_and_conditions_limitrange(self):
        plan = planner.build_translation_plan(FULL_INVENTORY, DECISIONS)
        joined = "\n".join(_by_id(plan)["tenancy"]["notes"])
        self.assertIn("Do not emit RBAC objects", joined)
        self.assertIn("RoleBindings", joined)
        self.assertIn("[PLANNED]", joined)
        self.assertIn("elicitation-based", joined)
        self.assertIn("never invented from the scan", joined)
        self.assertIn("Do not emit LimitRange objects unless an entry "
                      "records limit facts", joined)

    def test_identical_recordings_across_clusters_fold_to_one_entry(self):
        entry = {"name": "acme-shop", "deployments": 3}
        inventory = {"clusters": [
            {"name": "c1", "workloads": {"namespaces": [dict(entry)]}},
            {"name": "c2", "workloads": {"namespaces": [dict(entry)]}},
        ]}
        tenancy = _by_id(planner.build_translation_plan(inventory, {}))["tenancy"]
        self.assertEqual(tenancy["inputs"], {"namespaces": [entry]})
        self.assertNotIn("differing entries", "\n".join(tenancy["notes"]))

    def test_conflicting_recordings_keep_first_and_name_the_discrepancy(self):
        inventory = {"clusters": [
            {"name": "c1", "workloads": {"namespaces": [
                {"name": "acme-shop", "deployments": 2}]}},
            {"name": "c2", "workloads": {"namespaces": [
                {"name": "acme-shop", "deployments": 5},
                {"name": "tools", "deployments": 1}]}},
        ]}
        tenancy = _by_id(planner.build_translation_plan(inventory, {}))["tenancy"]
        # Sorted by name; the conflicted entry is c1's, kept verbatim.
        self.assertEqual(tenancy["inputs"]["namespaces"], [
            {"name": "acme-shop", "deployments": 2},
            {"name": "tools", "deployments": 1}])
        conflict_notes = [n for n in tenancy["notes"] if "differing entries" in n]
        self.assertEqual(len(conflict_notes), 1)
        self.assertIn("acme-shop", conflict_notes[0])
        self.assertIn("open_questions", conflict_notes[0])
        self.assertNotIn("tools", conflict_notes[0])


    def test_within_cluster_duplicate_recordings_also_surface(self):
        # A conflict is not only a cross-cluster event; one cluster listing
        # a name twice with differing entries gets the same treatment.
        inventory = {"clusters": [{"name": "c1", "workloads": {"namespaces": [
            {"name": "acme-shop", "deployments": 2},
            {"name": "acme-shop", "deployments": 7}]}}]}
        tenancy = _by_id(planner.build_translation_plan(inventory, {}))["tenancy"]
        self.assertEqual(tenancy["inputs"]["namespaces"],
                         [{"name": "acme-shop", "deployments": 2}])
        self.assertIn("differing entries", "\n".join(tenancy["notes"]))

    def test_unusable_entries_are_named_for_open_questions_never_issued(self):
        # A bare string is plausibly a name recorded in the wrong shape;
        # issuing from it would be a guess, so it routes to open_questions.
        inventory = {"clusters": [{"name": "c1", "workloads": {"namespaces": [
            "just-a-string", {"deployments": 4}]}}]}
        tenancy = _by_id(planner.build_translation_plan(inventory, {}))["tenancy"]
        self.assertEqual(tenancy["status"], "planned")
        self.assertEqual(tenancy["inputs"], {"namespaces": []})
        joined = "\n".join(tenancy["notes"])
        self.assertIn("none parses as a namespace entry with a name", joined)
        self.assertIn("do not parse as namespace entries", joined)
        self.assertIn("'just-a-string'", joined)
        self.assertIn("{'deployments': 4}", joined)
        self.assertIn("open_questions", joined)

    def test_non_mapping_workloads_section_is_unusable_not_a_crash(self):
        # clusters[].workloads is an open section: a recording that is a
        # list (or any non-mapping) must route to open_questions like every
        # other unparseable recording, never raise out of plan_translation.
        inventory = {"clusters": [
            {"name": "c1", "workloads": ["acme-shop"]},
            {"name": "c2", "workloads": {"namespaces": [
                {"name": "tools", "deployments": 1}]}}]}
        tenancy = _by_id(planner.build_translation_plan(inventory, {}))["tenancy"]
        self.assertEqual(tenancy["status"], "planned")
        self.assertEqual(tenancy["inputs"], {"namespaces": [
            {"name": "tools", "deployments": 1}]})
        joined = "\n".join(tenancy["notes"])
        self.assertIn("do not parse as namespace entries", joined)
        self.assertIn("['acme-shop']", joined)

    def test_scalar_recording_is_evidence_but_never_enumerated(self):
        # The gateway family's posture: a truthy non-list recording still
        # plans the family; only its enumeration is guarded.
        inventory = {"clusters": [
            {"name": "c1", "workloads": {"namespaces": "acme-shop"}}]}
        tenancy = _by_id(planner.build_translation_plan(inventory, {}))["tenancy"]
        self.assertEqual(tenancy["status"], "planned")
        self.assertEqual(tenancy["inputs"], {"namespaces": []})
        self.assertIn("do not parse as namespace entries",
                      "\n".join(tenancy["notes"]))

    def test_no_namespaces_anywhere_gives_a_truthful_placeholder(self):
        # Clusters recorded, namespaces empty: still literally true.
        inventory = {"clusters": [{"name": "c1", "workloads": {"namespaces": []}},
                                  {"name": "c2"}]}
        tenancy = _by_id(planner.build_translation_plan(inventory, {}))["tenancy"]
        self.assertEqual(tenancy["status"], "skipped")
        self.assertTrue(tenancy["placeholder"])
        self.assertIn("inventory.clusters[].workloads.namespaces recorded "
                      "no namespaces", tenancy["notes"][0])
        self.assertEqual(tenancy["inputs"], {"namespaces": []})

    def test_placeholder_carries_no_issuance_brief(self):
        plan = planner.build_translation_plan({}, {})
        tenancy = _by_id(plan)["tenancy"]
        self.assertEqual(tenancy["status"], "skipped")
        joined = "\n".join(tenancy["notes"])
        self.assertNotIn("ResourceQuota", joined)
        self.assertNotIn("quota", joined)




class ClusterDnsFamilyTest(unittest.TestCase):
    """The cluster-dns unit: facts and fences in the brief, the mapping elsewhere."""

    def test_planned_with_the_verbatim_text_and_the_mode(self):
        plan = planner.build_translation_plan(FULL_INVENTORY, DECISIONS)
        unit = _by_id(plan)["cluster-dns"]
        self.assertEqual(unit["status"], "planned")
        self.assertFalse(unit["placeholder"])
        self.assertEqual(unit["inputs"]["cluster_dns"], FULL_INVENTORY["cluster_dns"])
        # The legacy key keeps the recorded karpenter token; the mode is the
        # registry projection's string, threaded beside it.
        self.assertEqual(unit["inputs"]["decision"], "GKE_STANDARD_NAP")
        self.assertEqual(unit["inputs"]["cluster_mode"], "standard")
        self.assertEqual(unit["inputs"]["decisions"], {
            "karpenter": "GKE_STANDARD_NAP", "privileged_daemonsets": "GKE_STANDARD",
            "gpu_tpu": "GKE_STANDARD_SPECIALIZED"})
        text = "\n".join(unit["notes"])
        self.assertIn("VERBATIM", text)
        self.assertIn("configmap coredns at k8s/coredns.yaml (manifest)", text)
        self.assertIn("inputs.cluster_mode = standard", text)
        self.assertIn("cluster-dns-translation.md", text)
        # The mapping itself stays out of the brief.
        self.assertNotIn("stubDomains", text)
        self.assertIn("Never emit google_container_cluster", text)
        self.assertEqual(unit["covers"], planner.FAMILY_COVERS["cluster-dns"])

    def test_the_mode_is_the_registry_projection(self):
        # Reader parity: the cluster-dns unit's mode and the node-pool skip
        # both follow decisions_lib.cluster_mode over the same choices and
        # triggers (coverage-guards G0 compatibility rows 2, 3, 4, 5).
        cases = [
            ({"karpenter": "GKE_AUTOPILOT", "privileged_daemonsets": "GKE_AUTOPILOT_BYPASS",
              "gpu_tpu": "GKE_STANDARD_SPECIALIZED"},
             {"karpenter": True, "privileged_daemonsets": True, "gpu_tpu": False, "vpc_peering": False},
             "autopilot"),
            ({"karpenter": "GKE_AUTOPILOT", "privileged_daemonsets": "GKE_STANDARD"},
             {"karpenter": True, "privileged_daemonsets": True, "gpu_tpu": False, "vpc_peering": False},
             None),
            ({"karpenter": "GKE_AUTOPILOT", "privileged_daemonsets": "GKE_STANDARD"},
             {"karpenter": True, "privileged_daemonsets": False, "gpu_tpu": False, "vpc_peering": False},
             "autopilot"),
            ({"karpenter": "GKE_STANDARD_NAP", "privileged_daemonsets": "GKE_AUTOPILOT_BYPASS"},
             {"karpenter": False, "privileged_daemonsets": True, "gpu_tpu": False, "vpc_peering": False},
             "autopilot"),
        ]
        for choices, triggers, expected in cases:
            with self.subTest(choices=choices):
                inventory = dict(FULL_INVENTORY, triggers=triggers)
                plan = planner.build_translation_plan(inventory, choices)
                units = _by_id(plan)
                self.assertEqual(units["cluster-dns"]["inputs"]["cluster_mode"], expected)
                self.assertEqual(plan["derived"]["cluster_mode"]["choice"], expected)
                self.assertEqual(units["node-pool-system"]["status"],
                                 "skipped" if expected == "autopilot" else "planned")
                self.assertEqual(exports_lib._cluster_type(choices, triggers)[0], expected)

    def test_every_consuming_family_carries_its_decision_ids(self):
        plan = planner.build_translation_plan(FULL_INVENTORY, DECISIONS)
        for unit in plan["units"]:
            ids = planner.FAMILY_DECISIONS.get(unit["kind"], ())
            if ids:
                self.assertEqual(set(unit["inputs"]["decisions"]), set(ids), unit["unit_id"])
                for did in ids:
                    self.assertEqual(unit["inputs"]["decisions"][did], DECISIONS[did])
            else:
                self.assertNotIn("decisions", unit["inputs"], unit["unit_id"])
        # Unrecorded ids are present as null, so the key set never depends
        # on what was recorded.
        plan = planner.build_translation_plan(FULL_INVENTORY, {"karpenter": "GKE_STANDARD_NAP"})
        self.assertEqual(_by_id(plan)["node-pool-system"]["inputs"]["decisions"],
                         {"karpenter": "GKE_STANDARD_NAP", "gpu_tpu": None})

    def test_plan_derived_values_are_typed(self):
        plan = planner.build_translation_plan(FULL_INVENTORY, DECISIONS)
        derived = plan["derived"]
        self.assertEqual(derived["cluster_mode"]["choice"], "standard")
        self.assertEqual(derived["karpenter_replacement"]["choice"], "nap")
        self.assertEqual(derived["nap_enabled"]["choice"], True)
        self.assertEqual(derived["nap_enabled"]["trigger_path"],
                         ["triggers.karpenter", "triggers.privileged_daemonsets"])
        self.assertEqual(derived["cluster_mode"]["trigger_path"],
                         ["triggers.karpenter", "triggers.privileged_daemonsets", "triggers.gpu_tpu"])
        autoscaling = _by_id(plan)["autoscaling-karpenter"]
        self.assertEqual(autoscaling["inputs"]["derived_decisions"]["nap_enabled"]["choice"], True)
        self.assertIn("nap_enabled", "\n".join(autoscaling["notes"]))
        self.assertNotIn("GKE_STANDARD_NAP", "\n".join(autoscaling["notes"]))

    def test_disagreeing_decisions_carry_one_conflict_finding(self):
        plan = planner.build_translation_plan(
            dict(FULL_INVENTORY, triggers={"karpenter": True, "privileged_daemonsets": True,
                                           "gpu_tpu": False, "vpc_peering": False}),
            {"karpenter": "GKE_AUTOPILOT", "privileged_daemonsets": "GKE_STANDARD"})
        kinds = [(f["kind"], f["severity"], f["subject"]) for f in plan["findings"]
                 if f["kind"] == "decisions-disagree"]
        self.assertEqual(kinds, [("decisions-disagree", "conflict", "cluster_mode")])
        self.assertIn("karpenter=GKE_AUTOPILOT", plan["findings"][0]["value"])
        lines = planner.render_findings(plan)
        self.assertTrue(lines[0].startswith("CONFLICT [decisions-disagree]"))
        self.assertIn("CONFLICT [decisions-disagree] cluster_mode", planner.summarize_plan(plan))

    def test_advisory_mismatch_is_a_finding_only_when_considered(self):
        triggers = {"karpenter": True, "privileged_daemonsets": True, "gpu_tpu": True, "vpc_peering": False}
        choices = {"karpenter": "GKE_AUTOPILOT", "privileged_daemonsets": "GKE_AUTOPILOT_BYPASS",
                   "gpu_tpu": "GKE_STANDARD_SPECIALIZED"}
        plan = planner.build_translation_plan(dict(FULL_INVENTORY, triggers=triggers), choices)
        disagree = [(f["kind"], f["subject"]) for f in plan["findings"] if f["kind"] == "decisions-disagree"]
        self.assertEqual(disagree, [("decisions-disagree", "gpu_tpu")])
        self.assertEqual(plan["derived"]["cluster_mode"]["choice"], "autopilot")
        # Row 2: the same choices with gpu_tpu unfired carry no disagree record.
        plan = planner.build_translation_plan(
            dict(FULL_INVENTORY, triggers=dict(triggers, gpu_tpu=False)), choices)
        self.assertEqual([f for f in plan["findings"] if f["kind"] == "decisions-disagree"], [])

    def test_a_null_mode_is_said_in_the_brief(self):
        plan = planner.build_translation_plan(FULL_INVENTORY, {"vpc_peering": "PRIVATE_ONLY_PEERING"})
        unit = _by_id(plan)["cluster-dns"]
        self.assertIsNone(unit["inputs"]["decision"])
        self.assertIsNone(unit["inputs"]["cluster_mode"])
        self.assertEqual(unit["inputs"]["decision_reason"], "none recorded")
        self.assertIn("inputs.cluster_mode is null", "\n".join(unit["notes"]))
        self.assertIn("say in assumptions that the mode was not recorded", "\n".join(unit["notes"]))
        # The mapping stays out of the brief even here.
        self.assertNotIn("kube-dns ConfigMap (it is read", "\n".join(unit["notes"]))

    def test_placeholder_without_sources_and_with_an_unread_warning(self):
        plan = planner.build_translation_plan({}, {})
        unit = _by_id(plan)["cluster-dns"]
        self.assertEqual(unit["status"], "skipped")
        self.assertTrue(unit["placeholder"])
        self.assertEqual(unit["inputs"], {
            "cluster_dns": {}, "decision": None, "cluster_mode": None,
            "decision_reason": "none recorded",
            "decisions": {"karpenter": None, "privileged_daemonsets": None, "gpu_tpu": None},
            "scan_notes": []})
        self.assertNotIn("WARNING", "\n".join(unit["notes"]))
        self.assertNotIn("Never emit", "\n".join(unit["notes"]))
        plan = planner.build_translation_plan(
            {"cluster_dns": {}, "cluster_dns_scan_notes": [
                "aws_eks_addon.coredns (eks.tf:1): the coredns configuration is read from "
                "var.corefile, which the scan does not follow — not recorded; ask for it"]}, {})
        unit = _by_id(plan)["cluster-dns"]
        self.assertTrue(unit["placeholder"])
        self.assertIn("WARNING", unit["notes"][1])
        self.assertIn("var.corefile", unit["notes"][1])

    def test_a_notes_only_section_does_not_plan_the_unit(self):
        plan = planner.build_translation_plan({"cluster_dns": {"sources": []}}, {})
        self.assertTrue(_by_id(plan)["cluster-dns"]["placeholder"])

    def test_the_addons_brief_hands_coredns_to_this_unit(self):
        plan = planner.build_translation_plan(FULL_INVENTORY, DECISIONS)
        text = "\n".join(_by_id(plan)["cluster-addons"]["notes"])
        self.assertIn("cluster-dns unit", text)

    def test_disagreeing_decisions_are_named_not_called_unrecorded(self):
        # FULL_INVENTORY fires both voting triggers, so a non-default pair
        # that implies two modes is a disagreement (v9 row 3).
        plan = planner.build_translation_plan(
            FULL_INVENTORY, {"karpenter": "GKE_AUTOPILOT", "privileged_daemonsets": "GKE_STANDARD"})
        unit = _by_id(plan)["cluster-dns"]
        self.assertEqual(unit["inputs"]["decision"], "GKE_AUTOPILOT")
        self.assertIsNone(unit["inputs"]["cluster_mode"])
        self.assertEqual(unit["inputs"]["decision_reason"],
                         "disagree: karpenter=GKE_AUTOPILOT, privileged_daemonsets=GKE_STANDARD")
        text = "\n".join(unit["notes"])
        self.assertIn("imply different cluster modes", text)
        self.assertIn("karpenter=GKE_AUTOPILOT, privileged_daemonsets=GKE_STANDARD", text)
        self.assertNotIn("no landing-zone decision recorded", text)
        # Node pools stay planned under an unresolved mode (compatibility row 3).
        self.assertEqual(_by_id(plan)["node-pool-system"]["status"], "planned")

    def test_every_unread_phrasing_earns_the_placeholder_warning(self):
        for note in (
                "module.eks (eks.tf:1): cluster_addons is an expression, so whether it configures coredns, and with what, was not read",
                "helm_release.dns (helm.tf:1): Helm release of chart coredns — its values were not read; the cluster DNS configuration it carries is unknown",
                "kubernetes_config_map.coredns (dns.tf:1): ConfigMap coredns declares no literal data map; its Corefile was not read",
                "kubectl_manifest.c (k8s.tf:1): yaml_body mentions coredns but was not read (not parseable as YAML)",
                "k8s/c.yaml: mentions coredns but was not read (not parseable as YAML)",
                "k8s/kube-system/coredns.yaml: skipped, larger than 2097152 bytes",
                "dns/node-local-dns.yaml: unreadable (PermissionError)"):
            with self.subTest(note=note[:40]):
                plan = planner.build_translation_plan(
                    {"cluster_dns": {}, "cluster_dns_scan_notes": [note]}, {})
                self.assertIn("WARNING", "\n".join(_by_id(plan)["cluster-dns"]["notes"]))
        for routine in (
                "aws_eks_addon.coredns (eks.tf:1): managed coredns add-on with no configuration_values — default configuration, nothing to carry over",
                "3 YAML file(s) under Helm chart roots (charts/dns) were not read for cluster DNS configuration: chart templates are not parsed, and no rendered form of them is read either",
                "2 Terraform file(s) the confirmed scope excludes were not read for cluster DNS configuration",
                "envs/prod/eks.tf: a block is never closed, so that block and everything after it were not read",
                "envs/prod/huge.tf: skipped, larger than 2097152 bytes",
                "cluster_dns covers Terraform .tf files and plain Kubernetes YAML only — Helm chart templates, Kustomize generators and patches, .tf.json, CloudFormation, CDK and eksctl are not parsed, so an empty section for one of those means not scanned, not absent."):
            with self.subTest(routine=routine[:40]):
                plan = planner.build_translation_plan(
                    {"cluster_dns": {}, "cluster_dns_scan_notes": [routine]}, {})
                self.assertNotIn("WARNING", "\n".join(_by_id(plan)["cluster-dns"]["notes"]))

    def test_qualifying_scan_notes_ride_into_the_brief_and_inputs(self):
        inventory = dict(FULL_INVENTORY)
        inventory["cluster_dns_scan_notes"] = [
            "k8s/coredns.yaml (document 1): text longer than 16384 bytes was truncated; the rest was not recorded",
            "cluster_dns covers Terraform .tf files and plain Kubernetes YAML only"]
        unit = _by_id(planner.build_translation_plan(inventory, DECISIONS))["cluster-dns"]
        self.assertEqual(unit["inputs"]["scan_notes"], inventory["cluster_dns_scan_notes"])
        self.assertIn("truncated", unit["notes"][0])
        self.assertNotIn("covers Terraform", unit["notes"][0])
        # A second Corefile the scan could not read is named beside the one it recorded.
        inventory["cluster_dns_scan_notes"] = [
            "aws_eks_addon.other (eks.tf:9): the coredns configuration is read from var.corefile, which the scan does not follow — not recorded; ask for it"]
        unit = _by_id(planner.build_translation_plan(inventory, DECISIONS))["cluster-dns"]
        self.assertIn("var.corefile", unit["notes"][0])
        inventory["cluster_dns_scan_notes"] = [
            "kubernetes_config_map.coredns (dns.tf:1): the ConfigMap's namespace is not a literal; recorded on the assumption that it is kube-system"]
        unit = _by_id(planner.build_translation_plan(inventory, DECISIONS))["cluster-dns"]
        self.assertIn("on the assumption", unit["notes"][0])

    def test_a_non_dict_source_still_plans_the_unit(self):
        # The coverage gate counts it as a fact; a placeholder here would be
        # an omission finding at validate with no route back.
        plan = planner.build_translation_plan({"cluster_dns": {"sources": ["odd"]}}, {})
        unit = _by_id(plan)["cluster-dns"]
        self.assertEqual(unit["status"], "planned")
        self.assertIn("unexpected shape", unit["notes"][0])


if __name__ == "__main__":
    unittest.main()


CC_DECISIONS = dict(DECISIONS, karpenter="GKE_STANDARD_COMPUTECLASS")
AP_DECISIONS = {"karpenter": "GKE_AUTOPILOT", "privileged_daemonsets": "GKE_AUTOPILOT_BYPASS",
                "gpu_tpu": "GKE_STANDARD_SPECIALIZED", "vpc_peering": "PUBLIC_AUTHORIZED_NETS"}


class ComputeClassFamilyTest(unittest.TestCase):
    """One ComputeClass per typed NodePool, keyed on the derived Karpenter
    replacement, never on a token."""

    def test_planned_under_the_computeclass_choice_with_the_typed_pool(self):
        plan = planner.build_translation_plan(FULL_INVENTORY, CC_DECISIONS)
        unit = _by_id(plan)["compute-class-shop-burst"]
        self.assertEqual(unit["status"], "planned")
        self.assertFalse(unit["placeholder"])
        self.assertEqual(unit["inputs"]["nodepool"], SHOP_BURST)
        self.assertEqual(unit["inputs"]["decisions"], {"karpenter": "GKE_STANDARD_COMPUTECLASS"})
        self.assertEqual(unit["inputs"]["derived_decisions"]["karpenter_replacement"]["choice"],
                         "computeclass")
        self.assertEqual(unit["inputs"]["derived_decisions"]["nap_enabled"]["choice"], False)
        self.assertEqual(unit["inputs"]["cluster_mode"], "standard")
        self.assertEqual(unit["covers"], planner.FAMILY_COVERS["compute-class"])
        text = "\n".join(unit["notes"])
        self.assertIn("metadata.name `shop-burst`", text)
        self.assertIn("spot priorities first, then an on-demand floor", text)
        self.assertIn("cpu=64", text)
        self.assertIn("no-constraint row for amd64", text)
        self.assertIn("nodePoolAutoCreation.enabled: true", text)
        self.assertIn("1.33.3", text)
        # The mapping and the family table stay out of the brief.
        self.assertNotIn("n4", text)
        self.assertNotIn("GKE_STANDARD", text)

    def test_the_autoscaling_unit_is_skipped_under_computeclass_with_facts_kept(self):
        plan = planner.build_translation_plan(FULL_INVENTORY, CC_DECISIONS)
        unit = _by_id(plan)["autoscaling-karpenter"]
        self.assertEqual(unit["status"], "skipped")
        self.assertFalse(unit["placeholder"])
        self.assertIn("carry the Karpenter replacement", unit["notes"][0])
        self.assertEqual(unit["inputs"]["derived_decisions"]["nap_enabled"]["choice"], False)
        # The plan is still reviewable (not placeholder-only) and the row is
        # satisfied through the explicit skip, so nothing is omitted.
        self.assertFalse(planner.all_placeholders(plan))
        self.assertEqual(plan["derived"]["karpenter_replacement"]["choice"], "computeclass")

    def test_skipped_with_the_reason_under_every_other_arm(self):
        cases = [
            (DECISIONS, FULL_INVENTORY["triggers"], "node auto-provisioning"),
            (AP_DECISIONS, FULL_INVENTORY["triggers"], "Autopilot manages nodes"),
            ({"karpenter": "GKE_AUTOPILOT", "privileged_daemonsets": "GKE_STANDARD"},
             {"karpenter": True, "privileged_daemonsets": True, "gpu_tpu": False, "vpc_peering": False},
             "cluster mode is unresolved"),
            ({"privileged_daemonsets": "GKE_STANDARD"},
             {"karpenter": True, "privileged_daemonsets": True, "gpu_tpu": False, "vpc_peering": False},
             "no Karpenter replacement was decided"),
        ]
        for decisions, triggers, phrase in cases:
            with self.subTest(decisions=decisions):
                plan = planner.build_translation_plan(dict(FULL_INVENTORY, triggers=triggers), decisions)
                unit = _by_id(plan)["compute-class-shop-burst"]
                self.assertEqual(unit["status"], "skipped")
                self.assertFalse(unit["placeholder"])
                self.assertIn(phrase, unit["notes"][0])
                self.assertEqual(unit["inputs"]["nodepool"], SHOP_BURST)

    def test_no_typed_pool_is_a_placeholder_with_a_warning_when_karpenter_is_recorded(self):
        inventory = dict(FULL_INVENTORY, autoscaling={"karpenter": True, "karpenter_nodepools": []})
        plan = planner.build_translation_plan(inventory, CC_DECISIONS)
        unit = _by_id(plan)["compute-classes"]
        self.assertTrue(unit["placeholder"])
        self.assertEqual(unit["status"], "skipped")
        self.assertIn("recorded no typed NodePool", unit["notes"][0])
        self.assertIn("WARNING", unit["notes"][1])
        self.assertIn("re-extract", unit["notes"][1])
        # Without any Karpenter evidence the placeholder carries no warning.
        plan = planner.build_translation_plan({}, {})
        self.assertEqual(len(_by_id(plan)["compute-classes"]["notes"]), 1)

    def test_computeclass_choice_without_a_typed_pool_keeps_the_nap_unit_planned_with_a_finding(self):
        # A pre-registry inventory (no typed field) under the ComputeClass
        # choice: no class can be planned, so the autoscaling unit stays the
        # planned safety net with a WARNING, and the plan carries a conflict
        # record — never an all-skipped pass at validate.
        inventory = dict(FULL_INVENTORY, autoscaling={"karpenter": True, "cluster_autoscaler": False})
        plan = planner.build_translation_plan(inventory, CC_DECISIONS)
        self.assertTrue(_by_id(plan)["compute-classes"]["placeholder"])
        autoscaling = _by_id(plan)["autoscaling-karpenter"]
        self.assertEqual(autoscaling["status"], "planned")
        self.assertIn("WARNING", autoscaling["notes"][0])
        self.assertIn("records no typed NodePool", autoscaling["notes"][0])
        records = [f for f in plan["findings"] if f["kind"] == "facts-vs-decision"]
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["severity"], "conflict")
        self.assertIn("types no NodePool", records[0]["note"])

    def test_other_units_are_fenced_off_the_computeclass_kind_under_the_arm(self):
        plan = planner.build_translation_plan(FULL_INVENTORY, CC_DECISIONS)
        units = _by_id(plan)
        for uid in ("cluster-addons", "node-pool-system"):
            self.assertTrue(any("Do not emit a ComputeClass" in n for n in units[uid]["notes"]), uid)
        plan = planner.build_translation_plan(FULL_INVENTORY, DECISIONS)
        units = _by_id(plan)
        for uid in ("cluster-addons", "node-pool-system"):
            self.assertFalse(any("Do not emit a ComputeClass" in n for n in units[uid]["notes"]), uid)

    def test_the_skipped_autoscaling_unit_keeps_its_generation_brief(self):
        plan = planner.build_translation_plan(FULL_INVENTORY, CC_DECISIONS)
        unit = _by_id(plan)["autoscaling-karpenter"]
        self.assertIn("compute-class-shop-burst", unit["notes"][0])
        self.assertTrue(any("Order the NAP guardrail only when" in n for n in unit["notes"][1:]))

    def test_capacity_brief_branches_on_membership_not_list_equality(self):
        pool = dict(SHOP_BURST, capacity_types=["spot", "reserved"])
        inventory = dict(FULL_INVENTORY, autoscaling={"karpenter": True, "karpenter_nodepools": [pool]})
        text = "\n".join(_by_id(planner.build_translation_plan(inventory, CC_DECISIONS))["compute-class-shop-burst"]["notes"])
        self.assertIn("spot priorities; an on-demand floor the source did not have", text)
        self.assertIn("`reserved` capacity type", text)
        self.assertNotIn("no priority may set `spot: true`", text)

    def test_two_pools_get_two_units_with_unique_ids(self):
        second = dict(SHOP_BURST, name="Shop Burst", capacity_types=["spot"],
                      requirements_unreduced=["karpenter.k8s.aws/instance-family"],
                      instance_families=[], taints=[{"key": "team", "value": "shop", "effect": "NoSchedule"}])
        inventory = dict(FULL_INVENTORY, autoscaling={
            "karpenter": True, "karpenter_nodepools": [SHOP_BURST, second]})
        plan = planner.build_translation_plan(inventory, CC_DECISIONS)
        ids = [u["unit_id"] for u in plan["units"] if u["kind"] == "compute-class"]
        self.assertEqual(ids, ["compute-class-shop-burst", "compute-class-shop-burst-2"])
        text = "\n".join(_by_id(plan)["compute-class-shop-burst-2"]["notes"])
        self.assertIn("spot priorities; an on-demand floor the source did not have", text)
        self.assertIn("karpenter.k8s.aws/instance-family", text)
        self.assertIn("route each to open_questions by key", text)
        self.assertIn("nodePoolConfig.taints", text)

    def test_facts_vs_decision_finding_when_the_predicate_argues_against_nap(self):
        plan = planner.build_translation_plan(FULL_INVENTORY, DECISIONS)
        records = [f for f in plan["findings"] if f["kind"] == "facts-vs-decision"]
        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertEqual((record["severity"], record["subject"], record["value"], record["expected"]),
                         ("conflict", "karpenter", "GKE_STANDARD_NAP", "GKE_STANDARD_COMPUTECLASS"))
        self.assertIn("shop-burst.capacity_types has 2 entries", record["note"])
        self.assertIn("both imply the same cluster mode", record["note"])
        line = planner.render_findings(plan)[0]
        self.assertTrue(line.startswith("CONFLICT [facts-vs-decision] karpenter"))
        # An Autopilot choice over the same pool names the mode difference.
        plan = planner.build_translation_plan(FULL_INVENTORY, AP_DECISIONS)
        record = [f for f in plan["findings"] if f["kind"] == "facts-vs-decision"][0]
        self.assertIn("the recorded choice implies autopilot, the recommendation standard", record["note"])
        # No finding when the recorded choice is the recommendation, or the
        # typed field is absent.
        self.assertEqual([f["kind"] for f in planner.build_translation_plan(FULL_INVENTORY, CC_DECISIONS)["findings"]], [])
        inventory = dict(FULL_INVENTORY, autoscaling={"karpenter": True})
        self.assertEqual(planner.build_translation_plan(inventory, DECISIONS)["findings"], [])
