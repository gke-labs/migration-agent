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

"""Unit tests for the deterministic fragment merger. No GCS, no LLM."""

import unittest

from servers.phases.discovery.discovery_extract_3 import merger


class DeriveStorageTest(unittest.TestCase):

    def test_ebs_csi_derived_from_prefixed_addon_name(self):
        # The 2026-08-11 Acme run: aws-ebs-csi-driver addon with StorageClass
        # evidence, storage section left at its defaults by the worker.
        inventory = merger.merge_fragments([{
            "addons": [{
                "name": "aws-ebs-csi-driver",
                "evidence": ["StorageClass gp3-encrypted with provisioner ebs.csi.aws.com in k8s/orders-storage.yaml"],
            }],
        }])
        self.assertTrue(inventory["storage"]["ebs_csi"])
        self.assertFalse(inventory["storage"]["efs_csi"])
        self.assertEqual(inventory["storage"]["storage_classes"], ["gp3-encrypted"])

    def test_ebs_csi_derived_from_unprefixed_addon_name(self):
        # The 2026-08-12 run: same driver arrived named 'ebs-csi-driver'.
        inventory = merger.merge_fragments([{
            "addons": [{"name": "ebs-csi-driver", "evidence": []}],
        }])
        self.assertTrue(inventory["storage"]["ebs_csi"])

    def test_storage_class_name_recovered_when_quoted(self):
        # The 2026-08-12 run's evidence phrasing quotes the name.
        inventory = merger.merge_fragments([{
            "addons": [{
                "name": "ebs-csi-driver",
                "evidence": ["StorageClass 'gp3-encrypted' uses provisioner ebs.csi.aws.com in k8s/orders-storage.yaml"],
            }],
        }])
        self.assertEqual(inventory["storage"]["storage_classes"], ["gp3-encrypted"])

    def test_efs_csi_derived_from_provisioner_evidence_alone(self):
        inventory = merger.merge_fragments([{
            "addons": [{
                "name": "storage-drivers",
                "evidence": ["helm_release installing efs.csi.aws.com provisioner"],
            }],
        }])
        self.assertTrue(inventory["storage"]["efs_csi"])
        self.assertFalse(inventory["storage"]["ebs_csi"])

    def test_unrelated_csi_addon_sets_no_flags(self):
        inventory = merger.merge_fragments([{
            "addons": [{"name": "secrets-store-csi-driver", "evidence": []}],
        }])
        self.assertFalse(inventory["storage"]["ebs_csi"])
        self.assertFalse(inventory["storage"]["efs_csi"])

    def test_worker_provided_storage_facts_are_kept_and_unioned(self):
        inventory = merger.merge_fragments([
            {"storage": {"ebs_csi": True, "storage_classes": ["fast-io"]}},
            {"addons": [{
                "name": "aws-ebs-csi-driver",
                "evidence": ["StorageClass gp3-encrypted with provisioner ebs.csi.aws.com"],
            }]},
        ])
        self.assertTrue(inventory["storage"]["ebs_csi"])
        self.assertEqual(inventory["storage"]["storage_classes"], ["fast-io", "gp3-encrypted"])

    def test_storage_class_names_deduplicated_across_addons(self):
        inventory = merger.merge_fragments([{
            "addons": [
                {"name": "aws-ebs-csi-driver", "evidence": ["StorageClass gp3-encrypted in a.yaml"]},
                {"name": "ebs-csi-driver", "evidence": ["StorageClass gp3-encrypted in b.yaml"]},
            ],
        }])
        self.assertEqual(inventory["storage"]["storage_classes"], ["gp3-encrypted"])

    def test_non_storage_addon_evidence_is_never_mined_for_names(self):
        # Adversarial-review reproduction: "StorageClass" in a velero addon's
        # free-text evidence must not flip the planner's skipped-with-WARNING
        # net into a planned unit carrying a garbage name.
        inventory = merger.merge_fragments([{
            "addons": [{"name": "velero", "evidence": ["backup hooks; no StorageClass found here"]}],
        }])
        self.assertEqual(inventory["storage"]["storage_classes"], [])
        self.assertFalse(inventory["storage"]["ebs_csi"])

    def test_bare_prose_word_after_storageclass_is_rejected(self):
        # Even inside a storage addon's evidence, a bare all-alpha token is
        # prose, not a name ("no StorageClass found ..." -> "found").
        inventory = merger.merge_fragments([{
            "addons": [{
                "name": "ebs-csi-driver",
                "evidence": ["no StorageClass found in this chunk", "the default StorageClass is unset"],
            }],
        }])
        self.assertTrue(inventory["storage"]["ebs_csi"])
        self.assertEqual(inventory["storage"]["storage_classes"], [])

    def test_quoted_prose_word_is_trusted_as_a_name(self):
        # Quotes are an explicit naming signal, whatever the token looks like.
        inventory = merger.merge_fragments([{
            "addons": [{"name": "ebs-csi-driver", "evidence": ["StorageClass 'standard' is default"]}],
        }])
        self.assertEqual(inventory["storage"]["storage_classes"], ["standard"])

    def test_list_form_storage_fragment_is_dropped_with_a_note(self):
        # The schema's resource-level list arm is fragment-valid but the merge
        # only folds the dict summary; the drop must be visible, not silent.
        inventory = merger.merge_fragments([{
            "storage": [{"kind": "storage_class", "name": "gp3-encrypted", "provisioner": "ebs.csi.aws.com"}],
        }])
        self.assertEqual(
            inventory["storage"], {"ebs_csi": False, "efs_csi": False, "storage_classes": []}
        )
        self.assertTrue(any(n.startswith("storage: a fragment supplied") for n in inventory["merge_notes"]))


class PromoteClusterIrsaTest(unittest.TestCase):

    def test_cluster_scoped_bindings_promoted_as_strings(self):
        # The 2026-08-12 run: IRSA facts filed only under the cluster entry
        # (as objects, per the cluster schema); top-level list left empty —
        # which silently starved the workload-identity translation unit.
        inventory = merger.merge_fragments([{
            "clusters": [{
                "name": "acme-prod",
                "workloads": {
                    "irsa_bindings": [
                        {"namespace": "acme-shop", "sa": "orders",
                         "role_arn": "arn:aws:iam::869935070097:role/acme-prod-orders"},
                    ],
                },
            }],
        }])
        self.assertEqual(inventory["workloads"]["irsa_bindings"], ["acme-shop/orders"])
        # The role detail stays on the cluster entry.
        self.assertEqual(
            inventory["clusters"][0]["workloads"]["irsa_bindings"][0]["role_arn"],
            "arn:aws:iam::869935070097:role/acme-prod-orders",
        )

    def test_promotion_deduplicates_against_top_level(self):
        inventory = merger.merge_fragments([{
            "workloads": {"irsa_bindings": ["acme-shop/orders"]},
            "clusters": [{
                "name": "acme-prod",
                "workloads": {
                    "irsa_bindings": [{"namespace": "acme-shop", "sa": "orders", "role_arn": "arn:x"}],
                },
            }],
        }])
        self.assertEqual(inventory["workloads"]["irsa_bindings"], ["acme-shop/orders"])

    def test_malformed_cluster_bindings_are_skipped(self):
        inventory = merger.merge_fragments([{
            "clusters": [{
                "name": "acme-prod",
                "workloads": {"irsa_bindings": [{"namespace": "x"}, 42, "kept/as-is"]},
            }],
        }])
        self.assertEqual(inventory["workloads"]["irsa_bindings"], ["kept/as-is"])


class BlockerSignalCaptureTest(unittest.TestCase):
    """The two capture fields added for the Step 4 blocker categories."""

    def test_host_path_volumes_union_across_fragments(self):
        inventory = merger.merge_fragments([
            {"workloads": {"host_path_volumes": ["acme-shop/node-agent: /proc"]}},
            {"workloads": {"host_path_volumes": [
                "acme-shop/node-agent: /proc",
                "acme-shop/log-shipper: /var/log",
            ]}},
        ])
        self.assertEqual(
            inventory["workloads"]["host_path_volumes"],
            ["acme-shop/node-agent: /proc", "acme-shop/log-shipper: /var/log"],
        )

    def test_host_path_volumes_defaults_to_an_empty_list(self):
        # The skeleton must carry the key even when no fragment mentions it,
        # so downstream readers need no presence check.
        inventory = merger.merge_fragments([{}])
        self.assertEqual(inventory["workloads"]["host_path_volumes"], [])

    def test_cluster_secrets_kms_key_merges_as_a_scalar(self):
        inventory = merger.merge_fragments([
            {"clusters": [{"name": "acme-prod"}]},
            {"clusters": [{"name": "acme-prod",
                           "secrets_kms_key_arn": "arn:aws:kms:us-east-1:1:key/k1"}]},
        ])
        self.assertEqual(
            inventory["clusters"][0]["secrets_kms_key_arn"],
            "arn:aws:kms:us-east-1:1:key/k1",
        )

    def test_conflicting_kms_keys_keep_the_first_and_leave_a_note(self):
        inventory = merger.merge_fragments([
            {"clusters": [{"name": "acme-prod", "secrets_kms_key_arn": "arn:first"}]},
            {"clusters": [{"name": "acme-prod", "secrets_kms_key_arn": "arn:second"}]},
        ])
        self.assertEqual(inventory["clusters"][0]["secrets_kms_key_arn"], "arn:first")
        self.assertTrue(any("secrets_kms_key_arn" in n for n in inventory["merge_notes"]))


class ExistingBehaviorTest(unittest.TestCase):

    def test_triggers_still_derived_from_structured_evidence(self):
        inventory = merger.merge_fragments([{
            "autoscaling": {"karpenter": True},
            "workloads": {"privileged_daemonsets": [{"name": "node-agent"}]},
            "network": {"vpc_peering": True},
        }])
        triggers = inventory["triggers"]
        self.assertTrue(triggers["karpenter"])
        self.assertTrue(triggers["privileged_daemonsets"])
        self.assertTrue(triggers["vpc_peering"])
        self.assertFalse(triggers["gpu_tpu"])

    def test_karpenter_nodepools_default_to_an_empty_list(self):
        inventory = merger.merge_fragments([{"autoscaling": {"karpenter": True}}])
        self.assertEqual(inventory["autoscaling"]["karpenter_nodepools"], [])
        self.assertEqual(merger.merge_fragments([])["autoscaling"]["karpenter_nodepools"], [])

    def test_karpenter_nodepools_merge_by_name_across_fragments(self):
        a = {"autoscaling": {"karpenter_nodepools": [
            {"name": "shop-burst", "kind": "NodePool", "api_version": "karpenter.sh/v1beta1",
             "requirements": [{"key": "karpenter.sh/capacity-type", "operator": "In",
                               "values": ["spot", "on-demand"]}],
             "limits": {"cpu": 64}, "source_files": ["k8s/karpenter-nodepool.yaml"]}]}}
        b = {"autoscaling": {"karpenter_nodepools": [
            {"name": "shop-burst", "kind": "NodePool",
             "requirements": [{"key": "kubernetes.io/arch", "operator": "In", "values": ["amd64"]}],
             "source_files": ["k8s/karpenter-nodepool.yaml"]}]}}
        inventory = merger.merge_fragments([a, b])
        pools = inventory["autoscaling"]["karpenter_nodepools"]
        self.assertEqual(len(pools), 1)
        pool = pools[0]
        self.assertEqual(sorted(r["key"] for r in pool["requirements"]),
                         ["karpenter.sh/capacity-type", "kubernetes.io/arch"])
        self.assertEqual(pool["capacity_types"], ["spot", "on-demand"])
        self.assertEqual(pool["architectures"], ["amd64"])
        self.assertEqual(pool["instance_families"], [])
        self.assertEqual(pool["requirements_unreduced"], [])
        # Bare numbers are stringified, recorded as written and never parsed.
        self.assertEqual(pool["limits"], {"cpu": "64"})
        # A typed pool is Karpenter present: flag and trigger follow.
        self.assertTrue(inventory["autoscaling"]["karpenter"])
        self.assertTrue(inventory["triggers"]["karpenter"])

    def test_derived_lists_overwrite_worker_values(self):
        fragment = {"autoscaling": {"karpenter_nodepools": [
            {"name": "p", "kind": "NodePool", "capacity_types": ["spot"],
             "requirements": [{"key": "karpenter.sh/capacity-type", "operator": "In",
                               "values": ["on-demand"]}]}]}}
        pool = merger.merge_fragments([fragment])["autoscaling"]["karpenter_nodepools"][0]
        self.assertEqual(pool["capacity_types"], ["on-demand"])

    def test_non_in_operators_are_recorded_as_unreduced(self):
        fragment = {"autoscaling": {"karpenter_nodepools": [
            {"name": "p", "kind": "NodePool", "requirements": [
                {"key": "karpenter.k8s.aws/instance-family", "operator": "NotIn", "values": ["t3"]},
                {"key": "karpenter.k8s.aws/instance-cpu", "operator": "Gt", "values": ["8"]},
                {"key": "kubernetes.io/arch", "values": ["arm64"]}]}]}}
        pool = merger.merge_fragments([fragment])["autoscaling"]["karpenter_nodepools"][0]
        self.assertEqual(pool["instance_families"], [])
        self.assertEqual(pool["requirements_unreduced"], ["karpenter.k8s.aws/instance-family"])
        # A missing operator reads as In (the Karpenter default).
        self.assertEqual(pool["architectures"], ["arm64"])

    def test_a_provisioner_entry_is_kept_with_its_kind(self):
        fragment = {"autoscaling": {"karpenter_nodepools": [
            {"name": "default", "kind": "Provisioner", "api_version": "karpenter.sh/v1alpha5",
             "requirements": [], "disruption": {"provisioner": {"consolidation_enabled": True}}}]}}
        pool = merger.merge_fragments([fragment])["autoscaling"]["karpenter_nodepools"][0]
        self.assertEqual(pool["kind"], "Provisioner")
        self.assertEqual(pool["disruption"]["provisioner"], {"consolidation_enabled": True})
        self.assertEqual(pool["capacity_types"], [])

    def test_same_named_pools_in_different_directories_stay_apart(self):
        prod = {"autoscaling": {"karpenter_nodepools": [
            {"name": "default", "kind": "NodePool", "source_files": ["clusters/prod/karpenter.yaml"],
             "requirements": [{"key": "karpenter.sh/capacity-type", "operator": "In", "values": ["spot"]}]}]}}
        staging = {"autoscaling": {"karpenter_nodepools": [
            {"name": "default", "kind": "NodePool", "source_files": ["clusters/staging/karpenter.yaml"],
             "requirements": [{"key": "karpenter.sh/capacity-type", "operator": "In", "values": ["on-demand"]}]}]}}
        inventory = merger.merge_fragments([prod, staging])
        pools = inventory["autoscaling"]["karpenter_nodepools"]
        self.assertEqual([p["capacity_types"] for p in pools], [["spot"], ["on-demand"]])
        self.assertTrue(any("declared in more than one place" in n for n in inventory["merge_notes"]))
        # Two chunks of ONE file still fold into one entry.
        again = merger.merge_fragments([prod, prod])
        self.assertEqual(len(again["autoscaling"]["karpenter_nodepools"]), 1)

    def test_a_pool_without_source_files_folds_into_the_placed_one(self):
        placed = {"autoscaling": {"karpenter_nodepools": [
            {"name": "default", "kind": "NodePool", "source_files": ["k8s/np.yaml"],
             "requirements": [{"key": "karpenter.sh/capacity-type", "operator": "In", "values": ["spot"]}]}]}}
        bare = {"autoscaling": {"karpenter_nodepools": [
            {"name": "default", "kind": "NodePool", "limits": {"cpu": 64}}]}}
        inventory = merger.merge_fragments([placed, bare])
        pools = inventory["autoscaling"]["karpenter_nodepools"]
        self.assertEqual(len(pools), 1)
        self.assertEqual(pools[0]["limits"], {"cpu": "64"})
        self.assertFalse(any("more than one place" in n for n in inventory["merge_notes"]))
        # Type-only differences never conflict: limits are stringified before the fold.
        again = merger.merge_fragments([placed, {"autoscaling": {"karpenter_nodepools": [
            {"name": "default", "kind": "NodePool", "source_files": ["k8s/np.yaml"], "limits": {"cpu": 64}}]}},
            {"autoscaling": {"karpenter_nodepools": [
            {"name": "default", "kind": "NodePool", "source_files": ["k8s/np.yaml"], "limits": {"cpu": "64"}}]}}])
        self.assertFalse(any("conflicting" in n for n in again["merge_notes"]))

    def test_the_manual_path_derives_the_same_lists(self):
        inventory = {"autoscaling": {"karpenter": False, "karpenter_nodepools": [
            {"name": "p", "kind": "NodePool", "capacity_types": ["wrong"],
             "requirements": [{"key": "karpenter.sh/capacity-type", "operator": "In", "values": ["spot"]}]}]}}
        merger.derive_nodepool_summaries(inventory)
        self.assertEqual(inventory["autoscaling"]["karpenter_nodepools"][0]["capacity_types"], ["spot"])
        self.assertTrue(inventory["autoscaling"]["karpenter"])
        self.assertTrue(inventory["triggers"]["karpenter"])
        self.assertEqual(merger.derive_nodepool_summaries({"clusters": []}), {"clusters": []})

    def test_a_provisioner_and_a_nodepool_of_one_name_stay_apart(self):
        fragment = {"autoscaling": {"karpenter_nodepools": [
            {"name": "default", "kind": "Provisioner", "source_files": ["k8s/karpenter.yaml"]},
            {"name": "default", "kind": "NodePool", "source_files": ["k8s/karpenter.yaml"]}]}}
        pools = merger.merge_fragments([fragment])["autoscaling"]["karpenter_nodepools"]
        self.assertEqual(sorted(p["kind"] for p in pools), ["NodePool", "Provisioner"])

    def test_a_key_with_any_non_in_requirement_is_wholly_unreduced(self):
        fragment = {"autoscaling": {"karpenter_nodepools": [
            {"name": "p", "kind": "NodePool", "requirements": [
                {"key": "kubernetes.io/arch", "operator": "In", "values": ["amd64", "arm64"]},
                {"key": "kubernetes.io/arch", "operator": "NotIn", "values": ["arm64"]}]}]}}
        pool = merger.merge_fragments([fragment])["autoscaling"]["karpenter_nodepools"][0]
        self.assertEqual(pool["architectures"], [])
        self.assertEqual(pool["requirements_unreduced"], ["kubernetes.io/arch"])

    def test_a_realistic_fragment_validates_against_the_schema(self):
        import json, os
        try:
            import jsonschema
        except ImportError:
            self.skipTest("jsonschema not installed")
        path = os.path.join(os.path.dirname(__file__), "..", "..", "..", "dag", "server",
                            "schema", "inventory.json")
        with open(os.path.normpath(path)) as f:
            schema = json.load(f)
        fragment = {"autoscaling": {"karpenter": True, "karpenter_nodepools": [
            {"name": "shop-burst", "kind": "NodePool", "api_version": "karpenter.sh/v1beta1",
             "requirements": [{"key": "karpenter.sh/capacity-type", "operator": "In",
                               "values": ["spot", "on-demand"]},
                              {"key": "karpenter.k8s.aws/instance-cpu", "operator": "Gt", "values": [8]},
                              {"key": "karpenter.sh/nodepool", "operator": "Exists"}],
             "weight": "10", "taints": [{"key": "team", "value": 1, "effect": "NoSchedule"}],
             "labels": None, "limits": {"cpu": 64, "memory": "256Gi"}, "disruption": None,
             "node_class_ref": {"name": "default"}, "source_files": ["k8s/np.yaml"]},
            {"name": "from-values", "kind": None, "requirements": None}]}}
        jsonschema.validate(fragment, schema)

    def test_nameless_nodepool_entries_are_dropped_like_other_named_lists(self):
        fragment = {"autoscaling": {"karpenter_nodepools": [{"kind": "NodePool"}]}}
        self.assertEqual(merger.merge_fragments([fragment])["autoscaling"]["karpenter_nodepools"], [])

    def test_schema_declares_the_nodepool_field_under_autoscaling(self):
        import json, os
        path = os.path.join(os.path.dirname(__file__), "..", "..", "..", "dag", "server",
                            "schema", "inventory.json")
        with open(os.path.normpath(path)) as f:
            schema = json.load(f)
        props = schema["properties"]["autoscaling"]["properties"]["karpenter_nodepools"]
        fields = set(props["items"]["properties"])
        for name in ("name", "kind", "api_version", "requirements", "capacity_types",
                     "instance_families", "architectures", "requirements_unreduced", "weight",
                     "taints", "labels", "limits", "disruption", "node_class_ref", "source_files"):
            self.assertIn(name, fields)
        self.assertEqual(props["items"]["required"], ["name"])

    def test_non_dict_fragments_ignored(self):
        inventory = merger.merge_fragments([None, "junk", {"addons": [{"name": "karpenter"}]}])
        self.assertEqual(len(inventory["addons"]), 1)

    def test_deterministic(self):
        fragments = [
            {"addons": [{"name": "aws-ebs-csi-driver", "evidence": ["StorageClass gp3-encrypted x"]}]},
            {"clusters": [{"name": "acme-prod", "workloads": {"irsa_bindings": [
                {"namespace": "acme-shop", "sa": "orders", "role_arn": "arn:x"}]}}]},
        ]
        self.assertEqual(merger.merge_fragments(fragments), merger.merge_fragments(fragments))

    def test_repairs_are_recorded_in_merge_notes(self):
        inventory = merger.merge_fragments([{
            "addons": [{
                "name": "ebs-csi-driver",
                "evidence": ["StorageClass 'gp3-encrypted' uses provisioner ebs.csi.aws.com"],
            }],
            "clusters": [{"name": "acme-prod", "workloads": {"irsa_bindings": [
                {"namespace": "acme-shop", "sa": "orders", "role_arn": "arn:x"}]}}],
        }])
        notes = " | ".join(inventory["merge_notes"])
        self.assertIn("storage.ebs_csi: derived from addon 'ebs-csi-driver'", notes)
        self.assertIn("storage.storage_classes: recovered ['gp3-encrypted']", notes)
        self.assertIn("workloads.irsa_bindings: promoted ['acme-shop/orders']", notes)

    def test_no_repair_notes_when_workers_filed_facts_correctly(self):
        inventory = merger.merge_fragments([{
            "storage": {"ebs_csi": True, "storage_classes": ["gp3-encrypted"]},
            "workloads": {"irsa_bindings": ["acme-shop/orders"]},
            "addons": [{
                "name": "aws-ebs-csi-driver",
                "evidence": ["StorageClass gp3-encrypted with provisioner ebs.csi.aws.com"],
            }],
            "clusters": [{"name": "acme-prod", "workloads": {"irsa_bindings": [
                {"namespace": "acme-shop", "sa": "orders", "role_arn": "arn:x"}]}}],
        }])
        self.assertEqual(inventory["merge_notes"], [])


if __name__ == "__main__":
    unittest.main()
