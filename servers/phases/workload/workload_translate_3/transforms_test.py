"""Unit tests for the deterministic workload transforms ([PLANNED] pass).

The full decision table per transform: image hit / self_service / miss /
null map, swap hit / missing binding / null bindings / non-SA untouched,
className present / absent / menu-null, strip only-after-swap, composition
order, idempotence, unknown documents byte-identical.
"""

import copy
import json
import unittest

from servers.phases.workload.workload_translate_3 import transforms

IMAGE_MAP = {
    "ecr/app:1": {"dest_ref": "ar/app:1", "status": "replicated"},
    "ecr/self:2": {"dest_ref": "ar/self:2", "status": "self_service"},
    "ecr/unresolved:3": {"dest_ref": None, "status": "self_service"},
}

EXPORTS = {
    "artifact_registry": {"destinations": ["ar"], "image_map": IMAGE_MAP},
    "gsa_bindings": {"acme-shop/orders": "orders@p.iam.gserviceaccount.com"},
    "storage_class_menu": ["gp3-encrypted"],
}


def deployment(image, kind="Deployment"):
    return {"apiVersion": "apps/v1", "kind": kind,
            "metadata": {"name": "app", "namespace": "ns"},
            "spec": {"template": {"spec": {
                "containers": [{"name": "c", "image": image}]}}}}


def service_account(name="orders", namespace="acme-shop", annotations=None):
    metadata = {"name": name} if name else {}
    if namespace:
        metadata["namespace"] = namespace
    if annotations is not None:
        metadata["annotations"] = annotations
    return {"apiVersion": "v1", "kind": "ServiceAccount", "metadata": metadata}


IRSA = {"eks.amazonaws.com/role-arn": "arn:aws:iam::1:role/orders",
        "eks.amazonaws.com/audience": "sts.amazonaws.com",
        "eks.amazonaws.com/sts-regional-endpoints": "true"}


class ImageRewriteTest(unittest.TestCase):

    def categories(self, findings):
        return [f["category"] for f in findings]

    def test_replicated_hit_rewrites(self):
        new, findings = transforms.rewrite_image_references(
            deployment("ecr/app:1"), IMAGE_MAP)
        self.assertEqual(
            new["spec"]["template"]["spec"]["containers"][0]["image"],
            "ar/app:1")
        self.assertEqual(self.categories(findings), ["change"])

    def test_self_service_is_flagged_but_never_written(self):
        """A non-replicated dest_ref is a plan, not an address: writing it
        would produce a manifest that reviews clean and then fails to pull."""
        original = deployment("ecr/self:2")
        new, findings = transforms.rewrite_image_references(
            copy.deepcopy(original), IMAGE_MAP)
        self.assertEqual(new, original)
        self.assertEqual(self.categories(findings), ["open_question"])
        self.assertIn("self_service", findings[0]["detail"])
        self.assertIn("ar/self:2", findings[0]["detail"])  # named, not written

    def test_an_unknown_status_is_treated_as_not_replicated(self):
        image_map = {"ecr/x:1": {"dest_ref": "ar/x:1", "status": "failed"}}
        original = deployment("ecr/x:1")
        new, findings = transforms.rewrite_image_references(
            copy.deepcopy(original), image_map)
        self.assertEqual(new, original)
        self.assertEqual(self.categories(findings), ["open_question"])
        self.assertIn("'failed'", findings[0]["detail"])

    def test_unresolved_dest_stays_untouched(self):
        original = deployment("ecr/unresolved:3")
        new, findings = transforms.rewrite_image_references(
            copy.deepcopy(original), IMAGE_MAP)
        self.assertEqual(new, original)
        self.assertEqual(self.categories(findings), ["open_question"])
        self.assertIn("unresolved", findings[0]["detail"])

    def test_unmapped_ref_stays_untouched_with_finding(self):
        original = deployment("ecr/ghost:9")
        new, findings = transforms.rewrite_image_references(
            copy.deepcopy(original), IMAGE_MAP)
        self.assertEqual(new, original)
        self.assertEqual(self.categories(findings), ["open_question"])
        self.assertIn("no image_map entry", findings[0]["detail"])

    def test_null_map_rewrites_nothing_with_one_finding(self):
        original = deployment("ecr/app:1")
        new, findings = transforms.rewrite_image_references(
            copy.deepcopy(original), None)
        self.assertEqual(new, original)
        self.assertEqual(self.categories(findings), ["open_question"])
        self.assertIn("unpublished", findings[0]["detail"])

    def test_cronjob_and_pod_paths_are_walked(self):
        cron = {"apiVersion": "batch/v1", "kind": "CronJob",
                "metadata": {"name": "j"},
                "spec": {"jobTemplate": {"spec": {"template": {"spec": {
                    "initContainers": [{"name": "i", "image": "ecr/app:1"}]}}}}}}
        new, findings = transforms.rewrite_image_references(cron, IMAGE_MAP)
        self.assertEqual(
            new["spec"]["jobTemplate"]["spec"]["template"]["spec"]
            ["initContainers"][0]["image"], "ar/app:1")
        pod = {"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "p"},
               "spec": {"containers": [{"name": "c", "image": "ecr/app:1"}]}}
        new, _ = transforms.rewrite_image_references(pod, IMAGE_MAP)
        self.assertEqual(new["spec"]["containers"][0]["image"], "ar/app:1")

    def test_kind_outside_the_path_table_is_byte_identical(self):
        cr = {"apiVersion": "x/v1", "kind": "FancyCR",
              "metadata": {"name": "x"},
              "spec": {"image": "ecr/app:1"}}
        new, findings = transforms.rewrite_image_references(
            copy.deepcopy(cr), IMAGE_MAP)
        self.assertEqual(new, cr)
        self.assertEqual(findings, [])


class SwapTest(unittest.TestCase):

    BINDINGS = EXPORTS["gsa_bindings"]

    def test_hit_swaps_role_arn_for_wi_annotation(self):
        new, findings = transforms.swap_irsa_annotation(
            service_account(annotations=dict(IRSA)), self.BINDINGS)
        annotations = new["metadata"]["annotations"]
        self.assertNotIn(transforms.ROLE_ARN_ANNOTATION, annotations)
        self.assertEqual(annotations[transforms.WI_ANNOTATION],
                         "orders@p.iam.gserviceaccount.com")
        self.assertEqual([f["category"] for f in findings], ["change"])
        # Companions are the strip transform's job, not the swap's.
        self.assertIn("eks.amazonaws.com/audience", annotations)

    def test_missing_binding_names_the_exact_key(self):
        original = service_account(name="ghost", annotations=dict(IRSA))
        new, findings = transforms.swap_irsa_annotation(
            copy.deepcopy(original), self.BINDINGS)
        self.assertEqual(new, original)
        self.assertIn("'acme-shop/ghost'", findings[0]["detail"])

    def test_null_bindings_is_a_finding_not_a_guess(self):
        original = service_account(annotations=dict(IRSA))
        new, findings = transforms.swap_irsa_annotation(
            copy.deepcopy(original), None)
        self.assertEqual(new, original)
        self.assertIn("unpublished", findings[0]["detail"])

    def test_namespaceless_sa_is_not_guessed_at(self):
        original = service_account(namespace=None, annotations=dict(IRSA))
        new, findings = transforms.swap_irsa_annotation(
            copy.deepcopy(original), self.BINDINGS)
        self.assertEqual(new, original)
        self.assertIn("no metadata.namespace", findings[0]["detail"])

    def test_the_finding_names_the_field_that_is_actually_missing(self):
        nameless = service_account(name=None, annotations=dict(IRSA))
        _, findings = transforms.swap_irsa_annotation(nameless, self.BINDINGS)
        self.assertIn("no metadata.name", findings[0]["detail"])
        self.assertNotIn("metadata.namespace", findings[0]["detail"])
        both = service_account(name=None, namespace=None,
                               annotations=dict(IRSA))
        _, findings = transforms.swap_irsa_annotation(both, self.BINDINGS)
        self.assertIn("metadata.name and metadata.namespace",
                      findings[0]["detail"])

    def test_non_service_account_untouched_no_findings(self):
        original = deployment("ecr/app:1")
        new, findings = transforms.swap_irsa_annotation(
            copy.deepcopy(original), self.BINDINGS)
        self.assertEqual(new, original)
        self.assertEqual(findings, [])

    def test_sa_without_irsa_untouched_no_findings(self):
        original = service_account(annotations={})
        new, findings = transforms.swap_irsa_annotation(
            copy.deepcopy(original), self.BINDINGS)
        self.assertEqual(new, original)
        self.assertEqual(findings, [])


class ClassNameCheckTest(unittest.TestCase):

    MENU = EXPORTS["storage_class_menu"]

    def pvc(self, class_name):
        spec = {"storageClassName": class_name} if class_name else {}
        return {"apiVersion": "v1", "kind": "PersistentVolumeClaim",
                "metadata": {"name": "cache"}, "spec": spec}

    def test_member_is_ok_absent_is_warning(self):
        _, ok = transforms.check_storage_class_references(
            self.pvc("gp3-encrypted"), self.MENU)
        self.assertEqual([f["category"] for f in ok], ["ok"])
        _, warn = transforms.check_storage_class_references(
            self.pvc("io2-fast"), self.MENU)
        self.assertEqual([f["category"] for f in warn], ["warning"])
        self.assertIn("ABSENT", warn[0]["detail"])

    def test_a_non_string_menu_entry_renders_instead_of_raising(self):
        """exports is customer-shaped: a menu holding a null or an int must
        still produce a readable warning, not a TypeError mid-pass."""
        _, findings = transforms.check_storage_class_references(
            self.pvc("io2-fast"), ["gp3-encrypted", None, 7])
        self.assertEqual([f["category"] for f in findings], ["warning"])
        self.assertIn("gp3-encrypted, None, 7", findings[0]["detail"])

    def test_null_menu_is_one_skipped_finding(self):
        _, findings = transforms.check_storage_class_references(
            self.pvc("gp3-encrypted"), None)
        self.assertEqual(len(findings), 1)
        self.assertIn("check was skipped", findings[0]["detail"])

    def test_statefulset_vcts_are_checked(self):
        sts = {"apiVersion": "apps/v1", "kind": "StatefulSet",
               "metadata": {"name": "db"},
               "spec": {"volumeClaimTemplates": [
                   {"metadata": {"name": "data"},
                    "spec": {"storageClassName": "io2-fast"}}]}}
        _, findings = transforms.check_storage_class_references(sts, self.MENU)
        self.assertEqual([f["category"] for f in findings], ["warning"])
        self.assertIn("data", findings[0]["detail"])

    def test_never_mutates_and_no_verdicts_without_class_names(self):
        original = self.pvc(None)
        new, findings = transforms.check_storage_class_references(
            copy.deepcopy(original), self.MENU)
        self.assertEqual(new, original)
        self.assertEqual(findings, [])


class StripTest(unittest.TestCase):

    def test_strip_only_after_swap(self):
        # Pre-swap shape (role-arn still present): nothing is stripped.
        pre = service_account(annotations=dict(IRSA))
        new, findings = transforms.strip_aws_annotations(copy.deepcopy(pre))
        self.assertEqual(new, pre)
        self.assertEqual(findings, [])
        # Post-swap shape: companions are stripped, each with a finding.
        swapped, _ = transforms.swap_irsa_annotation(
            pre, EXPORTS["gsa_bindings"])
        stripped, strip_findings = transforms.strip_aws_annotations(swapped)
        annotations = stripped["metadata"]["annotations"]
        for key in transforms.IRSA_COMPANION_ANNOTATIONS:
            self.assertNotIn(key, annotations)
        self.assertEqual([f["category"] for f in strip_findings],
                         ["change", "change"])
        self.assertIn(transforms.WI_ANNOTATION, annotations)

    def test_alb_annotations_are_never_touched(self):
        ingress = {"apiVersion": "networking.k8s.io/v1", "kind": "Ingress",
                   "metadata": {"name": "web", "annotations": {
                       "alb.ingress.kubernetes.io/scheme": "internet-facing"}}}
        new, findings = transforms.strip_aws_annotations(copy.deepcopy(ingress))
        self.assertEqual(new, ingress)
        self.assertEqual(findings, [])

    def test_unswapped_sa_keeps_every_eks_annotation(self):
        # No WI annotation: the swap did not run; nothing may be stripped.
        sa = service_account(annotations={
            "eks.amazonaws.com/audience": "sts.amazonaws.com"})
        new, findings = transforms.strip_aws_annotations(copy.deepcopy(sa))
        self.assertEqual(new, sa)
        self.assertEqual(findings, [])


class CompositionTest(unittest.TestCase):

    def docs(self):
        return [
            service_account(annotations=dict(IRSA)),
            deployment("ecr/app:1"),
            {"apiVersion": "v1", "kind": "PersistentVolumeClaim",
             "metadata": {"name": "cache"},
             "spec": {"storageClassName": "gp3-encrypted"}},
            {"apiVersion": "v1", "kind": "ConfigMap",
             "metadata": {"name": "cfg"}, "data": {"k": "v"}},
        ]

    def test_fixed_order_swap_strip_images_class_check(self):
        out, findings = transforms.apply_transforms(self.docs(), EXPORTS)
        annotations = out[0]["metadata"]["annotations"]
        self.assertEqual(annotations, {transforms.WI_ANNOTATION:
                                       "orders@p.iam.gserviceaccount.com"})
        self.assertEqual(
            out[1]["spec"]["template"]["spec"]["containers"][0]["image"],
            "ar/app:1")
        per_transform = [f["transform"] for f in findings]
        self.assertEqual(per_transform, [
            "swap_irsa_annotation", "strip_aws_annotations",
            "strip_aws_annotations", "rewrite_image_references",
            "check_storage_class_references"])

    def test_idempotent_on_documents(self):
        once, _ = transforms.apply_transforms(self.docs(), EXPORTS)
        twice, _ = transforms.apply_transforms(
            copy.deepcopy(once), EXPORTS)
        self.assertEqual(json.dumps(once, sort_keys=True),
                         json.dumps(twice, sort_keys=True))

    def test_unknown_documents_come_back_byte_identical(self):
        stranger = {"apiVersion": "x/v1", "kind": "Mystery",
                    "metadata": {"name": "m"}, "spec": {"image": "ecr/app:1"}}
        out, findings = transforms.apply_transforms(
            [copy.deepcopy(stranger)], EXPORTS)
        self.assertEqual(out, [stranger])
        self.assertEqual(findings, [])

    def test_null_exports_degrades_per_transform(self):
        out, findings = transforms.apply_transforms(self.docs(), None)
        self.assertEqual(json.dumps(out[1], sort_keys=True),
                         json.dumps(self.docs()[1], sort_keys=True))
        details = " | ".join(f["detail"] for f in findings)
        self.assertIn("gsa_bindings is unpublished", details)
        self.assertIn("image_map is unpublished", details)
        self.assertIn("check was skipped", details)



class PodDnsFieldsTest(unittest.TestCase):
    """The shared pod DNS reader: planner facts and validate contract read
    the same fields through it."""

    def test_fields_are_normalised(self):
        spec = {"dnsPolicy": "None", "hostNetwork": True,
                "dnsConfig": {"nameservers": ["10.1.1.1", 2], "searches": ["a.b"],
                              "options": [{"name": "ndots", "value": 2}, {"name": "single-request-reopen"}, "junk"]},
                "hostAliases": [{"ip": "10.0.0.5", "hostnames": ["db", "db.x"]}, "junk"]}
        fields = transforms.pod_dns_fields(spec)
        self.assertEqual(fields["dns_policy"], "None")
        self.assertTrue(fields["host_network"])
        self.assertEqual(fields["nameservers"], ["10.1.1.1", "2"])
        self.assertEqual(fields["options"], [{"name": "ndots", "value": "2"},
                                             {"name": "single-request-reopen", "value": None}])
        self.assertEqual(fields["host_aliases"], [{"ip": "10.0.0.5", "hostnames": ["db", "db.x"]}])
        self.assertTrue(transforms.dns_bearing(fields))

    def test_wrong_shapes_read_as_absent(self):
        fields = transforms.pod_dns_fields({"dnsPolicy": ["x"], "dnsConfig": "nope", "hostAliases": {}})
        self.assertEqual(fields, {"dns_policy": None, "host_network": False, "nameservers": [],
                                  "searches": [], "options": [], "host_aliases": []})
        self.assertFalse(transforms.dns_bearing(fields))

    def test_nodes_are_found_at_any_depth_but_not_in_value_bags(self):
        doc = {"kind": "Rollout", "metadata": {"annotations": {"dnsPolicy": "not a pod"}},
               "spec": {"strategy": {}, "template": {"spec": {"dnsPolicy": "Default",
                        "containers": [{"name": "a"}]}}},
               "data": {"dnsConfig": "text"}}
        nodes = transforms.pod_dns_nodes(doc)
        self.assertEqual([(p, n["dnsPolicy"]) for p, n in nodes], [("spec.template.spec", "Default")])
        self.assertIs(transforms.node_at(doc, "spec.template.spec"), nodes[0][1])
        self.assertIsNone(transforms.node_at(doc, "spec.nope.spec"))
        self.assertEqual(transforms.pod_dns_nodes(deployment("ecr/app:1")), [])
        listed = {"kind": "List", "items": [{"kind": "Pod", "spec": {"hostNetwork": True, "containers": []}}]}
        self.assertEqual([p for p, _ in transforms.pod_dns_nodes(listed)], ["items.0.spec"])
        psp = {"kind": "PodSecurityPolicy", "spec": {"hostNetwork": True, "privileged": True}}
        self.assertEqual(transforms.pod_dns_nodes(psp), [])  # a DNS key without containers is not a pod

    def test_host_network_strings(self):
        self.assertTrue(transforms.pod_dns_fields({"hostNetwork": "true"})["host_network"])
        self.assertFalse(transforms.pod_dns_fields({"hostNetwork": "false"})["host_network"])
        self.assertFalse(transforms.pod_dns_fields({"hostNetwork": 1})["host_network"])


def placed(node_selector=None, affinity=None, tolerations=None, kind="Deployment", spread=None):
    spec = {"containers": [{"name": "c", "image": "img"}]}
    if node_selector is not None:
        spec["nodeSelector"] = node_selector
    if affinity is not None:
        spec["affinity"] = affinity
    if tolerations is not None:
        spec["tolerations"] = tolerations
    if spread is not None:
        spec["topologySpreadConstraints"] = spread
    return {"apiVersion": "apps/v1", "kind": kind,
            "metadata": {"name": "app", "namespace": "ns"},
            "spec": {"template": {"spec": spec}}}


def _spec(doc):
    return doc["spec"]["template"]["spec"]


def required(*exprs):
    return {"nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": {
        "nodeSelectorTerms": [{"matchExpressions": list(exprs)}]}}}


OD_EXPR = {"key": "cloud.google.com/gke-spot", "operator": "DoesNotExist"}


class KarpenterPlacementTest(unittest.TestCase):
    MENU = ["shop-burst"]

    def rewrite(self, doc, menu=MENU, generation=1):
        return transforms.rewrite_karpenter_node_placement(doc, menu, generation)

    def cats(self, findings):
        return [f["category"] for f in findings]

    def test_pool_selector_swaps_to_the_compute_class_label_when_listed(self):
        new, findings = self.rewrite(placed({"karpenter.sh/nodepool": "shop-burst"}))
        self.assertEqual(_spec(new)["nodeSelector"], {"cloud.google.com/compute-class": "shop-burst"})
        self.assertEqual(self.cats(findings), ["change"])
        new2, findings2 = self.rewrite(placed({"karpenter.sh/provisioner-name": "shop-burst"}))
        self.assertEqual(_spec(new2)["nodeSelector"], {"cloud.google.com/compute-class": "shop-burst"})

    def test_pool_not_in_the_menu_is_an_open_question_never_a_guess(self):
        doc = placed({"karpenter.sh/nodepool": "other"})
        new, findings = self.rewrite(doc)
        self.assertIs(new, doc)
        self.assertEqual(self.cats(findings), ["open_question"])
        self.assertIn("names no published ComputeClass", findings[0]["detail"])
        self.assertIn("shop-burst", findings[0]["detail"])
        # An empty menu (the NAP or Autopilot arm) reads the same way.
        _, findings = self.rewrite(doc, menu=[])
        self.assertIn("empty", findings[0]["detail"])
        self.assertIn("NAP or Autopilot", findings[0]["detail"])

    def test_null_menu_is_worded_by_the_translation_generation(self):
        doc = placed({"karpenter.sh/nodepool": "shop-burst"})
        _, before = self.rewrite(doc, menu=None, generation=0)
        self.assertEqual(self.cats(before), ["open_question"])
        self.assertIn("has not published yet", before[0]["detail"])
        _, after = self.rewrite(doc, menu=None, generation=3)
        self.assertIn("published before compute_classes existed, or compute-class units are still pending",
                      after[0]["detail"])
        self.assertIn("refresh_exports", after[0]["detail"])

    def test_capacity_type_spot_becomes_gke_spot(self):
        new, findings = self.rewrite(placed({"karpenter.sh/capacity-type": "spot"}))
        self.assertEqual(_spec(new)["nodeSelector"], {"cloud.google.com/gke-spot": "true"})
        self.assertEqual(self.cats(findings), ["change"])

    def test_capacity_type_on_demand_becomes_a_does_not_exist_affinity(self):
        new, findings = self.rewrite(placed({"karpenter.sh/capacity-type": "on-demand",
                                             "karpenter.sh/nodepool": "shop-burst"}))
        self.assertEqual(_spec(new)["nodeSelector"], {"cloud.google.com/compute-class": "shop-burst"})
        terms = _spec(new)["affinity"]["nodeAffinity"]["requiredDuringSchedulingIgnoredDuringExecution"]["nodeSelectorTerms"]
        self.assertEqual(terms, [{"matchExpressions": [OD_EXPR]}])
        self.assertEqual(self.cats(findings), ["change", "change"])
        # Alone, the selector map goes away entirely.
        new, _ = self.rewrite(placed({"karpenter.sh/capacity-type": "on-demand"}))
        self.assertNotIn("nodeSelector", _spec(new))

    def test_on_demand_from_the_selector_joins_every_existing_required_term(self):
        affinity = {"nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": {
            "nodeSelectorTerms": [{"matchExpressions": [{"key": "zone", "operator": "In", "values": ["a"]}]},
                                  {"matchExpressions": [{"key": "zone", "operator": "In", "values": ["b"]}]}]}}}
        new, _ = self.rewrite(placed({"karpenter.sh/capacity-type": "on-demand"}, affinity=affinity))
        terms = _spec(new)["affinity"]["nodeAffinity"]["requiredDuringSchedulingIgnoredDuringExecution"]["nodeSelectorTerms"]
        for term in terms:
            self.assertIn(OD_EXPR, term["matchExpressions"])
            self.assertEqual(len(term["matchExpressions"]), 2)

    def test_on_demand_inside_one_or_term_is_replaced_in_place_only(self):
        # Terms are OR'd: the constraint must not narrow the other alternative.
        affinity = {"nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": {
            "nodeSelectorTerms": [
                {"matchExpressions": [{"key": "karpenter.sh/capacity-type", "operator": "In", "values": ["on-demand"]}]},
                {"matchExpressions": [{"key": "zone", "operator": "In", "values": ["b"]}]}]}}}
        new, findings = self.rewrite(placed(affinity=affinity))
        terms = _spec(new)["affinity"]["nodeAffinity"]["requiredDuringSchedulingIgnoredDuringExecution"]["nodeSelectorTerms"]
        self.assertEqual(terms, [{"matchExpressions": [OD_EXPR]},
                                 {"matchExpressions": [{"key": "zone", "operator": "In", "values": ["b"]}]}])
        self.assertEqual(self.cats(findings), ["change"])

    def test_a_disagreeing_gke_key_already_present_is_an_open_question_not_an_overwrite(self):
        doc = placed({"karpenter.sh/nodepool": "shop-burst", "cloud.google.com/compute-class": "other"})
        new, findings = self.rewrite(doc)
        self.assertIs(new, doc)
        self.assertEqual(self.cats(findings), ["open_question"])
        doc = placed({"karpenter.sh/capacity-type": "on-demand", "cloud.google.com/gke-spot": "true"})
        new, findings = self.rewrite(doc)
        self.assertIs(new, doc)
        self.assertEqual(self.cats(findings), ["open_question"])
        # An agreeing pair is a plain rewrite (the duplicate collapses).
        doc = placed({"karpenter.sh/nodepool": "shop-burst", "cloud.google.com/compute-class": "shop-burst"})
        new, findings = self.rewrite(doc)
        self.assertEqual(_spec(new)["nodeSelector"], {"cloud.google.com/compute-class": "shop-burst"})

    def test_pod_affinity_topology_keys_are_open_questions(self):
        affinity = {"podAntiAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": [
            {"topologyKey": "karpenter.sh/nodepool", "labelSelector": {}}]}}
        doc = placed(affinity=affinity)
        new, findings = self.rewrite(doc)
        self.assertIs(new, doc)
        self.assertEqual(self.cats(findings), ["open_question"])

    def test_unknown_capacity_value_is_an_open_question(self):
        doc = placed({"karpenter.sh/capacity-type": "reserved"})
        new, findings = self.rewrite(doc)
        self.assertIs(new, doc)
        self.assertEqual(self.cats(findings), ["open_question"])

    def test_aws_instance_type_is_an_open_question_and_a_gce_shape_is_not(self):
        doc = placed({"node.kubernetes.io/instance-type": "m6i.4xlarge"})
        new, findings = self.rewrite(doc)
        self.assertIs(new, doc)
        self.assertEqual(self.cats(findings), ["open_question"])
        self.assertIn("AWS instance type", findings[0]["detail"])
        gce = placed({"node.kubernetes.io/instance-type": "n4-standard-16"})
        self.assertEqual(self.rewrite(gce), (gce, []))

    def test_arch_is_portable_and_silent(self):
        doc = placed({"kubernetes.io/arch": "arm64"})
        self.assertEqual(self.rewrite(doc), (doc, []))

    def test_required_affinity_in_with_one_value_is_rewritten(self):
        doc = placed(affinity=required(
            {"key": "karpenter.sh/nodepool", "operator": "In", "values": ["shop-burst"]},
            {"key": "karpenter.sh/capacity-type", "operator": "In", "values": ["spot"]},
            {"key": "topology.kubernetes.io/zone", "operator": "In", "values": ["us-east-1a"]}))
        new, findings = self.rewrite(doc)
        exprs = _spec(new)["affinity"]["nodeAffinity"]["requiredDuringSchedulingIgnoredDuringExecution"]["nodeSelectorTerms"][0]["matchExpressions"]
        self.assertEqual([e["key"] for e in exprs],
                         ["cloud.google.com/compute-class", "cloud.google.com/gke-spot", "topology.kubernetes.io/zone"])
        self.assertEqual(exprs[1]["values"], ["true"])
        self.assertEqual(self.cats(findings), ["change", "change"])

    def test_other_operators_and_multi_values_are_open_questions(self):
        for expr in ({"key": "karpenter.sh/nodepool", "operator": "NotIn", "values": ["x"]},
                     {"key": "karpenter.sh/capacity-type", "operator": "Exists"},
                     {"key": "karpenter.sh/nodepool", "operator": "In", "values": ["a", "b"]}):
            with self.subTest(expr=expr):
                doc = placed(affinity=required(expr))
                new, findings = self.rewrite(doc)
                self.assertIs(new, doc)
                self.assertEqual(self.cats(findings), ["open_question"])

    def test_a_disagreeing_gke_key_inside_a_term_is_an_open_question(self):
        doc = placed(affinity=required(
            {"key": "karpenter.sh/nodepool", "operator": "In", "values": ["shop-burst"]},
            {"key": "cloud.google.com/compute-class", "operator": "In", "values": ["other"]}))
        new, findings = self.rewrite(doc)
        self.assertIs(new, doc)
        self.assertEqual(self.cats(findings), ["open_question"])
        doc = placed(affinity=required(
            {"key": "karpenter.sh/capacity-type", "operator": "In", "values": ["spot"]},
            OD_EXPR))
        new, findings = self.rewrite(doc)
        self.assertIs(new, doc)
        self.assertEqual(self.cats(findings), ["open_question"])

    def test_two_pool_keys_in_one_selector_do_not_collapse_silently(self):
        doc = placed({"karpenter.sh/nodepool": "shop-burst", "karpenter.sh/provisioner-name": "other"})
        new, findings = self.rewrite(doc, menu=["shop-burst", "other"])
        self.assertIn("open_question", self.cats(findings))
        self.assertEqual(_spec(new)["nodeSelector"].get("karpenter.sh/provisioner-name"), "other")

    def test_a_pre_existing_empty_term_is_left_alone(self):
        affinity = {"nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": {
            "nodeSelectorTerms": [{"matchExpressions": []}]}}}
        doc = placed(affinity=affinity, tolerations=[{"key": "karpenter.sh/disruption", "operator": "Exists"}])
        new, findings = self.rewrite(doc)
        self.assertEqual(self.cats(findings), ["change"])
        self.assertEqual(_spec(new)["affinity"], affinity)

    def test_on_demand_in_place_keeps_the_term_and_a_removed_pool_key_never_empties_it(self):
        doc = placed(affinity=required(
            {"key": "karpenter.sh/capacity-type", "operator": "In", "values": ["on-demand"]}))
        new, findings = self.rewrite(doc)
        terms = _spec(new)["affinity"]["nodeAffinity"]["requiredDuringSchedulingIgnoredDuringExecution"]["nodeSelectorTerms"]
        self.assertEqual(terms, [{"matchExpressions": [OD_EXPR]}])
        # A removed expression with nothing to add leaves no empty term behind.
        doc = placed(affinity=required(
            {"key": "karpenter.sh/nodepool", "operator": "In", "values": ["shop-burst"]}))
        new, _ = self.rewrite(doc)
        exprs = _spec(new)["affinity"]["nodeAffinity"]["requiredDuringSchedulingIgnoredDuringExecution"]["nodeSelectorTerms"][0]["matchExpressions"]
        self.assertEqual(exprs[0]["key"], "cloud.google.com/compute-class")

    def test_preferred_terms_and_topology_spread_are_open_questions(self):
        affinity = {"nodeAffinity": {"preferredDuringSchedulingIgnoredDuringExecution": [
            {"weight": 1, "preference": {"matchExpressions": [
                {"key": "karpenter.sh/capacity-type", "operator": "In", "values": ["spot"]}]}}]}}
        doc = placed(affinity=affinity, spread=[{"topologyKey": "karpenter.sh/nodepool", "maxSkew": 1}])
        new, findings = self.rewrite(doc)
        self.assertIs(new, doc)
        self.assertEqual(self.cats(findings), ["open_question", "open_question"])

    def test_karpenter_tolerations_are_removed_and_others_kept(self):
        doc = placed(tolerations=[{"key": "karpenter.sh/disruption", "operator": "Exists", "effect": "NoSchedule"},
                                  {"key": "team", "value": "shop", "effect": "NoSchedule"}])
        new, findings = self.rewrite(doc)
        self.assertEqual(_spec(new)["tolerations"], [{"key": "team", "value": "shop", "effect": "NoSchedule"}])
        self.assertEqual(self.cats(findings), ["change"])
        only = placed(tolerations=[{"key": "karpenter.sh/disruption", "operator": "Exists"}])
        new, _ = self.rewrite(only)
        self.assertNotIn("tolerations", _spec(new))

    def test_idempotent_and_kinds_outside_the_table_untouched(self):
        doc = placed({"karpenter.sh/nodepool": "shop-burst", "karpenter.sh/capacity-type": "on-demand"},
                     tolerations=[{"key": "karpenter.sh/disruption", "operator": "Exists"}])
        once, findings = self.rewrite(doc)
        self.assertTrue(findings)
        twice, again = self.rewrite(once)
        self.assertEqual(twice, once)
        self.assertEqual(again, [])
        svc = {"apiVersion": "v1", "kind": "Service", "metadata": {"name": "s"},
               "spec": {"selector": {"karpenter.sh/nodepool": "x"}}}
        self.assertEqual(self.rewrite(svc), (svc, []))
        cron = {"apiVersion": "batch/v1", "kind": "CronJob", "metadata": {"name": "c"},
                "spec": {"jobTemplate": {"spec": {"template": {"spec": {"nodeSelector": {"karpenter.sh/capacity-type": "spot"}}}}}}}
        new, findings = self.rewrite(cron)
        self.assertEqual(new["spec"]["jobTemplate"]["spec"]["template"]["spec"]["nodeSelector"],
                         {"cloud.google.com/gke-spot": "true"})

    def test_apply_transforms_runs_it_last_with_the_exports_menu_and_generation(self):
        exports = dict(EXPORTS, compute_classes=["shop-burst"], generations={"translation": 2})
        docs = [placed({"karpenter.sh/nodepool": "shop-burst"})]
        out, findings = transforms.apply_transforms(docs, exports)
        self.assertEqual(_spec(out[0])["nodeSelector"], {"cloud.google.com/compute-class": "shop-burst"})
        self.assertEqual(findings[-1]["transform"], "rewrite_karpenter_node_placement")
        # Without the field (an exports document from before it existed) the
        # pass degrades to an open question worded by the generation.
        out, findings = transforms.apply_transforms(docs, dict(EXPORTS, generations={"translation": 2}))
        self.assertEqual(_spec(out[0])["nodeSelector"], {"karpenter.sh/nodepool": "shop-burst"})
        self.assertIn("refresh_exports", findings[-1]["detail"])
        out, findings = transforms.apply_transforms(docs, None)
        self.assertIn("has not published yet", findings[-1]["detail"])

if __name__ == "__main__":
    unittest.main()
