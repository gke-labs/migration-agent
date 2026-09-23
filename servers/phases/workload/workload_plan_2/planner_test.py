"""Unit tests for the pure workload planning core.

No binaries, no GCS: renders are injected pre-rendered streams, files live
in a temp source root. Covers the closed classification table + residual
bucket, the four-units invariant, the StatefulSet cross-cite, the chart
carrier, kustomize remote-ref refusal, degradation paths, the exports
stamp, brief literal truth per fact regime, and determinism.
"""

import json
import os
import shutil
import tempfile
import unittest

from servers.phases.workload.workload_plan_2 import planner

INCLUDE_ALL = {"included": ["**"], "excluded": []}

EXPORTS = {
    "gateway": None,
    "cluster": {"name": "gke-1", "type": "autopilot", "location": "us-central1"},
    "node_shapes": ["default: m5.large"],
    "storage_class_menu": ["gp3-encrypted"],
    "gsa_bindings": {"acme-shop/orders": "orders@proj.iam.gserviceaccount.com"},
    "artifact_registry": {"destinations": [], "image_map": {}},
    "staging_bucket": None,
    "generated_at": "2026-08-14T00:00:00+00:00",
    "generations": {"discovery": 3, "translation": 2, "deployment": 1},
}


# The classifier keys on (apiGroup, kind), so a fixture that omits the group
# is a fixture about a DIFFERENT kind. Fixtures declare the real apiVersion.
API_VERSIONS = {
    "Deployment": "apps/v1", "DaemonSet": "apps/v1", "StatefulSet": "apps/v1",
    "HorizontalPodAutoscaler": "autoscaling/v2",
    "PodDisruptionBudget": "policy/v1",
    "Job": "batch/v1", "CronJob": "batch/v1",
    "Ingress": "networking.k8s.io/v1",
    "StorageClass": "storage.k8s.io/v1",
    "CustomResourceDefinition": "apiextensions.k8s.io/v1",
    "VirtualService": "networking.istio.io/v1beta1",
    "TargetGroupBinding": "elbv2.k8s.aws/v1beta1",
    "SecretProviderClass": "secrets-store.csi.x-k8s.io/v1",
    "ExternalSecret": "external-secrets.io/v1beta1",
}


def doc(kind, name, namespace="acme-shop", api_version=None, **extra):
    text = {"apiVersion": api_version or API_VERSIONS.get(kind, "v1"),
            "kind": kind,
            "metadata": {"name": name, "namespace": namespace}}
    text.update(extra)
    return text


def dump(*docs):
    import yaml
    return yaml.safe_dump_all(list(docs), sort_keys=False)


class PlannerTestBase(unittest.TestCase):

    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="wkld_plan_test_")
        self.addCleanup(shutil.rmtree, self.root, True)

    def write(self, rel, content):
        full = os.path.join(self.root, rel)
        os.makedirs(os.path.dirname(full), exist_ok=True)
        with open(full, "w", encoding="utf-8") as f:
            f.write(content)

    def plan(self, scope=None, exports=EXPORTS, renderers=None):
        return planner.build_workload_plan(
            "test-component", scope or INCLUDE_ALL, self.root, exports,
            renderers=renderers or {"helm": self._no_render,
                                    "kustomize": self._no_render})

    @staticmethod
    def _no_render(source_root, rel_dir):
        raise AssertionError(f"unexpected render of {rel_dir}")

    def unit(self, plan, family):
        matches = [u for u in plan["units"] if u["unit_id"] == family]
        self.assertEqual(len(matches), 1, family)
        return matches[0]


class ClassificationTest(unittest.TestCase):
    """The closed table is the classification — every row plus residuals."""

    def test_every_table_row(self):
        for (group, kind), family in planner.KIND_TO_FAMILY.items():
            with self.subTest(group=group, kind=kind):
                api_version = f"{group}/v1" if group else "v1"
                got_family, classification = planner.classify_kind(
                    kind, api_version)
                self.assertEqual(got_family, family)
                self.assertEqual(classification, "portable")

    def test_residual_kinds_land_in_manifests_as_unknown(self):
        for kind in ("Secret", "SecretProviderClass", "ExternalSecret",
                     "TargetGroupBinding", "VirtualService", "FancyAppCR",
                     "StorageClass", "Namespace", "CustomResourceDefinition"):
            with self.subTest(kind=kind):
                family, classification = planner.classify_kind(
                    kind, API_VERSIONS.get(kind, "v1"))
                self.assertEqual(family, "wkld-manifests")
                self.assertEqual(classification, "aws_coupled_or_unknown")

    def test_a_custom_kind_reusing_a_core_name_is_residual(self):
        """The table is keyed on (group, kind): an Ingress from a foreign
        group is somebody's CRD, not the routing family's Ingress."""
        for api_version in ("acme.io/v1", "networking.example.com/v1alpha1"):
            with self.subTest(api_version=api_version):
                family, classification = planner.classify_kind(
                    "Ingress", api_version)
                self.assertEqual(family, "wkld-manifests")
                self.assertEqual(classification, "aws_coupled_or_unknown")
        # And a group-less apiVersion does not smuggle a grouped kind in.
        self.assertEqual(planner.classify_kind("Deployment", "v1"),
                         ("wkld-manifests", "aws_coupled_or_unknown"))

    def test_api_group_reads_the_group_half_only(self):
        self.assertEqual(planner.api_group("apps/v1"), "apps")
        self.assertEqual(planner.api_group("v1"), "")
        self.assertEqual(planner.api_group(None), "")
        self.assertEqual(planner.api_group(7), "")
        self.assertEqual(planner.api_group("a/b/c"), "a")


class FourUnitsInvariantTest(PlannerTestBase):
    """Exactly four units, one per family, statuses per the fact regime."""

    def test_all_placeholders_when_nothing_is_in_scope(self):
        plan = self.plan()
        self.assertEqual([u["unit_id"] for u in plan["units"]],
                         list(planner.FAMILIES))
        self.assertTrue(all(u["status"] == "skipped" for u in plan["units"]))
        self.assertTrue(all(u["placeholder"] for u in plan["units"]))
        self.assertTrue(planner.all_placeholders(plan))
        for unit in plan["units"]:
            self.assertIn("Skipped placeholder", unit["notes"][0])

    def test_placeholder_skip_note_names_what_is_missing(self):
        self.write("k8s/app.yaml", dump(doc("Deployment", "web")))
        plan = self.plan()
        self.assertEqual(self.unit(plan, "wkld-manifests")["status"], "planned")
        self.assertIn("no ServiceAccount documents",
                      self.unit(plan, "wkld-identity")["notes"][0])
        self.assertIn("no PersistentVolumeClaim documents",
                      self.unit(plan, "wkld-storage")["notes"][0])
        self.assertIn("no Ingress documents",
                      self.unit(plan, "wkld-routing")["notes"][0])
        self.assertFalse(planner.all_placeholders(plan))

    def test_routing_parks_on_null_gateway(self):
        self.write("k8s/ing.yaml", dump(doc("Ingress", "web")))
        unit = self.unit(self.plan(), "wkld-routing")
        self.assertEqual(unit["status"], "parked")
        self.assertFalse(unit["placeholder"])
        self.assertTrue(any("exports.gateway is null" in n
                            for n in unit["notes"]))

    def test_routing_parks_when_exports_is_absent(self):
        self.write("k8s/ing.yaml", dump(doc("Ingress", "web")))
        unit = self.unit(self.plan(exports=None), "wkld-routing")
        self.assertEqual(unit["status"], "parked")

    def test_routing_plans_when_gateway_is_published(self):
        self.write("k8s/ing.yaml", dump(doc("Ingress", "web")))
        exports = dict(EXPORTS)
        exports["gateway"] = {"name": "shared-gw", "namespace": "gateways"}
        unit = self.unit(self.plan(exports=exports), "wkld-routing")
        self.assertEqual(unit["status"], "planned")
        self.assertTrue(any("gateways/shared-gw" in n for n in unit["notes"]))


class CrossCiteAndBriefTest(PlannerTestBase):
    """StatefulSet cross-cite + brief literal truth per fact regime."""

    def sts(self):
        return doc("StatefulSet", "db", spec={
            "template": {"spec": {"containers": [{"name": "db"}]}},
            "volumeClaimTemplates": [
                {"metadata": {"name": "data"},
                 "spec": {"storageClassName": "gp3-encrypted"}}]})

    def test_statefulset_stays_in_manifests_and_is_cited_by_storage(self):
        self.write("k8s/db.yaml", dump(self.sts()))
        self.write("k8s/pvc.yaml", dump(doc(
            "PersistentVolumeClaim", "order-cache",
            spec={"storageClassName": "gp3-encrypted"})))
        plan = self.plan()
        manifests = self.unit(plan, "wkld-manifests")
        self.assertEqual([d["kind"] for d in manifests["inputs"]["documents"]],
                         ["StatefulSet"])
        storage = self.unit(plan, "wkld-storage")
        self.assertEqual([d["kind"] for d in storage["inputs"]["documents"]],
                         ["PersistentVolumeClaim"])
        cite = [n for n in storage["notes"] if "volumeClaimTemplates" in n]
        self.assertEqual(len(cite), 1)
        self.assertIn("StatefulSet 'db'", cite[0])
        self.assertIn("k8s/db.yaml#doc0", cite[0])
        self.assertIn("wkld-manifests unit", cite[0])

    def test_sts_cite_survives_an_empty_storage_family(self):
        self.write("k8s/db.yaml", dump(self.sts()))
        storage = self.unit(self.plan(), "wkld-storage")
        self.assertEqual(storage["status"], "skipped")
        self.assertTrue(storage["placeholder"])
        self.assertIn("no PersistentVolumeClaim documents",
                      storage["notes"][0])
        self.assertTrue(any("volumeClaimTemplates" in n
                            for n in storage["notes"]))

    def test_autopilot_brief_only_when_exports_records_it(self):
        self.write("k8s/app.yaml", dump(doc("Deployment", "web")))
        autopilot = self.unit(self.plan(), "wkld-manifests")["notes"]
        self.assertTrue(any("cluster.type 'autopilot'" in n
                            for n in autopilot))
        degraded = self.unit(self.plan(exports=None), "wkld-manifests")["notes"]
        self.assertFalse(any("cluster.type 'autopilot'" in n
                             for n in degraded))
        self.assertTrue(any("does not publish the target cluster type" in n
                            for n in degraded))
        self.assertTrue(any("Autopilot defaults" in n for n in degraded))

    def test_node_placement_facts_are_persisted_and_briefed_against_the_menu(self):
        self.write("k8s/app.yaml", dump(doc("Deployment", "web", spec={
            "template": {"spec": {
                "nodeSelector": {"karpenter.sh/nodepool": "shop-burst",
                                 "karpenter.sh/capacity-type": "on-demand",
                                 "kubernetes.io/arch": "amd64", "team": "shop"},
                "tolerations": [{"key": "karpenter.sh/disruption", "operator": "Exists"},
                                {"key": "team", "value": "shop"}],
                "containers": [{"name": "a"}]}}})))
        self.write("k8s/quiet.yaml", dump(doc("Deployment", "quiet", spec={
            "template": {"spec": {"nodeSelector": {"team": "shop"},
                                  "containers": [{"name": "q"}]}}})))
        exports = dict(EXPORTS, compute_classes=["shop-burst"])
        unit = self.unit(self.plan(exports=exports), "wkld-manifests")
        facts = unit["inputs"]["node_placement_facts"]
        self.assertEqual(len(facts), 1)
        self.assertEqual(facts[0]["label"], "web")
        self.assertEqual(facts[0]["node_selector"],
                         {"karpenter.sh/nodepool": "shop-burst", "karpenter.sh/capacity-type": "on-demand"})
        self.assertEqual(facts[0]["tolerations"][0]["key"], "karpenter.sh/disruption")
        self.assertEqual(len(facts[0]["tolerations"]), 1)
        notes = "\n".join(unit["notes"])
        self.assertIn("IS a published ComputeClass", notes)
        self.assertIn("karpenter.sh/capacity-type, karpenter.sh/disruption", notes)
        absent = self.unit(self.plan(exports=dict(EXPORTS, compute_classes=[])), "wkld-manifests")
        self.assertIn("ABSENT from exports compute_classes (empty)", "\n".join(absent["notes"]))
        unpublished = self.unit(self.plan(), "wkld-manifests")
        self.assertIn("compute_classes is unpublished", "\n".join(unpublished["notes"]))
        # The literal key list stays pinned to the transform's.
        from servers.phases.workload.workload_translate_3 import transforms
        self.assertEqual(planner.KARPENTER_PLACEMENT_KEYS, transforms.KARPENTER_PLACEMENT_KEYS)
        self.assertEqual(planner.KARPENTER_TOLERATION_PREFIX, transforms.KARPENTER_TOLERATION_PREFIX)

    def test_no_placement_facts_no_placement_lines(self):
        self.write("k8s/app.yaml", dump(doc("Deployment", "web")))
        unit = self.unit(self.plan(), "wkld-manifests")
        self.assertEqual(unit["inputs"]["node_placement_facts"], [])
        self.assertFalse(any("Karpenter" in n for n in unit["notes"]))

    def test_daemonset_host_facts_only_when_the_booleans_are_set(self):
        self.write("k8s/agent.yaml", dump(doc("DaemonSet", "agent", spec={
            "template": {"spec": {
                "hostNetwork": True, "hostPID": True,
                "containers": [{"name": "a",
                                "securityContext": {"privileged": True}}]}}})))
        self.write("k8s/quiet.yaml", dump(doc("DaemonSet", "quiet", spec={
            "template": {"spec": {"containers": [{"name": "q"}]}}})))
        notes = self.unit(self.plan(), "wkld-manifests")["notes"]
        agent = [n for n in notes if "DaemonSet 'agent'" in n]
        self.assertEqual(len(agent), 1)
        for flag in ("hostNetwork", "hostPID", "privileged"):
            self.assertIn(flag, agent[0])
        self.assertFalse(any("DaemonSet 'quiet'" in n for n in notes))

    def test_identity_brief_names_hit_and_miss_bindings(self):
        self.write("k8s/sa.yaml", dump(
            doc("ServiceAccount", "orders",
                **{"metadata": {"name": "orders", "namespace": "acme-shop",
                                "annotations": {"eks.amazonaws.com/role-arn":
                                                "arn:aws:iam::1:role/x"}}}),
            doc("ServiceAccount", "ghost", **{
                "metadata": {"name": "ghost", "namespace": "acme-shop",
                             "annotations": {"eks.amazonaws.com/role-arn":
                                             "arn:aws:iam::1:role/y"}}})))
        notes = self.unit(self.plan(), "wkld-identity")["notes"]
        self.assertTrue(any("'acme-shop/orders' -> orders@proj.iam"
                            in n for n in notes))
        self.assertTrue(any("no entry for 'acme-shop/ghost'" in n
                            for n in notes))
        self.assertTrue(any("Ghost rule" in n for n in notes))
        self.assertTrue(any("Co-existence" in n for n in notes))

    def test_identity_brief_with_null_bindings_says_unpublished(self):
        self.write("k8s/sa.yaml", dump(doc("ServiceAccount", "orders", **{
            "metadata": {"name": "orders", "namespace": "acme-shop",
                         "annotations": {"eks.amazonaws.com/role-arn": "arn"}}})))
        notes = self.unit(self.plan(exports=None), "wkld-identity")["notes"]
        self.assertTrue(any("gsa_bindings is not published" in n
                            for n in notes))
        self.assertFalse(any("orders@proj" in n for n in notes))

    def test_storage_brief_conditions_on_menu_and_staging(self):
        self.write("k8s/pvc.yaml", dump(
            doc("PersistentVolumeClaim", "order-cache",
                spec={"storageClassName": "gp3-encrypted"}),
            doc("PersistentVolumeClaim", "scratch",
                spec={"storageClassName": "io2-fast"})))
        notes = self.unit(self.plan(), "wkld-storage")["notes"]
        self.assertTrue(any("'gp3-encrypted', present in" in n for n in notes))
        self.assertTrue(any("'io2-fast', which is ABSENT" in n for n in notes))
        # Data-vs-config: EVERY PVC gets the conditional transport question.
        for name in ("order-cache", "scratch"):
            self.assertTrue(any(f"If PVC '{name}' holds real data" in n
                                for n in notes), name)
        self.assertTrue(any("records no staging_bucket" in n for n in notes))
        staged = dict(EXPORTS)
        staged["staging_bucket"] = "gs://stage-bkt"
        staged_notes = self.unit(self.plan(exports=staged), "wkld-storage")["notes"]
        self.assertTrue(any("gs://stage-bkt" in n for n in staged_notes))
        null_menu = self.unit(self.plan(exports=None), "wkld-storage")["notes"]
        self.assertTrue(any("storage_class_menu is unpublished" in n
                            for n in null_menu))

    def test_residual_and_leak_notes(self):
        self.write("k8s/mix.yaml", dump(
            doc("Deployment", "web"),
            doc("Secret", "creds"),
            doc("TargetGroupBinding", "tgb"),
            doc("StorageClass", "gp3-encrypted", namespace=None)))
        unit = self.unit(self.plan(), "wkld-manifests")
        docs = unit["inputs"]["documents"]
        residual = [d for d in docs
                    if d["classification"] == "aws_coupled_or_unknown"]
        self.assertEqual(sorted(d["kind"] for d in residual),
                         ["Secret", "StorageClass", "TargetGroupBinding"])
        notes = unit["notes"]
        self.assertTrue(any("Residual bucket" in n and "Secret" in n
                            for n in notes))
        self.assertTrue(any("leaked past the scope step" in n
                            and "StorageClass" in n for n in notes))


class ChartAndKustomizeTest(PlannerTestBase):
    """Carrier recording, cross-cites, remote-ref refusal, degradation."""

    RENDERED = dump(
        doc("Deployment", "orders"),
        doc("ServiceAccount", "orders", **{
            "metadata": {"name": "orders", "namespace": "acme-shop",
                         "annotations": {"eks.amazonaws.com/role-arn": "arn"}}}),
        doc("PersistentVolumeClaim", "order-cache",
            spec={"storageClassName": "gp3-encrypted"}))

    def chart(self):
        self.write("charts/orders/Chart.yaml", "name: orders\nversion: 1.0.0\n")
        self.write("charts/orders/values.yaml", "replicas: 1\n")
        self.write("charts/orders/templates/all.yaml", "# go template\n")

    def render_ok(self, source_root, rel_dir):
        return self.RENDERED, None

    def test_chart_carrier_and_cross_cites(self):
        self.chart()
        plan = self.plan(renderers={"helm": self.render_ok,
                                    "kustomize": self._no_render})
        self.assertEqual(len(plan["carriers"]), 1)
        carrier = plan["carriers"][0]
        self.assertEqual(carrier["chart_path"], "charts/orders")
        self.assertEqual(carrier["owner"], "wkld-manifests")
        self.assertEqual(len(carrier["values_digest"]), 64)
        self.assertEqual(plan["sources"]["charts"][0]["values_digest"],
                         carrier["values_digest"])
        for family in ("wkld-identity", "wkld-storage"):
            unit = self.unit(plan, family)
            self.assertEqual(unit["status"], "planned")
            docs = unit["inputs"]["documents"]
            self.assertTrue(all(
                d["rendered_from"] == {"type": "helm",
                                       "chart_path": "charts/orders"}
                for d in docs))
            self.assertTrue(any("file-level diffs" in n and
                                "charts/orders" in n for n in unit["notes"]))
        manifests = self.unit(plan, "wkld-manifests")
        self.assertTrue(any("re-render deterministically" in n
                            for n in manifests["notes"]))
        # Chart files are consumed via render: never classified raw.
        self.assertFalse(any(d["path"].endswith("Chart.yaml")
                             for d in manifests["inputs"]["documents"]))

    def test_chart_render_failure_degrades_with_open_question(self):
        self.chart()
        plan = self.plan(renderers={
            "helm": lambda root, rel: (None, "boom"),
            "kustomize": self._no_render})
        self.assertEqual(plan["carriers"], [])
        self.assertFalse(plan["sources"]["charts"][0]["rendered"])
        self.assertTrue(any("helm render failed: boom" in n
                            for n in plan["notes"]))
        manifests = self.unit(plan, "wkld-manifests")
        self.assertEqual(manifests["status"], "planned")
        self.assertTrue(any("Open question" in n and "charts/orders" in n
                            for n in manifests["notes"]))

    def test_kustomize_remote_ref_refusal(self):
        self.write("overlay/kustomization.yaml",
                   "resources:\n- deploy.yaml\n- https://github.com/org/repo//base\n")
        self.write("overlay/deploy.yaml", dump(doc("Deployment", "web")))
        plan = self.plan()  # no renderer must be called: refusal pre-scan
        entry = plan["sources"]["kustomize"][0]
        self.assertFalse(entry["rendered"])
        self.assertEqual(
            entry["remote_refs"],
            ["overlay -> https://github.com/org/repo//base (a remote base)"])
        self.assertTrue(any("not rendered" in n for n in plan["notes"]))
        manifests = self.unit(plan, "wkld-manifests")
        self.assertTrue(any("Open question" in n and "remote base" in n
                            for n in manifests["notes"]))
        # The overlay's member files are consumed by the source, never raw.
        self.assertEqual(manifests["inputs"]["documents"], [])

    def test_kustomize_render_records_digest_and_documents(self):
        self.write("overlay/kustomization.yaml", "resources:\n- deploy.yaml\n")
        self.write("overlay/deploy.yaml", dump(doc("Deployment", "web")))
        plan = self.plan(renderers={
            "helm": self._no_render,
            "kustomize": lambda root, rel: (dump(doc("Deployment", "web")), None)})
        entry = plan["sources"]["kustomize"][0]
        self.assertTrue(entry["rendered"])
        self.assertEqual(len(entry["input_digest"]), 64)
        docs = self.unit(plan, "wkld-manifests")["inputs"]["documents"]
        self.assertEqual(len(docs), 1)
        self.assertEqual(docs[0]["rendered_from"],
                         {"type": "kustomize", "kustomize_dir": "overlay"})


class CarrierTranscriptionBriefTest(PlannerTestBase):
    """Family-3 ownership honesty: a carrier unit's brief hands the worker
    the exports literals to transcribe (the deterministic pass cannot edit
    chart sources), while a plain unit keeps the pass as the owner."""

    ECR = "111.dkr.ecr.us-east-1.amazonaws.com/acme/orders:v1"
    DEST = "us-docker.pkg.dev/proj/repo/orders:v1"
    RENDERED = dump(
        doc("Deployment", "orders", spec={"template": {"spec": {
            "containers": [{"name": "app", "image": ECR}]}}}),
        doc("ServiceAccount", "orders", **{
            "metadata": {"name": "orders", "namespace": "acme-shop",
                         "annotations": {"eks.amazonaws.com/role-arn": "arn"}}}))

    def chart_plan(self, image_map):
        self.write("charts/orders/Chart.yaml", "name: orders\nversion: 1.0\n")
        self.write("charts/orders/values.yaml", "replicas: 1\n")
        self.write("charts/orders/templates/all.yaml", "# go template\n")
        exports = dict(EXPORTS, artifact_registry=(
            None if image_map is None
            else {"destinations": [], "image_map": image_map}))
        return self.plan(exports=exports, renderers={
            "helm": lambda root, rel: (self.RENDERED, None),
            "kustomize": self._no_render})

    def test_replicated_map_entry_becomes_a_transcription_literal(self):
        plan = self.chart_plan(
            {self.ECR: {"dest_ref": self.DEST, "status": "replicated"}})
        notes = self.unit(plan, "wkld-manifests")["notes"]
        self.assertTrue(any("Ownership split" in n for n in notes))
        self.assertTrue(any("Transcribe into the carrier source" in n
                            and self.ECR in n and self.DEST in n
                            for n in notes))
        # The unconditional plain-only ban must NOT appear on a carrier unit.
        self.assertFalse(any(n.startswith(
            "Image references are the deterministic pass") for n in notes))

    def test_unreplicated_map_entry_stays_untouched_with_a_question(self):
        plan = self.chart_plan(
            {self.ECR: {"dest_ref": self.DEST, "status": "self_service"}})
        notes = self.unit(plan, "wkld-manifests")["notes"]
        self.assertTrue(any("not 'replicated'" in n and "open question" in n
                            for n in notes))
        self.assertFalse(any("Transcribe into the carrier source" in n
                             for n in notes))

    def test_unpublished_map_is_one_honest_gap_note(self):
        plan = self.chart_plan(None)
        notes = self.unit(plan, "wkld-manifests")["notes"]
        self.assertTrue(any("image_map is unpublished" in n for n in notes))

    def test_plain_unit_keeps_the_deterministic_pass_ownership(self):
        self.write("k8s/app.yaml", dump(
            doc("Deployment", "web", spec={"template": {"spec": {
                "containers": [{"name": "app", "image": self.ECR}]}}})))
        plan = self.plan()
        notes = self.unit(plan, "wkld-manifests")["notes"]
        self.assertTrue(any(n.startswith(
            "Image references are the deterministic pass") for n in notes))
        self.assertFalse(any("Transcribe into the carrier source" in n
                             for n in notes))

    def test_identity_chart_branch_transcribes_the_published_email(self):
        plan = self.chart_plan({})
        notes = self.unit(plan, "wkld-identity")["notes"]
        self.assertTrue(any(
            "transcribe the published email into the carrier source" in n
            and "orders@proj.iam.gserviceaccount.com" in n for n in notes))
        self.assertFalse(any("the deterministic pass replaces" in n
                             for n in notes))

    def test_identity_plain_branch_keeps_the_deterministic_pass(self):
        self.write("k8s/sa.yaml", dump(doc("ServiceAccount", "orders", **{
            "metadata": {"name": "orders", "namespace": "acme-shop",
                         "annotations": {
                             "eks.amazonaws.com/role-arn": "arn"}}})))
        plan = self.plan()
        notes = self.unit(plan, "wkld-identity")["notes"]
        self.assertTrue(any("the deterministic pass replaces" in n
                            for n in notes))
        self.assertFalse(any("carrier source" in n for n in notes))


class KustomizeGraphTest(PlannerTestBase):
    """Decision 8 is TRANSITIVE: the whole reachable graph has to be local,
    and a base an in-scope overlay already renders is not rendered again."""

    def setUp(self):
        super().setUp()
        self.rendered = []

    def record(self, source_root, rel_dir):
        self.rendered.append(rel_dir)
        return dump(doc("Deployment", "web")), None

    def kplan(self):
        return self.plan(renderers={"helm": self._no_render,
                                    "kustomize": self.record})

    def base_and_overlay(self):
        self.write("base/kustomization.yaml", "resources:\n- deploy.yaml\n")
        self.write("base/deploy.yaml", dump(doc("Deployment", "web")))
        self.write("overlay/kustomization.yaml", "resources:\n- ../base\n")

    def test_a_base_an_overlay_includes_is_not_rendered_twice(self):
        self.base_and_overlay()
        plan = self.kplan()
        self.assertEqual(self.rendered, ["overlay"])
        self.assertEqual(
            [e["kustomize_dir"] for e in plan["sources"]["kustomize"]],
            ["overlay"])
        self.assertTrue(any("rendered as a base of overlay" in n
                            for n in plan["notes"]))
        self.assertEqual(
            len(self.unit(plan, "wkld-manifests")["inputs"]["documents"]), 1)

    def test_the_input_digest_covers_every_dir_the_graph_reaches(self):
        self.base_and_overlay()
        first = self.kplan()["sources"]["kustomize"][0]
        self.assertEqual(first["input_dirs"], ["base", "overlay"])
        self.write("base/deploy.yaml", dump(doc("Deployment", "moved")))
        second = self.kplan()["sources"]["kustomize"][0]
        self.assertNotEqual(first["input_digest"], second["input_digest"])

    def test_a_remote_ref_a_base_deep_refuses_the_whole_render(self):
        self.base_and_overlay()
        self.write("base/kustomization.yaml",
                   "resources:\n- deploy.yaml\n- git::https://h/r//b\n")
        plan = self.kplan()
        self.assertEqual(self.rendered, [])  # kubectl is never invoked
        entry = plan["sources"]["kustomize"][0]
        self.assertFalse(entry["rendered"])
        self.assertEqual(entry["remote_refs"],
                         ["base -> git::https://h/r//b (a remote base)"])

    REMOTE_FORMS = (
        "https://github.com/org/repo//base",
        "ssh://git@github.com/org/repo//base?ref=v1",
        "git::https://github.com/org/repo//base",
        "git@github.com:org/repo.git//base",
        "github.com/org/repo//base?ref=v1.2.3",           # no scheme at all
        "bitbucket.org/org/repo/overlays/prod",
    )

    def test_every_remote_form_kustomize_accepts_is_refused(self):
        for entry in self.REMOTE_FORMS:
            with self.subTest(entry=entry):
                self.write("overlay/kustomization.yaml",
                           f"resources:\n- {entry}\n")
                refusals = planner.kustomize_remote_refs(self.root, "overlay")
                self.assertEqual(len(refusals), 1, refusals)
                self.assertIn("a remote base", refusals[0])

    def test_a_local_directory_that_merely_looks_remote_still_renders(self):
        """The remote patterns are tested only AFTER local resolution fails,
        so a real directory named like a host is not refused on its name."""
        self.write("overlay/kustomization.yaml",
                   "resources:\n- github.com/vendored\n")
        self.write("overlay/github.com/vendored/kustomization.yaml",
                   "resources:\n- deploy.yaml\n")
        self.write("overlay/github.com/vendored/deploy.yaml",
                   dump(doc("Deployment", "web")))
        self.assertEqual(planner.kustomize_remote_refs(self.root, "overlay"), [])

    def test_an_absent_local_path_is_refused_as_absent_not_as_remote(self):
        self.write("overlay/kustomization.yaml", "resources:\n- ../gone\n")
        refusals = planner.kustomize_remote_refs(self.root, "overlay")
        self.assertEqual(len(refusals), 1)
        self.assertIn("no such path inside the source root", refusals[0])

    def test_an_unreadable_kustomization_fails_closed(self):
        """kubectl's parser is more permissive than ours: "we could not read
        it, so it names no remote base" is exactly the wrong conclusion."""
        self.write("overlay/kustomization.yaml", "a: &x [1]\nresources: *x\n")
        refusals = planner.kustomize_remote_refs(self.root, "overlay")
        self.assertEqual(len(refusals), 1)
        self.assertIn("unreadable under the hardened posture", refusals[0])


class CoverageAndDedupTest(PlannerTestBase):
    """What the renders did NOT account for, and one object -> one record."""

    def chart(self, *extra_templates):
        self.write("charts/orders/Chart.yaml", "name: orders\nversion: 1.0\n")
        self.write("charts/orders/values.yaml", "replicas: 1\n")
        self.write("charts/orders/templates/all.yaml", "# go template\n")
        for name in extra_templates:
            self.write(f"charts/orders/templates/{name}", "# go template\n")

    def helm_out(self, source_root, rel_dir):
        return ("# Source: orders/templates/all.yaml\n"
                + dump(doc("Deployment", "web"))), None

    def hplan(self, scope=None):
        return self.plan(scope=scope, renderers={"helm": self.helm_out,
                                                 "kustomize": self._no_render})

    def test_a_vendored_subchart_is_not_rendered_separately(self):
        self.chart()
        self.write("charts/orders/charts/sub/Chart.yaml", "name: sub\n")
        plan = self.hplan()
        self.assertEqual([e["chart_path"] for e in plan["sources"]["charts"]],
                         ["charts/orders"])
        self.assertTrue(any("charts/orders/charts/sub" in n and "nested" in n
                            for n in plan["notes"]))

    def test_an_in_scope_file_the_render_ignored_is_reported(self):
        self.chart("orphan.yaml")
        notes = self.hplan()["notes"]
        hit = [n for n in notes if "produced no document" in n]
        self.assertEqual(len(hit), 1)
        self.assertIn("charts/orders/templates/orphan.yaml", hit[0])
        self.assertNotIn("templates/all.yaml", hit[0])  # helm named it

    def test_an_exclusion_inside_a_render_source_says_it_cannot_be_honoured(self):
        self.chart("secret.yaml")
        notes = self.hplan(scope={
            "included": ["**"],
            "excluded": ["charts/orders/templates/secret.yaml"]})["notes"]
        hit = [n for n in notes if "cannot be honoured" in n]
        self.assertEqual(len(hit), 1)
        self.assertIn("templates/secret.yaml", hit[0])
        self.assertIn("read them off disk", hit[0])

    def test_one_object_from_two_streams_is_classified_once(self):
        self.chart()
        self.write("k8s/app.yaml", dump(doc("Deployment", "web")))
        plan = self.hplan()
        docs = self.unit(plan, "wkld-manifests")["inputs"]["documents"]
        self.assertEqual(len(docs), 1)
        self.assertEqual(docs[0]["path"], "k8s/app.yaml")  # first wins
        self.assertTrue(any("repeat an object identity" in n
                            for n in plan["notes"]))

    def test_a_repeat_that_differs_only_by_namespace_is_kept(self):
        self.write("k8s/a.yaml", dump(doc("Deployment", "web", namespace="a")))
        self.write("k8s/b.yaml", dump(doc("Deployment", "web", namespace="b")))
        plan = self.plan()
        self.assertEqual(
            len(self.unit(plan, "wkld-manifests")["inputs"]["documents"]), 2)
        self.assertFalse(any("repeat an object identity" in n
                             for n in plan["notes"]))

    def test_a_repo_whose_root_is_the_chart_is_recorded_as_dot(self):
        self.write("Chart.yaml", "name: orders\nversion: 1.0\n")
        self.write("templates/all.yaml", "# go template\n")
        plan = self.plan(renderers={
            "helm": lambda root, rel: (dump(doc("Deployment", "web")), None),
            "kustomize": self._no_render})
        self.assertEqual(plan["sources"]["charts"][0]["chart_path"], ".")
        self.assertEqual(plan["carriers"][0]["chart_path"], ".")


class DegradationAndStampTest(PlannerTestBase):

    def test_parse_failure_is_a_note_not_a_crash(self):
        self.write("k8s/bad.yaml", "a: [unclosed\n")
        self.write("k8s/good.yaml", dump(doc("Deployment", "web")))
        plan = self.plan()
        self.assertTrue(any("k8s/bad.yaml" in n and "excluded" in n
                            for n in plan["notes"]))
        docs = self.unit(plan, "wkld-manifests")["inputs"]["documents"]
        self.assertEqual([d["path"] for d in docs], ["k8s/good.yaml"])

    def test_alias_documents_are_refused_by_the_hardened_posture(self):
        self.write("k8s/bomb.yaml", "a: &x [1]\nb: *x\n")
        plan = self.plan()
        self.assertTrue(any("aliases" in n for n in plan["notes"]))

    def test_symlink_escape_is_skipped_with_a_note(self):
        outside = tempfile.mkdtemp(prefix="wkld_outside_")
        self.addCleanup(shutil.rmtree, outside, True)
        with open(os.path.join(outside, "evil.yaml"), "w") as f:
            f.write(dump(doc("Deployment", "evil")))
        os.symlink(os.path.join(outside, "evil.yaml"),
                   os.path.join(self.root, "evil.yaml"))
        plan = self.plan()
        self.assertTrue(any("symlink escapes" in n for n in plan["notes"]))
        self.assertTrue(planner.all_placeholders(plan))

    def test_a_scalar_where_the_schema_says_mapping_is_not_a_crash(self):
        """Customer YAML puts scalars in odd places; every brief helper reads
        sub-documents through _mapping/_sequence, so this must plan."""
        self.write("k8s/odd.yaml", dump(
            doc("DaemonSet", "agent", spec="oops"),
            doc("StatefulSet", "db", spec={"volumeClaimTemplates": "nope"}),
            doc("PersistentVolumeClaim", "cache", spec=["not", "a", "map"]),
            doc("ServiceAccount", "sa", metadata="also-not-a-map")))
        plan = self.plan()
        kinds = sorted(d["kind"] for d
                       in self.unit(plan, "wkld-manifests")["inputs"]["documents"])
        self.assertEqual(kinds, ["DaemonSet", "StatefulSet"])
        self.assertEqual(self.unit(plan, "wkld-storage")["status"], "planned")

    def test_a_document_missing_its_key_fields_is_named_not_guessed(self):
        self.write("k8s/sa.yaml", dump(
            {"apiVersion": "v1", "kind": "ServiceAccount",
             "metadata": {"namespace": "acme-shop", "annotations": {
                 "eks.amazonaws.com/role-arn": "arn"}}}))
        notes = self.unit(self.plan(), "wkld-identity")["notes"]
        self.assertTrue(any("metadata.name" in n for n in notes), notes)

    def test_exports_stamp_present_and_degraded(self):
        self.write("k8s/app.yaml", dump(doc("Deployment", "web")))
        stamped = self.plan()["exports_stamp"]
        self.assertEqual(stamped["generated_at"], EXPORTS["generated_at"])
        self.assertEqual(stamped["generations"], EXPORTS["generations"])
        self.assertEqual(stamped["non_gateway_digest"],
                         planner.non_gateway_digest(EXPORTS))
        degraded = self.plan(exports=None)
        self.assertEqual(degraded["exports_stamp"],
                         {"generated_at": None, "generations": None,
                          "non_gateway_digest": None})
        self.assertTrue(any("explicit nulls" in n for n in degraded["notes"]))


class DeterminismAndUtilityTest(PlannerTestBase):

    def fill(self):
        self.write("k8s/app.yaml", dump(doc("Deployment", "web"),
                                        doc("Service", "web")))
        self.write("k8s/pvc.yaml", dump(doc(
            "PersistentVolumeClaim", "cache",
            spec={"storageClassName": "gp3-encrypted"})))
        self.write("k8s/ing.yaml", dump(doc("Ingress", "web")))

    def test_two_runs_are_byte_identical(self):
        self.fill()
        first = json.dumps(self.plan(), sort_keys=True)
        second = json.dumps(self.plan(), sort_keys=True)
        self.assertEqual(first, second)

    def test_no_private_keys_leak_into_the_plan(self):
        self.fill()
        self.assertNotIn('"_doc"', json.dumps(self.plan()))

    def test_scope_excludes_carve_out_of_includes(self):
        self.fill()
        plan = self.plan(scope={"included": ["k8s/"],
                                "excluded": ["k8s/ing.yaml"]})
        self.assertEqual(self.unit(plan, "wkld-routing")["status"], "skipped")
        self.assertEqual(self.unit(plan, "wkld-manifests")["status"], "planned")

    def test_set_unit_status_and_summary(self):
        self.fill()
        plan = self.plan()
        updated, notes = planner.set_unit_status(
            plan, ["wkld-storage", "nope"], "skipped")
        self.assertIn("wkld-storage -> skipped", notes)
        self.assertIn("'nope' not found in plan (ignored)", notes)
        self.assertEqual(
            [u["status"] for u in plan["units"]
             if u["unit_id"] == "wkld-storage"], ["planned"])  # pure
        summary = planner.summarize_plan(updated)
        self.assertIn("wkld-routing", summary)
        self.assertIn("exports_stamp", summary)


class RestoreAndEmptyPlanTest(PlannerTestBase):
    """Unskip is the one status edit that can make the plan lie."""

    def test_a_placeholder_unit_is_refused_not_restored(self):
        self.write("k8s/app.yaml", dump(doc("Deployment", "web")))
        plan = self.plan()
        restored, notes = planner.restore_units(plan, ["wkld-storage"])
        unit = self.unit(restored, "wkld-storage")
        self.assertEqual(unit["status"], "skipped")
        self.assertTrue(unit["placeholder"])
        self.assertTrue(any("NOT restored" in n and "empty inputs" in n
                            for n in notes))

    def test_a_parked_unit_comes_back_parked_not_promoted(self):
        self.write("k8s/ing.yaml", dump(doc("Ingress", "web")))
        plan = self.plan()  # exports.gateway is null -> routing parks
        self.assertEqual(self.unit(plan, "wkld-routing")["status"], "parked")
        skipped, _ = planner.set_unit_status(plan, ["wkld-routing"], "skipped")
        restored, notes = planner.restore_units(skipped, ["wkld-routing"])
        self.assertEqual(self.unit(restored, "wkld-routing")["status"],
                         "parked")
        self.assertTrue(any("the status the facts produced" in n
                            for n in notes))

    def test_a_planned_unit_round_trips_and_unknown_ids_are_reported(self):
        self.write("k8s/app.yaml", dump(doc("Deployment", "web")))
        plan = self.plan()
        skipped, _ = planner.set_unit_status(
            plan, ["wkld-manifests"], "skipped")
        restored, notes = planner.restore_units(skipped,
                                                ["wkld-manifests", "nope"])
        self.assertEqual(self.unit(restored, "wkld-manifests")["status"],
                         "planned")
        self.assertIn("'nope' not found in plan (ignored)", notes)

    def test_a_render_that_failed_leaves_an_empty_plan_the_flags_cannot_see(self):
        self.write("charts/orders/Chart.yaml", "name: orders\nversion: 1.0\n")
        self.write("charts/orders/templates/all.yaml", "# go template\n")
        plan = self.plan(renderers={"helm": lambda root, rel: (None, "boom"),
                                    "kustomize": self._no_render})
        self.assertFalse(planner.all_placeholders(plan))  # manifests is planned
        self.assertEqual(planner.classified_documents(plan), 0)
        reason = planner.empty_plan_reason(plan)
        self.assertIn("failed to render", reason)
        self.assertIn("boom", reason)

    def test_a_plan_with_documents_has_no_empty_reason(self):
        self.write("k8s/app.yaml", dump(doc("Deployment", "web")))
        self.assertIsNone(planner.empty_plan_reason(self.plan()))
        self.assertIn("no classifiable document",
                      planner.empty_plan_reason(self.plan(
                          scope={"included": [], "excluded": []})))


GATEWAY_EXPORTS = {
    **EXPORTS,
    "gateway": {"name": "shared-gateway", "namespace": "gateway-infra"},
    "generated_at": "2026-08-15T01:00:00+00:00",
    "generations": {"discovery": 3, "translation": 3, "deployment": 1},
}


def ingress_doc(name="web", annotations=None, spec=None):
    document = doc("Ingress", name, spec=spec or {
        "rules": [{"host": "shop.acme.example", "http": {"paths": [
            {"path": "/", "pathType": "Prefix",
             "backend": {"service": {"name": "frontend",
                                     "port": {"number": 80}}}}]}}]})
    if annotations:
        document["metadata"]["annotations"] = annotations
    return document


class RoutingLiveAndRefreshTest(PlannerTestBase):
    """M4: the live routing brief, the disposition table, and the pure
    unpark refresh (spec v2 §2 M4)."""

    ANNOTATIONS = {
        "alb.ingress.kubernetes.io/scheme": "internet-facing",
        "alb.ingress.kubernetes.io/certificate-arn": "arn:aws:acm:x",
        "kubernetes.io/ingress.class": "alb",
    }

    def routing(self, plan):
        return self.unit(plan, "wkld-routing")

    def test_live_brief_carries_verbatim_parent_refs(self):
        self.write("k8s/ing.yaml", dump(ingress_doc()))
        unit = self.routing(self.plan(exports=GATEWAY_EXPORTS))
        self.assertEqual(unit["status"], "planned")
        self.assertEqual(unit["planned_status"], "planned")
        joined = "\n".join(unit["notes"])
        self.assertIn("'shared-gateway'", joined)
        self.assertIn("'gateway-infra'", joined)
        self.assertIn("never add a sectionName", joined)
        self.assertIn("gateway-infra/shared-gateway", joined)
        self.assertIn("Rule precedence differs", joined)
        self.assertNotIn("[PLANNED]: translation", joined)

    def test_disposition_rows_exactly_for_present_annotations(self):
        self.write("k8s/ing.yaml",
                   dump(ingress_doc(annotations=self.ANNOTATIONS)))
        unit = self.routing(self.plan(exports=GATEWAY_EXPORTS))
        rows = [n for n in unit["notes"] if n.startswith("Disposition — ")]
        named = {n.split(" ", 3)[2] for n in rows}
        self.assertEqual(named, set(self.ANNOTATIONS))
        self.assertEqual(len(rows), len(self.ANNOTATIONS))

    def test_absent_annotations_get_no_rows(self):
        self.write("k8s/ing.yaml", dump(ingress_doc()))
        unit = self.routing(self.plan(exports=GATEWAY_EXPORTS))
        self.assertEqual(
            [n for n in unit["notes"] if n.startswith("Disposition — ")], [])

    def test_unknown_alb_annotation_is_open_question_by_rule(self):
        self.write("k8s/ing.yaml", dump(ingress_doc(annotations={
            "alb.ingress.kubernetes.io/brand-new-knob": "x",
            "alb.ingress.kubernetes.io/healthcheck-path": "/hc"})))
        unit = self.routing(self.plan(exports=GATEWAY_EXPORTS))
        rows = [n for n in unit["notes"] if n.startswith("Disposition — ")]
        self.assertEqual(len(rows), 2)
        for row in rows:
            self.assertIn("open question", row)
        self.assertTrue(any("never silently dropped" in r for r in rows))
        self.assertTrue(any("does not infer health checks" in r for r in rows))

    def test_prefix_rows_and_non_alb_rows_come_from_the_reference_table(self):
        """The table lives in reference/api-translation.md: a prefix row
        covers the open-ended knobs, an exact row covers a non-ALB
        annotation, and a key no row covers is still the rule."""
        self.write("k8s/ing.yaml", dump(ingress_doc(annotations={
            "alb.ingress.kubernetes.io/auth-idp-cognito": "{}",
            "alb.ingress.kubernetes.io/actions.redirect": "{}",
            "external-dns.alpha.kubernetes.io/hostname": "shop.acme.example",
            "nginx.ingress.kubernetes.io/rewrite-target": "/"})))
        unit = self.routing(self.plan(exports=GATEWAY_EXPORTS))
        rows = {n.split(" ", 3)[2]: n for n in unit["notes"]
                if n.startswith("Disposition — ")}
        self.assertEqual(len(rows), 4)
        self.assertIn(": open question — ALB-native Cognito/OIDC",
                      rows["alb.ingress.kubernetes.io/auth-idp-cognito"])
        self.assertIn(": mapped — a redirect action becomes an HTTPRoute",
                      rows["alb.ingress.kubernetes.io/actions.redirect"])
        self.assertIn(": mapped — keep verbatim on the HTTPRoute",
                      rows["external-dns.alpha.kubernetes.io/hostname"])
        self.assertIn("never silently dropped",
                      rows["nginx.ingress.kubernetes.io/rewrite-target"])

    def test_regex_path_mode_forbids_a_provisional_prefix(self):
        """With use-regex-path-match present, the ImplementationSpecific
        note defers to the row (no PathPrefix assumption); without it the
        plain-prefix allowance stays."""
        spec = {"rules": [{"host": "shop.acme.example", "http": {"paths": [
            {"path": "/api/v[0-9]+/.*", "pathType": "ImplementationSpecific",
             "backend": {"service": {"name": "api",
                                     "port": {"number": 80}}}}]}}]}
        self.write("k8s/ing.yaml", dump(ingress_doc(spec=spec, annotations={
            "alb.ingress.kubernetes.io/use-regex-path-match": "true"})))
        joined = "\n".join(
            self.routing(self.plan(exports=GATEWAY_EXPORTS))["notes"])
        self.assertIn("never assume a PathPrefix", joined)
        self.assertNotIn("PathPrefix may be chosen", joined)
        self.assertIn("use-regex-path-match (on Ingress web): open question",
                      joined)
        # Without the annotation the plain-prefix allowance stays.
        self.write("k8s/ing.yaml", dump(ingress_doc(spec=spec)))
        joined = "\n".join(
            self.routing(self.plan(exports=GATEWAY_EXPORTS))["notes"])
        self.assertIn("PathPrefix may be chosen", joined)
        self.assertNotIn("never assume a PathPrefix", joined)

    def test_action_backed_paths_are_not_named_ports(self):
        """The ALB action idiom: backend service = the action's name, port
        name = use-annotation. The brief names it as an action-backed path
        and keeps it out of the named-port rule; a real named port on the
        same Ingress still gets the rule."""
        spec = {"rules": [{"host": "shop.acme.example", "http": {"paths": [
            {"path": "/old", "pathType": "Prefix",
             "backend": {"service": {"name": "redirect-old",
                                     "port": {"name": "use-annotation"}}}},
            {"path": "/", "pathType": "Prefix",
             "backend": {"service": {"name": "frontend",
                                     "port": {"name": "http"}}}}]}}]}
        self.write("k8s/ing.yaml", dump(ingress_doc(spec=spec, annotations={
            "alb.ingress.kubernetes.io/actions.redirect-old": "{}"})))
        joined = "\n".join(
            self.routing(self.plan(exports=GATEWAY_EXPORTS))["notes"])
        self.assertIn("backed by an ALB action (redirect-old; port name "
                      "use-annotation)", joined)
        self.assertIn("named backend port(s) (http)", joined)
        self.assertNotIn("named backend port(s) (http, use-annotation)", joined)
        self.assertIn("actions.redirect-old (on Ingress web): mapped", joined)

    def test_disposition_row_is_the_parsed_table(self):
        from servers.dag.server import api_translation
        self.assertEqual(
            planner.disposition_row("alb.ingress.kubernetes.io/scheme"),
            api_translation.load_annotation_dispositions()["exact"][
                "alb.ingress.kubernetes.io/scheme"])
        self.assertIs(planner.disposition_row("x.example/y"),
                      api_translation.UNKNOWN_ROW)

    def test_conditional_live_lines_fire_on_facts_only(self):
        spec = {"defaultBackend": {"service": {"name": "fallback",
                                               "port": {"number": 80}}},
                "tls": [{"hosts": ["shop.acme.example"]}],
                "ingressClassName": "alb",
                "rules": [{"host": "shop.acme.example", "http": {"paths": [
                    {"path": "/app", "pathType": "ImplementationSpecific",
                     "backend": {"service": {"name": "frontend",
                                             "port": {"name": "http"}}}}]}}]}
        self.write("k8s/ing.yaml", dump(ingress_doc(spec=spec)))
        joined = "\n".join(
            self.routing(self.plan(exports=GATEWAY_EXPORTS))["notes"])
        for expected in ("ImplementationSpecific", "named backend port",
                         "spec.defaultBackend", "spec.tls",
                         "spec.ingressClassName 'alb'"):
            self.assertIn(expected, joined)

    def test_parked_arm_regression_no_dispositions_no_live_contract(self):
        self.write("k8s/ing.yaml",
                   dump(ingress_doc(annotations=self.ANNOTATIONS)))
        unit = self.routing(self.plan())  # EXPORTS: gateway null
        self.assertEqual(unit["status"], "parked")
        joined = "\n".join(unit["notes"])
        self.assertIn("exports.gateway is null", joined)
        self.assertIn("carries annotations", joined)
        self.assertIn("NOT stripped by the deterministic pass", joined)
        self.assertNotIn("Disposition — ", joined)
        self.assertNotIn("parentRefs", joined)

    def test_ingress_facts_are_persisted_on_the_unit(self):
        self.write("k8s/ing.yaml",
                   dump(ingress_doc(annotations=self.ANNOTATIONS)))
        for exports in (EXPORTS, GATEWAY_EXPORTS):
            unit = self.routing(self.plan(exports=exports))
            facts = unit["inputs"]["ingress_facts"]
            self.assertEqual(len(facts), 1)
            self.assertEqual(facts[0]["label"], "web")
            self.assertEqual(facts[0]["rules"][0]["host"], "shop.acme.example")
            self.assertEqual(facts[0]["rules"][0]["paths"][0]["path_type"],
                             "Prefix")
            self.assertEqual(facts[0]["annotations"],
                             sorted(self.ANNOTATIONS))

    def test_refresh_unparks_to_the_fresh_plan_unit(self):
        self.write("k8s/ing.yaml",
                   dump(ingress_doc(annotations=self.ANNOTATIONS)))
        parked_plan = json.loads(json.dumps(self.plan()))  # as persisted
        fresh_plan = self.plan(exports=GATEWAY_EXPORTS)
        refreshed, changed, refusal = planner.refresh_parked_routing(
            parked_plan, GATEWAY_EXPORTS)
        self.assertTrue(changed)
        self.assertEqual(refusal, "")
        self.assertEqual(self.routing(refreshed), self.routing(fresh_plan))
        self.assertEqual(refreshed["exports_stamp"],
                         fresh_plan["exports_stamp"])
        again, changed_again, _ = planner.refresh_parked_routing(
            json.loads(json.dumps(refreshed)), GATEWAY_EXPORTS)
        self.assertFalse(changed_again)  # nothing left parked
        self.assertEqual(again["units"], refreshed["units"])

    def test_refresh_touches_nothing_but_routing_and_stamp(self):
        self.write("k8s/ing.yaml", dump(ingress_doc()))
        self.write("k8s/app.yaml", dump(doc("Deployment", "web"),
                                        doc("ServiceAccount", "orders")))
        parked_plan = json.loads(json.dumps(self.plan()))
        refreshed, changed, _ = planner.refresh_parked_routing(
            parked_plan, GATEWAY_EXPORTS)
        self.assertTrue(changed)
        for unit_id in ("wkld-manifests", "wkld-identity", "wkld-storage"):
            self.assertEqual(self.unit(refreshed, unit_id),
                             self.unit(parked_plan, unit_id), unit_id)
        self.assertEqual(refreshed["notes"], parked_plan["notes"])
        self.assertEqual(refreshed["sources"], parked_plan["sources"])
        self.assertEqual(refreshed["carriers"], parked_plan["carriers"])

    def test_refresh_is_a_no_op_without_gateway_or_parked_units(self):
        self.write("k8s/ing.yaml", dump(ingress_doc()))
        parked_plan = self.plan()
        for exports in (None, {}, EXPORTS):  # gateway still null / absent
            same, changed, refusal = planner.refresh_parked_routing(
                parked_plan, exports)
            self.assertFalse(changed)
            self.assertEqual(refusal, "")
            self.assertIs(same, parked_plan)
        no_facts = json.loads(json.dumps(parked_plan))
        del self.routing(no_facts)["inputs"]["ingress_facts"]
        same, changed, _ = planner.refresh_parked_routing(no_facts,
                                                          GATEWAY_EXPORTS)
        self.assertFalse(changed)  # pre-v0.4 plan shape: re-plan instead
        self.assertIs(same, no_facts)

    def test_refresh_preserves_the_chart_carrier_cross_cite(self):
        self.write("chart/Chart.yaml", "apiVersion: v2\nname: shop\n")
        helm = lambda root, rel: (dump(ingress_doc()), None)
        parked_plan = json.loads(json.dumps(self.plan(
            renderers={"helm": helm, "kustomize": self._no_render})))
        fresh_plan = self.plan(exports=GATEWAY_EXPORTS,
                               renderers={"helm": helm,
                                          "kustomize": self._no_render})
        refreshed, changed, _ = planner.refresh_parked_routing(
            parked_plan, GATEWAY_EXPORTS)
        self.assertTrue(changed)
        self.assertEqual(self.routing(refreshed), self.routing(fresh_plan))
        self.assertTrue(any("rendered from chart" in n
                            for n in self.routing(refreshed)["notes"]))

    # -- the unpark's blast radius: only `gateway` may have moved -----------

    def test_refresh_refuses_when_a_non_gateway_export_also_moved(self):
        """The unpark rebuilds ONE unit but re-stamps the WHOLE plan.

        That is only sound while every other unit's brief is still current,
        i.e. while nothing outside `gateway` changed. When the storage menu
        moves too, silently re-stamping would bless three stale briefs and
        blind decision 8; the unpark refuses and names the replan action.
        """
        self.write("k8s/ing.yaml", dump(ingress_doc()))
        self.write("k8s/app.yaml", dump(doc("Deployment", "web")))
        parked_plan = json.loads(json.dumps(self.plan()))
        moved = {**GATEWAY_EXPORTS,
                 "storage_class_menu": ["gp3-encrypted", "premium-rwo"]}
        same, changed, refusal = planner.refresh_parked_routing(parked_plan,
                                                                moved)
        self.assertFalse(changed)
        self.assertIs(same, parked_plan)
        self.assertIn("BEYOND", refusal)
        self.assertIn("approve_workload_translation(action='replan')",
                      refusal)

    def test_refresh_requeues_already_done_units(self):
        """Re-stamping alone would strand `done` units at the OLD brief.

        `done` is not a pending status, so a re-stamped plan would never
        re-run them — yet validate cross-checks their blobs against CURRENT
        exports and bounces the component. Demoting them to `planned` is what
        makes "whole-plan re-translation" true rather than aspirational.
        """
        self.write("k8s/ing.yaml", dump(ingress_doc()))
        self.write("k8s/app.yaml", dump(doc("Deployment", "web"),
                                        doc("ServiceAccount", "orders")))
        parked_plan = json.loads(json.dumps(self.plan()))
        for unit in parked_plan["units"]:
            if unit["unit_id"] != "wkld-routing":
                unit["status"] = "done"
                unit["feedback"] = "stale review note"
        refreshed, changed, _ = planner.refresh_parked_routing(
            parked_plan, GATEWAY_EXPORTS)
        self.assertTrue(changed)
        requeued = planner.redone_at_unpark(parked_plan, refreshed)
        self.assertEqual(requeued, sorted(
            u["unit_id"] for u in parked_plan["units"]
            if u["unit_id"] != "wkld-routing"))
        for unit in refreshed["units"]:
            if unit["unit_id"] in requeued:
                self.assertEqual(unit["status"], "planned")
                self.assertIsNone(unit["feedback"])

    def test_refresh_leaves_skipped_and_parked_non_routing_units_alone(self):
        self.write("k8s/ing.yaml", dump(ingress_doc()))
        self.write("k8s/app.yaml", dump(doc("Deployment", "web")))
        parked_plan = json.loads(json.dumps(self.plan()))
        for unit in parked_plan["units"]:
            if unit["unit_id"] != "wkld-routing":
                unit["status"] = "skipped"
        refreshed, changed, _ = planner.refresh_parked_routing(
            parked_plan, GATEWAY_EXPORTS)
        self.assertTrue(changed)
        self.assertEqual(planner.redone_at_unpark(parked_plan, refreshed), [])
        for unit in refreshed["units"]:
            if unit["unit_id"] != "wkld-routing":
                self.assertEqual(unit["status"], "skipped")

    def test_non_gateway_digest_ignores_gateway_and_bookkeeping(self):
        base = planner.non_gateway_digest(EXPORTS)
        self.assertEqual(base, planner.non_gateway_digest(GATEWAY_EXPORTS))
        self.assertNotEqual(base, planner.non_gateway_digest(
            {**EXPORTS, "staging_bucket": "gs://b"}))
        self.assertIsNone(planner.non_gateway_digest(None))

    def test_non_gateway_digest_ignores_the_data_gate(self):
        """`data_gate` moves on every operator report of a database landing
        and is input to no brief. Left in the digest, refresh_parked_routing
        would refuse the gateway unpark — "exports changed BEYOND gateway,
        re-plan instead" — on every component whose plan predates a
        mark_data_service_migrated. The symptom is remote from the cause,
        which is why this needs a test of its own; the validate-side twin
        (_brief_generations) has two."""
        base = planner.non_gateway_digest(EXPORTS)
        self.assertEqual(base, planner.non_gateway_digest(
            {**EXPORTS, "data_gate": {"schema_version": 1, "scanned": True,
                                      "services": [{"identifier": "orders-db",
                                                    "status": "migrated"}]}}))

    # -- a half-published gateway is not an attach point -------------------

    def test_partial_gateway_parks_and_says_which_field_is_missing(self):
        self.write("k8s/ing.yaml", dump(ingress_doc()))
        for gateway in ({"name": "shared-gateway"},
                        {"namespace": "gateway-infra"},
                        {"name": "shared-gateway", "namespace": "   "}):
            with self.subTest(gateway=gateway):
                unit = self.routing(self.plan(
                    exports={**EXPORTS, "gateway": gateway}))
                self.assertEqual(unit["status"], "parked")
                joined = "\n".join(unit["notes"])
                self.assertIn("exports.gateway is PARTIAL", joined)
                # The facts are recorded, but no live contract is issued.
                self.assertNotIn("Disposition — ", joined)
                self.assertNotIn("never add a sectionName", joined)
        # And the unpark declines it too, without a refusal message.
        parked_plan = json.loads(json.dumps(self.plan()))
        same, changed, refusal = planner.refresh_parked_routing(
            parked_plan, {**EXPORTS, "gateway": {"name": "g"}})
        self.assertFalse(changed)
        self.assertEqual(refusal, "")
        self.assertIs(same, parked_plan)

    def test_parked_routing_title_says_it_is_parked(self):
        self.write("k8s/ing.yaml", dump(ingress_doc()))
        self.assertIn("parked", self.routing(self.plan())["title"])
        self.assertNotIn(
            "parked", self.routing(self.plan(exports=GATEWAY_EXPORTS))
            ["title"])

    # -- the cross-namespace attach contract --------------------------------

    def test_live_brief_states_the_attach_label_contract(self):
        """The brief states the platform's guaranteed label contract (the
        tenancy/gateway halves are machine-checked platform-side) instead
        of narrating an unpublished unknown; the residual open question is
        only a namespace the platform did not issue."""
        self.write("k8s/ing.yaml", dump(ingress_doc()))
        joined = "\n".join(
            self.routing(self.plan(exports=GATEWAY_EXPORTS))["notes"])
        self.assertIn('gkma.dev/gateway-access: "shared"', joined)
        self.assertIn("guarantees attachment", joined)
        self.assertIn("NOT issued by the platform tenancy unit", joined)
        self.assertIn("open question", joined)
        # The dead-end narration is gone with the contract in place.
        self.assertNotIn("which exports does NOT publish", joined)
        self.assertNotIn("ReferenceGrant to the platform", joined)

    # -- non-ALB annotations are facts too ---------------------------------

    def test_non_alb_annotations_are_persisted_bookkeeping_is_not(self):
        self.write("k8s/ing.yaml", dump(ingress_doc(annotations={
            "alb.ingress.kubernetes.io/scheme": "internet-facing",
            "external-dns.alpha.kubernetes.io/hostname": "shop.acme.example",
            "kubectl.kubernetes.io/last-applied-configuration": "{}",
            "meta.helm.sh/release-name": "shop"})))
        unit = self.routing(self.plan())
        self.assertEqual(unit["inputs"]["ingress_facts"][0]["annotations"],
                         ["alb.ingress.kubernetes.io/scheme",
                          "external-dns.alpha.kubernetes.io/hostname"])



class PodDnsFactsTest(PlannerTestBase):
    """The manifests unit persists pod DNS facts (inputs.pod_dns_facts) and
    the brief carries them as facts plus the contract fence, never a mapping."""

    def test_the_key_is_always_written_and_empty_without_facts(self):
        self.write("k8s/web.yaml", dump(doc("Deployment", "web", spec={
            "template": {"spec": {"containers": [{"name": "w", "image": "x"}]}}})))
        unit = self.unit(self.plan(), "wkld-manifests")
        self.assertEqual(unit["inputs"]["pod_dns_facts"], [])
        self.assertFalse(any("Pod DNS facts" in n for n in unit["notes"]))

    def test_the_placeholder_unit_carries_the_empty_key(self):
        self.write("k8s/sa.yaml", dump(doc("ServiceAccount", "orders")))
        unit = self.unit(self.plan(), "wkld-manifests")
        self.assertTrue(unit["placeholder"])
        self.assertEqual(unit["inputs"]["pod_dns_facts"], [])

    def test_facts_are_recorded_per_document_with_the_brief_lines(self):
        self.write("k8s/mailer.yaml", dump(
            doc("Job", "mailer", spec={"template": {"spec": {
                "dnsPolicy": "None",
                "dnsConfig": {"nameservers": ["172.20.0.10", "10.20.0.53"],
                              "options": [{"name": "ndots", "value": "2"}]},
                "hostAliases": [{"ip": "10.0.5.5", "hostnames": ["db.internal"]}],
                "containers": [{"name": "m", "image": "x"}]}}}),
            doc("Deployment", "web", spec={"template": {"spec": {
                "containers": [{"name": "w", "image": "x"}]}}})))
        unit = self.unit(self.plan(), "wkld-manifests")
        facts = unit["inputs"]["pod_dns_facts"]
        self.assertEqual([f["label"] for f in facts], ["mailer"])
        self.assertEqual(facts[0]["kind"], "Job")
        self.assertEqual(facts[0]["namespace"], "acme-shop")
        self.assertEqual(facts[0]["node_path"], "spec.template.spec")
        self.assertEqual(facts[0]["dns_policy"], "None")
        self.assertEqual(facts[0]["nameservers"], ["172.20.0.10", "10.20.0.53"])
        self.assertEqual(facts[0]["options"], [{"name": "ndots", "value": "2"}])
        self.assertEqual(facts[0]["host_aliases"], [{"ip": "10.0.5.5", "hostnames": ["db.internal"]}])
        line = [n for n in unit["notes"] if n.startswith("Pod DNS facts of Job mailer")]
        self.assertEqual(len(line), 1)
        for literal in ("dnsPolicy None", "172.20.0.10, 10.20.0.53", "ndots=2", "10.0.5.5 -> db.internal"):
            self.assertIn(literal, line[0])
        fence = [n for n in unit["notes"] if "persisted as inputs.pod_dns_facts" in n]
        self.assertEqual(len(fence), 1)
        self.assertIn("pod-dns-translation.md", fence[0])
        self.assertIn("None rule", fence[0])
        # No mapping in the brief: what an address becomes is the knowledge's.
        self.assertFalse(any("drop" in n.lower() and "172.20.0.10" in n for n in unit["notes"]))

    def test_a_pod_template_in_a_residual_kind_is_facted(self):
        self.write("k8s/rollout.yaml", dump(doc(
            "Rollout", "web", api_version="argoproj.io/v1alpha1",
            spec={"template": {"spec": {"dnsConfig": {"searches": ["corp.acme.internal"]},
                                        "containers": [{"name": "w", "image": "x"}]}}})))
        unit = self.unit(self.plan(), "wkld-manifests")
        self.assertEqual([(f["kind"], f["searches"]) for f in unit["inputs"]["pod_dns_facts"]],
                         [("Rollout", ["corp.acme.internal"])])

    def test_helm_release_name_is_the_source_basename_slug(self):
        self.assertEqual(planner.helm_release_name("charts/Orders_API"), "orders-api")
        self.assertEqual(planner.helm_release_name("."), "wkld-render")
        self.assertEqual(planner.helm_release_name(""), "wkld-render")

    def test_a_nameless_document_is_facted_under_its_stream_label(self):
        import yaml
        bundle = {"apiVersion": "v1", "kind": "List", "items": [
            doc("Pod", "p", spec={"dnsPolicy": "Default", "containers": [{"name": "a", "image": "x"}]})]}
        self.write("k8s/bundle.yaml", yaml.safe_dump(bundle, sort_keys=False))
        unit = self.unit(self.plan(), "wkld-manifests")
        [fact] = unit["inputs"]["pod_dns_facts"]
        self.assertEqual((fact["kind"], fact["name"], fact["label"], fact["node_path"]),
                         ("List", None, "k8s/bundle.yaml#0", "items.0.spec"))
        line = [n for n in unit["notes"] if n.startswith("Pod DNS facts of List k8s/bundle.yaml#0")]
        self.assertEqual(len(line), 1)
        self.assertIn("no metadata.name", line[0])
        self.assertEqual(unit["inputs"]["pod_dns_unread"], [])

    def test_an_unrendered_chart_is_recorded_as_unread(self):
        self.write("charts/web/Chart.yaml", "apiVersion: v2\nname: web\nversion: 0.1.0\n")
        self.write("charts/web/templates/d.yaml", "apiVersion: apps/v1\nkind: Deployment\n")

        def failing(source_root, rel_dir):
            return None, "boom"
        unit = self.unit(self.plan(renderers={"helm": failing, "kustomize": self._no_render}),
                         "wkld-manifests")
        self.assertTrue(any("could not be rendered" in n for n in unit["inputs"]["pod_dns_unread"]))
        self.assertTrue(any("did not render at plan time" in n for n in unit["notes"]))

    def test_plan_is_deterministic_with_facts(self):
        self.write("k8s/web.yaml", dump(doc("Deployment", "web", spec={
            "template": {"spec": {"hostNetwork": True, "dnsPolicy": "ClusterFirstWithHostNet",
                                  "containers": [{"name": "w", "image": "x"}]}}})))
        first = self.unit(self.plan(), "wkld-manifests")["inputs"]["pod_dns_facts"]
        second = self.unit(self.plan(), "wkld-manifests")["inputs"]["pod_dns_facts"]
        self.assertEqual(first, second)
        self.assertEqual(first[0]["dns_policy"], "ClusterFirstWithHostNet")
        self.assertTrue(first[0]["host_network"])


if __name__ == "__main__":
    unittest.main()
