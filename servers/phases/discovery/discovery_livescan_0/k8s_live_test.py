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

"""Tests for the in-cluster walk.

The whole Kubernetes surface is a single injected `get_json(path)` — the
fake below returns canned list objects keyed by path, None for a 404 (how an
uninstalled CRD group answers). No kubernetes package, no cluster. The
guarantees under test: everything persisted is projected, no string value
outside projection's allowlists survives, and an absent collection is noted
rather than read as empty.
"""

import base64
import unittest

from servers.phases.discovery.discovery_livescan_0 import (k8s_live, live_schema,
                                                          projection)


def _b64(text):
    return base64.b64encode(text.encode("utf-8")).decode("ascii")


class FakeGetJson:
    """Maps a base path to a response; unknown paths answer 404 (None).

    A response may be a single list object (one page) or a list of pages,
    selected by the numeric `continue` token the walk echoes back.
    """

    def __init__(self, responses):
        self._responses = responses
        self.paths = []

    def __call__(self, path):
        self.paths.append(path)
        base, _, query = path.partition("?")
        if base not in self._responses:
            return None  # 404 — resource/CRD group not served.
        resp = self._responses[base]
        if resp is None:
            return None
        if isinstance(resp, list):
            params = dict(p.split("=", 1) for p in query.split("&") if "=" in p)
            index = int(params.get("continue", "0"))
            return resp[index]
        return resp


def _items(*items):
    return {"items": list(items), "metadata": {}}


def _deploy(name, namespace="default", containers=None):
    return {"metadata": {"name": name, "namespace": namespace, "uid": "u"},
            "spec": {"replicas": 2, "template": {"spec": {
                "serviceAccountName": f"{name}-sa",
                "containers": containers or [{"name": "c", "image": f"{name}:1"}],
            }}}}


class DiscoverClusterHappyPathTest(unittest.TestCase):

    def setUp(self):
        self.responses = {
            "/version": {"gitVersion": "v1.30.0-eks-fake"},
            "/api/v1/namespaces": _items(
                {"metadata": {"name": "shop"}}),
            "/apis/apps/v1/deployments": _items(_deploy("web", "shop")),
            "/apis/apps/v1/statefulsets": _items(),
            "/apis/apps/v1/daemonsets": _items(),
            "/apis/batch/v1/cronjobs": _items(),
            "/apis/autoscaling/v2/horizontalpodautoscalers": _items({
                "metadata": {"name": "web", "namespace": "shop"},
                "spec": {"scaleTargetRef": {"kind": "Deployment", "name": "web"},
                         "minReplicas": 2, "maxReplicas": 8,
                         "metrics": [{"type": "Resource"}]}}),
            "/api/v1/services": _items({
                "metadata": {"name": "web", "namespace": "shop"},
                "spec": {"type": "LoadBalancer", "clusterIP": "10.1.2.3"}}),
            "/apis/networking.k8s.io/v1/ingresses": _items(),
            "/apis/networking.k8s.io/v1/ingressclasses": _items(),
            "/api/v1/serviceaccounts": _items({
                "metadata": {"name": "web-sa", "namespace": "shop",
                             "annotations": {
                                 k8s_live.IRSA_ANNOTATION:
                                     "arn:aws:iam::1:role/web"}}}),
            "/api/v1/persistentvolumeclaims": _items({
                "metadata": {"name": "data", "namespace": "shop"},
                "spec": {"storageClassName": "gp3", "volumeName": "pvc-1"}}),
            "/api/v1/persistentvolumes": _items(),
            "/apis/storage.k8s.io/v1/storageclasses": _items({
                "metadata": {"name": "gp3"}, "provisioner": "ebs.csi.aws.com"}),
            "/api/v1/configmaps": _items(
                {"metadata": {"name": "kube-root-ca.crt", "namespace": "shop"},
                 "data": {"ca.crt": "..."}},
                {"metadata": {"name": "app-config", "namespace": "shop"},
                 "data": {"log.level": "info"}}),
            "/api/v1/secrets": _items(
                {"metadata": {"name": "sa-token", "namespace": "shop"},
                 "type": "kubernetes.io/service-account-token",
                 "data": {"token": _b64("xyz")}},
                {"metadata": {"name": "db", "namespace": "shop"},
                 "type": "Opaque",
                 "data": {"password": _b64("super-secret")}}),
            "/api/v1/nodes": _items({
                "metadata": {"name": "ip-10-0-0-1", "labels": {
                    "node.kubernetes.io/instance-type": "m5.large",
                    "topology.kubernetes.io/zone": "us-east-1a"}},
                "spec": {"taints": [{"key": "x", "value": "y",
                                     "effect": "NoSchedule"}]},
                "status": {"allocatable": {"cpu": "2", "memory": "8Gi"},
                           "nodeInfo": {"kubeletVersion": "v1.29.0"}}}),
            "/api/v1/pods": _items(
                {"metadata": {"name": "web-abc", "namespace": "shop",
                              "ownerReferences": [{"kind": "ReplicaSet"}]},
                 "spec": {"containers": [{"name": "c", "image": "web:1"}]}},
                {"metadata": {"name": "debug", "namespace": "shop"},
                 "spec": {"containers": [{"name": "c", "image": "busybox:1"}]}}),
        }
        self.ir = k8s_live.discover_cluster(FakeGetJson(self.responses))

    def test_namespaces_and_workloads_collected(self):
        self.assertEqual(len(self.ir["namespaces"]), 1)
        self.assertEqual(len(self.ir["workloads"]["Deployment"]), 1)
        self.assertEqual(self.ir["workloads"]["Deployment"][0]["kind"],
                         "Deployment")

    def test_hpa_collected_from_v2(self):
        self.assertEqual(len(self.ir["autoscaling"]["hpas"]), 1)

    def test_irsa_binding_extracted(self):
        irsa = self.ir["identity"]["irsa"]
        self.assertEqual(len(irsa), 1)
        self.assertEqual(irsa[0]["role_arn"], "arn:aws:iam::1:role/web")
        self.assertEqual(irsa[0]["sa"], "web-sa")

    def test_kube_root_ca_configmap_skipped(self):
        names = [(cm.get("metadata") or {}).get("name")
                 for cm in self.ir["config"]["configmaps"]]
        self.assertEqual(names, ["app-config"])

    def test_service_account_token_secret_skipped_and_counted(self):
        names = [(s.get("metadata") or {}).get("name")
                 for s in self.ir["config"]["secrets"]]
        self.assertEqual(names, ["db"])
        self.assertEqual(self.ir["config"]["skipped_secrets"]
                         ["service_account_tokens"], 1)

    def test_secret_record_is_type_and_key_names_only(self):
        secret = self.ir["config"]["secrets"][0]
        self.assertEqual(secret["data"], {"password": projection.OMITTED})
        self.assertEqual(secret["type"], "Opaque")
        self.assertNotIn("super-secret", str(self.ir))

    def test_nodes_reduced_to_capacity_fields(self):
        node = self.ir["nodes"][0]
        self.assertEqual(node["instance_type"], "m5.large")
        self.assertEqual(node["zone"], "us-east-1a")
        self.assertEqual(node["allocatable"], {"cpu": "2", "memory": "8Gi"})

    def test_pods_bare_count_and_running_images(self):
        self.assertEqual(self.ir["pod_count"], 2)
        bare_names = [(p.get("metadata") or {}).get("name")
                      for p in self.ir["bare_pods"]]
        self.assertEqual(bare_names, ["debug"])  # only the owner-less pod
        self.assertEqual(self.ir["images_running"], ["busybox:1", "web:1"])

    def test_counts_summarize_the_walk(self):
        counts = self.ir["counts"]
        self.assertEqual(counts["Deployment"], 1)
        self.assertEqual(counts["Secret"], 1)
        self.assertEqual(counts["Node"], 1)
        self.assertEqual(counts["Pod"], 2)
        self.assertEqual(counts["BarePod"], 1)

    def test_karpenter_absent_is_noted(self):
        self.assertTrue(any("Karpenter CRDs not present" in note
                            for note in self.ir["notes"]))

    def test_walk_output_matches_the_live_ir_schema(self):
        # The richest single-cluster walk in the suite (a projected Secret,
        # a reduced node, a bare pod, an IRSA binding), wrapped in the
        # smallest cluster record: the in-cluster shape held to its contract.
        live_schema.validate_live_ir({
            "live_ir_version": "2.0", "regions": ["us-east-1"], "notes": [],
            "clusters": [{"name": "prod", "region": "us-east-1",
                          "kubernetes": self.ir}],
            "summary": {
                "regions_scanned": 1, "regions_unreachable": [],
                "clusters_found": 1,
                "clusters_walked": 1, "clusters_unreachable": [],
                "clusters_using_karpenter": 0,
                "workload_totals": self.ir["counts"],
                "note_count": 0}})


class FallbackAndPaginationTest(unittest.TestCase):

    def test_hpa_falls_back_to_v2beta2(self):
        responses = _empty_cluster()
        del responses["/apis/autoscaling/v2/horizontalpodautoscalers"]  # 404
        responses["/apis/autoscaling/v2beta2/horizontalpodautoscalers"] = _items(
            {"metadata": {"name": "old-hpa", "namespace": "shop"}, "spec": {}})
        ir = k8s_live.discover_cluster(FakeGetJson(responses))
        self.assertEqual(len(ir["autoscaling"]["hpas"]), 1)

    def test_hpa_v2_served_but_empty_does_not_fall_back(self):
        # Served-and-empty is an answer: a cluster with no HPAs on v2 must not
        # be re-read from the deprecated version (which may 404, or on an old
        # control plane serve the same objects twice).
        responses = _empty_cluster()
        responses["/apis/autoscaling/v2beta2/horizontalpodautoscalers"] = _items(
            {"metadata": {"name": "old-hpa", "namespace": "shop"}, "spec": {}})
        get_json = FakeGetJson(responses)
        ir = k8s_live.discover_cluster(get_json)
        self.assertEqual(ir["autoscaling"]["hpas"], [])
        self.assertFalse(any("v2beta2" in path for path in get_json.paths))

    def test_karpenter_v1beta1_fallback_is_used(self):
        responses = _empty_cluster()
        responses["/apis/karpenter.sh/v1beta1/nodepools"] = _items(
            {"metadata": {"name": "default"}, "spec": {}})
        ir = k8s_live.discover_cluster(FakeGetJson(responses))
        self.assertEqual(len(ir["autoscaling"]["karpenter"]["nodepools"]), 1)
        self.assertFalse(any("Karpenter CRDs not present" in note
                             for note in ir["notes"]))

    def test_karpenter_installed_with_no_objects_is_not_noted_as_absent(self):
        # The CRDs are served but hold nothing yet: a Karpenter cluster with
        # nothing provisioned, not a managed-nodegroups-only cluster.
        responses = _empty_cluster()
        responses["/apis/karpenter.sh/v1/nodepools"] = _items()
        responses["/apis/karpenter.k8s.aws/v1/ec2nodeclasses"] = _items()
        ir = k8s_live.discover_cluster(FakeGetJson(responses))
        self.assertEqual(ir["autoscaling"]["karpenter"]["nodepools"], [])
        self.assertFalse(any("Karpenter CRDs not present" in note
                             for note in ir["notes"]))

    def test_list_pagination_follows_continue_token(self):
        responses = _empty_cluster()
        responses["/apis/apps/v1/deployments"] = [
            {"items": [_deploy("a")], "metadata": {"continue": "1"}},
            {"items": [_deploy("b")], "metadata": {}},
        ]
        ir = k8s_live.discover_cluster(FakeGetJson(responses))
        names = [(d.get("metadata") or {}).get("name")
                 for d in ir["workloads"]["Deployment"]]
        self.assertEqual(names, ["a", "b"])


class NamespaceScopeTest(unittest.TestCase):

    def test_aws_system_namespaces_are_skipped_by_default_and_noted(self):
        # EKS-managed plumbing (aws-node, the CloudWatch agent) has no GKE
        # translation; a default walk must keep it out of the IR — including
        # the running-image inventory — and say how much it dropped.
        responses = _empty_cluster()
        responses["/apis/apps/v1/deployments"] = _items(
            _deploy("web", "shop"), _deploy("aws-node", "kube-system"))
        responses["/api/v1/pods"] = _items(
            {"metadata": {"name": "cw", "namespace": "amazon-cloudwatch"},
             "spec": {"containers": [{"name": "c",
                                      "image": "cloudwatch-agent:1"}]}})
        ir = k8s_live.discover_cluster(FakeGetJson(responses))
        names = [(d.get("metadata") or {}).get("name")
                 for d in ir["workloads"]["Deployment"]]
        self.assertEqual(names, ["web"])
        self.assertEqual(ir["pod_count"], 0)
        self.assertEqual(ir["images_running"], [])
        self.assertEqual(ir["counts"]["Deployment"], 1)
        self.assertTrue(any("AWS system namespaces" in note
                            for note in ir["notes"]))

    def test_explicit_target_narrows_the_walk_and_notes_the_drop(self):
        responses = _empty_cluster()
        responses["/apis/apps/v1/deployments"] = _items(
            _deploy("web", "shop"), _deploy("api", "billing"))
        ir = k8s_live.discover_cluster(FakeGetJson(responses),
                                       namespaces=["shop"])
        names = [(d.get("metadata") or {}).get("name")
                 for d in ir["workloads"]["Deployment"]]
        self.assertEqual(names, ["web"])
        self.assertTrue(any("scan restricted to namespace(s)" in note
                            for note in ir["notes"]))

    def test_naming_a_system_namespace_deliberately_includes_it(self):
        # An explicit target overrides the default skip — asking for
        # kube-system means the operator wants kube-system.
        responses = _empty_cluster()
        responses["/apis/apps/v1/deployments"] = _items(
            _deploy("aws-node", "kube-system"), _deploy("web", "shop"))
        ir = k8s_live.discover_cluster(FakeGetJson(responses),
                                       namespaces=["kube-system"])
        names = [(d.get("metadata") or {}).get("name")
                 for d in ir["workloads"]["Deployment"]]
        self.assertEqual(names, ["aws-node"])

    def test_cluster_scoped_objects_are_never_filtered(self):
        # PVs, nodes and the Namespace list itself carry no namespace; a
        # narrowed walk must still collect all of them — they are the
        # estate's shape, not workloads in it.
        responses = _empty_cluster()
        responses["/api/v1/namespaces"] = _items(
            {"metadata": {"name": "shop"}},
            {"metadata": {"name": "kube-system"}})
        responses["/api/v1/persistentvolumes"] = _items(
            {"metadata": {"name": "pv-1"}, "spec": {}})
        responses["/api/v1/nodes"] = _items({"metadata": {"name": "n1"}})
        ir = k8s_live.discover_cluster(FakeGetJson(responses),
                                       namespaces=["shop"])
        self.assertEqual(len(ir["namespaces"]), 2)
        self.assertEqual(len(ir["storage"]["pvs"]), 1)
        self.assertEqual(len(ir["nodes"]), 1)

    def test_config_and_secrets_respect_the_scope(self):
        responses = _empty_cluster()
        responses["/api/v1/configmaps"] = _items(
            {"metadata": {"name": "app", "namespace": "shop"},
             "data": {"k": "v"}},
            {"metadata": {"name": "aws-auth", "namespace": "kube-system"},
             "data": {"mapRoles": "..."}})
        responses["/api/v1/secrets"] = _items(
            {"metadata": {"name": "db", "namespace": "shop"},
             "type": "Opaque", "data": {"password": _b64("x")}},
            {"metadata": {"name": "sys", "namespace": "kube-system"},
             "type": "Opaque", "data": {"password": _b64("y")}})
        ir = k8s_live.discover_cluster(FakeGetJson(responses))
        self.assertEqual([cm["metadata"]["name"]
                          for cm in ir["config"]["configmaps"]], ["app"])
        self.assertEqual([s["metadata"]["name"]
                          for s in ir["config"]["secrets"]], ["db"])


class RoutingCrdTest(unittest.TestCase):

    def test_gateway_api_objects_are_collected_and_counted(self):
        responses = _empty_cluster()
        responses["/apis/gateway.networking.k8s.io/v1/gatewayclasses"] = _items(
            {"metadata": {"name": "gke-l7"},
             "spec": {"controllerName": "example.io/gateway"}})
        responses["/apis/gateway.networking.k8s.io/v1/gateways"] = _items(
            {"metadata": {"name": "web-gw", "namespace": "shop"},
             "spec": {"gatewayClassName": "gke-l7",
                      "listeners": [{"protocol": "HTTPS", "port": 443,
                                     "hostname": "shop.example.com"}]}})
        responses["/apis/gateway.networking.k8s.io/v1/httproutes"] = _items(
            {"metadata": {"name": "web-route", "namespace": "shop"},
             "spec": {"parentRefs": [{"name": "web-gw"}],
                      "hostnames": ["shop.example.com"]}})
        ir = k8s_live.discover_cluster(FakeGetJson(responses))
        gateway_api = ir["networking"]["gateway_api"]
        self.assertEqual(len(gateway_api["gateway_classes"]), 1)
        self.assertEqual(len(gateway_api["gateways"]), 1)
        self.assertEqual(len(gateway_api["http_routes"]), 1)
        self.assertEqual(gateway_api["gateways"][0]["kind"], "Gateway")
        self.assertEqual(ir["counts"]["GatewayAPIGateway"], 1)
        self.assertEqual(ir["counts"]["HTTPRoute"], 1)

    def test_gateway_api_falls_back_to_v1beta1(self):
        responses = _empty_cluster()
        responses["/apis/gateway.networking.k8s.io/v1beta1/httproutes"] = \
            _items({"metadata": {"name": "old", "namespace": "shop"},
                    "spec": {}})
        ir = k8s_live.discover_cluster(FakeGetJson(responses))
        self.assertEqual(
            len(ir["networking"]["gateway_api"]["http_routes"]), 1)

    def test_istio_and_traefik_objects_are_collected(self):
        # Group versions fall back per kind: Istio gateways answer at v1
        # while its virtual services only exist at v1beta1, and Traefik
        # serves the legacy traefik.containo.us group.
        responses = _empty_cluster()
        responses["/apis/networking.istio.io/v1/gateways"] = _items(
            {"metadata": {"name": "mesh-gw", "namespace": "shop"},
             "spec": {"selector": {"istio": "ingressgateway"},
                      "servers": [{"hosts": ["*.example.com"]}]}})
        responses["/apis/networking.istio.io/v1beta1/virtualservices"] = \
            _items({"metadata": {"name": "web-vs", "namespace": "shop"},
                    "spec": {"hosts": ["shop.example.com"],
                             "gateways": ["mesh-gw"]}})
        responses["/apis/traefik.containo.us/v1alpha1/ingressroutes"] = _items(
            {"metadata": {"name": "web-ir", "namespace": "shop"},
             "spec": {"entryPoints": ["websecure"],
                      "routes": [{"match": "Host(`shop.example.com`)"}]}})
        ir = k8s_live.discover_cluster(FakeGetJson(responses))
        self.assertEqual(len(ir["networking"]["istio"]["gateways"]), 1)
        self.assertEqual(len(ir["networking"]["istio"]["virtual_services"]), 1)
        self.assertEqual(
            len(ir["networking"]["traefik"]["ingress_routes"]), 1)
        self.assertEqual(ir["counts"]["IstioGateway"], 1)
        self.assertEqual(ir["counts"]["IstioVirtualService"], 1)
        self.assertEqual(ir["counts"]["TraefikIngressRoute"], 1)

    def test_absent_routing_stacks_earn_no_note(self):
        # Unlike Karpenter (whose absence is itself a finding), Gateway API /
        # Istio / Traefik not being installed is the normal case: empty
        # groups, no note.
        ir = k8s_live.discover_cluster(FakeGetJson(_empty_cluster()))
        self.assertEqual(ir["networking"]["gateway_api"],
                         {"gateway_classes": [], "gateways": [],
                          "http_routes": []})
        self.assertEqual(ir["networking"]["istio"],
                         {"gateways": [], "virtual_services": []})
        self.assertEqual(ir["networking"]["traefik"],
                         {"ingress_routes": [], "middlewares": []})
        self.assertFalse(any("Gateway" in note or "Istio" in note
                             or "Traefik" in note for note in ir["notes"]))

    def test_routing_crds_respect_the_namespace_scope(self):
        responses = _empty_cluster()
        responses["/apis/gateway.networking.k8s.io/v1/httproutes"] = _items(
            {"metadata": {"name": "sys-route", "namespace": "kube-system"},
             "spec": {}},
            {"metadata": {"name": "web-route", "namespace": "shop"},
             "spec": {}})
        ir = k8s_live.discover_cluster(FakeGetJson(responses))
        names = [(r.get("metadata") or {}).get("name")
                 for r in ir["networking"]["gateway_api"]["http_routes"]]
        self.assertEqual(names, ["web-route"])


class ExternalSecretsTest(unittest.TestCase):

    def test_external_secret_objects_are_collected_and_noted(self):
        responses = _empty_cluster()
        responses["/apis/external-secrets.io/v1/externalsecrets"] = _items(
            {"metadata": {"name": "db-creds", "namespace": "shop"},
             "spec": {"secretStoreRef": {"kind": "ClusterSecretStore",
                                         "name": "aws-sm"},
                      "data": [{"secretKey": "password",
                                "remoteRef": {"key": "prod/db"}}]}})
        responses["/apis/external-secrets.io/v1beta1/secretstores"] = _items(
            {"metadata": {"name": "aws-sm-ns", "namespace": "shop"},
             "spec": {"provider": {"aws": {"service": "SecretsManager"}}}})
        responses["/apis/external-secrets.io/v1/clustersecretstores"] = _items(
            {"metadata": {"name": "aws-sm"},
             "spec": {"provider": {"aws": {"service": "SecretsManager"}}}})
        ir = k8s_live.discover_cluster(FakeGetJson(responses))
        external = ir["config"]["external_secrets"]
        self.assertEqual(len(external["external_secrets"]), 1)
        self.assertEqual(len(external["secret_stores"]), 1)
        self.assertEqual(len(external["cluster_secret_stores"]), 1)
        self.assertEqual(ir["counts"]["ExternalSecret"], 1)
        self.assertEqual(ir["counts"]["ExternalSecretStore"], 2)
        self.assertTrue(any("Google Secret Manager" in note
                            for note in ir["notes"]))

    def test_secret_provider_class_parameters_keep_keys_only(self):
        # A SecretProviderClass's spec.parameters is free-form provider
        # config: an inlined key, a client secret and a benign objects list
        # all leave as their key names, the provider survives as-is.
        responses = _empty_cluster()
        responses[
            "/apis/secrets-store.csi.x-k8s.io/v1/secretproviderclasses"] = \
            _items({"metadata": {"name": "aws-spc", "namespace": "shop"},
                    "spec": {"provider": "aws", "parameters": {
                        "accessKey": "AKIAIOSFODNN7EXAMPLE",
                        "clientSecret": "wJalrSpcSecretValue",
                        "objects": "- objectName: prod/db"}}})
        ir = k8s_live.discover_cluster(FakeGetJson(responses))
        spc = ir["config"]["external_secrets"]["secret_provider_classes"][0]
        self.assertEqual(spc["spec"]["provider"], "aws")
        self.assertEqual(spc["spec"]["parameters"], {
            "accessKey": projection.OMITTED,
            "clientSecret": projection.OMITTED,
            "objects": projection.OMITTED})
        self.assertNotIn("AKIAIOSFODNN7EXAMPLE", str(ir))
        self.assertNotIn("wJalrSpcSecretValue", str(ir))
        self.assertEqual(ir["counts"]["SecretProviderClass"], 1)

    def test_absent_external_secret_stacks_earn_no_note(self):
        ir = k8s_live.discover_cluster(FakeGetJson(_empty_cluster()))
        self.assertEqual(ir["config"]["external_secrets"],
                         {"external_secrets": [], "secret_stores": [],
                          "cluster_secret_stores": [],
                          "secret_provider_classes": []})
        self.assertFalse(any("external-secret" in note
                             for note in ir["notes"]))


class ReachabilityProbeTest(unittest.TestCase):

    def test_server_version_is_recorded_from_the_probe(self):
        responses = _empty_cluster()
        responses["/version"] = {"gitVersion": "v1.29.4-eks-abc"}
        ir = k8s_live.discover_cluster(FakeGetJson(responses))
        self.assertEqual(ir["server_version"], "v1.29.4-eks-abc")

    def test_a_probe_404_fails_the_cluster_before_any_collection(self):
        # Every API server serves /version: a 404 is a proxy, a WAF or the
        # wrong endpoint, which would 404 every list below into a served,
        # empty cluster with no note.
        responses = _empty_cluster()
        del responses["/version"]
        fake = FakeGetJson(responses)
        with self.assertRaises(RuntimeError) as raised:
            k8s_live.discover_cluster(fake)
        self.assertIn("404 to /version", str(raised.exception))
        self.assertEqual(fake.paths, ["/version"])

    def test_an_unrecognized_version_answer_leaves_no_version(self):
        # A /version without gitVersion records no version; the walk itself
        # must complete normally.
        responses = _empty_cluster()
        responses["/version"] = {"major": "1"}
        ir = k8s_live.discover_cluster(FakeGetJson(responses))
        self.assertNotIn("server_version", ir)
        self.assertIn("counts", ir)

    def test_unreachable_control_plane_propagates_before_any_collection(self):
        # Every list call downgrades a failure to a note, so without the
        # probe an unreachable cluster would "succeed" into an IR that is
        # nothing but failure notes. The probe's exception must escape — and
        # before any collection ran.
        calls = []

        def get_json(path):
            calls.append(path)
            raise RuntimeError("connection refused")

        with self.assertRaises(RuntimeError):
            k8s_live.discover_cluster(get_json)
        self.assertEqual(calls, ["/version"])

    def test_every_read_denied_fails_the_cluster_with_the_grant_to_make(self):
        # The identity authenticates (/version answers) but is authorized for
        # nothing: without this check the walk "succeeds" into forty-odd
        # per-collection 403 notes that read as an empty cluster. The
        # operator gets one line: what happened and the grant to make.
        class Denied(Exception):
            status = 403

        def get_json(path):
            if path == "/version":
                return {"gitVersion": "v1.31.14-eks-abc"}
            raise Denied("HTTP 403 Forbidden: namespaces is forbidden: User "
                         "\"arn:aws:sts::1:assumed-role/r/u\" cannot list "
                         "resource \"namespaces\"")

        with self.assertRaises(PermissionError) as ctx:
            k8s_live.discover_cluster(get_json)
        text = str(ctx.exception)
        self.assertIn("authenticated to the control plane (v1.31.14-eks-abc)", text)
        self.assertIn("authorized for no read", text)
        self.assertIn("cannot list resource", text)     # the first denial
        self.assertIn("AmazonEKSViewPolicy", text)     # the fix

    def test_a_partial_denial_stays_a_per_collection_note(self):
        # One collection refused among many that answered is a coverage gap
        # in that section, not an unreachable cluster.
        class Denied(Exception):
            status = 403

        fake = FakeGetJson(_empty_cluster())

        def get_json(path):
            if path.startswith("/api/v1/secrets"):
                raise Denied("HTTP 403 Forbidden: secrets is forbidden")
            return fake(path)

        ir = k8s_live.discover_cluster(get_json)
        self.assertIn("counts", ir)
        self.assertEqual(
            [n for n in ir["notes"] if "Secret not collected" in n],
            ["Secret not collected (/api/v1/secrets): HTTP 403 Forbidden: "
             "secrets is forbidden"])

    def test_errors_without_a_status_never_count_as_denials(self):
        # A timeout on every collection is not an authorization failure:
        # only refusals (status 401/403) get the grant-to-make advice. It
        # fails the cluster all the same, as the connectivity failure it
        # is (a walk that read nothing is not an empty cluster); a timeout
        # on some collections is a note beside the others' objects.
        def get_json(path):
            if path == "/version":
                return {"gitVersion": "v1.31.14-eks-abc"}
            raise RuntimeError("read timed out")

        with self.assertRaises(ConnectionError) as ctx:
            k8s_live.discover_cluster(get_json)
        self.assertNotIsInstance(ctx.exception, PermissionError)
        self.assertIn("read timed out", str(ctx.exception))
        self.assertNotIn("AmazonEKSViewPolicy", str(ctx.exception))

        inner = FakeGetJson(_empty_cluster())

        def get_json_some(path):
            if path.startswith("/api/v1/secrets"):
                raise RuntimeError("read timed out")
            return inner(path)

        ir = k8s_live.discover_cluster(get_json_some)
        self.assertIn("counts", ir)
        self.assertTrue(any("read timed out" in n for n in ir["notes"]))


def _empty_cluster():
    """Every list endpoint present but empty, so a test overrides just one."""
    return {
        "/version": {"gitVersion": "v1.30.0-eks-fake"},
        "/api/v1/namespaces": _items(),
        "/apis/apps/v1/deployments": _items(),
        "/apis/apps/v1/statefulsets": _items(),
        "/apis/apps/v1/daemonsets": _items(),
        "/apis/batch/v1/cronjobs": _items(),
        "/apis/autoscaling/v2/horizontalpodautoscalers": _items(),
        "/api/v1/services": _items(),
        "/apis/networking.k8s.io/v1/ingresses": _items(),
        "/apis/networking.k8s.io/v1/ingressclasses": _items(),
        "/api/v1/serviceaccounts": _items(),
        "/api/v1/persistentvolumeclaims": _items(),
        "/api/v1/persistentvolumes": _items(),
        "/apis/storage.k8s.io/v1/storageclasses": _items(),
        "/api/v1/configmaps": _items(),
        "/api/v1/secrets": _items(),
        "/api/v1/nodes": _items(),
        "/api/v1/pods": _items(),
    }


class CredentialLossEndsTheWalkTest(unittest.TestCase):
    """A 401 that carries a failed re-mint (eks_auth) means no later read
    can succeed: the walk ends the cluster on it, as one error with the
    credential advice, instead of noting every remaining collection."""

    def test_a_401_whose_re_mint_failed_ends_the_cluster_walk(self):
        class Refused(Exception):
            status = 401
            token_refresh_error = RuntimeError("sso session ended")

        calls = []

        def get_json(path):
            calls.append(path)
            if path == "/version":
                return {"gitVersion": "v1.31.14-eks-abc"}
            if path.startswith("/api/v1/namespaces"):
                return _items({"metadata": {"name": "shop"}})
            raise Refused("HTTP 401 Unauthorized: the token needed refreshing "
                          "and a fresh one could not be minted "
                          "(RuntimeError: sso session ended)")

        with self.assertRaises(Refused):
            k8s_live.discover_cluster(get_json)
        # One read after the namespaces answered, then no more.
        self.assertEqual(len(calls), 3)

    def test_a_401_without_one_stays_a_per_collection_note(self):
        # An unmapped identity that is denied one collection among many is
        # a coverage gap, not a lost credential: the walk goes on.
        class Denied(Exception):
            status = 401
            token_refresh_error = None

        def get_json(path):
            if path == "/version":
                return {"gitVersion": "v1.31.14-eks-abc"}
            if path.startswith("/apis/apps/v1/deployments"):
                raise Denied("HTTP 401 Unauthorized")
            return _items()

        ir = k8s_live.discover_cluster(get_json)
        self.assertEqual([n for n in ir["notes"] if "Deployment" in n],
                         ["Deployment not collected "
                          "(/apis/apps/v1/deployments): HTTP 401 Unauthorized"])


if __name__ == "__main__":
    unittest.main()
