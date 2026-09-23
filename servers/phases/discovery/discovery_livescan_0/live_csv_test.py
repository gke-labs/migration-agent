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

"""Tests for the live IR → CSV projection.

Pure derivation over a hand-built IR. The invariants under test: every table
is always present (a missing file cannot be told from an empty scan), the
storage target mapping is the fixed platform fact the module claims, and
image parsing classifies ECR without inventing an Artifact Registry name.
"""

import csv
import io
import unittest

from servers.phases.discovery.discovery_livescan_0 import live_csv, projection


def _rows(csv_text):
    return list(csv.reader(io.StringIO(csv_text)))


def _cluster():
    return {
        "region": "us-east-1", "name": "prod",
        "kubernetes_version": "1.29", "platform_version": "eks.5",
        "status": "ACTIVE",
        "endpoint_access": {"public": True, "private": False},
        "vpc": {"vpc_id": "vpc-1", "subnet_ids": ["subnet-1", "subnet-2"]},
        "oidc_issuer": "https://oidc/id",
        "addons": [{"name": "vpc-cni", "version": "v1.16"}],
        "nodegroups": [{
            "name": "ng-1", "status": "ACTIVE", "capacity_type": "ON_DEMAND",
            "instance_types": ["m5.large"], "ami_type": "AL2_x86_64",
            "release_version": "1.29.0", "disk_size_gb": 50,
            "scaling": {"min": 1, "max": 5, "desired": 2},
            "labels": {"role": "worker"},
            "taints": [{"key": "gpu", "value": "true", "effect": "NO_SCHEDULE"}],
            "asg_names": ["asg-1"]}],
        "load_balancers": [{
            "name": "web-alb", "type": "application", "scheme": "internet-facing",
            "dns_name": "web-alb.elb.amazonaws.com", "ingress_group": "shop/web"}],
        "kubernetes": {
            "workloads": {"Deployment": [{
                "metadata": {"name": "web", "namespace": "shop"},
                "spec": {"replicas": 3, "template": {"spec": {
                    "serviceAccountName": "web-sa",
                    "nodeSelector": {"disktype": "ssd"},
                    "tolerations": [{"key": "gpu"}],
                    "affinity": {"nodeAffinity": {}},
                    "topologySpreadConstraints": [{"maxSkew": 1}],
                    "containers": [{
                        "name": "c",
                        "image": "123456789012.dkr.ecr.us-east-1"
                                 ".amazonaws.com/web:v2",
                        "resources": {"requests": {"cpu": "500m", "memory": "1Gi"},
                                      "limits": {"cpu": "1", "memory": "2Gi"}}}]}}}}]},
            "autoscaling": {
                "hpas": [{"metadata": {"name": "web", "namespace": "shop"},
                          "spec": {"scaleTargetRef": {"kind": "Deployment",
                                                      "name": "web"},
                                   "minReplicas": 2, "maxReplicas": 8,
                                   "metrics": [{"type": "Resource"}]}}],
                "karpenter": {"nodepools": [{
                    "metadata": {"name": "default"},
                    "spec": {"limits": {"cpu": "1000"}}}]}},
            "networking": {
                "ingresses": [{
                    "metadata": {"name": "web", "namespace": "shop",
                                 "annotations": {
                                     "alb.ingress.kubernetes.io/scheme":
                                         "internet-facing"}},
                    "spec": {"ingressClassName": "alb",
                             "rules": [{"host": "shop.example.com"}]}}],
                "ingress_classes": [{
                    "metadata": {"name": "alb"},
                    "spec": {"controller": "ingress.k8s.aws/alb"}}],
                "services": [{
                    "metadata": {"name": "web", "namespace": "shop"},
                    "spec": {"type": "LoadBalancer"}}]},
            "identity": {
                "irsa": [{"namespace": "shop", "sa": "web-sa",
                          "role_arn": "arn:aws:iam::1:role/web",
                          "role": {"exists": True,
                                   "trusted_subjects": ["system:sa:shop:web-sa"],
                                   "attached_policies": ["arn:policy/s3"]}}]},
            "storage": {
                "pvcs": [{"metadata": {"name": "data", "namespace": "shop"},
                          "spec": {"storageClassName": "gp3",
                                   "resources": {"requests": {"storage": "20Gi"}},
                                   "accessModes": ["ReadWriteOnce"]}}],
                "storage_classes": [{"metadata": {"name": "gp3"},
                                     "provisioner": "ebs.csi.aws.com"}]},
            "config": {
                "configmaps": [{"metadata": {"name": "app", "namespace": "shop"},
                                "data": {"log.level": "<omitted>"}}],
                "secrets": [{"metadata": {"name": "db", "namespace": "shop"},
                             "type": "Opaque",
                             "data": {"password": "<omitted>"}}]},
            "images_running": [
                "123456789012.dkr.ecr.us-east-1.amazonaws.com/web:v2"],
        },
    }


class RenderTablesTest(unittest.TestCase):

    def setUp(self):
        self.tables = live_csv.render_tables({"clusters": [_cluster()]})

    def test_all_tables_always_present(self):
        self.assertEqual(set(self.tables), set(live_csv.TABLES))

    def test_clusters_table(self):
        rows = _rows(self.tables["clusters"])
        self.assertEqual(rows[0][:3], ["region", "cluster", "kubernetes_version"])
        self.assertEqual(rows[1][0], "us-east-1")
        self.assertEqual(rows[1][1], "prod")
        # subnet_ids are joined with ';'.
        self.assertIn("subnet-1;subnet-2", rows[1])

    def test_boolean_cells_render_lower_case_in_every_table(self):
        # `endpoint_public`, `endpoint_private` and `role_exists` bypassed
        # _cell and came out `True`/`False` beside `has_affinity`'s `true`.
        rows = _rows(self.tables["clusters"])
        cluster = dict(zip(rows[0], rows[1]))
        self.assertEqual(cluster["endpoint_public"], "true")
        self.assertEqual(cluster["endpoint_private"], "false")
        identity = _rows(self.tables["identity"])
        irsa = dict(zip(identity[0],
                        next(r for r in identity[1:] if r[2] == "IRSA")))
        self.assertEqual(irsa["role_exists"], "true")

    def test_an_unknown_boolean_is_the_empty_cell(self):
        cluster = _cluster()
        del cluster["endpoint_access"]
        cluster["kubernetes"]["identity"]["irsa"][0]["role"] = {
            "exists": None, "error": "AccessDenied"}
        tables = live_csv.render_tables({"clusters": [cluster]})
        rows = _rows(tables["clusters"])
        self.assertEqual(dict(zip(rows[0], rows[1]))["endpoint_public"], "")
        identity = _rows(tables["identity"])
        irsa = dict(zip(identity[0],
                        next(r for r in identity[1:] if r[2] == "IRSA")))
        self.assertEqual(irsa["role_exists"], "")
        self.assertEqual(irsa["detail"], "AccessDenied")

    def test_a_value_less_taint_and_an_unversioned_addon_render_empty(self):
        # EKS returns a taint without a value as `value: None`; the cell
        # is `dedicated=:NO_SCHEDULE`, not the word None. Same for an
        # addon with no version.
        cluster = _cluster()
        cluster["nodegroups"][0]["taints"] = [
            {"key": "dedicated", "value": None, "effect": "NO_SCHEDULE"}]
        cluster["addons"] = [{"name": "coredns", "version": None}]
        tables = live_csv.render_tables({"clusters": [cluster]})
        self.assertIn("dedicated=:NO_SCHEDULE", _rows(tables["nodegroups"])[1])
        self.assertTrue(any("coredns=" in cell
                            for cell in _rows(tables["clusters"])[1]))
        self.assertNotIn("None", tables["nodegroups"] + tables["clusters"])

    def test_nodegroups_table(self):
        rows = _rows(self.tables["nodegroups"])
        self.assertEqual(rows[1][2], "ng-1")
        self.assertIn("gpu=true:NO_SCHEDULE", rows[1])
        self.assertIn("asg-1", rows[1])

    def test_workloads_table_carries_resources_and_scheduling(self):
        rows = _rows(self.tables["workloads"])
        header, row = rows[0], rows[1]
        record = dict(zip(header, row))
        self.assertEqual(record["kind"], "Deployment")
        self.assertEqual(record["replicas"], "3")
        self.assertEqual(record["service_account"], "web-sa")
        self.assertEqual(record["requests_cpu"], "500m")
        self.assertEqual(record["limits_memory"], "2Gi")
        self.assertEqual(record["has_affinity"], "true")
        self.assertEqual(record["topology_spread_constraints"], "1")

    def test_autoscaling_table_has_hpa_and_karpenter(self):
        rows = _rows(self.tables["autoscaling"])
        kinds = {row[2] for row in rows[1:]}
        self.assertEqual(kinds, {"HorizontalPodAutoscaler", "KarpenterNodePool"})

    def test_networking_table_lists_lb_ingress_and_service(self):
        rows = _rows(self.tables["networking"])
        kinds = {row[2] for row in rows[1:]}
        self.assertIn("CloudLoadBalancer", kinds)
        self.assertIn("IngressClass", kinds)
        self.assertIn("Ingress", kinds)
        self.assertIn("ServiceLoadBalancer", kinds)

    def test_identity_table_has_the_irsa_binding(self):
        rows = _rows(self.tables["identity"])
        self.assertEqual({row[2] for row in rows[1:]}, {"IRSA"})
        irsa_row = dict(zip(rows[0], rows[1]))
        self.assertEqual(irsa_row["trusted_subjects"], "system:sa:shop:web-sa")
        self.assertEqual(irsa_row["attached_policies"], "arn:policy/s3")

    def test_storage_table_maps_ebs_to_persistent_disk(self):
        rows = _rows(self.tables["storage"])
        record = dict(zip(rows[0], rows[1]))
        self.assertEqual(record["storage_class"], "gp3")
        self.assertEqual(record["provisioner"], "ebs.csi.aws.com")
        self.assertEqual(record["requested"], "20Gi")
        self.assertIn("Persistent Disk", record["gke_target"])

    def test_config_table_lists_configmap_and_secret_keys(self):
        rows = _rows(self.tables["config"])
        records = [dict(zip(rows[0], r)) for r in rows[1:]]
        by_kind = {r["kind"]: r for r in records}
        self.assertEqual(by_kind["ConfigMap"]["keys"], "log.level")
        self.assertEqual(by_kind["Secret"]["keys"], "password")

    def test_images_table_classifies_ecr_and_marks_running(self):
        rows = _rows(self.tables["images"])
        record = dict(zip(rows[0], rows[1]))
        self.assertEqual(record["registry_kind"], "ecr")
        self.assertEqual(record["repository"],
                         "123456789012.dkr.ecr.us-east-1.amazonaws.com/web")
        self.assertEqual(record["tag"], "v2")
        self.assertEqual(record["running"], "true")
        # No Artifact Registry destination is invented here.
        self.assertNotIn("artifact", self.tables["images"].lower())


class StoragePvJoinTest(unittest.TestCase):

    def test_static_s3_pv_joins_its_pvc_and_maps_to_gcs_fuse(self):
        # A statically-provisioned S3 CSI volume has no StorageClass; its
        # driver lives on the PV spec, joined to the PVC through the PV's own
        # claimRef. The fixed platform mapping sends it to GCS FUSE.
        cluster = {
            "name": "prod",
            "kubernetes": {"storage": {
                "pvcs": [{
                    "metadata": {"name": "ml-data", "namespace": "shop"},
                    "spec": {
                        "resources": {"requests": {"storage": "100Gi"}},
                        "accessModes": ["ReadWriteMany"]}}],
                "pvs": [{
                    "metadata": {"name": "s3-pv"},
                    "spec": {"claimRef": {"namespace": "shop",
                                          "name": "ml-data"},
                             "csi": {"driver": "s3.csi.aws.com",
                                     "volumeHandle": "bucket"}}}],
                "storage_classes": []}}}
        rows = _rows(live_csv.render_tables({"clusters": [cluster]})["storage"])
        self.assertEqual(len(rows), 2)  # the claimed PV is not double-listed
        record = dict(zip(rows[0], rows[1]))
        self.assertEqual(record["pvc"], "ml-data")
        self.assertEqual(record["pv"], "s3-pv")
        self.assertEqual(record["provisioner"], "s3.csi.aws.com")
        self.assertEqual(record["requested"], "100Gi")
        self.assertIn("GCS FUSE", record["gke_target"])

    def test_unclaimed_pv_gets_its_own_row(self):
        # A released volume no collected PVC claims still holds data that
        # must land somewhere on GKE — a row, not a blind spot.
        cluster = {
            "name": "prod",
            "kubernetes": {"storage": {
                "pvcs": [],
                "pvs": [{
                    "metadata": {"name": "orphan-pv"},
                    "spec": {"capacity": {"storage": "50Gi"},
                             "accessModes": ["ReadWriteOnce"],
                             "claimRef": {"namespace": "old-ns",
                                          "name": "old-claim"},
                             "awsElasticBlockStore": {"volumeID": "vol-9"}}}],
                "storage_classes": []}}}
        rows = _rows(live_csv.render_tables({"clusters": [cluster]})["storage"])
        record = dict(zip(rows[0], rows[1]))
        self.assertEqual(record["pv"], "orphan-pv")
        self.assertEqual(record["namespace"], "old-ns")
        self.assertEqual(record["pvc"], "old-claim")
        self.assertEqual(record["provisioner"], "kubernetes.io/aws-ebs")
        self.assertEqual(record["requested"], "50Gi")
        self.assertIn("Persistent Disk", record["gke_target"])


class RoutingRowsTest(unittest.TestCase):

    def test_networking_table_lists_gateway_istio_and_traefik_objects(self):
        cluster = {
            "name": "prod",
            "kubernetes": {"networking": {
                "gateway_api": {
                    "gateway_classes": [{
                        "metadata": {"name": "gke-l7"},
                        "spec": {"controllerName": "example.io/gw"}}],
                    "gateways": [{
                        "metadata": {"name": "web-gw", "namespace": "shop"},
                        "spec": {"gatewayClassName": "gke-l7",
                                 "listeners": [{
                                     "protocol": "HTTPS", "port": 443,
                                     "hostname": "shop.example.com"}]}}],
                    "http_routes": [{
                        "metadata": {"name": "web-rt", "namespace": "shop"},
                        "spec": {"parentRefs": [{"name": "web-gw"}],
                                 "hostnames": ["shop.example.com"]}}]},
                "istio": {
                    "gateways": [{
                        "metadata": {"name": "mesh-gw",
                                     "namespace": "istio-system"},
                        "spec": {"selector": {"istio": "ingressgateway"},
                                 "servers": [{"hosts": ["*.example.com"]}]}}],
                    "virtual_services": [{
                        "metadata": {"name": "web-vs", "namespace": "shop"},
                        "spec": {"hosts": ["shop.example.com"],
                                 "gateways": ["mesh-gw"]}}]},
                "traefik": {"ingress_routes": [{
                    "metadata": {"name": "web-ir", "namespace": "shop"},
                    "spec": {"entryPoints": ["websecure"],
                             "routes": [{"match": "<omitted>",
                                         "services": [{"name": "web-svc"}]}]}}]}}}}
        rows = _rows(live_csv.render_tables(
            {"clusters": [cluster]})["networking"])
        records = {r[3]: dict(zip(rows[0], r)) for r in rows[1:]}
        self.assertEqual(records["gke-l7"]["kind"], "GatewayClass")
        self.assertEqual(records["gke-l7"]["class_or_controller"],
                         "example.io/gw")
        self.assertEqual(records["web-gw"]["kind"], "GatewayAPIGateway")
        self.assertEqual(records["web-gw"]["class_or_controller"], "gke-l7")
        self.assertEqual(records["web-gw"]["hosts_or_dns"], "shop.example.com")
        self.assertEqual(records["web-gw"]["details"], "HTTPS:443")
        self.assertEqual(records["web-rt"]["kind"], "HTTPRoute")
        self.assertEqual(records["web-rt"]["class_or_controller"], "web-gw")
        self.assertEqual(records["mesh-gw"]["kind"], "IstioGateway")
        self.assertEqual(records["mesh-gw"]["class_or_controller"],
                         "istio=ingressgateway")
        self.assertEqual(records["mesh-gw"]["hosts_or_dns"], "*.example.com")
        self.assertEqual(records["web-vs"]["kind"], "IstioVirtualService")
        self.assertEqual(records["web-vs"]["class_or_controller"], "mesh-gw")
        self.assertEqual(records["web-ir"]["kind"], "TraefikIngressRoute")
        self.assertEqual(records["web-ir"]["class_or_controller"], "websecure")
        self.assertEqual(records["web-ir"]["hosts_or_dns"], projection.OMITTED)
        self.assertEqual(records["web-ir"]["details"], "web-svc")


class ExternalSecretRowsTest(unittest.TestCase):

    def test_config_table_lists_external_secret_references(self):
        cluster = {
            "name": "prod",
            "kubernetes": {"config": {
                "configmaps": [], "secrets": [],
                "external_secrets": {
                    "external_secrets": [{
                        "metadata": {"name": "db-creds", "namespace": "shop"},
                        "spec": {
                            "secretStoreRef": {"kind": "ClusterSecretStore",
                                               "name": "aws-sm"},
                            "data": [{"secretKey": "password",
                                      "remoteRef": {"key": "prod/db"}}],
                            "dataFrom": [{"extract": {"key": "prod/all"}}]}}],
                    "secret_stores": [{
                        "metadata": {"name": "sm-ns", "namespace": "shop"},
                        "spec": {"provider": {"aws": {}}}}],
                    "cluster_secret_stores": [{
                        "metadata": {"name": "aws-sm"},
                        "spec": {"provider": {"aws": {}}}}],
                    "secret_provider_classes": [{
                        "metadata": {"name": "spc", "namespace": "shop"},
                        "spec": {"provider": "aws",
                                 "parameters": {
                                     "objects": "- objectName: prod/db",
                                     "region": "us-east-1"}}}]}}}}
        rows = _rows(live_csv.render_tables({"clusters": [cluster]})["config"])
        records = {r[3]: dict(zip(rows[0], r)) for r in rows[1:]}
        self.assertEqual(records["db-creds"]["kind"], "ExternalSecret")
        self.assertEqual(records["db-creds"]["type"],
                         "ClusterSecretStore/aws-sm")
        # The remote keys the Secret Manager re-pointing works from —
        # both data entries and dataFrom extracts.
        self.assertEqual(records["db-creds"]["keys"], "prod/db;prod/all")
        self.assertEqual(records["sm-ns"]["kind"], "SecretStore")
        self.assertEqual(records["sm-ns"]["type"], "aws")
        self.assertEqual(records["aws-sm"]["kind"], "ClusterSecretStore")
        self.assertEqual(records["aws-sm"]["type"], "aws")
        self.assertEqual(records["spc"]["kind"], "SecretProviderClass")
        self.assertEqual(records["spc"]["type"], "aws")
        self.assertEqual(records["spc"]["keys"], "objects;region")


class ImagesCountTest(unittest.TestCase):

    def test_declared_by_workloads_counts_workloads_not_containers(self):
        # One workload runs the same image in two containers — it is one user
        # of that image, not two.
        cluster = {
            "region": "us-east-1", "name": "prod",
            "kubernetes": {"images_running": [], "workloads": {"Deployment": [{
                "metadata": {"name": "web", "namespace": "shop"},
                "spec": {"template": {"spec": {"containers": [
                    {"name": "a", "image": "repo/app:v1"},
                    {"name": "b", "image": "repo/app:v1"}]}}}}]}}}
        rows = _rows(live_csv.render_tables({"clusters": [cluster]})["images"])
        record = dict(zip(rows[0], rows[1]))
        self.assertEqual(record["image"], "repo/app:v1")
        self.assertEqual(record["declared_by_workloads"], "1")


class EmptyEstateTest(unittest.TestCase):

    def test_every_table_has_only_a_header(self):
        tables = live_csv.render_tables({"clusters": []})
        self.assertEqual(set(tables), set(live_csv.TABLES))
        for name, text in tables.items():
            rows = _rows(text)
            self.assertEqual(len(rows), 1, f"{name} should be header-only")
            self.assertTrue(rows[0], f"{name} header must name its columns")


class ImageRowTest(unittest.TestCase):
    """The image table speaks the static scan's registry vocabulary and
    splits refs the same way, so the two inventories join cleanly."""

    def _row(self, image):
        tables = live_csv.render_tables({"clusters": [
            {"name": "c", "kubernetes": {"images_running": [image]}}]})
        rows = _rows(tables["images"])
        return dict(zip(rows[0], rows[1]))

    def test_ecr_image_with_tag(self):
        row = self._row(
            "123456789012.dkr.ecr.eu-west-1.amazonaws.com/team/app:1.2.3")
        self.assertEqual(row["registry_kind"], "ecr")
        self.assertEqual(row["repository"],
                         "123456789012.dkr.ecr.eu-west-1.amazonaws.com/team/app")
        self.assertEqual(row["tag"], "1.2.3")
        self.assertEqual(row["digest"], "")

    def test_public_ecr_gallery_is_ecr_too(self):
        self.assertEqual(self._row("public.ecr.aws/x/y:1")["registry_kind"],
                         "ecr")

    def test_docker_hub_image_with_digest(self):
        row = self._row("docker.io/library/nginx@sha256:abc123")
        self.assertEqual(row["registry_kind"], "dockerhub")
        self.assertEqual(row["repository"], "docker.io/library/nginx")
        self.assertEqual(row["tag"], "")
        self.assertEqual(row["digest"], "sha256:abc123")

    def test_registry_port_is_not_mistaken_for_a_tag(self):
        row = self._row("registry:5000/app:v1")
        self.assertEqual(row["registry_kind"], "other")
        self.assertEqual(row["repository"], "registry:5000/app")
        self.assertEqual(row["tag"], "v1")


class ResourcesJoinTest(unittest.TestCase):

    def test_same_resource_across_containers_is_joined_not_summed(self):
        requests, _ = live_csv._resources([
            {"resources": {"requests": {"cpu": "250m"}}},
            {"resources": {"requests": {"cpu": "500m"}}}])
        self.assertEqual(requests["cpu"], "250m+500m")


if __name__ == "__main__":
    unittest.main()
