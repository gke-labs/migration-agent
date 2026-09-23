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


"""Tests for the keys-only projection.

The guarantee under test is the package's whole security claim: no
free-form value a customer authored reaches the ledger — what survives is
an identifier, a reference, an enumeration or a scheduling and routing
contract. Fixed examples pin each carrier a migration reads (env, args,
probes, ConfigMaps, Secrets, labels versus selectors, annotations,
Karpenter, routing CRDs, external secrets, StorageClass parameters); a
random-tree property test pins the rule itself.
"""

import copy
import random
import unittest

from servers.phases.discovery.discovery_livescan_0 import projection
from servers.phases.discovery.discovery_livescan_0.projection import OMITTED


def _container(**extra):
    container = {"name": "web", "image": "registry/shop/web:1.2",
                 "imagePullPolicy": "IfNotPresent"}
    container.update(extra)
    return container


def _deployment(container, pod_extra=None, metadata=None):
    pod = {"serviceAccountName": "web-sa", "containers": [container]}
    pod.update(pod_extra or {})
    return {"apiVersion": "apps/v1", "kind": "Deployment",
            "metadata": metadata or {"name": "web", "namespace": "shop"},
            "spec": {"replicas": 2, "selector": {"matchLabels": {"app": "web"}},
                     "template": {"metadata": {"labels": {"app": "web"}},
                                  "spec": pod}}}


def _pod_spec(projected):
    return projected["spec"]["template"]["spec"]


def _strings(value):
    """Every string leaf in a tree, in document order."""
    if isinstance(value, dict):
        return [s for v in value.values() for s in _strings(v)]
    if isinstance(value, list):
        return [s for v in value for s in _strings(v)]
    return [value] if isinstance(value, str) else []


class WorkloadProjectionTest(unittest.TestCase):

    def test_env_literals_are_omitted_and_references_kept(self):
        out = projection.project(_deployment(_container(env=[
            {"name": "DB_PASSWORD", "value": "hunter2"},
            {"name": "DB_HOST", "value": "db.shop.svc"},
            {"name": "DB_SECRET", "valueFrom": {"secretKeyRef": {
                "name": "db-creds", "key": "password", "optional": False}}},
            {"name": "POD_IP", "valueFrom": {"fieldRef": {
                "fieldPath": "status.podIP"}}}])))
        env = _pod_spec(out)["containers"][0]["env"]
        self.assertEqual(env, [
            {"name": "DB_PASSWORD", "value": OMITTED},
            {"name": "DB_HOST", "value": OMITTED},
            {"name": "DB_SECRET", "valueFrom": {"secretKeyRef": {
                "name": "db-creds", "key": "password", "optional": False}}},
            {"name": "POD_IP", "valueFrom": {"fieldRef": {
                "fieldPath": "status.podIP"}}}])

    def test_command_and_args_keep_their_length_and_no_token(self):
        out = projection.project(_deployment(_container(
            command=["sh", "-c", "curl -H 'X-Token: t0k' http://warm/up"],
            args=["--token=abc", "--port=8080"])))
        container = _pod_spec(out)["containers"][0]
        self.assertEqual(container["command"], [OMITTED] * 3)
        self.assertEqual(container["args"], [OMITTED] * 2)

    def test_identity_fields_survive(self):
        out = projection.project(_deployment(_container(), {
            "imagePullSecrets": [{"name": "regcred"}],
            "priorityClassName": "high", "restartPolicy": "Always",
            "volumes": [{"name": "data", "persistentVolumeClaim": {
                "claimName": "data-0"}},
                        {"name": "cfg", "configMap": {"name": "app-config"}},
                        {"name": "tls", "secret": {"secretName": "shop-tls"}}]}))
        pod = _pod_spec(out)
        self.assertEqual(pod["serviceAccountName"], "web-sa")
        self.assertEqual(pod["imagePullSecrets"], [{"name": "regcred"}])
        self.assertEqual(pod["priorityClassName"], "high")
        self.assertEqual(pod["volumes"][0]["persistentVolumeClaim"],
                         {"claimName": "data-0"})
        self.assertEqual(pod["volumes"][1]["configMap"], {"name": "app-config"})
        self.assertEqual(pod["volumes"][2]["secret"], {"secretName": "shop-tls"})
        container = pod["containers"][0]
        self.assertEqual(container["image"], "registry/shop/web:1.2")
        self.assertEqual(container["imagePullPolicy"], "IfNotPresent")
        self.assertEqual(out["kind"], "Deployment")
        self.assertEqual(out["apiVersion"], "apps/v1")
        self.assertEqual(out["metadata"], {"name": "web", "namespace": "shop"})
        self.assertEqual(out["spec"]["replicas"], 2)

    def test_probes_keep_port_and_scheme_not_path_headers_or_exec(self):
        out = projection.project(_deployment(_container(
            livenessProbe={"httpGet": {"path": "/healthz?key=k3y", "port": 8080,
                                       "scheme": "HTTPS", "httpHeaders": [
                                           {"name": "X-Api-Key",
                                            "value": "k3y"}]},
                           "periodSeconds": 10},
            readinessProbe={"exec": {"command": ["curl", "-H", "X: y"]}},
            startupProbe={"tcpSocket": {"port": "http", "host": "10.0.0.1"}},
            lifecycle={"postStart": {"exec": {"command": ["sh", "-c", "x"]}}})))
        container = _pod_spec(out)["containers"][0]
        self.assertEqual(container["livenessProbe"], {
            "httpGet": {"path": OMITTED, "port": 8080, "scheme": "HTTPS",
                        "httpHeaders": [{"name": "X-Api-Key",
                                         "value": OMITTED}]},
            "periodSeconds": 10})
        self.assertEqual(container["readinessProbe"],
                         {"exec": {"command": [OMITTED] * 3}})
        self.assertEqual(container["startupProbe"],
                         {"tcpSocket": {"port": "http", "host": "10.0.0.1"}})
        self.assertEqual(container["lifecycle"],
                         {"postStart": {"exec": {"command": [OMITTED] * 3}}})

    def test_resources_and_scheduling_contract_are_kept_whole(self):
        out = projection.project(_deployment(
            _container(resources={"requests": {"cpu": "250m", "memory": "256Mi",
                                               "nvidia.com/gpu": "1"},
                                  "limits": {"memory": "512Mi"}}),
            {"nodeSelector": {"disktype": "ssd", "topology.kubernetes.io/zone":
                              "us-east-1a"},
             "tolerations": [{"key": "dedicated", "operator": "Equal",
                              "value": "gpu", "effect": "NoSchedule"}],
             "affinity": {"nodeAffinity": {
                 "requiredDuringSchedulingIgnoredDuringExecution": {
                     "nodeSelectorTerms": [{"matchExpressions": [
                         {"key": "kubernetes.io/arch", "operator": "In",
                          "values": ["amd64", "arm64"]}]}]}}},
             "topologySpreadConstraints": [{
                 "maxSkew": 1, "topologyKey": "topology.kubernetes.io/zone",
                 "whenUnsatisfiable": "DoNotSchedule",
                 "labelSelector": {"matchLabels": {"app": "web"}}}]}))
        pod = _pod_spec(out)
        self.assertEqual(pod["containers"][0]["resources"], {
            "requests": {"cpu": "250m", "memory": "256Mi", "nvidia.com/gpu": "1"},
            "limits": {"memory": "512Mi"}})
        self.assertEqual(pod["nodeSelector"], {
            "disktype": "ssd", "topology.kubernetes.io/zone": "us-east-1a"})
        self.assertEqual(pod["tolerations"][0]["value"], "gpu")
        terms = (pod["affinity"]["nodeAffinity"]
                 ["requiredDuringSchedulingIgnoredDuringExecution"]
                 ["nodeSelectorTerms"][0]["matchExpressions"][0])
        self.assertEqual(terms["values"], ["amd64", "arm64"])
        self.assertEqual(pod["topologySpreadConstraints"][0]["labelSelector"],
                         {"matchLabels": {"app": "web"}})
        self.assertEqual(out["spec"]["selector"], {"matchLabels": {"app": "web"}})

    def test_labels_keep_key_names_only_while_selectors_keep_values(self):
        # A label value is free text (an owner's e-mail, a ticket, a cost
        # centre); the selector that matches it is the scheduling contract.
        out = projection.project(_deployment(_container(), metadata={
            "name": "web", "namespace": "shop",
            "labels": {"app": "web", "owner": "alice@example.com"}}))
        self.assertEqual(out["metadata"]["labels"],
                         {"app": OMITTED, "owner": OMITTED})
        self.assertEqual(out["spec"]["template"]["metadata"]["labels"],
                         {"app": OMITTED})
        self.assertEqual(out["spec"]["selector"]["matchLabels"], {"app": "web"})

    def test_annotations_keep_the_allowlisted_values_only(self):
        out = projection.project({
            "kind": "Service", "metadata": {"name": "web", "annotations": {
                "service.beta.kubernetes.io/aws-load-balancer-type": "nlb",
                "service.beta.kubernetes.io/aws-load-balancer-scheme": "internal",
                "nginx.ingress.kubernetes.io/auth-secret": "basic-auth",
                "password": "e2eAnnotationPw",
                "kubectl.kubernetes.io/last-applied-configuration": "{...}",
                "kubernetes.io/ingress.class": ["not", "a", "string"]}},
            "spec": {"type": "LoadBalancer", "clusterIP": "10.1.2.3",
                     "selector": {"app": "web"},
                     "ports": [{"port": 80, "targetPort": 8080,
                                "protocol": "TCP", "nodePort": 30080}]}})
        self.assertEqual(out["metadata"]["annotations"], {
            "service.beta.kubernetes.io/aws-load-balancer-type": "nlb",
            "service.beta.kubernetes.io/aws-load-balancer-scheme": "internal",
            "nginx.ingress.kubernetes.io/auth-secret": OMITTED,
            "password": OMITTED,
            "kubernetes.io/ingress.class": [OMITTED] * 3})
        self.assertEqual(out["spec"], {
            "type": "LoadBalancer", "clusterIP": OMITTED,
            "selector": {"app": "web"},
            "ports": [{"port": 80, "targetPort": 8080, "protocol": "TCP",
                       "nodePort": 30080}]})

    def test_status_and_runtime_metadata_are_dropped(self):
        out = projection.project({
            "kind": "Deployment",
            "metadata": {"name": "web", "uid": "u", "resourceVersion": "1",
                         "creationTimestamp": "2026-01-01T00:00:00Z",
                         "managedFields": [{"manager": "kubectl"}],
                         "ownerReferences": [{"kind": "X", "name": "y"}],
                         "generation": 3, "finalizers": ["f"]},
            "spec": {"template": {"metadata": {"creationTimestamp": None,
                                               "name": "t"}}},
            "status": {"readyReplicas": 2, "conditions": [{"message": "m"}]}})
        self.assertEqual(out, {
            "kind": "Deployment", "metadata": {"name": "web"},
            "spec": {"template": {"metadata": {"name": "t"}}}})

    def test_numbers_booleans_and_nulls_survive_outside_opaque_maps(self):
        out = projection.project({"kind": "X", "metadata": {"name": "x"},
                                  "spec": {"n": 3, "f": 1.5, "b": True,
                                           "z": None, "s": "text"}})
        self.assertEqual(out["spec"], {"n": 3, "f": 1.5, "b": True,
                                       "z": None, "s": OMITTED})

    def test_the_input_is_never_mutated(self):
        manifest = _deployment(_container(env=[{"name": "A", "value": "v"}]),
                               metadata={"name": "w", "labels": {"a": "b"},
                                         "uid": "u"})
        manifest["status"] = {"x": 1}
        before = copy.deepcopy(manifest)
        projection.project(manifest)
        self.assertEqual(manifest, before)


class ConfigProjectionTest(unittest.TestCase):

    def test_configmap_values_are_omitted_whatever_their_key(self):
        # Keys that collide with allowlisted field names (`name`, `host`,
        # `type`, `key`) are still customer values under `data`.
        out = projection.project({
            "kind": "ConfigMap", "metadata": {"name": "app-config"},
            "data": {"LOG_LEVEL": "info", "db-password": "e2eCmPassw0rd",
                     "name": "x", "host": "db", "type": "t", "key": "k",
                     "config.yaml": "db:\n  password: hunter2\n"},
            "binaryData": {"blob": "AAAA"}})
        self.assertEqual(out["data"], {
            "LOG_LEVEL": OMITTED, "db-password": OMITTED, "name": OMITTED,
            "host": OMITTED, "type": OMITTED, "key": OMITTED,
            "config.yaml": OMITTED})
        self.assertEqual(out["binaryData"], {"blob": OMITTED})

    def test_nested_maps_and_numbers_under_data_are_omitted_too(self):
        out = projection.project({
            "kind": "ConfigMap", "metadata": {"name": "c"},
            "data": {"n": 5, "m": {"name": "inner", "port": 80},
                     "l": ["a", 1]}})
        self.assertEqual(out["data"], {
            "n": OMITTED, "m": {"name": OMITTED, "port": OMITTED},
            "l": [OMITTED, OMITTED]})

    def test_secret_keeps_type_and_key_names_only(self):
        out = projection.project({
            "kind": "Secret", "type": "Opaque",
            "metadata": {"name": "db", "namespace": "shop",
                         "annotations": {"note": "rotate me"}},
            "data": {"password": "aHVudGVyMg==", "username": "YXBw"},
            "stringData": {"url": "postgres://app:pw@db/app"}})
        self.assertEqual(out, {
            "kind": "Secret", "type": "Opaque",
            "metadata": {"name": "db", "namespace": "shop",
                         "annotations": {"note": OMITTED}},
            "data": {"password": OMITTED, "username": OMITTED},
            "stringData": {"url": OMITTED}})

    def test_external_secret_keeps_remote_keys_and_omits_templates(self):
        out = projection.project({
            "kind": "ExternalSecret", "metadata": {"name": "db-creds"},
            "spec": {"refreshInterval": "1h",
                     "secretStoreRef": {"kind": "ClusterSecretStore",
                                        "name": "aws-sm"},
                     "target": {"name": "db-creds", "creationPolicy": "Owner",
                                "template": {"type": "Opaque", "data": {
                                    "url": "postgres://{{ .user }}:{{ .pw }}@db"}}},
                     "data": [{"secretKey": "password",
                               "remoteRef": {"key": "prod/db",
                                             "property": "password",
                                             "version": "AWSCURRENT"}}],
                     "dataFrom": [{"extract": {"key": "prod/all"}}]}})
        self.assertEqual(out["spec"], {
            "refreshInterval": "1h",
            "secretStoreRef": {"kind": "ClusterSecretStore", "name": "aws-sm"},
            "target": {"name": "db-creds", "creationPolicy": "Owner",
                       "template": {"type": "Opaque", "data": {"url": OMITTED}}},
            "data": [{"secretKey": "password",
                      "remoteRef": {"key": "prod/db", "property": "password",
                                    "version": "AWSCURRENT"}}],
            "dataFrom": [{"extract": {"key": "prod/all"}}]})

    def test_secret_store_and_provider_class_keep_provider_shape_only(self):
        store = projection.project({
            "kind": "SecretStore", "metadata": {"name": "aws-sm"},
            "spec": {"provider": {
                "aws": {"service": "SecretsManager", "region": "us-east-1",
                        "auth": {"jwt": {"serviceAccountRef": {"name": "es-sa"}}}},
                "webhook": {"url": "https://vault.internal/v1?token=t",
                            "headers": {"Authorization": "Bearer t"}}}}})
        self.assertEqual(store["spec"]["provider"]["aws"], {
            "service": "SecretsManager", "region": "us-east-1",
            "auth": {"jwt": {"serviceAccountRef": {"name": "es-sa"}}}})
        self.assertEqual(store["spec"]["provider"]["webhook"], {
            "url": OMITTED, "headers": {"Authorization": OMITTED}})
        spc = projection.project({
            "kind": "SecretProviderClass", "metadata": {"name": "aws-spc"},
            "spec": {"provider": "aws",
                     "parameters": {"objects": "- objectName: prod/db",
                                    "accessKey": "AKIAIOSFODNN7EXAMPLE",
                                    "region": "us-east-1"},
                     "secretObjects": [{"secretName": "db", "type": "Opaque",
                                        "data": [{"objectName": "prod/db",
                                                  "key": "password"}]}]}})
        self.assertEqual(spc["spec"], {
            "provider": "aws",
            "parameters": {"objects": OMITTED, "accessKey": OMITTED,
                           "region": OMITTED},
            "secretObjects": [{"secretName": "db", "type": "Opaque",
                               "data": [{"objectName": "prod/db",
                                         "key": "password"}]}]})


class PlatformProjectionTest(unittest.TestCase):

    def test_karpenter_requirements_kept_user_data_and_tags_omitted(self):
        nodepool = projection.project({
            "kind": "NodePool", "metadata": {"name": "default"},
            "spec": {"template": {"spec": {
                "nodeClassRef": {"group": "karpenter.k8s.aws",
                                 "kind": "EC2NodeClass", "name": "default"},
                "requirements": [
                    {"key": "karpenter.sh/capacity-type", "operator": "In",
                     "values": ["spot", "on-demand"]},
                    {"key": "node.kubernetes.io/instance-type",
                     "operator": "In", "values": ["m6i.large"]}],
                "taints": [{"key": "gpu", "value": "true",
                            "effect": "NoSchedule"}]}},
                     "limits": {"cpu": "1000", "memory": "1000Gi"},
                     "disruption": {"consolidationPolicy": "WhenEmpty",
                                    "consolidateAfter": "30s"}}})
        spec = nodepool["spec"]
        self.assertEqual(spec["template"]["spec"]["requirements"][0]["values"],
                         ["spot", "on-demand"])
        self.assertEqual(spec["template"]["spec"]["taints"],
                         [{"key": "gpu", "value": "true", "effect": "NoSchedule"}])
        self.assertEqual(spec["limits"], {"cpu": "1000", "memory": "1000Gi"})
        self.assertEqual(spec["disruption"], {"consolidationPolicy": "WhenEmpty",
                                              "consolidateAfter": "30s"})
        node_class = projection.project({
            "kind": "EC2NodeClass", "metadata": {"name": "default"},
            "spec": {"amiFamily": "AL2023", "role": "KarpenterNodeRole",
                     "userData": "#!/bin/bash\nexport AWS_SECRET_ACCESS_KEY=x\n",
                     "tags": {"team": "shop", "db-password": "hunter2"},
                     "subnetSelectorTerms": [{"tags": {"karpenter.sh/discovery":
                                                       "shop-prod"}}],
                     "blockDeviceMappings": [{"deviceName": "/dev/xvda", "ebs": {
                         "volumeSize": "100Gi", "volumeType": "gp3",
                         "encrypted": True}}],
                     "metadataOptions": {"httpTokens": "required",
                                         "httpPutResponseHopLimit": 1}}})
        self.assertEqual(node_class["spec"], {
            "amiFamily": "AL2023", "role": "KarpenterNodeRole",
            "userData": OMITTED,
            "tags": {"team": OMITTED, "db-password": OMITTED},
            "subnetSelectorTerms": [{"tags": {"karpenter.sh/discovery": OMITTED}}],
            "blockDeviceMappings": [{"deviceName": "/dev/xvda", "ebs": {
                "volumeSize": "100Gi", "volumeType": "gp3", "encrypted": True}}],
            "metadataOptions": {"httpTokens": "required",
                                "httpPutResponseHopLimit": 1}})

    def test_routing_objects_keep_hosts_classes_and_backends_not_headers(self):
        ingress = projection.project({
            "kind": "Ingress", "metadata": {"name": "web"},
            "spec": {"ingressClassName": "alb",
                     "tls": [{"hosts": ["shop.example.com"],
                              "secretName": "shop-tls"}],
                     "rules": [{"host": "shop.example.com", "http": {"paths": [
                         {"path": "/api", "pathType": "Prefix",
                          "backend": {"service": {"name": "web",
                                                  "port": {"number": 80}}}}]}}]}})
        self.assertEqual(ingress["spec"], {
            "ingressClassName": "alb",
            "tls": [{"hosts": ["shop.example.com"], "secretName": "shop-tls"}],
            "rules": [{"host": "shop.example.com", "http": {"paths": [
                {"path": "/api", "pathType": "Prefix",
                 "backend": {"service": {"name": "web",
                                         "port": {"number": 80}}}}]}}]})
        route = projection.project({
            "kind": "HTTPRoute", "metadata": {"name": "r"},
            "spec": {"parentRefs": [{"name": "gw", "sectionName": "https"}],
                     "hostnames": ["shop.example.com"],
                     "rules": [{
                         "matches": [{"path": {"type": "PathPrefix",
                                               "value": "/"},
                                      "headers": [{"name": "X-Api-Key",
                                                   "value": "k3y"}]}],
                         "filters": [{"type": "RequestHeaderModifier",
                                      "requestHeaderModifier": {"set": [
                                          {"name": "X-Api-Key",
                                           "value": "k3y"}]}}],
                         "backendRefs": [{"name": "web", "port": 80,
                                          "weight": 100}]}]}})
        rule = route["spec"]["rules"][0]
        self.assertEqual(route["spec"]["hostnames"], ["shop.example.com"])
        self.assertEqual(rule["matches"][0], {
            "path": {"type": "PathPrefix", "value": "/"},
            "headers": [{"name": "X-Api-Key", "value": OMITTED}]})
        self.assertEqual(rule["filters"][0]["requestHeaderModifier"],
                         {"set": [{"name": "X-Api-Key", "value": OMITTED}]})
        self.assertEqual(rule["backendRefs"],
                         [{"name": "web", "port": 80, "weight": 100}])
        istio = projection.project({
            "kind": "VirtualService", "metadata": {"name": "vs"},
            "spec": {"hosts": ["shop.example.com"], "gateways": ["mesh-gw"],
                     "http": [{"route": [{"destination": {"host": "web",
                                                          "subset": "v2",
                                                          "port": {"number": 80}}}],
                               "headers": {"request": {"set": {
                                   "X-Api-Key": "k3y"}}}}]}})
        http = istio["spec"]["http"][0]
        self.assertEqual(istio["spec"]["hosts"], ["shop.example.com"])
        self.assertEqual(http["route"][0]["destination"],
                         {"host": "web", "subset": "v2", "port": {"number": 80}})
        self.assertEqual(http["headers"],
                         {"request": {"set": {"X-Api-Key": OMITTED}}})
        traefik = projection.project({
            "kind": "IngressRoute", "metadata": {"name": "ir"},
            "spec": {"entryPoints": ["websecure"],
                     "routes": [{"match": "Host(`shop.example.com`)",
                                 "kind": "Rule",
                                 "services": [{"name": "web", "port": 80}],
                                 "middlewares": [{"name": "auth"}]}],
                     "tls": {"secretName": "shop-tls", "certResolver": "le"}}})
        self.assertEqual(traefik["spec"], {
            "entryPoints": ["websecure"],
            "routes": [{"match": OMITTED, "kind": "Rule",
                        "services": [{"name": "web", "port": 80}],
                        "middlewares": [{"name": "auth"}]}],
            "tls": {"secretName": "shop-tls", "certResolver": "le"}})
        middleware = projection.project({
            "kind": "Middleware", "metadata": {"name": "auth"},
            "spec": {"basicAuth": {"secret": "users", "realm": "shop"},
                     "headers": {"customRequestHeaders": {"X-Token": "t"}},
                     "plugin": {"jwt": {"secret": "s", "deep": {"x": 1}}},
                     "forwardAuth": {"address": "http://auth:4181"}}})
        self.assertEqual(middleware["spec"], {
            "basicAuth": {"secret": OMITTED, "realm": OMITTED},
            "headers": {"customRequestHeaders": {"X-Token": OMITTED}},
            "plugin": {"jwt": {"secret": OMITTED, "deep": {"x": OMITTED}}},
            "forwardAuth": {"address": OMITTED}})

    def test_storage_objects_keep_drivers_classes_and_sizes(self):
        pv = projection.project({
            "kind": "PersistentVolume", "metadata": {"name": "pv-1"},
            "spec": {"capacity": {"storage": "20Gi"},
                     "accessModes": ["ReadWriteOnce"],
                     "persistentVolumeReclaimPolicy": "Delete",
                     "storageClassName": "gp2",
                     "csi": {"driver": "ebs.csi.aws.com",
                             "volumeHandle": "vol-0e2e", "fsType": "ext4",
                             "volumeAttributes": {"bucketName": "b",
                                                  "mountOptions": "--pw x"}},
                     "mountOptions": ["password=hunter2"],
                     "claimRef": {"namespace": "shop", "name": "data-0"}}})
        self.assertEqual(pv["spec"], {
            "capacity": {"storage": "20Gi"}, "accessModes": ["ReadWriteOnce"],
            "persistentVolumeReclaimPolicy": "Delete", "storageClassName": "gp2",
            "csi": {"driver": "ebs.csi.aws.com", "volumeHandle": "vol-0e2e",
                    "fsType": "ext4",
                    "volumeAttributes": {"bucketName": "b",
                                         "mountOptions": OMITTED}},
            "mountOptions": [OMITTED],
            "claimRef": {"namespace": "shop", "name": "data-0"}})
        storage_class = projection.project({
            "kind": "StorageClass", "metadata": {"name": "gp2"},
            "provisioner": "ebs.csi.aws.com", "reclaimPolicy": "Delete",
            "volumeBindingMode": "WaitForFirstConsumer",
            "allowVolumeExpansion": True,
            "parameters": {"type": "gp3", "encrypted": "true",
                           "tagSpecification_1": "owner=shop-team"}})
        self.assertEqual(storage_class, {
            "kind": "StorageClass", "metadata": {"name": "gp2"},
            "provisioner": "ebs.csi.aws.com", "reclaimPolicy": "Delete",
            "volumeBindingMode": "WaitForFirstConsumer",
            "allowVolumeExpansion": True,
            "parameters": {"type": "gp3", "encrypted": "true",
                           "tagSpecification_1": OMITTED}})

    def test_route_matches_are_kept_while_header_matches_and_rewrites_are_not(self):
        # A route's path match is the public URL surface a load balancer
        # must reproduce; a header match, a rewrite or redirect target, a
        # Traefik rule (an expression that can embed a header literal) and
        # a Traefik TLS option's free fields are not.
        virtual_service = projection.project({
            "kind": "VirtualService", "metadata": {"name": "web"},
            "spec": {"hosts": ["shop.example.com"], "gateways": ["shop-gw"],
                     "http": [{
                         "match": [{"uri": {"prefix": "/api"},
                                    "headers": {"x-api-key": {"exact": "k3y"}}}],
                         "rewrite": {"uri": "/internal?token=t"},
                         "route": [{"destination": {"host": "web", "subset": "v1",
                                                    "port": {"number": 80}}}]}]}})
        http = virtual_service["spec"]["http"][0]
        self.assertEqual(http["match"], [{"uri": {"prefix": "/api"},
                                          "headers": {"x-api-key": {"exact": OMITTED}}}])
        self.assertEqual(http["rewrite"], {"uri": OMITTED})
        self.assertEqual(http["route"], [{"destination": {"host": "web", "subset": "v1",
                                                          "port": {"number": 80}}}])
        http_route = projection.project({
            "kind": "HTTPRoute", "metadata": {"name": "web"},
            "spec": {"rules": [{
                "matches": [{"path": {"type": "PathPrefix", "value": "/api"},
                             "method": "GET"}],
                "timeouts": {"request": "10s", "backendRequest": "5s"},
                "filters": [
                    {"type": "URLRewrite", "urlRewrite": {
                        "hostname": "web.internal",
                        "path": {"type": "ReplacePrefixMatch",
                                 "replacePrefixMatch": "/internal?token=t"}}},
                    {"type": "RequestRedirect", "requestRedirect": {
                        "scheme": "https", "hostname": "shop.example.com",
                        "statusCode": 301,
                        "path": {"type": "ReplaceFullPath",
                                 "replaceFullPath": "/login?key=abc"}}}],
                "backendRefs": [{"name": "web", "port": 80}]}]}})
        rule = http_route["spec"]["rules"][0]
        self.assertEqual(rule["matches"], [{"path": {"type": "PathPrefix", "value": "/api"},
                                            "method": "GET"}])
        self.assertEqual(rule["timeouts"], {"request": "10s", "backendRequest": "5s"})
        self.assertEqual(rule["filters"], [
            {"type": "URLRewrite", "urlRewrite": {
                "hostname": "web.internal",
                "path": {"type": OMITTED, "replacePrefixMatch": OMITTED}}},
            {"type": "RequestRedirect", "requestRedirect": {
                "scheme": "https", "hostname": "shop.example.com", "statusCode": 301,
                "path": {"type": OMITTED, "replaceFullPath": OMITTED}}}])
        # A kept entry keeps its scalar whatever the type; a nested map or
        # list under it is still the opaque walk.
        self.assertEqual(
            projection.project({"kind": "HTTPRoute", "metadata": {"name": "r"},
                                "spec": {"rules": [{"filters": [{"requestRedirect": {
                                    "statusCode": {"nested": "x"}, "port": ["a"]}}]}]}}
                               )["spec"]["rules"][0]["filters"][0]["requestRedirect"],
            {"statusCode": {"nested": OMITTED}, "port": [OMITTED]})
        ingress_route = projection.project({
            "kind": "IngressRoute", "metadata": {"name": "web"},
            "spec": {"entryPoints": ["websecure"],
                     "routes": [{"match": "Host(`shop.example.com`) && Headers(`X-Api-Key`, `k3y`)",
                                 "kind": "Rule",
                                 "services": [{"name": "web", "port": 80}],
                                 "middlewares": [{"name": "auth"}]}],
                     "tls": {"secretName": "shop-tls",
                             "options": {"name": "modern", "namespace": "traefik",
                                         "cipherSuites": "TLS_AES_128_GCM_SHA256"}}}})
        self.assertEqual(ingress_route["spec"]["routes"][0],
                         {"match": OMITTED, "kind": "Rule",
                          "services": [{"name": "web", "port": 80}],
                          "middlewares": [{"name": "auth"}]})
        self.assertEqual(ingress_route["spec"]["tls"],
                         {"secretName": "shop-tls",
                          "options": {"name": "modern", "namespace": "traefik",
                                      "cipherSuites": OMITTED}})
        # `prefix` on its own is not a route match.
        self.assertEqual(projection.project({"kind": "X", "spec": {"prefix": "s3://b/p"}}),
                         {"kind": "X", "spec": {"prefix": OMITTED}})

    def test_external_dns_hostname_and_sni_hosts_are_kept(self):
        # A DNS name external-dns publishes and a TLS SNI match are the
        # public surface a load balancer must reproduce, like an Ingress host.
        service = projection.project({
            "kind": "Service", "metadata": {"name": "web", "annotations": {
                "external-dns.alpha.kubernetes.io/hostname": "shop.example.com",
                "external-dns.alpha.kubernetes.io/ttl": "60"}},
            "spec": {"type": "LoadBalancer"}})
        self.assertEqual(service["metadata"]["annotations"], {
            "external-dns.alpha.kubernetes.io/hostname": "shop.example.com",
            "external-dns.alpha.kubernetes.io/ttl": OMITTED})
        virtual_service = projection.project({
            "kind": "VirtualService", "metadata": {"name": "web"},
            "spec": {"tls": [{"match": [{"sniHosts": ["shop.example.com"], "port": 443}],
                              "route": [{"destination": {"host": "web"}}]}]}})
        self.assertEqual(virtual_service["spec"]["tls"][0]["match"],
                         [{"sniHosts": ["shop.example.com"], "port": 443}])

    def test_platform_labels_and_parameter_references_are_kept_by_exact_key(self):
        namespace = projection.project({
            "kind": "Namespace", "metadata": {"name": "shop", "labels": {
                "istio-injection": "enabled",
                "pod-security.kubernetes.io/enforce": "restricted",
                "team": "payments"}}})
        self.assertEqual(namespace["metadata"]["labels"], {
            "istio-injection": "enabled",
            "pod-security.kubernetes.io/enforce": "restricted",
            "team": OMITTED})
        ingress_class = projection.project({
            "kind": "IngressClass", "metadata": {"name": "alb"},
            "spec": {"controller": "ingress.k8s.aws/alb",
                     "parameters": {"apiGroup": "elbv2.k8s.aws",
                                    "kind": "IngressClassParams", "name": "shop"}}})
        self.assertEqual(ingress_class["spec"], {
            "controller": "ingress.k8s.aws/alb",
            "parameters": {"apiGroup": "elbv2.k8s.aws",
                           "kind": "IngressClassParams", "name": "shop"}})

    def test_quantities_source_ranges_and_host_paths_are_kept(self):
        hpa = projection.project({
            "kind": "HorizontalPodAutoscaler", "metadata": {"name": "web"},
            "spec": {"scaleTargetRef": {"kind": "Deployment", "name": "web"},
                     "minReplicas": 2, "maxReplicas": 10,
                     "metrics": [{"type": "Resource", "resource": {
                         "name": "memory", "target": {"type": "AverageValue",
                                                      "averageValue": "500Mi"}}}]}})
        self.assertEqual(hpa["spec"]["metrics"][0]["resource"]["target"],
                         {"type": "AverageValue", "averageValue": "500Mi"})
        service = projection.project({
            "kind": "Service", "metadata": {"name": "web"},
            "spec": {"type": "LoadBalancer",
                     "loadBalancerSourceRanges": ["10.0.0.0/8", "192.168.1.0/24"],
                     "externalIPs": ["203.0.113.7"]}})
        self.assertEqual(service["spec"], {
            "type": "LoadBalancer",
            "loadBalancerSourceRanges": ["10.0.0.0/8", "192.168.1.0/24"],
            "externalIPs": [OMITTED]})
        pod = projection.project(_deployment(_container(), pod_extra={"volumes": [
            {"name": "logs", "hostPath": {"path": "/var/log/app", "type": "Directory"}},
            {"name": "cfg", "configMap": {"name": "app-cfg",
                                          "items": [{"key": "app.yaml", "path": "app.yaml"}]}}]}))
        self.assertEqual(pod["spec"]["template"]["spec"]["volumes"], [
            {"name": "logs", "hostPath": {"path": "/var/log/app", "type": "Directory"}},
            {"name": "cfg", "configMap": {"name": "app-cfg",
                                          "items": [{"key": "app.yaml", "path": OMITTED}]}}])

    def test_a_subtree_past_the_depth_limit_is_the_marker_not_a_crash(self):
        deep = {}
        cursor = deep
        for _ in range(1500):
            cursor["x"] = {}
            cursor = cursor["x"]
        for field in ("plugin", "deep"):
            out = projection.project({"kind": "Middleware",
                                      "metadata": {"name": "m"},
                                      "spec": {field: deep}})
            cursor = out["spec"][field]
            levels = 0
            while isinstance(cursor, dict):
                cursor = cursor["x"]
                levels += 1
            self.assertEqual(cursor, OMITTED)
            self.assertLessEqual(levels, projection.MAX_DEPTH)

    def test_keys_only_reduces_an_aws_string_map(self):
        self.assertEqual(projection.keys_only({"env": "prod", "db_password": "x"}),
                         {"env": OMITTED, "db_password": OMITTED})
        self.assertEqual(projection.keys_only(None), {})
        self.assertEqual(projection.keys_only({}), {})
        # The raw EC2 tag list is refused: stringified, each pair would
        # carry its value inside the "key".
        with self.assertRaises(TypeError):
            projection.keys_only([{"Key": "db_password", "Value": "hunter2"}])


# --- the rule itself, on random trees -------------------------------------

_KEY_POOL = (sorted(projection.KEPT_KEYS)[::4]
             + sorted(projection.KEPT_SUBTREES)
             + sorted(projection.OPAQUE_SUBTREES)
             + sorted(projection.KEPT_ENTRIES["annotations"])[:3]
             + sorted(projection.KEPT_ENTRIES["parameters"])[:3]
             + sorted(projection.KEPT_ENTRIES["labels"])[:2]
             + ["metadata", "annotations", "spec", "template", "containers",
                "env", "args", "value", "path", "userData", "match", "url",
                "uid", "ownerReferences", "creationTimestamp", "status",
                "kubectl.kubernetes.io/last-applied-configuration", "x"])


def _random_tree(rng, depth, counter):
    roll = rng.random()
    if depth == 0 or roll < 0.3:
        kind = rng.random()
        if kind < 0.6:
            counter[0] += 1
            return f"s{counter[0]}"
        return rng.choice([0, 1, 2.5, True, False, None])
    if roll < 0.7:
        return {rng.choice(_KEY_POOL): _random_tree(rng, depth - 1, counter)
                for _ in range(rng.randint(1, 4))}
    return [_random_tree(rng, depth - 1, counter)
            for _ in range(rng.randint(0, 3))]


def _allowed_strings(value, key, keep, top=False):
    """An independent, path-based statement of which strings may survive:
    under an opaque dict only that map's KEPT_ENTRIES scalars; otherwise a
    string under a KEPT_SUBTREES ancestor or a KEPT_KEYS key. Runtime
    metadata, `status` and the applied-configuration annotation contribute
    nothing because the projection drops them."""
    if isinstance(value, dict):
        if key in projection.OPAQUE_SUBTREES:
            kept = projection.KEPT_ENTRIES.get(key, ())
            return {v for k, v in value.items()
                    if k in kept and isinstance(v, str)}
        keep = keep or key in projection.KEPT_SUBTREES
        out = set()
        for k, v in value.items():
            if top and k == "status":
                continue
            if key == "metadata" and k in projection._RUNTIME_METADATA:
                continue
            out |= _allowed_strings(v, k, keep)
        return out
    if isinstance(value, list):
        keep = keep or key in projection.KEPT_SUBTREES
        return set().union(*(_allowed_strings(v, None, keep) for v in value))
    if isinstance(value, str) and (keep or key in projection.KEPT_KEYS):
        return {value}
    return set()


def _paths(value, prefix=()):
    if isinstance(value, dict):
        for k, v in value.items():
            yield from _paths(v, prefix + (k,))
    elif isinstance(value, list):
        for i, v in enumerate(value):
            yield from _paths(v, prefix + (i,))
    else:
        yield prefix, value


class ProjectionPropertyTest(unittest.TestCase):

    def test_only_allowlisted_strings_survive_and_the_shape_is_preserved(self):
        rng = random.Random(20260915)
        for _ in range(400):
            counter = [0]
            tree = {rng.choice(_KEY_POOL): _random_tree(rng, 5, counter)
                    for _ in range(rng.randint(1, 5))}
            tree.setdefault("status", {"name": "st"})
            before = copy.deepcopy(tree)
            out = projection.project(tree)
            self.assertEqual(tree, before)
            self.assertNotIn("status", out)
            survivors = {s for s in _strings(out) if s != OMITTED}
            self.assertEqual(survivors, _allowed_strings(tree, None, False,
                                                         top=True), tree)
            # Every surviving leaf sits where its source did, with the same
            # keys around it: the projection replaces, it never moves.
            source = dict(_paths(tree))
            for path, leaf in _paths(out):
                self.assertIn(path, source, (path, tree))
                if leaf != OMITTED:
                    self.assertEqual(leaf, source[path], (path, tree))

    def test_nothing_under_an_opaque_map_survives_at_any_depth(self):
        rng = random.Random(7)
        for _ in range(200):
            counter = [0]
            inner = _random_tree(rng, 4, counter)
            opaque = rng.choice(sorted(projection.OPAQUE_SUBTREES))
            out = projection.project({"kind": "X", "metadata": {"name": "x"},
                                      "spec": {opaque: {"k": inner}}})
            leaves = [leaf for _, leaf in _paths(out["spec"][opaque])]
            # "k" is on no map's KEPT_ENTRIES list.
            self.assertTrue(all(leaf == OMITTED for leaf in leaves),
                            (opaque, inner, out))


if __name__ == "__main__":
    unittest.main()
