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

"""Live IR → CSV tables.

Pure derivation: every cell traces to a field of the live IR, and nothing
here re-reads AWS or the cluster. The tables exist because the IR is sized
for machines and the ledger, not for a context window — an agent working a
later step reads one bounded table, not a multi-megabyte JSON document.

One deliberately opinionated column: storage.csv's `gke_target` maps the
CSI driver to its GKE counterpart (EBS → Persistent Disk, EFS → Filestore
CSI, S3 → GCS FUSE CSI). That mapping is a fixed fact of the two platforms,
not a per-estate judgment, so stating it here does not pre-empt any review.
The ECR → Artifact Registry image mapping is NOT emitted here for the
opposite reason: the AR destination is decided at deployment (provision_
artifact_registry), and inventing one earlier would put two answers in the
ledger. The image table's registry vocabulary (ecr, gcr_ar, ghcr, quay,
dockerhub, other) and ref parsing are the static scan's
(discovery_init_1/images.py), so the live and static inventories agree on
what an image is when the reconciliation step joins them.
"""

import csv
import io

from ..discovery_init_1 import images
from . import projection

# CSI driver / legacy provisioner → what it becomes on GKE.
_DRIVER_TARGETS = {
    "ebs.csi.aws.com": "GKE Persistent Disk (pd-balanced / pd-ssd)",
    "kubernetes.io/aws-ebs": "GKE Persistent Disk (pd-balanced / pd-ssd)",
    "efs.csi.aws.com": "Filestore (Filestore CSI driver)",
    "s3.csi.aws.com": "Cloud Storage (GCS FUSE CSI driver)",
}

TABLES = ("clusters", "nodegroups", "workloads", "autoscaling", "networking",
          "identity", "storage", "config", "images")


def render_tables(live_ir: dict) -> dict:
    """Returns {table name → CSV text} for every table in TABLES.

    Every table is always present, even when empty — a missing file cannot
    be told apart from a scan that found nothing, and the header row says
    which one it is.
    """
    clusters = live_ir.get("clusters") or []
    return {
        "clusters": _clusters_csv(clusters),
        "nodegroups": _nodegroups_csv(clusters),
        "workloads": _workloads_csv(clusters),
        "autoscaling": _autoscaling_csv(clusters),
        "networking": _networking_csv(clusters),
        "identity": _identity_csv(clusters),
        "storage": _storage_csv(clusters),
        "config": _config_csv(clusters),
        "images": _images_csv(clusters),
    }


def _csv(header, rows) -> str:
    out = io.StringIO()
    writer = csv.writer(out, lineterminator="\n")
    writer.writerow(header)
    writer.writerows(rows)
    return out.getvalue()


def _cell(value) -> str:
    """A CSV cell: a boolean as `true`/`false` — the spelling every CSV
    reader and the JSON beside it agree on, not Python's — and anything
    else as str()."""
    if value is True:
        return "true"
    if value is False:
        return "false"
    return str(value)


def _text(value) -> str:
    """A value inside a composed cell (`key=value`, `kind/name`): its text,
    or nothing for None — never the word `None`."""
    return "" if value is None else str(value)


def _flag(value):
    """A boolean cell whose value may also be unknown: `true`/`false` through
    _cell, None left to the writer as the empty cell it renders."""
    return _cell(value) if isinstance(value, bool) else value


def _join(values) -> str:
    return ";".join(_cell(value) for value in values if value not in (None, ""))


def _kube(cluster) -> dict:
    kubernetes_ir = cluster.get("kubernetes")
    return kubernetes_ir if isinstance(kubernetes_ir, dict) else {}


def _meta(manifest) -> dict:
    return manifest.get("metadata") or {}


def _clusters_csv(clusters) -> str:
    rows = [[
        cluster.get("region"), cluster.get("name"),
        cluster.get("kubernetes_version"), cluster.get("platform_version"),
        cluster.get("status"),
        _flag((cluster.get("endpoint_access") or {}).get("public")),
        _flag((cluster.get("endpoint_access") or {}).get("private")),
        (cluster.get("vpc") or {}).get("vpc_id"),
        _join((cluster.get("vpc") or {}).get("subnet_ids") or []),
        cluster.get("oidc_issuer"),
        _join(f"{_text(addon.get('name'))}={_text(addon.get('version'))}"
              for addon in cluster.get("addons") or []),
        "" if cluster.get("kubernetes_error") is None
        else cluster["kubernetes_error"],
    ] for cluster in clusters]
    return _csv(["region", "cluster", "kubernetes_version", "platform_version",
                 "status", "endpoint_public", "endpoint_private", "vpc_id",
                 "subnet_ids", "oidc_issuer", "addons", "workload_scan_error"],
                rows)


def _nodegroups_csv(clusters) -> str:
    rows = []
    for cluster in clusters:
        for nodegroup in cluster.get("nodegroups") or []:
            scaling = nodegroup.get("scaling") or {}
            rows.append([
                cluster.get("region"), cluster.get("name"),
                nodegroup.get("name"), nodegroup.get("status"),
                nodegroup.get("capacity_type"),
                _join(nodegroup.get("instance_types") or []),
                nodegroup.get("ami_type"), nodegroup.get("release_version"),
                nodegroup.get("disk_size_gb"),
                scaling.get("min"), scaling.get("max"), scaling.get("desired"),
                _join(f"{key}={value}" for key, value
                      in (nodegroup.get("labels") or {}).items()),
                _join(f"{_text(taint.get('key'))}={_text(taint.get('value'))}"
                      f":{_text(taint.get('effect'))}"
                      for taint in nodegroup.get("taints") or []),
                _join(nodegroup.get("asg_names") or []),
            ])
    return _csv(["region", "cluster", "nodegroup", "status", "capacity_type",
                 "instance_types", "ami_type", "release_version",
                 "disk_size_gb", "min", "max", "desired", "labels", "taints",
                 "asg_names"], rows)


def _workloads_csv(clusters) -> str:
    rows = []
    for cluster in clusters:
        for kind, items in (_kube(cluster).get("workloads") or {}).items():
            for manifest in items:
                pod_spec = _pod_spec(kind, manifest)
                containers = pod_spec.get("containers") or []
                requests, limits = _resources(containers)
                rows.append([
                    cluster.get("name"), _meta(manifest).get("namespace"),
                    kind, _meta(manifest).get("name"),
                    (manifest.get("spec") or {}).get("replicas"),
                    _join(container.get("image") for container in containers),
                    pod_spec.get("serviceAccountName"),
                    _join(f"{key}={value}" for key, value
                          in (pod_spec.get("nodeSelector") or {}).items()),
                    len(pod_spec.get("tolerations") or []),
                    _cell(bool(pod_spec.get("affinity"))),
                    len(pod_spec.get("topologySpreadConstraints") or []),
                    requests.get("cpu"), requests.get("memory"),
                    limits.get("cpu"), limits.get("memory"),
                ])
    return _csv(["cluster", "namespace", "kind", "name", "replicas", "images",
                 "service_account", "node_selector", "tolerations",
                 "has_affinity", "topology_spread_constraints",
                 "requests_cpu", "requests_memory", "limits_cpu",
                 "limits_memory"], rows)


def _autoscaling_csv(clusters) -> str:
    rows = []
    for cluster in clusters:
        autoscaling = _kube(cluster).get("autoscaling") or {}
        for hpa in autoscaling.get("hpas") or []:
            spec = hpa.get("spec") or {}
            target = spec.get("scaleTargetRef") or {}
            rows.append([
                cluster.get("name"), _meta(hpa).get("namespace"),
                "HorizontalPodAutoscaler", _meta(hpa).get("name"),
                f"{target.get('kind')}/{target.get('name')}",
                spec.get("minReplicas"), spec.get("maxReplicas"),
                _join((metric.get("type") or "") for metric
                      in spec.get("metrics") or []),
            ])
        karpenter = autoscaling.get("karpenter") or {}
        for key, kind in (("nodepools", "KarpenterNodePool"),
                          ("provisioners", "KarpenterProvisioner"),
                          ("node_classes", "KarpenterNodeClass")):
            for item in karpenter.get(key) or []:
                limits = ((item.get("spec") or {}).get("limits") or {})
                rows.append([
                    cluster.get("name"), _meta(item).get("namespace"),
                    kind, _meta(item).get("name"), "", "", "",
                    _join(f"{resource}={amount}" for resource, amount
                          in (limits.get("resources") or limits).items()
                          if isinstance(amount, (str, int, float))),
                ])
    return _csv(["cluster", "namespace", "kind", "name", "target", "min",
                 "max", "metrics_or_limits"], rows)


def _networking_csv(clusters) -> str:
    rows = []
    for cluster in clusters:
        for lb in cluster.get("load_balancers") or []:
            rows.append([cluster.get("name"), "", "CloudLoadBalancer",
                         lb.get("name"), lb.get("type"), lb.get("scheme"),
                         lb.get("dns_name"),
                         lb.get("ingress_group") or lb.get("service") or ""])
        networking = _kube(cluster).get("networking") or {}
        for ingress_class in networking.get("ingress_classes") or []:
            rows.append([cluster.get("name"), "", "IngressClass",
                         _meta(ingress_class).get("name"),
                         (ingress_class.get("spec") or {}).get("controller"),
                         "", "", ""])
        for ingress in networking.get("ingresses") or []:
            spec = ingress.get("spec") or {}
            rows.append([
                cluster.get("name"), _meta(ingress).get("namespace"),
                "Ingress", _meta(ingress).get("name"),
                spec.get("ingressClassName")
                or (_meta(ingress).get("annotations") or {})
                .get("kubernetes.io/ingress.class") or "",
                "",
                _join(rule.get("host") for rule in spec.get("rules") or []),
                _join(sorted(key for key
                             in _meta(ingress).get("annotations") or {}
                             if key.startswith("alb.ingress.kubernetes.io/"))),
            ])
        for service in networking.get("services") or []:
            if (service.get("spec") or {}).get("type") != "LoadBalancer":
                continue
            rows.append([
                cluster.get("name"), _meta(service).get("namespace"),
                "ServiceLoadBalancer", _meta(service).get("name"),
                (_meta(service).get("annotations") or {})
                .get("service.beta.kubernetes.io/aws-load-balancer-type", ""),
                "", "", ""])
        # OSS routing CRDs, one row per object like Ingress: what routes
        # traffic today is what the GKE Gateway/mesh translation must cover.
        gateway_api = networking.get("gateway_api") or {}
        for gateway_class in gateway_api.get("gateway_classes") or []:
            rows.append([cluster.get("name"), "", "GatewayClass",
                         _meta(gateway_class).get("name"),
                         (gateway_class.get("spec") or {}).get("controllerName"),
                         "", "", ""])
        for gateway in gateway_api.get("gateways") or []:
            spec = gateway.get("spec") or {}
            listeners = spec.get("listeners") or []
            rows.append([
                cluster.get("name"), _meta(gateway).get("namespace"),
                "GatewayAPIGateway", _meta(gateway).get("name"),
                spec.get("gatewayClassName"), "",
                _join(listener.get("hostname") for listener in listeners),
                _join(f"{listener.get('protocol')}:{listener.get('port')}"
                      for listener in listeners),
            ])
        for route in gateway_api.get("http_routes") or []:
            spec = route.get("spec") or {}
            rows.append([
                cluster.get("name"), _meta(route).get("namespace"),
                "HTTPRoute", _meta(route).get("name"),
                _join(parent.get("name")
                      for parent in spec.get("parentRefs") or []),
                "", _join(spec.get("hostnames") or []), ""])
        istio = networking.get("istio") or {}
        for gateway in istio.get("gateways") or []:
            spec = gateway.get("spec") or {}
            rows.append([
                cluster.get("name"), _meta(gateway).get("namespace"),
                "IstioGateway", _meta(gateway).get("name"),
                _join(f"{key}={value}" for key, value
                      in sorted((spec.get("selector") or {}).items())),
                "",
                _join(host for server in spec.get("servers") or []
                      for host in server.get("hosts") or []),
                ""])
        for virtual_service in istio.get("virtual_services") or []:
            spec = virtual_service.get("spec") or {}
            rows.append([
                cluster.get("name"), _meta(virtual_service).get("namespace"),
                "IstioVirtualService", _meta(virtual_service).get("name"),
                _join(spec.get("gateways") or []), "",
                _join(spec.get("hosts") or []), ""])
        traefik = networking.get("traefik") or {}
        for route in traefik.get("ingress_routes") or []:
            spec = route.get("spec") or {}
            rows.append([
                cluster.get("name"), _meta(route).get("namespace"),
                "TraefikIngressRoute", _meta(route).get("name"),
                # A Traefik rule is a match expression the IR omits, so the
                # hosts cell says so rather than reading as "no host".
                _join(spec.get("entryPoints") or []), "", projection.OMITTED,
                _join(service.get("name")
                      for rule in spec.get("routes") or []
                      for service in rule.get("services") or [])])
    return _csv(["cluster", "namespace", "kind", "name", "class_or_controller",
                 "scheme", "hosts_or_dns", "details"], rows)


def _identity_csv(clusters) -> str:
    rows = []
    for cluster in clusters:
        identity = _kube(cluster).get("identity") or {}
        for binding in identity.get("irsa") or []:
            role = binding.get("role") or {}
            rows.append([
                cluster.get("name"), binding.get("namespace"), "IRSA",
                binding.get("sa"), binding.get("role_arn"),
                _flag(role.get("exists")),
                _join(role.get("trusted_subjects") or []),
                _join(role.get("attached_policies") or []),
                role.get("error") or "",
            ])
    return _csv(["cluster", "namespace", "kind", "name", "role_arn",
                 "role_exists", "trusted_subjects", "attached_policies",
                 "detail"], rows)


def _storage_csv(clusters) -> str:
    rows = []
    for cluster in clusters:
        storage = _kube(cluster).get("storage") or {}
        provisioner_of = {
            _meta(sc).get("name"): sc.get("provisioner")
            for sc in storage.get("storage_classes") or []}
        # PVs joined to their PVC by the PV's own claimRef: a
        # statically-provisioned volume — the canonical S3 CSI mount — has
        # no StorageClass, so its driver is visible only on the PV spec
        # itself.
        pv_by_claim = {}
        for pv in storage.get("pvs") or []:
            claim = (pv.get("spec") or {}).get("claimRef") or {}
            if claim.get("name"):
                pv_by_claim[(claim.get("namespace"), claim["name"])] = pv
        claimed_pv_names = set()
        for pvc in storage.get("pvcs") or []:
            spec = pvc.get("spec") or {}
            namespace = _meta(pvc).get("namespace")
            name = _meta(pvc).get("name")
            pv = pv_by_claim.get((namespace, name))
            pv_name = _meta(pv).get("name") if pv is not None else ""
            if pv_name:
                claimed_pv_names.add(pv_name)
            storage_class = spec.get("storageClassName")
            provisioner = provisioner_of.get(storage_class) or _pv_driver(pv)
            rows.append([
                cluster.get("name"), namespace, name, pv_name,
                storage_class, provisioner,
                ((spec.get("resources") or {}).get("requests")
                 or {}).get("storage"),
                _join(spec.get("accessModes") or []),
                _DRIVER_TARGETS.get(provisioner, "review required"),
            ])
        # PVs no collected PVC claims — released volumes, or claims in a
        # namespace outside the walk's scope. Their data still has to land
        # somewhere on GKE, so an unclaimed PV is a row, not a blind spot.
        for pv in storage.get("pvs") or []:
            if _meta(pv).get("name") in claimed_pv_names:
                continue
            spec = pv.get("spec") or {}
            claim = spec.get("claimRef") or {}
            driver = _pv_driver(pv)
            rows.append([
                cluster.get("name"), claim.get("namespace") or "",
                claim.get("name") or "", _meta(pv).get("name"),
                spec.get("storageClassName"), driver,
                (spec.get("capacity") or {}).get("storage"),
                _join(spec.get("accessModes") or []),
                _DRIVER_TARGETS.get(driver, "review required"),
            ])
    return _csv(["cluster", "namespace", "pvc", "pv", "storage_class",
                 "provisioner", "requested", "access_modes", "gke_target"],
                rows)


def _pv_driver(pv):
    """The driver a PV's own volume source names — how a statically-
    provisioned volume (no StorageClass) declares its backend."""
    if not isinstance(pv, dict):
        return None
    spec = pv.get("spec") or {}
    csi = spec.get("csi")
    if isinstance(csi, dict) and csi.get("driver"):
        return csi["driver"]
    if "awsElasticBlockStore" in spec:
        return "kubernetes.io/aws-ebs"
    flex = spec.get("flexVolume")
    if isinstance(flex, dict) and flex.get("driver"):
        return flex["driver"]
    if "nfs" in spec:
        return "nfs"
    return None


def _config_csv(clusters) -> str:
    rows = []
    for cluster in clusters:
        config = _kube(cluster).get("config") or {}
        for configmap in config.get("configmaps") or []:
            rows.append([
                cluster.get("name"), _meta(configmap).get("namespace"),
                "ConfigMap", _meta(configmap).get("name"), "",
                _join(sorted((configmap.get("data") or {}))),
            ])
        for secret in config.get("secrets") or []:
            rows.append([
                cluster.get("name"), _meta(secret).get("namespace"),
                "Secret", _meta(secret).get("name"), secret.get("type"),
                _join(sorted(secret.get("data") or {})),
            ])
        # External secret references: `type` names the store they point at,
        # `keys` the remote keys/parameters — the inventory the Google
        # Secret Manager re-pointing works from. No value ever sits here.
        external = config.get("external_secrets") or {}
        for item in external.get("external_secrets") or []:
            spec = item.get("spec") or {}
            store = spec.get("secretStoreRef") or {}
            remote_keys = [(entry.get("remoteRef") or {}).get("key")
                           for entry in spec.get("data") or []]
            remote_keys += [(entry.get("extract") or {}).get("key")
                            for entry in spec.get("dataFrom") or []]
            rows.append([
                cluster.get("name"), _meta(item).get("namespace"),
                "ExternalSecret", _meta(item).get("name"),
                f"{store.get('kind') or 'SecretStore'}/{_text(store.get('name'))}",
                _join(remote_keys)])
        for kind_label, key in (("SecretStore", "secret_stores"),
                                ("ClusterSecretStore", "cluster_secret_stores")):
            for item in external.get(key) or []:
                provider = (item.get("spec") or {}).get("provider")
                rows.append([
                    cluster.get("name"), _meta(item).get("namespace"),
                    kind_label, _meta(item).get("name"),
                    _join(sorted(provider)) if isinstance(provider, dict)
                    else (provider or ""),
                    ""])
        for item in external.get("secret_provider_classes") or []:
            spec = item.get("spec") or {}
            rows.append([
                cluster.get("name"), _meta(item).get("namespace"),
                "SecretProviderClass", _meta(item).get("name"),
                spec.get("provider"),
                _join(sorted(spec.get("parameters") or {}))])
    return _csv(["cluster", "namespace", "kind", "name", "type", "keys"], rows)


def _images_csv(clusters) -> str:
    """Unique images across all clusters: where declared, whether running."""
    declared = {}
    running = set()
    for cluster in clusters:
        kube = _kube(cluster)
        for image in kube.get("images_running") or []:
            running.add(image)
        for kind, items in (kube.get("workloads") or {}).items():
            for manifest in items:
                pod_spec = _pod_spec(kind, manifest)
                # Count the workload once per distinct image it declares — a
                # two-container workload on the same image is one user, not two.
                images_in_workload = {
                    container.get("image")
                    for container in ((pod_spec.get("containers") or [])
                                      + (pod_spec.get("initContainers") or []))
                    if container.get("image")}
                for image in images_in_workload:
                    entry = declared.setdefault(
                        image, {"clusters": set(), "workloads": 0})
                    entry["clusters"].add(cluster.get("name"))
                    entry["workloads"] += 1
    rows = []
    for image in sorted(set(declared) | running):
        repository, tag, digest, _ = images.parse_image_ref(image)
        entry = declared.get(image, {"clusters": set(), "workloads": 0})
        rows.append([
            image, images.classify_registry(image), repository,
            tag or "", digest or "",
            _join(sorted(entry["clusters"])), entry["workloads"],
            _cell(image in running),
        ])
    return _csv(["image", "registry_kind", "repository", "tag", "digest",
                 "clusters", "declared_by_workloads", "running"], rows)


def _pod_spec(kind, manifest) -> dict:
    spec = manifest.get("spec") or {}
    if kind == "CronJob":
        spec = (spec.get("jobTemplate") or {}).get("spec") or {}
    template_spec = (spec.get("template") or {}).get("spec")
    return template_spec if isinstance(template_spec, dict) else {}


def _resources(containers) -> tuple:
    """Requests/limits summed per resource across containers — sums of mixed
    units are not attempted; same-unit values are joined verbatim instead,
    because a wrong sum in a sizing table is worse than a list."""
    requests, limits = {}, {}
    for container in containers:
        resources = container.get("resources") or {}
        for bucket, out in (("requests", requests), ("limits", limits)):
            for resource, amount in (resources.get(bucket) or {}).items():
                out.setdefault(resource, [])
                out[resource].append(str(amount))
    return ({resource: "+".join(amounts) for resource, amounts in requests.items()},
            {resource: "+".join(amounts) for resource, amounts in limits.items()})
