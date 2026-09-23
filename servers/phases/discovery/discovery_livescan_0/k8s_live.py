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


"""Read-only walk of one EKS cluster's API into a keys-only live IR.

The walk is pure over an injected `get_json(path) -> dict | None` (None for
a 404) so it can be exercised in-process against a fake control plane; the
seam that speaks to a real API server lives in eks_auth.

`namespaces` narrows the walk to exactly those namespaces. Left empty,
every namespace is walked except the EKS-managed system ones; the
cluster-scoped inventory (nodes, PVs, classes, the Namespace list) is never
filtered because it is the estate's shape, not a workload in it.

Every object persisted leaves through projection.project: its structure
and the strings a migration reproduces literally — names and references,
images, classes, ports, drivers, the scheduling and routing contracts, an
exact-key list of platform labels and annotations — with every other
string value (a Secret or ConfigMap value, an env literal, a command token,
a custom annotation or label value, a Karpenter userData script) replaced
by the OMITTED marker. Nothing is parsed or classified, so there is no
credential shape to miss: what is not on the allowlist is not written.
Nodes are reduced to capacity fields and the values of their well-known
platform labels rather than projected, and pods contribute their images,
their count and the owner-less ones among them.

Each collection notes its own failure and the walk goes on; the two ways a
cluster can "walk" into nothing — an endpoint that is not an API server,
an identity authorized for no read — end the cluster with one line of
advice instead (see discover_cluster).
"""

from . import projection

# List pagination: chunk size per request, and a cap on chunks per
# collection. 8 × 500 objects of one kind is beyond any estate this agent
# migrates; hitting the cap is noted, never silent.
PAGE_LIMIT = 500
MAX_PAGES = 8

# kind → list path. Workload kinds feed the image inventory; every kind
# is persisted projected (projection.project).
WORKLOAD_PATHS = (
    ("Deployment", "/apis/apps/v1/deployments"),
    ("StatefulSet", "/apis/apps/v1/statefulsets"),
    ("DaemonSet", "/apis/apps/v1/daemonsets"),
    ("CronJob", "/apis/batch/v1/cronjobs"),
)

# Karpenter's API has moved twice; each kind is tried newest-first, the
# first group version of a CRD that answers standing for it, and a kind
# that spans two CRDs (EC2NodeClass and the AWSNodeTemplate it replaced)
# collects both when both are served (_first_serving). All-None means
# Karpenter is not installed, which is itself a finding (no NodePools to
# translate to NAP).
KARPENTER_PATHS = (
    ("nodepools", ("/apis/karpenter.sh/v1/nodepools",
                   "/apis/karpenter.sh/v1beta1/nodepools")),
    ("provisioners", ("/apis/karpenter.sh/v1alpha5/provisioners",)),
    ("node_classes", ("/apis/karpenter.k8s.aws/v1/ec2nodeclasses",
                      "/apis/karpenter.k8s.aws/v1beta1/ec2nodeclasses",
                      "/apis/karpenter.k8s.aws/v1alpha1/awsnodetemplates")),
)

# OSS routing beyond Ingress, each kind tried newest group version first
# like KARPENTER_PATHS (Traefik's two API groups both collected when both
# are served): an estate on the Gateway API, Istio, or Traefik
# shows an empty `ingresses` list while its real routing sits in these CRDs,
# so skipping them would misread "routed differently" as "nothing routed".
GATEWAY_API_PATHS = (
    ("gateway_classes", "GatewayClass",
     ("/apis/gateway.networking.k8s.io/v1/gatewayclasses",
      "/apis/gateway.networking.k8s.io/v1beta1/gatewayclasses")),
    ("gateways", "Gateway",
     ("/apis/gateway.networking.k8s.io/v1/gateways",
      "/apis/gateway.networking.k8s.io/v1beta1/gateways")),
    ("http_routes", "HTTPRoute",
     ("/apis/gateway.networking.k8s.io/v1/httproutes",
      "/apis/gateway.networking.k8s.io/v1beta1/httproutes")),
)
ISTIO_PATHS = (
    ("gateways", "Gateway",
     ("/apis/networking.istio.io/v1/gateways",
      "/apis/networking.istio.io/v1beta1/gateways")),
    ("virtual_services", "VirtualService",
     ("/apis/networking.istio.io/v1/virtualservices",
      "/apis/networking.istio.io/v1beta1/virtualservices")),
)
TRAEFIK_PATHS = (
    ("ingress_routes", "IngressRoute",
     ("/apis/traefik.io/v1alpha1/ingressroutes",
      "/apis/traefik.containo.us/v1alpha1/ingressroutes")),
    # What a route adds to a request lives in Middlewares
    # (`basicAuth.secret`, `forwardAuth.address`), not on the route.
    ("middlewares", "Middleware",
     ("/apis/traefik.io/v1alpha1/middlewares",
      "/apis/traefik.containo.us/v1alpha1/middlewares")),
)

# External secret references: the objects that say "this Secret's value
# lives in AWS Secrets Manager / Parameter Store". They carry pointers, not
# values (the materialized values arrive as ordinary Secrets, projected
# to their key names), and each pointer must be re-aimed at Google
# Secret Manager — invisible to a walk that reads only Secrets.
EXTERNAL_SECRETS_PATHS = (
    ("external_secrets", "ExternalSecret",
     ("/apis/external-secrets.io/v1/externalsecrets",
      "/apis/external-secrets.io/v1beta1/externalsecrets")),
    ("secret_stores", "SecretStore",
     ("/apis/external-secrets.io/v1/secretstores",
      "/apis/external-secrets.io/v1beta1/secretstores")),
    ("cluster_secret_stores", "ClusterSecretStore",
     ("/apis/external-secrets.io/v1/clustersecretstores",
      "/apis/external-secrets.io/v1beta1/clustersecretstores")),
    ("secret_provider_classes", "SecretProviderClass",
     ("/apis/secrets-store.csi.x-k8s.io/v1/secretproviderclasses",
      "/apis/secrets-store.csi.x-k8s.io/v1alpha1/secretproviderclasses")),
)

# Namespaces that are EKS-managed plumbing, not customer workloads —
# skipped by default so the IR reflects what has to migrate. An explicit
# `namespaces` target overrides the skip: naming kube-system scans it.
_AWS_SYSTEM_NAMESPACES = frozenset({
    "kube-system", "kube-public", "kube-node-lease",
    "amazon-cloudwatch", "aws-observability", "amazon-guardduty",
})

IRSA_ANNOTATION = "eks.amazonaws.com/role-arn"

# Secret types that are runtime artifacts, not configuration: SA tokens are
# minted by the control plane, and helm.sh/release.v1 blobs are the chart
# archive itself (huge, and derived from sources the static scan owns).
_SKIPPED_SECRET_TYPES = {
    "kubernetes.io/service-account-token": "service_account_tokens",
    "helm.sh/release.v1": "helm_releases",
}
# The same release record under Helm's configmap storage driver
# (`sh.helm.release.v1.<release>.v<n>`, the archive gzip+base64 under
# `release`): the chart and the release's whole values, skipped the same
# way.
_HELM_RELEASE_CONFIGMAP_PREFIX = "sh.helm.release.v1."

# Node labels worth carrying: where the capacity came from and what shape
# it is. Live nodes are the one inventory Karpenter-provisioned capacity
# appears in — no nodegroup describes it. Their values are platform-written
# enumerations (an instance type, a zone, a pool name), the same keys
# projection.KEPT_ENTRIES keeps on a projected object; every other label is
# a key name only.
_NODE_LABELS = (
    ("instance_type", "node.kubernetes.io/instance-type"),
    ("zone", "topology.kubernetes.io/zone"),
    ("arch", "kubernetes.io/arch"),
    ("nodegroup", "eks.amazonaws.com/nodegroup"),
    ("karpenter_nodepool", "karpenter.sh/nodepool"),
    ("capacity_type_eks", "eks.amazonaws.com/capacityType"),
    ("capacity_type_karpenter", "karpenter.sh/capacity-type"),
)


def discover_cluster(get_json, namespaces=None) -> dict:
    """Walks one cluster's API and returns its projected live IR.

    `namespaces` narrows the walk to exactly those namespaces (naming an AWS
    system namespace deliberately includes it). Left empty, every namespace
    is walked except the EKS-managed system ones (_AWS_SYSTEM_NAMESPACES).
    Cluster-scoped objects — nodes, PVs, StorageClasses, IngressClasses,
    Karpenter and Gateway API classes, the Namespace list itself — are never
    filtered: they are the estate's shape, not workloads in it.
    """
    notes = []
    ir = {"notes": notes}

    # One probe before any collection, OUTSIDE the note-and-continue
    # discipline: every _list_all below downgrades a failure to a note, so
    # without it an unreachable control plane would "walk" successfully into
    # an IR that is nothing but failure notes and read as a cluster that
    # answered. /version is served by every API server; letting its
    # exception propagate, and raising on a 404, is what marks the cluster
    # unreachable in the IR.
    version = get_json("/version")
    if version is None:
        # A 404 here is not an older API server — every one serves
        # /version. Something else answered (a proxy, a WAF, the wrong
        # endpoint), and it would 404 every list below into a served,
        # empty cluster with no note; that is not an estate either.
        raise RuntimeError(
            "control plane answered 404 to /version — not a Kubernetes API "
            "server at this endpoint (a proxy, a WAF or the wrong "
            "endpoint); nothing was read")
    if isinstance(version, dict) and version.get("gitVersion"):
        ir["server_version"] = version["gitVersion"]

    # The second way a cluster can "walk" into nothing: the identity
    # authenticates (/version answered) but is authorized for no read, so
    # every collection below downgrades a 401/403 into its own note and the
    # IR is forty-odd denials read as an empty cluster. Count the denials
    # and, if every single read was one, fail the cluster as a whole with
    # the one line the operator needs (see the check after the walk).
    get_json, access = _counting_denials(get_json)

    target = frozenset(namespaces or ())
    dropped = [0]

    def scoped(items):
        # Drops namespaced items outside the walk's namespace scope. A
        # cluster-scoped item carries no metadata.namespace and always
        # passes, so every collection can be filtered uniformly.
        kept = []
        for item in items:
            namespace = (item.get("metadata") or {}).get("namespace")
            if namespace is None or (
                    namespace in target if target
                    else namespace not in _AWS_SYSTEM_NAMESPACES):
                kept.append(item)
            else:
                dropped[0] += 1
        return kept

    ir["namespaces"] = [
        projection.project(item) for item in
        _list_all(get_json, "/api/v1/namespaces", "Namespace", notes)]

    workloads = {}
    for kind, path in WORKLOAD_PATHS:
        workloads[kind] = [
            projection.project(item)
            for item in scoped(_list_all(get_json, path, kind, notes))]
    ir["workloads"] = workloads

    ir["autoscaling"] = {
        "hpas": [projection.project(item) for item in
                 scoped(_list_hpas(get_json, notes))],
        "karpenter": _karpenter(get_json, notes),
    }

    services = [projection.project(item) for item in scoped(
        _list_all(get_json, "/api/v1/services", "Service", notes))]
    ir["networking"] = {
        "services": services,
        "ingresses": [projection.project(item) for item in scoped(
            _list_all(get_json, "/apis/networking.k8s.io/v1/ingresses",
                      "Ingress", notes))],
        "ingress_classes": [projection.project(item) for item in _list_all(
            get_json, "/apis/networking.k8s.io/v1/ingressclasses",
            "IngressClass", notes)],
        "gateway_api": _crd_group(get_json, notes, scoped, GATEWAY_API_PATHS),
        "istio": _crd_group(get_json, notes, scoped, ISTIO_PATHS),
        "traefik": _crd_group(get_json, notes, scoped, TRAEFIK_PATHS),
    }

    service_accounts = scoped(_list_all(
        get_json, "/api/v1/serviceaccounts", "ServiceAccount", notes))
    ir["identity"] = {
        "service_accounts": [projection.project(item)
                             for item in service_accounts],
        "irsa": [{
            "namespace": (item.get("metadata") or {}).get("namespace"),
            "sa": (item.get("metadata") or {}).get("name"),
            "role_arn": ((item.get("metadata") or {}).get("annotations")
                         or {}).get(IRSA_ANNOTATION),
        } for item in service_accounts
            if ((item.get("metadata") or {}).get("annotations")
                or {}).get(IRSA_ANNOTATION)],
    }

    ir["storage"] = {
        "pvcs": [projection.project(item) for item in scoped(_list_all(
            get_json, "/api/v1/persistentvolumeclaims",
            "PersistentVolumeClaim", notes))],
        "pvs": [projection.project(item) for item in _list_all(
            get_json, "/api/v1/persistentvolumes", "PersistentVolume", notes)],
        "storage_classes": [projection.project(item) for item in _list_all(
            get_json, "/apis/storage.k8s.io/v1/storageclasses",
            "StorageClass", notes)],
    }

    ir["config"] = _config_and_secrets(get_json, notes, scoped)
    external = _crd_group(get_json, notes, scoped, EXTERNAL_SECRETS_PATHS)
    ir["config"]["external_secrets"] = external
    external_count = sum(len(items) for items in external.values())
    if external_count:
        notes.append(
            f"{external_count} external-secret object(s) (ExternalSecret/"
            "SecretStore/SecretProviderClass) reference values held in an "
            "external store, not the cluster — re-point them at Google "
            "Secret Manager during translation")

    ir["nodes"] = _nodes(get_json, notes)
    ir["bare_pods"], ir["pod_count"], ir["images_running"] = _pods(
        get_json, notes, scoped)

    if target:
        notes.append(
            f"scan restricted to namespace(s) {sorted(target)} — "
            f"{dropped[0]} object(s) in other namespaces not collected")
    elif dropped[0]:
        notes.append(
            f"{dropped[0]} object(s) in AWS system namespaces not collected "
            f"({', '.join(sorted(_AWS_SYSTEM_NAMESPACES))}) — EKS-managed "
            "plumbing with no GKE translation; pass namespaces=[...] to "
            "include one")

    if access["denied"] and access["denied"] == access["calls"]:
        raise PermissionError(
            f"authenticated to the control plane"
            f"{' (' + ir['server_version'] + ')' if ir.get('server_version') else ''}"
            f" but authorized for no read — all {access['calls']} collections "
            f"were denied; the first: {notes[0] if notes else 'n/a'}. Grant "
            "the calling identity cluster-wide read access (an EKS access "
            "entry with AmazonEKSViewPolicy at cluster scope, or an aws-auth "
            "mapping to a view ClusterRole) and re-run")
    if access["failed"] and access["failed"] == access["calls"]:
        # /version answered and then nothing did: a transport failure (a
        # proxy that passes the one path, a connection that dropped after
        # the first read), not an estate — and an IR of forty-odd "not
        # collected" notes would be read as an empty one.
        raise ConnectionError(
            f"authenticated to the control plane"
            f"{' (' + ir['server_version'] + ')' if ir.get('server_version') else ''}"
            f" but read nothing — all {access['calls']} collections failed; "
            f"the first: {notes[0] if notes else 'n/a'}. The endpoint "
            "answered /version and then stopped answering: check the "
            "network path to it (a proxy or firewall between this host and "
            "the cluster endpoint, the endpoint's private-access setting) "
            "and re-run")

    ir["counts"] = _counts(ir)
    return ir


def _counting_denials(get_json):
    """Wraps get_json to count reads and the 401/403 among them.

    The wrapper changes nothing about what the walk sees — every answer and
    every exception passes through — it only records, on the returned dict,
    how many reads were made, how many raised at all (`failed`) and how
    many of those the API server refused for authorization (`denied`: an
    exception carrying `status` 401 or 403; the auth seam's
    ControlPlaneError does, as does the kubernetes client's ApiException).
    """
    access = {"calls": 0, "denied": 0, "failed": 0}

    def counted(path):
        access["calls"] += 1
        try:
            return get_json(path)
        except Exception as e:
            access["failed"] += 1
            if getattr(e, "status", None) in (401, 403):
                access["denied"] += 1
            raise

    return counted, access


def _list_all(get_json, path, kind, notes) -> list:
    """Drains one list endpoint into a list; [] when it is not served."""
    return _list_served(get_json, path, kind, notes) or []


def _list_served(get_json, path, kind, notes):
    """Drains one list endpoint, stamping apiVersion/kind onto each item.

    Returns None when the path is not served (a 404: the group version or
    the whole CRD is absent) and the item list — possibly empty — when it
    is. The fallback callers live on that distinction: a CRD that is
    installed with no objects yet is served-and-empty, and must neither be
    read as absent nor make the walk fall through to an older version.

    Items inside a list response carry neither field (the list object owns
    them), and everything downstream — the CSV rows, a human reading the
    IR — needs them on the item.

    A list entry that is not an object with a metadata block is skipped
    and counted in one note; any other failure is one collection's note,
    and the walk goes on — except a 401 that carries `token_refresh_error`
    or `refused_after_refresh` (eks_auth): no fresh token can be signed, or
    a fresh one was refused too, so no later read can succeed either, and
    it is re-raised to end the cluster as a whole with the credential
    advice rather than noted forty-odd times as a thin estate.
    """
    api_version = _api_version_of(path)
    items = []
    skipped = 0
    continue_token = None
    try:
        for page in range(MAX_PAGES):
            query = f"?limit={PAGE_LIMIT}"
            if continue_token:
                query += f"&continue={continue_token}"
            response = get_json(path + query)
            if response is None:
                if page == 0:
                    return None     # 404: the resource is not served.
                # A later page gone (the resource removed mid-walk, its
                # continue token expired): what was read stands, as an
                # incomplete section, not as a served-and-empty one.
                notes.append(f"{kind} listing stopped after page {page} "
                             "(the next page answered 404) — the section "
                             "is incomplete")
                return items
            for item in response.get("items") or []:
                if (not isinstance(item, dict)
                        or not isinstance(item.get("metadata"), dict)):
                    skipped += 1    # not an object with a metadata block
                    continue
                item.setdefault("apiVersion", api_version)
                item.setdefault("kind", kind)
                items.append(item)
            continue_token = (response.get("metadata") or {}).get("continue")
            if not continue_token:
                return items
        remaining = (response.get("metadata") or {}).get("remainingItemCount")
        notes.append(
            f"{kind} listing stopped after {MAX_PAGES * PAGE_LIMIT} objects"
            + (f" (~{remaining} more not collected)" if remaining else "")
            + " — the section is incomplete")
    except Exception as e:
        if (getattr(e, "token_refresh_error", None) is not None
                or getattr(e, "refused_after_refresh", False)):
            raise
        notes.append(f"{kind} not collected ({path}): {e}")
    finally:
        if skipped:
            notes.append(f"{kind}: {skipped} malformed list entr"
                         f"{'y' if skipped == 1 else 'ies'} skipped "
                         "(not an object with a metadata block)")
    return items


def _api_version_of(path: str) -> str:
    if path.startswith("/api/v1/"):
        return "v1"
    parts = path.split("/")  # ['', 'apis', group, version, resource]
    return "/".join(parts[2:4])


def _list_hpas(get_json, notes) -> list:
    items = _list_served(get_json,
                         "/apis/autoscaling/v2/horizontalpodautoscalers",
                         "HorizontalPodAutoscaler", notes)
    if items is not None:
        return items
    # autoscaling/v2 is served on every version this agent migrates from
    # (1.23+); a 404 here means an old control plane, worth trying v2beta2.
    return _list_all(get_json,
                     "/apis/autoscaling/v2beta2/horizontalpodautoscalers",
                     "HorizontalPodAutoscaler", notes)


def _karpenter(get_json, notes) -> dict:
    """The Karpenter objects, and a note when the CRDs are not installed.

    Absence is judged on whether any Karpenter path is *served*, not on
    whether it holds objects: a cluster with Karpenter installed and no
    NodePool yet is a Karpenter cluster with nothing provisioned, and the
    note — which sends the reader to the managed nodegroups — would misstate
    it."""
    karpenter = {}
    served_any = False
    kinds = {"nodepools": "NodePool", "provisioners": "Provisioner",
             "node_classes": "EC2NodeClass"}
    for key, paths in KARPENTER_PATHS:
        items = _first_serving(get_json, kinds[key], paths, notes)
        served_any = served_any or items is not None
        karpenter[key] = [projection.project(item) for item in items or []]
    if not served_any:
        notes.append("Karpenter CRDs not present — node provisioning comes "
                     "from managed nodegroups only")
    return karpenter


# A group version that renamed the kind: the item is stamped with the name
# its own version knows, not the newest one's.
_PATH_KINDS = {
    "/apis/karpenter.k8s.aws/v1alpha1/awsnodetemplates": "AWSNodeTemplate",
}


def _crd_identity(path: str) -> tuple:
    """The (group, resource) a list path addresses —
    `/apis/<group>/<version>/<resource>` — the CRD itself, whichever of its
    versions the path asks for."""
    parts = path.strip("/").split("/")
    return (parts[1], parts[-1]) if len(parts) >= 4 else (path, "")


def _first_serving(get_json, kind, paths, notes):
    """Returns the items from the list paths (newest group version first)
    the control plane serves, or None when every path 404s. The paths
    address one CRD in several versions, or several CRDs — Karpenter's
    EC2NodeClass and the AWSNodeTemplate it replaced, Traefik's kinds
    under their two API groups — and a control plane can serve more than
    one of those at a time (a cluster mid-migration keeps both), so each
    CRD (_crd_identity) is read once, at the first of its versions that
    answers, and their items are joined; an installed CRD with no objects
    gives an empty list. A path whose listing failed for another reason
    (a 403, a timeout) answered — with its note — and its CRD is not
    asked again at an older version. An item that omits its kind is
    stamped with `kind`, or with the path's own where a group version
    renamed it (_PATH_KINDS)."""
    collected = None
    answered = set()
    for path in paths:
        identity = _crd_identity(path)
        if identity in answered:
            continue
        items = _list_served(get_json, path, _PATH_KINDS.get(path, kind),
                             notes)
        if items is None:
            continue
        answered.add(identity)
        collected = (collected or []) + items
    return collected


def _crd_group(get_json, notes, scoped, path_specs) -> dict:
    """Collects one optional CRD family into {key: [projected objects]}.

    Shared by the routing (Gateway API / Istio / Traefik) and external-secret
    families. Unlike Karpenter, absence earns no note: these stacks are
    genuinely optional, so not-installed is the normal case, not a coverage
    gap. Namespaced kinds respect the walk's namespace scope; cluster-scoped
    ones (GatewayClass, ClusterSecretStore) pass through it untouched."""
    return {
        key: [projection.project(item) for item in
              scoped(_first_serving(get_json, kind, paths, notes) or [])]
        for key, kind, paths in path_specs}


def _config_and_secrets(get_json, notes, scoped) -> dict:
    """ConfigMaps and Secrets as key names: projection turns every value
    under `data`/`stringData`/`binaryData` into the OMITTED marker, so a
    Secret record is its name, type and key names and nothing else. The
    runtime artifacts among them (service-account tokens, Helm release
    records under either storage driver) are skipped and counted."""
    configmaps = []
    skipped_configmaps = {"helm_releases": 0}
    for item in scoped(_list_all(get_json, "/api/v1/configmaps", "ConfigMap",
                                 notes)):
        name = (item.get("metadata") or {}).get("name")
        # Injected into every namespace by the control plane; pure noise.
        if name == "kube-root-ca.crt":
            continue
        if (isinstance(name, str)
                and name.startswith(_HELM_RELEASE_CONFIGMAP_PREFIX)):
            skipped_configmaps["helm_releases"] += 1
            continue
        configmaps.append(projection.project(item))

    secrets = []
    skipped = {label: 0 for label in _SKIPPED_SECRET_TYPES.values()}
    for item in scoped(_list_all(get_json, "/api/v1/secrets", "Secret",
                                 notes)):
        skip_label = _SKIPPED_SECRET_TYPES.get(item.get("type"))
        if skip_label:
            skipped[skip_label] += 1
            continue
        secrets.append(projection.project(item))
    for label, count in skipped.items():
        if count:
            notes.append(f"{count} {label.replace('_', ' ')} secret(s) "
                         "skipped — runtime artifacts, not configuration")
    for label, count in skipped_configmaps.items():
        if count:
            notes.append(f"{count} {label.replace('_', ' ')} configmap(s) "
                         "skipped — runtime artifacts, not configuration")
    return {"configmaps": configmaps, "secrets": secrets,
            "skipped_secrets": skipped,
            "skipped_configmaps": skipped_configmaps}


def _nodes(get_json, notes) -> list:
    """Live node inventory, reduced to the fields that shape capacity.

    Nodes are runtime by definition — the point of collecting them. Not
    projected manifests: a node object is huge and almost all of it is
    status; these fields are the capacity reality a static scan cannot see.
    """
    nodes = []
    for item in _list_all(get_json, "/api/v1/nodes", "Node", notes):
        labels = (item.get("metadata") or {}).get("labels") or {}
        status = item.get("status") or {}
        node = {"name": (item.get("metadata") or {}).get("name")}
        for field, label in _NODE_LABELS:
            if labels.get(label) is not None:
                node[field] = labels[label]
        node["taints"] = [
            {"key": taint.get("key"), "value": taint.get("value"),
             "effect": taint.get("effect")}
            for taint in (item.get("spec") or {}).get("taints") or []]
        allocatable = status.get("allocatable") or {}
        node["allocatable"] = {"cpu": allocatable.get("cpu"),
                               "memory": allocatable.get("memory")}
        node["kubelet_version"] = (status.get("nodeInfo")
                                   or {}).get("kubeletVersion")
        nodes.append(node)
    return nodes


def _pods(get_json, notes, scoped) -> tuple:
    """Bare pods (no owner — invisible to any controller inventory), the
    total pod count, and the set of images actually running.

    The running-image set is the drift signal: a Deployment says what should
    run, the pods say what does. All three respect the namespace scope, so
    a default walk's image inventory is not polluted by EKS system images
    (aws-node, kube-proxy) that will never move to Artifact Registry.
    """
    bare = []
    images = set()
    count = 0
    for item in scoped(_list_all(get_json, "/api/v1/pods", "Pod", notes)):
        count += 1
        spec = item.get("spec") or {}
        for container in ((spec.get("containers") or [])
                          + (spec.get("initContainers") or [])):
            if container.get("image"):
                images.add(container["image"])
        if not (item.get("metadata") or {}).get("ownerReferences"):
            bare.append(projection.project(item))
    return bare, count, sorted(images)


def _counts(ir: dict) -> dict:
    counts = {kind: len(items) for kind, items in ir["workloads"].items()}
    karpenter = ir["autoscaling"]["karpenter"]
    gateway_api = ir["networking"]["gateway_api"]
    external = ir["config"]["external_secrets"]
    counts.update({
        "Namespace": len(ir["namespaces"]),
        "HorizontalPodAutoscaler": len(ir["autoscaling"]["hpas"]),
        "KarpenterNodePool": len(karpenter.get("nodepools", [])),
        "KarpenterProvisioner": len(karpenter.get("provisioners", [])),
        "KarpenterNodeClass": len(karpenter.get("node_classes", [])),
        "Service": len(ir["networking"]["services"]),
        "Ingress": len(ir["networking"]["ingresses"]),
        "GatewayAPIGateway": len(gateway_api["gateways"]),
        "HTTPRoute": len(gateway_api["http_routes"]),
        "IstioGateway": len(ir["networking"]["istio"]["gateways"]),
        "IstioVirtualService": len(ir["networking"]["istio"]
                                   ["virtual_services"]),
        "TraefikIngressRoute": len(ir["networking"]["traefik"]
                                   ["ingress_routes"]),
        "ServiceAccount": len(ir["identity"]["service_accounts"]),
        "IRSABinding": len(ir["identity"]["irsa"]),
        "PersistentVolumeClaim": len(ir["storage"]["pvcs"]),
        "StorageClass": len(ir["storage"]["storage_classes"]),
        "ConfigMap": len(ir["config"]["configmaps"]),
        "Secret": len(ir["config"]["secrets"]),
        "ExternalSecret": len(external["external_secrets"]),
        "ExternalSecretStore": (len(external["secret_stores"])
                                + len(external["cluster_secret_stores"])),
        "SecretProviderClass": len(external["secret_provider_classes"]),
        "Node": len(ir["nodes"]),
        "BarePod": len(ir["bare_pods"]),
        "Pod": ir["pod_count"],
    })
    return counts
