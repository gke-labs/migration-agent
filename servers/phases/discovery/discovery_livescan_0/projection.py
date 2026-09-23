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


"""Keys-only projection of a Kubernetes object.

The ledger is readable by the whole workspace, so the live scan never writes
a free-form value a customer authored. An object is projected to its
structure — key names, list lengths, and outside its opaque maps the
numbers and booleans too — plus the strings under an allowlist of fields
whose values a migration reproduces literally: names and references,
images, classes, drivers, ports, and the scheduling and routing contracts
(selectors, taints and tolerations, requirements, resource quantities,
hostnames, route paths). Every other string becomes
OMITTED. Nothing is parsed or classified, so there is no credential shape
to miss: a ConfigMap value, an env literal, a command token, a probe path,
a Karpenter userData script, an annotation or a label value is omitted
whether or not it looks like a secret.

Three tables decide a string's fate, in this order of precedence:

  OPAQUE_SUBTREES  a dict under one of these keys is a user-keyed map or
                   free-form provider config: its key names survive and
                   nothing else does, however a key is spelled (a ConfigMap
                   entry named `name` is still a customer value) — except
                   the exact keys KEPT_ENTRIES lists for that map, whose
                   values are a class, a mode, an enumeration or a
                   reference a translation reads;
  KEPT_SUBTREES    under one of these keys every string is kept: the
                   scheduling and routing contracts a GKE node pool or load
                   balancer must reproduce literally (a route's path match
                   included, a rewrite or redirect target not);
  KEPT_KEYS        a string directly under one of these keys is kept.

Lists are transparent: an item is read under no key, so the tokens of
`args` are omitted (the key applies to the list, not to its members) while
the dicts inside an ExternalSecret's `spec.data` list keep their
`secretKey` and `remoteRef.key`. A subtree nested deeper than MAX_DEPTH
(no real object comes near it) is the marker, so a hostile CRD field
cannot overflow the walk and cost the cluster its record.
"""

OMITTED = "<omitted>"
MAX_DEPTH = 200

# Strings kept when they sit directly under one of these keys (outside an
# opaque map). Deliberately absent: `value` (env literals, header and tag
# values), `path` as a scalar (a probe's query string, a file name),
# `address`/`url`/`server` (userinfo can ride in a URL), `userData`,
# `secret`, `nodeName`, `clusterIP`, `prefix`, and `match` (a Traefik rule
# is an expression that can embed a header or query literal:
# Headers(`X-Api-Key`, `...`)).
KEPT_KEYS = frozenset("""
    name namespace kind apiVersion group resource id alias
    image imagePullPolicy serviceAccountName priorityClassName
    runtimeClassName schedulerName restartPolicy dnsPolicy hostname subdomain
    mountPath subPath subPathExpr medium sizeLimit fsType driver volumeHandle
    volumeID claimName secretName volumeName storageClassName volumeMode
    volumeBindingMode reclaimPolicy persistentVolumeReclaimPolicy provisioner
    key property version secretKey objectName fieldPath audience averageValue
    port targetPort containerPort nodePort protocol appProtocol scheme
    service host type pathType method timeout ingressClassName gatewayClassName
    controllerName controller loadBalancerClass externalTrafficPolicy
    internalTrafficPolicy sessionAffinity externalName ipFamilyPolicy
    topologyKey whenUnsatisfiable operator effect
    schedule concurrencyPolicy timeZone completionMode podManagementPolicy
    serviceName maxSurge maxUnavailable whenDeleted whenScaled
    sectionName from mode credentialName certResolver main
    consolidationPolicy consolidateAfter expireAfter amiFamily capacityType
    role instanceProfile deviceName volumeType volumeSize kmsKeyID httpTokens
    httpEndpoint region creationPolicy refreshInterval provider selectPolicy
    subset
""".split())

# Every string under one of these keys is kept. `paths`, `path` and `uri`
# are the route matches (an Ingress rule's paths, an HTTPRoute's
# `{type, value}`, an Istio `{prefix}`): the public URL surface a load
# balancer must reproduce. A scalar `path` (a probe's, a file's) is a
# KEPT_KEYS question, and is omitted; the Gateway API rewrite and redirect
# filters carry a `path` of the same dict shape whose value is a target,
# not a match, and those filters are opaque below. `mountOptions` is
# deliberately not here: a CIFS/SMB option list can carry `password=`.
KEPT_SUBTREES = frozenset("""
    selector nodeSelector matchLabels matchExpressions matchLabelExpressions
    tolerations taints startupTaints affinity topologySpreadConstraints
    requirements requests limits capacity allocatable accessModes
    hosts hostnames sniHosts gateways entryPoints capabilities dnsConfig sans
    ipFamilies
    paths path uri hostPath loadBalancerSourceRanges timeouts
""".split())

# A dict under one of these keys keeps its key names only, and wins over
# the other two tables at whatever depth it sits. The rule is for maps: a
# list under one of these names (`dnsConfig.options`, name/value pairs) is
# not a user-keyed map and follows its ancestors. The two Gateway API
# filters are here for their `path`, a rewrite or redirect target.
OPAQUE_SUBTREES = frozenset("""
    data stringData binaryData labels annotations parameters tags headers
    volumeAttributes attributes options config plugin
    urlRewrite requestRedirect
""".split())

# The exact keys inside an opaque map whose scalar value is kept: a class,
# a mode, a role ARN, a platform-written enumeration, a StorageClass's
# EBS/EFS provisioning knobs, an IngressClass's or Traefik's parameter
# reference, a redirect's scheme and hostname, a Mountpoint-for-S3
# volume's bucket, a customer-facing DNS name external-dns publishes for
# a Service. Anything not listed is the marker. The list is per map name,
# not per object kind: a SecretProviderClass's `spec.parameters` shares
# the name, and its
# providers (AWS `objects`/`region`, Vault `vaultAddress`/`roleName`,
# Azure `keyvaultName`/`tenantId`, GCP `secrets`) use none of the keys
# listed under `parameters`.
KEPT_ENTRIES = {
    "annotations": frozenset({
        "kubernetes.io/ingress.class",
        "ingressclass.kubernetes.io/is-default-class",
        "service.beta.kubernetes.io/aws-load-balancer-type",
        "service.beta.kubernetes.io/aws-load-balancer-scheme",
        "service.beta.kubernetes.io/aws-load-balancer-nlb-target-type",
        "service.beta.kubernetes.io/aws-load-balancer-internal",
        "alb.ingress.kubernetes.io/scheme",
        "alb.ingress.kubernetes.io/target-type",
        "alb.ingress.kubernetes.io/group.name",
        "storageclass.kubernetes.io/is-default-class",
        "eks.amazonaws.com/role-arn",
        "cluster-autoscaler.kubernetes.io/safe-to-evict",
        "external-dns.alpha.kubernetes.io/hostname",
    }),
    "labels": frozenset({
        "istio-injection", "istio.io/rev",
        "pod-security.kubernetes.io/enforce",
        "pod-security.kubernetes.io/audit",
        "pod-security.kubernetes.io/warn",
        "kubernetes.io/arch", "kubernetes.io/os",
        "node.kubernetes.io/instance-type",
        "topology.kubernetes.io/zone", "topology.kubernetes.io/region",
        "eks.amazonaws.com/nodegroup", "eks.amazonaws.com/capacityType",
        "karpenter.sh/capacity-type", "karpenter.sh/nodepool",
    }),
    "parameters": frozenset({
        "type", "encrypted", "iops", "iopsPerGB", "throughput", "fsType",
        "csi.storage.k8s.io/fstype", "kmsKeyId", "blockExpress", "blockSize",
        "allowAutoIOPSPerGBIncrease", "provisioningMode", "fileSystemId",
        "directoryPerms", "basePath",
        "apiGroup", "kind", "name", "namespace", "scope",
    }),
    "options": frozenset({"name", "namespace"}),
    "volumeAttributes": frozenset({"bucketName"}),
    "urlRewrite": frozenset({"hostname"}),
    "requestRedirect": frozenset({"hostname", "scheme", "statusCode", "port"}),
}
_LAST_APPLIED = "kubectl.kubernetes.io/last-applied-configuration"

# Control-plane bookkeeping on every metadata block (a pod template's too):
# not configuration, and noise in a diff against the declared sources.
_RUNTIME_METADATA = frozenset({
    "managedFields", "uid", "resourceVersion", "selfLink", "creationTimestamp",
    "generation", "ownerReferences", "finalizers", "deletionTimestamp",
    "deletionGracePeriodSeconds",
})

_KEEP, _OMIT = "keep", "omit"


def project(manifest: dict) -> dict:
    """The keys-only projection of one Kubernetes object (never mutated):
    `status` dropped, runtime metadata dropped, every string outside the
    allowlists replaced by OMITTED."""
    return _walk({k: v for k, v in manifest.items() if k != "status"},
                 None, None, 0)


def keys_only(mapping) -> dict:
    """A string map from the AWS side (cluster tags, nodegroup labels,
    subnet tags) reduced to its key names.

    A map only: handed the raw `[{Key, Value}]` list some EC2 calls return,
    each pair would be stringified into a "key" with the value inside it, so
    a list is refused rather than reduced. The caller builds the dict first.
    """
    if mapping is None:
        return {}
    if not isinstance(mapping, dict):
        raise TypeError(f"keys_only takes a map, not {type(mapping).__name__}")
    return {str(key): OMITTED for key in mapping}


def _walk(value, key, mode, depth):
    if depth > MAX_DEPTH:
        return OMITTED
    depth += 1
    if isinstance(value, dict):
        if mode == _OMIT:
            return {k: _walk(v, None, _OMIT, depth) for k, v in value.items()}
        if key in OPAQUE_SUBTREES:
            kept = KEPT_ENTRIES.get(key, ())
            return {k: v if k in kept and not isinstance(v, (dict, list))
                    else _walk(v, None, _OMIT, depth)
                    for k, v in value.items() if k != _LAST_APPLIED}
        if key == "metadata":
            value = {k: v for k, v in value.items()
                     if k not in _RUNTIME_METADATA}
        if key in KEPT_SUBTREES:
            mode = _KEEP
        return {k: _walk(v, k, mode, depth) for k, v in value.items()}
    if isinstance(value, list):
        if key in KEPT_SUBTREES and mode != _OMIT:
            mode = _KEEP
        return [_walk(item, None, mode, depth) for item in value]
    if mode == _OMIT:
        return OMITTED
    if isinstance(value, str):
        return value if mode == _KEEP or key in KEPT_KEYS else OMITTED
    return value
