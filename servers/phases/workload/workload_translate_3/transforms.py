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

"""Deterministic workload transforms.

**Operative at the translate step:** the translate step runs this module as a
server-side post-pass over each unit's gate-passing plain-manifest files
before persistence (tools.transforms_post_pass). Chart/kustomize sources
are out of its reach by design — the translation WORKER owns those rewrites
(the plan brief lists the exports literals to transcribe), the gap is still
recorded on the unit as an open question, and the workload validate step
runs this module detect-only over the rendered output of the shipped
sources: a left-over `change` there is a blocking finding (DESIGN §14
issue 20), never a silent skip.

Pure functions over parsed manifest documents plus exports subsets. Every
function returns (document(s), findings) — swap, strip, images, className
check, Karpenter node placement, in that order; findings are structured dicts
({"transform", "locator", "category", "detail"}) routed into the unit
envelope: category "change" -> tradeoffs (a recorded mechanical edit),
"open_question" -> open_questions, "warning" -> assumptions, and the whole
finding list rides result["transform_findings"] for the validation report.
Missing data is a finding, never an exception, and never a guess: an
unpublished map rewrites nothing, an absent binding invents no email.
"""

import copy

# The explicit pod-spec locations the image rewrite walks — a documented
# closed list, not a blind tree walk. Kinds absent from this table are left
# byte-identical (their images, if any, ride the residual-bucket review).
POD_SPEC_PATHS = {
    "Deployment": ("spec", "template", "spec"),
    "DaemonSet": ("spec", "template", "spec"),
    "StatefulSet": ("spec", "template", "spec"),
    "Job": ("spec", "template", "spec"),
    "CronJob": ("spec", "jobTemplate", "spec", "template", "spec"),
    "Pod": ("spec",),
}
CONTAINER_LIST_KEYS = ("containers", "initContainers", "ephemeralContainers")

ROLE_ARN_ANNOTATION = "eks.amazonaws.com/role-arn"
WI_ANNOTATION = "iam.gke.io/gcp-service-account"

# The explicit minimal strip list: the IRSA companion annotations that lose
# their meaning once role-arn is swapped (reference/api-translation.md maps
# all three to "drop"). Nothing else is ever stripped here — in particular
# alb.ingress.kubernetes.io/* stays (deferred to the wkld-routing family)
# and eks.amazonaws.com/* on documents whose swap did not run stays.
IRSA_COMPANION_ANNOTATIONS = (
    "eks.amazonaws.com/sts-regional-endpoints",
    "eks.amazonaws.com/audience",
    "eks.amazonaws.com/token-expiration",
)


def _locator(doc: dict) -> dict:
    metadata = doc.get("metadata") if isinstance(doc.get("metadata"), dict) else {}
    return {"kind": doc.get("kind"), "namespace": metadata.get("namespace"),
            "name": metadata.get("name")}


def _finding(transform: str, doc: dict, category: str, detail: str) -> dict:
    return {"transform": transform, "locator": _locator(doc),
            "category": category, "detail": detail}


# --- pod DNS fields --------------------------------------------------------
# One reader for the planner's persisted facts (inputs.pod_dns_facts) and the
# validate contract (poddns_contract.py): the facts the worker was briefed on
# and the verdict over its output must be computed over the same fields or
# they drift. Unlike the image walk, this one is NOT bound to POD_SPEC_PATHS:
# a pod template inside a kind the table does not know (an Argo Rollout, an
# operator CR) still carries a pod's DNS fields, and a cluster DNS address
# left in one breaks that pod exactly as it breaks a Deployment's. So every
# mapping in the document that carries one of the keys AND a container list
# is read, at any depth below a cap, skipping the opaque value bags (data,
# stringData, annotations, labels) whose keys are the customer's, not the
# API's. The container list is what makes a mapping a pod spec: a
# PodSecurityPolicy's `spec.hostNetwork`, an admission policy's pattern
# `dnsPolicy: "!None"`, a CRD's `properties.dnsConfig` all carry the key and
# none is a pod.
POD_DNS_KEYS = ("dnsPolicy", "dnsConfig", "hostAliases", "hostNetwork")
_DNS_OPAQUE_KEYS = ("data", "stringData", "binaryData", "annotations", "labels")
_DNS_WALK_DEPTH = 16


def pod_dns_fields(spec) -> dict:
    """The DNS-relevant fields of one pod-spec-like mapping, normalised to
    strings and lists. Absent fields read as None / False / []; a field that
    is not the shape the API defines (a string where a list belongs) reads as
    absent rather than raising, since the schema gate owns malformed output.
    """
    spec = spec if isinstance(spec, dict) else {}
    dns_config = spec.get("dnsConfig") if isinstance(spec.get("dnsConfig"), dict) else {}

    def _strings(value):
        return [str(v) for v in value if v is not None] if isinstance(value, list) else []

    options = []
    for option in dns_config.get("options") if isinstance(dns_config.get("options"), list) else []:
        if isinstance(option, dict) and option.get("name") is not None:
            value = option.get("value")
            options.append({"name": str(option["name"]),
                            "value": None if value is None else str(value)})
    aliases = []
    for alias in spec.get("hostAliases") if isinstance(spec.get("hostAliases"), list) else []:
        if isinstance(alias, dict):
            ip = alias.get("ip")
            aliases.append({"ip": None if ip is None else str(ip),
                            "hostnames": _strings(alias.get("hostnames"))})
    policy = spec.get("dnsPolicy")
    host_network = spec.get("hostNetwork")
    return {
        "dns_policy": str(policy) if isinstance(policy, str) else None,
        # Only the boolean true or the string "true" (a quoted chart value)
        # is host networking; "false" as a string is not.
        "host_network": host_network is True or str(host_network).lower() == "true",
        "nameservers": _strings(dns_config.get("nameservers")),
        "searches": _strings(dns_config.get("searches")),
        "options": options,
        "host_aliases": aliases,
    }


def dns_bearing(fields: dict) -> bool:
    """Does this pod-spec-like mapping say anything about DNS at all? A
    written dnsPolicy counts even when it is the default: it was written."""
    return bool(fields.get("dns_policy") or fields.get("host_network")
                or fields.get("nameservers") or fields.get("searches")
                or fields.get("options") or fields.get("host_aliases"))


def pod_dns_nodes(doc) -> list:
    """[(path, mapping)] for every pod spec in `doc` that carries a pod DNS
    key — a pod spec being a mapping with a container list — in document order. `path` is the dotted key path from the document
    root (`spec.template.spec`, `spec.jobTemplate.spec.template.spec`, list
    indices as numbers) — the persisted facts record it so the validate
    contract finds the SAME pod spec in the shipped document, whether or
    not it still carries a DNS key. Depth-capped; the opaque value bags are
    not descended into."""
    found = []

    def walk(node, path, depth):
        if depth > _DNS_WALK_DEPTH:
            return
        if isinstance(node, dict):
            if any(key in node for key in POD_DNS_KEYS) \
                    and any(isinstance(node.get(k), list) for k in CONTAINER_LIST_KEYS):
                found.append((".".join(path), node))
            for key, value in node.items():
                if str(key) not in _DNS_OPAQUE_KEYS:
                    walk(value, path + [str(key)], depth + 1)
        elif isinstance(node, list):
            for index, value in enumerate(node):
                walk(value, path + [str(index)], depth + 1)

    walk(doc, [], 0)
    return found


def node_at(doc, path: str):
    """The mapping at a dotted path recorded by pod_dns_nodes, or None."""
    node = doc
    for segment in path.split(".") if path else []:
        if isinstance(node, dict):
            node = node.get(segment)
        elif isinstance(node, list) and segment.isdigit() and int(segment) < len(node):
            node = node[int(segment)]
        else:
            return None
    return node if isinstance(node, dict) else None


def _pod_spec(doc: dict):
    """The pod spec mapping at the document kind's documented path, or None."""
    path = POD_SPEC_PATHS.get(str(doc.get("kind")))
    if path is None:
        return None
    node = doc
    for key in path:
        node = node.get(key) if isinstance(node, dict) else None
        if node is None:
            return None
    return node if isinstance(node, dict) else None


def rewrite_image_references(doc: dict, image_map) -> tuple:
    """Rewrites container image refs via the exports image_map. Never guesses.

    image_map: {source ref: {"dest_ref", "status"}} or None (unpublished).
    status "replicated" -> rewrite; any other status (self_service, failed,
    pending) -> untouched + an open question naming the status, because the
    destination does not exist yet; unmapped -> untouched + finding.
    """
    if not isinstance(doc, dict):
        return doc, []
    spec = _pod_spec(doc)
    if spec is None:
        return doc, []
    if image_map is None:
        return doc, [_finding(
            "rewrite_image_references", doc, "open_question",
            "exports artifact_registry/image_map is unpublished; no image "
            "reference was rewritten")]
    new_doc = copy.deepcopy(doc)
    findings = []
    for list_key in CONTAINER_LIST_KEYS:
        for container in _pod_spec(new_doc).get(list_key) or []:
            if isinstance(container, dict):
                findings.extend(_rewrite_container(
                    new_doc, container, image_map))
    return (new_doc if findings else doc), findings


def _rewrite_container(doc: dict, container: dict, image_map: dict) -> list:
    """Applies the map to one container in place. Returns findings."""
    ref = container.get("image")
    if not isinstance(ref, str) or not ref:
        return []
    entry = image_map.get(ref)
    name = container.get("name") or "(unnamed container)"
    if not isinstance(entry, dict):
        return [_finding(
            "rewrite_image_references", doc, "open_question",
            f"container '{name}': image '{ref}' has no image_map entry; "
            "left untouched — a guessed address is never written")]
    dest, status = entry.get("dest_ref"), entry.get("status")
    if not dest:
        return [_finding(
            "rewrite_image_references", doc, "open_question",
            f"container '{name}': image '{ref}' is mapped ({status}) but its "
            "dest_ref is unresolved; left untouched")]
    if status != "replicated":
        # The dest_ref of a non-replicated entry is a PLAN, not an address:
        # nothing is at it yet. Writing it produces a manifest that passes
        # review and then ImagePullBackOffs on deploy, so the rewrite waits
        # for the replication to land (knowledge manual §3-7).
        return [_finding(
            "rewrite_image_references", doc, "open_question",
            f"container '{name}': image '{ref}' maps to '{dest}' but its "
            f"replication status is '{status}', not 'replicated' — nothing "
            "is at the destination yet, so the reference is left untouched. "
            "Finish the replication, re-publish exports, and translate "
            "again")]
    container["image"] = dest
    return [_finding(
        "rewrite_image_references", doc, "change",
        f"container '{name}': '{ref}' -> '{dest}' (status {status})")]


def swap_irsa_annotation(doc: dict, gsa_bindings) -> tuple:
    """IRSA role-arn -> Workload Identity annotation on ServiceAccounts only.

    Looks up "<namespace>/<name>" in exports gsa_bindings. A missing binding
    or a null map leaves the document untouched with a finding naming the
    exact missing key; an email is never invented.
    """
    if not isinstance(doc, dict) or doc.get("kind") != "ServiceAccount":
        return doc, []
    metadata = doc.get("metadata") if isinstance(doc.get("metadata"), dict) else {}
    annotations = metadata.get("annotations") \
        if isinstance(metadata.get("annotations"), dict) else {}
    if ROLE_ARN_ANNOTATION not in annotations:
        return doc, []
    namespace, name = metadata.get("namespace"), metadata.get("name")
    if not namespace or not name:
        # Name the field that is ACTUALLY missing: a document with no
        # metadata.name is a different defect from one with no namespace,
        # and reporting the wrong one sends the reader to the wrong line.
        missing = " and ".join(
            f"metadata.{field}" for field, value
            in (("name", name), ("namespace", namespace)) if not value)
        return doc, [_finding(
            "swap_irsa_annotation", doc, "open_question",
            f"this ServiceAccount records no {missing}; a gsa_bindings key "
            "cannot be resolved without guessing — left untouched")]
    key = f"{namespace}/{name}"
    if not isinstance(gsa_bindings, dict) or key not in gsa_bindings:
        reason = ("exports gsa_bindings is unpublished"
                  if not isinstance(gsa_bindings, dict)
                  else f"exports gsa_bindings has no entry for '{key}'")
        return doc, [_finding(
            "swap_irsa_annotation", doc, "open_question",
            f"{reason}; the IRSA annotation is left untouched — an email is "
            "never invented")]
    new_doc = copy.deepcopy(doc)
    new_annotations = new_doc["metadata"]["annotations"]
    del new_annotations[ROLE_ARN_ANNOTATION]
    new_annotations[WI_ANNOTATION] = gsa_bindings[key]
    return new_doc, [_finding(
        "swap_irsa_annotation", doc, "change",
        f"'{key}': {ROLE_ARN_ANNOTATION} replaced with {WI_ANNOTATION}: "
        f"{gsa_bindings[key]}")]


def check_storage_class_references(doc: dict, storage_class_menu) -> tuple:
    """Read-only className existence check against the exports menu.

    Covers PVC spec.storageClassName and StatefulSet volumeClaimTemplates.
    Menu member -> ok finding; absent -> warning finding; menu null -> one
    "menu unpublished, check skipped" finding and no per-claim verdicts.
    Never mutates the document.
    """
    if not isinstance(doc, dict):
        return doc, []
    refs = []
    if doc.get("kind") == "PersistentVolumeClaim":
        refs.append(("claim", (doc.get("spec") or {}).get("storageClassName")))
    elif doc.get("kind") == "StatefulSet":
        for template in (doc.get("spec") or {}).get("volumeClaimTemplates") or []:
            if isinstance(template, dict):
                refs.append((
                    (template.get("metadata") or {}).get("name") or "(unnamed)",
                    (template.get("spec") or {}).get("storageClassName")))
    refs = [(label, name) for label, name in refs if name]
    if not refs:
        return doc, []
    if not isinstance(storage_class_menu, list):
        return doc, [_finding(
            "check_storage_class_references", doc, "open_question",
            "exports storage_class_menu is unpublished; the className "
            "existence check was skipped")]
    # exports is customer-shaped data: a menu entry may be a non-string (an
    # int-looking class name, a null from a half-filled export). str() every
    # member so the report renders instead of raising mid-pass.
    shown = [str(entry) for entry in storage_class_menu]
    findings = []
    for label, name in refs:
        if name in storage_class_menu:
            findings.append(_finding(
                "check_storage_class_references", doc, "ok",
                f"{label}: storageClassName '{name}' is in the exports menu"))
        else:
            findings.append(_finding(
                "check_storage_class_references", doc, "warning",
                f"{label}: storageClassName '{name}' is ABSENT from the "
                f"exports menu ({', '.join(shown) or 'empty'})"))
    return doc, findings


def strip_aws_annotations(doc: dict) -> tuple:
    """Strips the IRSA companion annotations, ONLY after a successful swap.

    "Swap succeeded in this pass" is decided from the document itself (the
    deterministic proxy, documented): kind ServiceAccount, WI_ANNOTATION
    present, ROLE_ARN_ANNOTATION absent — exactly the post-swap shape.
    Everything else is left untouched: never strip blindly.
    """
    if not isinstance(doc, dict) or doc.get("kind") != "ServiceAccount":
        return doc, []
    metadata = doc.get("metadata") if isinstance(doc.get("metadata"), dict) else {}
    annotations = metadata.get("annotations") \
        if isinstance(metadata.get("annotations"), dict) else {}
    if WI_ANNOTATION not in annotations or ROLE_ARN_ANNOTATION in annotations:
        return doc, []
    present = [k for k in IRSA_COMPANION_ANNOTATIONS if k in annotations]
    if not present:
        return doc, []
    new_doc = copy.deepcopy(doc)
    findings = []
    for key in present:
        del new_doc["metadata"]["annotations"][key]
        findings.append(_finding(
            "strip_aws_annotations", doc, "change",
            f"removed '{key}' (IRSA companion; meaningless after the "
            "role-arn swap — reference/api-translation.md maps it to drop)"))
    return new_doc, findings


# --- Karpenter node placement -> GKE ComputeClass / spot keys ---------------

COMPUTE_CLASS_LABEL = "cloud.google.com/compute-class"
GKE_SPOT_LABEL = "cloud.google.com/gke-spot"
KARPENTER_POOL_KEYS = ("karpenter.sh/nodepool", "karpenter.sh/provisioner-name")
KARPENTER_CAPACITY_KEY = "karpenter.sh/capacity-type"
INSTANCE_TYPE_KEY = "node.kubernetes.io/instance-type"
KARPENTER_TOLERATION_PREFIX = "karpenter.sh/"
# Keys the transform reads (the workload construct's detect literals); the
# portable kubernetes.io/arch is deliberately not here.
KARPENTER_PLACEMENT_KEYS = KARPENTER_POOL_KEYS + (KARPENTER_CAPACITY_KEY, INSTANCE_TYPE_KEY)
_TRANSFORM = "rewrite_karpenter_node_placement"
# An AWS instance type looks like m6i.4xlarge; a GCE shape like n4-standard-16.
_AWS_INSTANCE_TYPE_RE = None


def _is_aws_instance_type(value) -> bool:
    global _AWS_INSTANCE_TYPE_RE
    if _AWS_INSTANCE_TYPE_RE is None:
        import re
        _AWS_INSTANCE_TYPE_RE = re.compile(r"^[a-z][a-z0-9-]*\.[a-z0-9]+$")
    return isinstance(value, str) and bool(_AWS_INSTANCE_TYPE_RE.match(value))


def _menu_question(name: str, compute_classes, translation_generation) -> str:
    if compute_classes is None:
        if not translation_generation:
            return (f"the Karpenter pool selector '{name}': exports compute_classes is "
                    "unpublished (the platform translation has not published yet); left "
                    "untouched — translate again after it publishes")
        return (f"'{name}': exports compute_classes is null although the translation slice "
                "has published: the slice published before compute_classes existed, or "
                "compute-class units are still pending; left untouched — run refresh_exports "
                "once they are done and translate again")
    shown = ", ".join(str(c) for c in compute_classes) or "empty"
    return (f"'{name}' names no published ComputeClass (exports compute_classes: {shown}); "
            "left untouched — under a NAP or Autopilot landing zone map the selector to "
            "the target node pool label or drop it, never invent a class name")


def _collides(doc, selector: dict, target_key: str, value, findings: list, source_key: str) -> bool:
    """A half-migrated document already carries the GKE key with another
    value: rewriting would silently overwrite one of the two. Leave both and
    ask."""
    if target_key in selector and selector[target_key] != value:
        findings.append(_finding(
            _TRANSFORM, doc, "open_question",
            f"nodeSelector carries both {source_key}: {value} and {target_key}: "
            f"{selector[target_key]}; the two disagree, so neither was rewritten — decide "
            "which one stands"))
        return True
    return False


def _rewrite_selector_map(doc, selector: dict, compute_classes, translation_generation,
                          findings: list, out_affinity: list) -> dict:
    """Rewrites one nodeSelector map. Returns the new map (may be empty)."""
    new = {}
    for key, value in selector.items():
        if key in KARPENTER_POOL_KEYS:
            if isinstance(compute_classes, list) and value in compute_classes:
                # Collisions against the source map AND against what this pass
                # already wrote (two pool keys in one selector).
                if _collides(doc, selector, COMPUTE_CLASS_LABEL, value, findings, key) \
                        or _collides(doc, new, COMPUTE_CLASS_LABEL, value, findings, key):
                    new[key] = value
                    continue
                new[COMPUTE_CLASS_LABEL] = value
                findings.append(_finding(_TRANSFORM, doc, "change",
                                         f"nodeSelector {key}: {value} -> {COMPUTE_CLASS_LABEL}: {value}"))
            else:
                new[key] = value
                findings.append(_finding(_TRANSFORM, doc, "open_question",
                                         _menu_question(str(value), compute_classes, translation_generation)))
        elif key == KARPENTER_CAPACITY_KEY:
            if value == "spot":
                if _collides(doc, selector, GKE_SPOT_LABEL, "true", findings, key):
                    new[key] = value
                    continue
                new[GKE_SPOT_LABEL] = "true"
                findings.append(_finding(_TRANSFORM, doc, "change",
                                         f"nodeSelector {key}: spot -> {GKE_SPOT_LABEL}: \"true\""))
            elif value == "on-demand":
                if GKE_SPOT_LABEL in selector:
                    new[key] = value
                    findings.append(_finding(
                        _TRANSFORM, doc, "open_question",
                        f"nodeSelector carries {key}: on-demand beside {GKE_SPOT_LABEL}: "
                        f"{selector[GKE_SPOT_LABEL]}; the two contradict, so neither was "
                        "rewritten — decide which one stands"))
                    continue
                out_affinity.append(True)
                findings.append(_finding(
                    _TRANSFORM, doc, "change",
                    f"nodeSelector {key}: on-demand removed; a required nodeAffinity "
                    f"{GKE_SPOT_LABEL} DoesNotExist keeps the pod off spot nodes (GKE labels "
                    "spot nodes only, so a false-valued selector would match nothing)"))
            else:
                new[key] = value
                findings.append(_finding(_TRANSFORM, doc, "open_question",
                                         f"nodeSelector {key}: {value!r} is neither spot nor on-demand; "
                                         "left untouched"))
        elif key == INSTANCE_TYPE_KEY and _is_aws_instance_type(value):
            new[key] = value
            findings.append(_finding(
                _TRANSFORM, doc, "open_question",
                f"nodeSelector {key}: {value} is an AWS instance type with no deterministic "
                "GCE shape; left untouched — select the ComputeClass and let its priorities "
                "choose the shape, or name the GCE machine type yourself"))
        else:
            new[key] = value
    return new


_OD_EXPR = {"key": GKE_SPOT_LABEL, "operator": "DoesNotExist"}


def _rewrite_expressions(doc, expressions: list, compute_classes, translation_generation,
                         findings: list, out_affinity: list) -> list:
    """Rewrites one matchExpressions list under a required term. Returns the
    new list (entries may be dropped). An on-demand expression is replaced IN
    PLACE by the DoesNotExist expression: terms are OR'd alternatives, so
    adding it to every term would narrow the others."""
    new = []

    def present_with(target_key, wanted_values, wanted_operator="In"):
        # A term already carrying the GKE key with another meaning: rewriting
        # would AND two contradicting expressions into one term.
        for other in expressions:
            if not isinstance(other, dict) or other.get("key") != target_key:
                continue
            if other.get("operator") != wanted_operator or list(other.get("values") or []) != wanted_values:
                return other
        return None

    for expr in expressions:
        if not isinstance(expr, dict):
            new.append(expr)
            continue
        key, operator, values = expr.get("key"), expr.get("operator"), expr.get("values")
        if key not in KARPENTER_PLACEMENT_KEYS:
            new.append(expr)
            continue
        single = operator == "In" and isinstance(values, list) and len(values) == 1
        if not single:
            new.append(expr)
            findings.append(_finding(
                _TRANSFORM, doc, "open_question",
                f"nodeAffinity {key} {operator} {values!r}: only `In` with one value is "
                "rewritten deterministically; left untouched"))
            continue
        value = values[0]
        if key in KARPENTER_POOL_KEYS:
            clash = present_with(COMPUTE_CLASS_LABEL, [value])
            if clash is not None:
                new.append(expr)
                findings.append(_finding(
                    _TRANSFORM, doc, "open_question",
                    f"nodeAffinity term carries both {key} In [{value}] and {COMPUTE_CLASS_LABEL} "
                    f"{clash.get('operator')} {clash.get('values')!r}; the two disagree, so "
                    "neither was rewritten — decide which one stands"))
            elif isinstance(compute_classes, list) and value in compute_classes:
                new.append({**expr, "key": COMPUTE_CLASS_LABEL})
                findings.append(_finding(_TRANSFORM, doc, "change",
                                         f"nodeAffinity {key} In [{value}] -> {COMPUTE_CLASS_LABEL}"))
            else:
                new.append(expr)
                findings.append(_finding(_TRANSFORM, doc, "open_question",
                                         _menu_question(str(value), compute_classes, translation_generation)))
        elif key == KARPENTER_CAPACITY_KEY:
            spot_clash = present_with(GKE_SPOT_LABEL, ["true"]) if value == "spot" else \
                (present_with(GKE_SPOT_LABEL, [], "DoesNotExist") if value == "on-demand" else None)
            if value in ("spot", "on-demand") and spot_clash is not None:
                new.append(expr)
                findings.append(_finding(
                    _TRANSFORM, doc, "open_question",
                    f"nodeAffinity term carries both {key} In [{value}] and {GKE_SPOT_LABEL} "
                    f"{spot_clash.get('operator')} {spot_clash.get('values')!r}; the two "
                    "contradict, so neither was rewritten — decide which one stands"))
            elif value == "spot":
                new.append({**expr, "key": GKE_SPOT_LABEL, "values": ["true"]})
                findings.append(_finding(_TRANSFORM, doc, "change",
                                         f"nodeAffinity {key} In [spot] -> {GKE_SPOT_LABEL} In [\"true\"]"))
            elif value == "on-demand":
                if _OD_EXPR not in new and _OD_EXPR not in expressions:
                    new.append(dict(_OD_EXPR))
                findings.append(_finding(
                    _TRANSFORM, doc, "change",
                    f"nodeAffinity {key} In [on-demand] -> {GKE_SPOT_LABEL} DoesNotExist "
                    "(replaced in place, in the same term)"))
            else:
                new.append(expr)
                findings.append(_finding(_TRANSFORM, doc, "open_question",
                                         f"nodeAffinity {key} In [{value!r}] is neither spot nor "
                                         "on-demand; left untouched"))
        else:  # instance type
            new.append(expr)
            if _is_aws_instance_type(value):
                findings.append(_finding(
                    _TRANSFORM, doc, "open_question",
                    f"nodeAffinity {key} In [{value}] is an AWS instance type with no "
                    "deterministic GCE shape; left untouched"))
    return new


def _names_karpenter_key(node) -> bool:
    """True when a preferred term, a pod (anti)affinity term or a spread
    constraint mentions a Karpenter key (as `key` or `topologyKey`)."""
    if isinstance(node, dict):
        if node.get("key") in KARPENTER_PLACEMENT_KEYS \
                or node.get("topologyKey") in KARPENTER_PLACEMENT_KEYS:
            return True
        return any(_names_karpenter_key(v) for v in node.values())
    if isinstance(node, list):
        return any(_names_karpenter_key(v) for v in node)
    return False


def rewrite_karpenter_node_placement(doc: dict, compute_classes,
                                     translation_generation=None) -> tuple:
    """Karpenter placement keys -> GKE keys, over the pod spec.

    compute_classes: exports compute_classes — the published ComputeClass
    names, [] when the validated landing zone ships none, None when the
    translation slice has not published the field. translation_generation
    is exports.generations.translation, used only to word the None case.
    Deterministic and idempotent: a document already carrying the GKE keys
    yields no finding; nothing is invented (a name not in the menu is an
    open question, never a guess). Rows, per the design table: the pool
    key swaps to cloud.google.com/compute-class when the menu lists the
    name; capacity-type spot becomes gke-spot "true"; capacity-type
    on-demand becomes a required nodeAffinity gke-spot DoesNotExist (the
    label exists on spot nodes only); an AWS instance type, any other
    operator, a preferred term or a topology spread naming a Karpenter key
    is an open question; a karpenter.sh/* toleration is removed.
    """
    if not isinstance(doc, dict):
        return doc, []
    spec = _pod_spec(doc)
    if spec is None:
        return doc, []
    findings = []
    new_doc = copy.deepcopy(doc)
    new_spec = _pod_spec(new_doc)
    need_od_affinity = []

    selector = new_spec.get("nodeSelector")
    if isinstance(selector, dict) and selector:
        rewritten = _rewrite_selector_map(doc, selector, compute_classes, translation_generation,
                                          findings, need_od_affinity)
        if rewritten:
            new_spec["nodeSelector"] = rewritten
        else:
            del new_spec["nodeSelector"]

    affinity = new_spec.get("affinity") if isinstance(new_spec.get("affinity"), dict) else None
    node_affinity = affinity.get("nodeAffinity") if affinity and isinstance(affinity.get("nodeAffinity"), dict) else None
    if node_affinity is not None:
        required = node_affinity.get("requiredDuringSchedulingIgnoredDuringExecution")
        if isinstance(required, dict):
            terms = []
            for term in required.get("nodeSelectorTerms") or []:
                if not isinstance(term, dict):
                    terms.append(term)
                    continue
                expressions = term.get("matchExpressions")
                emptied = False
                if isinstance(expressions, list):
                    kept = _rewrite_expressions(doc, expressions, compute_classes,
                                                translation_generation, findings, need_od_affinity)
                    term = dict(term)
                    if kept or not expressions:
                        term["matchExpressions"] = kept
                    else:
                        del term["matchExpressions"]
                        emptied = True
                # Structure rule: a term THIS PASS emptied matches no node; drop
                # it. A term that arrived empty is the document's own business.
                if emptied and not term.get("matchFields"):
                    continue
                terms.append(term)
            if terms:
                required = dict(required, nodeSelectorTerms=terms)
                node_affinity["requiredDuringSchedulingIgnoredDuringExecution"] = required
            else:
                del node_affinity["requiredDuringSchedulingIgnoredDuringExecution"]
        preferred = node_affinity.get("preferredDuringSchedulingIgnoredDuringExecution")
        if _names_karpenter_key(preferred):
            findings.append(_finding(
                _TRANSFORM, doc, "open_question",
                "a preferred nodeAffinity term names a Karpenter key; preferences are not "
                "rewritten deterministically — left untouched, decide the GKE equivalent"))
    if _names_karpenter_key(new_spec.get("topologySpreadConstraints")):
        findings.append(_finding(
            _TRANSFORM, doc, "open_question",
            "a topologySpreadConstraints entry names a Karpenter key; left untouched, "
            "decide the GKE topology key"))
    if affinity and any(_names_karpenter_key(affinity.get(k))
                        for k in ("podAffinity", "podAntiAffinity")):
        findings.append(_finding(
            _TRANSFORM, doc, "open_question",
            "a podAffinity/podAntiAffinity term names a Karpenter key as its topologyKey; "
            "left untouched, decide the GKE topology key"))

    if need_od_affinity:
        # nodeSelector origin only: a nodeSelector ANDs with every term, so the
        # constraint joins every term (or creates the one term).
        expr = dict(_OD_EXPR)
        affinity = new_spec.setdefault("affinity", {})
        node_affinity = affinity.setdefault("nodeAffinity", {})
        required = node_affinity.setdefault("requiredDuringSchedulingIgnoredDuringExecution", {})
        terms = required.setdefault("nodeSelectorTerms", [])
        if not terms:
            terms.append({"matchExpressions": [expr]})
        else:
            for term in terms:
                if isinstance(term, dict):
                    exprs = term.setdefault("matchExpressions", [])
                    if expr not in exprs:
                        exprs.append(expr)

    tolerations = new_spec.get("tolerations")
    if isinstance(tolerations, list):
        kept = []
        for toleration in tolerations:
            key = toleration.get("key") if isinstance(toleration, dict) else None
            if isinstance(key, str) and key.startswith(KARPENTER_TOLERATION_PREFIX):
                findings.append(_finding(
                    _TRANSFORM, doc, "change",
                    f"toleration {key} removed: no GKE equivalent (reference/api-translation.md)"))
                continue
            kept.append(toleration)
        if kept:
            new_spec["tolerations"] = kept
        else:
            del new_spec["tolerations"]

    # Structure rule: prune containers this pass emptied so no empty affinity
    # block ships (a block that arrived empty was not touched above).
    affinity = new_spec.get("affinity")
    if isinstance(affinity, dict) and any(f["category"] == "change" for f in findings):
        na = affinity.get("nodeAffinity")
        if isinstance(na, dict) and not na:
            del affinity["nodeAffinity"]
        if not affinity:
            del new_spec["affinity"]

    changed = any(f["category"] == "change" for f in findings)
    return (new_doc if changed else doc), findings


def apply_transforms(docs: list, exports) -> tuple:
    """Runs the deterministic pass over a document list in the fixed,
    documented order: swap -> strip -> images -> className check -> Karpenter
    node placement. Returns (new document list, concatenated findings).
    Documents the transforms do not know are returned unchanged; the pass is
    idempotent on documents."""
    registry = (exports or {}).get("artifact_registry")
    image_map = registry.get("image_map") if isinstance(registry, dict) else None
    gsa_bindings = (exports or {}).get("gsa_bindings")
    menu = (exports or {}).get("storage_class_menu")
    compute_classes = (exports or {}).get("compute_classes")
    generations = (exports or {}).get("generations")
    translation_generation = generations.get("translation") if isinstance(generations, dict) else None
    out, findings = [], []
    for doc in docs or []:
        doc, swap_findings = swap_irsa_annotation(doc, gsa_bindings)
        doc, strip_findings = strip_aws_annotations(doc)
        doc, image_findings = rewrite_image_references(doc, image_map)
        doc, class_findings = check_storage_class_references(doc, menu)
        doc, placement_findings = rewrite_karpenter_node_placement(
            doc, compute_classes, translation_generation)
        out.append(doc)
        findings.extend(swap_findings + strip_findings + image_findings
                        + class_findings + placement_findings)
    return out, findings
