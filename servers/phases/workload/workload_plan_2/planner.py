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

"""Pure planning core of the workload pipeline (STATE_WKLD_PLAN).

Deterministic decomposition of a component's scoped source files into the
four workload unit families: wkld-manifests,
wkld-identity, wkld-storage, wkld-routing. Pure code, no LLM, no GCS: same
files + same exports document -> byte-identical plan (no wall clocks; the
stamped exports generated_at is consumed data). Local subprocess renders
(helm template / kubectl kustomize) live in dedicated functions that callers
inject around, so classification and brief logic unit-test without the
binaries.

The family briefs (unit notes) follow the landing-zone planner's
_storage_notes grammar: pure derivation, literally-true claims conditioned
on what the reachable inputs record, invent-nothing routing to
assumptions/open_questions. Their source material is the English manual copy
at servers/phases/workload/knowledge/acme-workload-translation-manual.md.
"""

import hashlib
import json
import os
import re

from servers.dag.server import api_translation as _api_translation
from servers.dag.server import exports as _exports_base
from servers.phases import k8s_manifests, scope_algebra
from servers.phases.workload.workload_scope_1.seed import CLUSTER_SCOPED_KINDS

FAMILIES = ("wkld-manifests", "wkld-identity", "wkld-storage", "wkld-routing")

# The closed (apiVersion group, kind) -> family table.
# Classification is this table, not judgment: any pair absent from it lands
# in the residual bucket (wkld-manifests, aws_coupled_or_unknown). The group
# is half the key on purpose — a CRD may reuse a core kind name
# (serving.knative.dev/v1 Service, an operator's own Ingress), and such a
# document is NOT the portable core object it is named after.
KIND_TO_FAMILY = {
    ("apps", "Deployment"): "wkld-manifests",
    ("apps", "DaemonSet"): "wkld-manifests",
    ("apps", "StatefulSet"): "wkld-manifests",
    ("", "Pod"): "wkld-manifests",
    ("", "Service"): "wkld-manifests",
    ("", "ConfigMap"): "wkld-manifests",
    ("autoscaling", "HorizontalPodAutoscaler"): "wkld-manifests",
    ("policy", "PodDisruptionBudget"): "wkld-manifests",
    ("batch", "Job"): "wkld-manifests",
    ("batch", "CronJob"): "wkld-manifests",
    ("", "ServiceAccount"): "wkld-identity",
    ("", "PersistentVolumeClaim"): "wkld-storage",
    ("networking.k8s.io", "Ingress"): "wkld-routing",
}

# Cluster-scoped kinds that are NOT in the seed's platform-owned set. Both
# lists drive the leak note, with different advice: the seed's members have a
# coverage-map row (amend the scope), these are cluster-wide objects whose
# ownership the developer must confirm before they ship.
CLUSTER_SCOPED_OTHER = {
    "APIService", "CSIDriver", "ClusterIssuer", "ClusterPolicy",
    "ClusterRole", "ClusterRoleBinding", "ClusterSecretStore",
    "CompositeResourceDefinition", "GatewayClass", "IngressClass",
    "MutatingWebhookConfiguration", "PersistentVolume", "PodSecurityPolicy",
    "PriorityClass", "RuntimeClass", "ValidatingWebhookConfiguration",
    "VolumeSnapshotClass",
}

TITLES = {
    "wkld-manifests": "Workload manifests (portable K8s objects + residual bucket)",
    "wkld-identity": "ServiceAccounts: IRSA -> Workload Identity (KSA half)",
    "wkld-storage": "PVC storage class usage (claims only; no data movement)",
    "wkld-routing": "Ingress -> HTTPRoute routing (attaches to the shared platform Gateway)",
}

# A parked routing unit must not advertise the attach point it is parked FOR:
# the default title asserts "attaches to the shared platform Gateway" in
# exactly the case where no Gateway is exported yet, contradicting the unit's
# own status in the same plan row.
PARKED_TITLES = {
    "wkld-routing": "Ingress -> HTTPRoute routing (parked: no exports.gateway yet)",
}


def _title(family: str, status: str) -> str:
    if status == "parked" and family in PARKED_TITLES:
        return PARKED_TITLES[family]
    return TITLES[family]

KUSTOMIZATION_NAMES = ("kustomization.yaml", "kustomization.yml", "Kustomization")

# Vendored/generated trees never contain component-owned manifests; skipping
# them mirrors the discovery scan's rule.
SKIP_DIRS = {".git", ".terraform", "node_modules", "vendor"}

RENDER_TIMEOUT_S = 120


# --- enumeration and partition --------------------------------------------


def enumerate_scoped_files(source_root: str, scope: dict) -> tuple:
    """(paths in scope, paths an `excluded` pattern removed, notes). Sorted.

    Walks the developer's local clone for .yaml/.yml files (plus the
    extensionless 'Kustomization' marker) and applies the component scope
    algebra: in scope = matches >=1 `included` pattern AND no `excluded` one
    (the select-from-nothing inversion, as seed.resolve_component_scope).
    Symlinks are never followed, and a file whose real path escapes the
    source root is skipped with a note — customer IaC is untrusted input.

    The carved-out paths come back separately because a render source is
    handed to helm/kubectl as a DIRECTORY: an exclusion inside one cannot be
    honoured, and the plan has to say so rather than claim it was.
    """
    real_root = os.path.realpath(source_root)
    included = (scope or {}).get("included") or []
    excluded = (scope or {}).get("excluded") or []
    paths, carved, notes = [], [], []
    for dirpath, dirnames, filenames in os.walk(source_root, followlinks=False):
        dirnames[:] = sorted(d for d in dirnames if d not in SKIP_DIRS)
        for filename in sorted(filenames):
            if not (filename.endswith((".yaml", ".yml"))
                    or filename in KUSTOMIZATION_NAMES):
                continue
            full = os.path.join(dirpath, filename)
            rel = os.path.relpath(full, source_root).replace(os.sep, "/")
            if rel.startswith(".."):
                continue
            if not any(scope_algebra.matches(rel, inc) for inc in included):
                continue
            real = os.path.realpath(full)
            if real != full and not real.startswith(real_root + os.sep):
                notes.append(f"{rel}: symlink escapes the source root; skipped")
                continue
            if any(scope_algebra.matches(rel, exc) for exc in excluded):
                carved.append(rel)
                continue
            paths.append(rel)
    return sorted(paths), sorted(carved), notes


def source_label(rel_dir: str) -> str:
    """The recorded and rendered form of a source directory.

    A repo whose root IS the chart or kustomization (the single-chart layout)
    enumerates that source as ""; `helm template <release> ""` and an empty
    plan field are both wrong, so the root is recorded and rendered as ".".
    """
    return rel_dir or "."


def _under(rel: str, source_dir: str) -> bool:
    """Is `rel` the source dir itself, or a path inside it? "" is the root."""
    return (source_dir == "" or rel == source_dir
            or rel.startswith(source_dir + "/"))


def _nested_sources(source_dirs: set) -> set:
    """Source dirs sitting inside another source dir. A chart under a chart
    is a vendored subchart the parent's render already emits; a kustomize dir
    under another is a base the parent already pulls in. Rendering both emits
    the same documents twice — and the chart carrier stops shipping ONCE."""
    return {d for d in source_dirs
            if any(other != d and _under(d, other) for other in source_dirs)}


def partition_sources(paths: list) -> dict:
    """Splits scoped paths into chart dirs, kustomize dirs and plain files.

    A directory whose Chart.yaml is in scope is a Helm chart source; one
    whose kustomization marker is in scope is a kustomize source. Files under
    either are consumed via render, never fed raw to the loader. A directory
    carrying both markers is treated as a chart (Chart.yaml wins,
    deterministically). NESTED sources are dropped from the render list and
    reported in `nested`: their files stay members of the outermost enclosing
    source, which is what actually renders them. Membership goes to the
    longest matching source dir. Returns {"charts", "kustomize", "plain",
    "nested", "members"}, all sorted.
    """
    charts, kustomize = set(), set()
    for rel in paths:
        parent, name = os.path.split(rel)
        if name == "Chart.yaml":
            charts.add(parent)
        elif name in KUSTOMIZATION_NAMES:
            kustomize.add(parent)
    kustomize -= charts
    nested = _nested_sources(charts | kustomize)
    charts, kustomize = charts - nested, kustomize - nested
    source_dirs = sorted(charts | kustomize, key=len, reverse=True)
    plain, members = [], {d: [] for d in source_dirs}
    for rel in paths:
        owner = next((d for d in source_dirs if _under(rel, d)), None)
        (plain if owner is None else members[owner]).append(rel)
    return {"charts": sorted(charts), "kustomize": sorted(kustomize),
            "plain": sorted(plain), "nested": sorted(nested),
            "members": {d: sorted(v) for d, v in members.items()}}


def _digest_dir(source_root: str, rel_dir: str) -> str:
    """sha256 over the directory's files, sorted by path (chart values digest
    / kustomize input digest). Raw bytes; vendored trees skipped."""
    digest = hashlib.sha256()
    base = os.path.join(source_root, rel_dir) if rel_dir else source_root
    for dirpath, dirnames, filenames in os.walk(base, followlinks=False):
        dirnames[:] = sorted(d for d in dirnames if d not in SKIP_DIRS)
        for filename in sorted(filenames):
            full = os.path.join(dirpath, filename)
            rel = os.path.relpath(full, base).replace(os.sep, "/")
            if os.path.islink(full) or not os.path.isfile(full):
                continue
            digest.update(rel.encode("utf-8") + b"\0")
            with open(full, "rb") as f:
                digest.update(f.read())
            digest.update(b"\0")
    return digest.hexdigest()


def _digest_graph(source_root: str, rel_dirs: list) -> str:
    """sha256 over EVERY directory a kustomization graph reaches, sorted.

    The overlay directory alone is not the render's input: `kubectl
    kustomize` reads ../base and its addons too, so an overlay-only digest
    cannot detect drift in most of what it actually renders — and this field
    is the plan's re-render pin.
    """
    digest = hashlib.sha256()
    for rel_dir in sorted(rel_dirs):
        digest.update(source_label(rel_dir).encode("utf-8") + b"\0")
        digest.update(_digest_dir(source_root, rel_dir).encode("utf-8") + b"\0")
    return digest.hexdigest()


# --- deterministic local renders (injectable) ------------------------------


def _run_render(cmd: list, cwd: str) -> tuple:
    """(stdout, error). Local subprocess, bounded; a failure is a string,
    never an exception — render problems degrade into plan notes."""
    import subprocess
    try:
        res = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True,
                             check=True, timeout=RENDER_TIMEOUT_S)
    except FileNotFoundError:
        return None, f"{cmd[0]} binary not found on this workstation"
    except subprocess.CalledProcessError as e:
        return None, (e.stderr or e.stdout or "").strip()[:500] or f"{cmd[0]} failed"
    except subprocess.TimeoutExpired:
        return None, f"timed out after {RENDER_TIMEOUT_S}s"
    return res.stdout, None


def helm_release_name(chart_dir: str) -> str:
    """The fixed `helm template` release name for a SOURCE chart path: its
    basename slug, `wkld-render` for a repo whose root is the chart. One
    function for every render of that chart — the plan, the translate-time
    re-render and the validate re-render of the materialized copy — so a
    `{{ .Release.Name }}`-derived name is the same name in every stream and
    the validate contracts can match documents the plan facted."""
    release = re.sub(r"[^a-z0-9-]+", "-", os.path.basename(chart_dir or "").lower())
    return release.strip("-") or "wkld-render"


def helm_template(source_root: str, chart_dir: str) -> tuple:
    """(rendered stream, error). `helm template` with the chart's in-repo
    values only — no --set, no external values files.
    `--include-crds` because a chart's `crds/` directory is otherwise not
    emitted at all: an in-scope CustomResourceDefinition would vanish from
    the plan, taking its cluster-scope warning with it. The release name is
    helm_release_name(chart_dir), fixed so the render is deterministic."""
    release = helm_release_name(chart_dir)
    return _run_render(
        ["helm", "template", release, chart_dir, "--include-crds"], source_root)


def kubectl_kustomize(source_root: str, rel_dir: str) -> tuple:
    """(rendered stream, error). `kubectl kustomize` over the directory;
    callers pre-scan for remote references and never reach this for them."""
    return _run_render(["kubectl", "kustomize", rel_dir], source_root)


# Kustomization fields whose entries name another source of documents. Each
# is a graph edge: a directory with its own kustomization, or a resource file.
KUSTOMIZE_REF_KEYS = ("resources", "bases", "components", "crds",
                      "configurations", "transformers", "generators")

# The remote forms kustomize accepts. Tested only AFTER local resolution has
# failed, so a local directory whose name happens to look like a host is not
# refused — and so a form nobody thought of still refuses, because it will
# not resolve locally either.
_REMOTE_ENTRY_RE = re.compile(
    r"^(?:[A-Za-z][A-Za-z0-9+.-]*://"       # https://, ssh://, git://
    r"|[A-Za-z][A-Za-z0-9+.-]*::"           # git::, hg::, s3:: getter prefixes
    r"|git@"                                # scp-style scm shorthand
    r"|[\w.-]+\.[A-Za-z]{2,}/)")            # github.com/org/repo//base?ref=v1


def _kustomization_marker(source_root: str, rel_dir: str):
    """The kustomization file name in rel_dir, or None."""
    for name in KUSTOMIZATION_NAMES:
        if os.path.isfile(os.path.join(source_root, rel_dir, name)):
            return name
    return None


def _load_kustomization(source_root: str, rel_dir: str, name: str) -> tuple:
    """(documents, error) under the shared hardened posture.

    A parse refusal is an ERROR, never an empty entry list: kubectl's parser
    is strictly more permissive than ours (aliases, size), so returning "no
    remote refs found" for a file we could not read would let the render
    proceed and fetch. This is the one place the hardened posture must fail
    closed — the downstream consumer does not share it.
    """
    try:
        with open(os.path.join(source_root, rel_dir, name), "r",
                  encoding="utf-8") as f:
            docs = k8s_manifests.load_manifest_documents(f.read())
        return [d for d in docs if isinstance(d, dict)], None
    except (OSError, UnicodeDecodeError, ValueError) as e:
        return None, str(e)


def _resolve_local(source_root: str, rel_dir: str, entry: str):
    """The entry as a path relative to the source root, or None when it does
    not resolve to an existing path INSIDE it (remote, absent, or an escape).
    Containment is checked on the real path: customer IaC is untrusted."""
    real_root = os.path.realpath(source_root)
    candidate = os.path.normpath(os.path.join(source_root, rel_dir, entry))
    if not os.path.exists(candidate):
        return None
    real = os.path.realpath(candidate)
    if real != real_root and not real.startswith(real_root + os.sep):
        return None
    rel = os.path.relpath(candidate, source_root).replace(os.sep, "/")
    return None if rel.startswith("..") else ("" if rel == "." else rel)


# --- parsing and classification --------------------------------------------


def api_group(api_version) -> str:
    """"apps/v1" -> "apps"; "v1" -> "" (the core group); absent -> ""."""
    text = str(api_version or "")
    return text.split("/")[0] if "/" in text else ""


def _ref_entries(docs: list) -> list:
    """The graph-edge strings of a kustomization, in document order."""
    return [entry.strip() for doc in docs for key in KUSTOMIZE_REF_KEYS
            for entry in _sequence(doc.get(key))
            if isinstance(entry, str) and entry.strip()]


def _scalars(node, depth: int = 0) -> list:
    """Every string scalar in a parsed kustomization, bounded."""
    if depth > 8 or isinstance(node, str):
        return [node] if isinstance(node, str) and node.strip() else []
    if isinstance(node, dict):
        return [s for v in node.values() for s in _scalars(v, depth + 1)]
    if isinstance(node, list):
        return [s for v in node for s in _scalars(v, depth + 1)]
    return []


def resolve_kustomize_graph(source_root: str, rel_dir: str) -> dict:
    """The transitive LOCAL closure of a kustomization, breadth-first.

    `kubectl kustomize` resolves resources/bases/components recursively, so a
    directory whose own kustomization is purely local still fetches whatever
    a base of a base names. Scanning only `rel_dir` called that safe and
    kubectl then went to the network — decision 8 says nothing is EVER
    fetched, so the whole reachable graph is what has to be clean.

    Returns {"dirs": kustomization dirs reached (the digest set), "files":
    local paths they name, "refusals": entries that refuse the render, each
    naming the directory that named it and why}.
    """
    dirs, files, refusals, seen, queue = [], set(), [], set(), [rel_dir]
    while queue:
        current = queue.pop(0)
        marker = _kustomization_marker(source_root, current)
        if current in seen or marker is None:
            seen.add(current)
            continue
        seen.add(current)
        dirs.append(current)
        files.add(f"{current}/{marker}" if current else marker)
        docs, error = _load_kustomization(source_root, current, marker)
        if error is not None:
            refusals.append(f"{source_label(current)}/{marker} -> unreadable "
                            f"under the hardened posture ({error})")
            continue
        for entry in _ref_entries(docs):
            resolved = _resolve_local(source_root, current, entry)
            if resolved is None:
                reason = ("a remote base" if _REMOTE_ENTRY_RE.match(entry)
                          else "no such path inside the source root")
                refusals.append(f"{source_label(current)} -> {entry} ({reason})")
            elif os.path.isdir(os.path.join(source_root, resolved)):
                queue.append(resolved)
            else:
                files.add(resolved)
        files.update(f for f in (_resolve_local(source_root, current, s)
                                 for s in _scalars(docs))
                     if f and os.path.isfile(os.path.join(source_root, f)))
    return {"dirs": dirs, "files": sorted(files),
            "refusals": sorted(set(refusals))}


def kustomize_remote_refs(source_root: str, rel_dir: str) -> list:
    """Entries ANYWHERE in the kustomization graph rooted at rel_dir that the
    render cannot satisfy from local files inside the source root: remote
    bases in every form kustomize accepts (`https://`, `git@`, `git::`,
    scheme-less `github.com/org/repo//base?ref=v1`), paths that escape the
    root, paths that are absent, and kustomizations the hardened loader
    refuses. Renders never fetch, so any hit refuses the render
    — including a hit several kustomizations deep, which is exactly how
    kubectl would have reached the network."""
    return resolve_kustomize_graph(source_root, rel_dir)["refusals"]


def classify_kind(kind: str, api_version=None) -> tuple:
    """(family, classification) via the closed (group, kind) table. Any pair
    not in the table is the residual bucket: carried by wkld-manifests,
    classified aws_coupled_or_unknown — Secrets,
    SecretProviderClass, ExternalSecret, TargetGroupBinding, service-mesh CRs,
    leaked cluster-scoped platform kinds, and any custom resource that reuses
    a core kind name under its own group, alike."""
    family = KIND_TO_FAMILY.get((api_group(api_version), str(kind or "")))
    if family is None:
        return "wkld-manifests", "aws_coupled_or_unknown"
    return family, "portable"


def _mapping(value) -> dict:
    """A mapping or {}. Customer YAML may put a scalar where the schema says
    mapping (`spec: oops`); `x or {}` only guards the falsy half of that, so
    every brief helper reads sub-documents through here — a malformed
    document degrades into a plan note, never an AttributeError."""
    return value if isinstance(value, dict) else {}


def _sequence(value) -> list:
    """A list or []. The sequence half of _mapping."""
    return value if isinstance(value, list) else []


def _doc_record(doc: dict, path: str, doc_index: int, rendered_from) -> dict:
    """One classified document locator: the unit `inputs` entry. Content is
    NOT inlined — the translate step fixes the worker input contract; the locator names the
    stream (source path or render source) and the index within it."""
    kind = str(doc.get("kind") or "")
    api_version = doc.get("apiVersion")
    metadata = _mapping(doc.get("metadata"))
    family, classification = classify_kind(kind, api_version)
    return {
        "path": path,
        "doc_index": doc_index,
        "api_version": str(api_version) if api_version is not None else None,
        "kind": kind,
        "namespace": metadata.get("namespace"),
        "name": metadata.get("name"),
        "classification": classification,
        "family": family,
        "rendered_from": rendered_from,
    }


def _parse_stream(text: str, path: str, rendered_from, notes: list) -> list:
    """Hardened parse of one stream into doc records; a refusal becomes a
    plan note and the stream is excluded (visible degradation)."""
    try:
        docs = k8s_manifests.load_manifest_documents(text)
    except ValueError as e:
        notes.append(f"{path}: {e}; excluded from classification")
        return []
    records = []
    for index, doc in enumerate(docs):
        if not isinstance(doc, dict) or not str(doc.get("kind") or "").strip():
            notes.append(f"{path}: document {index} carries no kind; "
                         "excluded from classification")
            continue
        record = _doc_record(doc, path, index, rendered_from)
        record["_doc"] = doc  # brief fact extraction only; stripped on emit
        records.append(record)
    return records


def _strip_private(record: dict) -> dict:
    return {k: v for k, v in record.items() if not k.startswith("_")}


# --- fact-derived family briefs --------------------------------------------
# Grammar: every claim is conditioned on what the reachable inputs record
# (exports fields are all nullable; open dicts included); anything the
# inputs do not state is routed to assumptions/open_questions, never
# invented. Source material: knowledge/acme-workload-translation-manual.md.


def _autopilot_notes(exports) -> list:
    cluster = (exports or {}).get("cluster")
    ctype = cluster.get("type") if isinstance(cluster, dict) else None
    shapes = (exports or {}).get("node_shapes")
    if shapes:
        tier3 = ("(3) a conservative estimate from exports node_shapes ("
                 + "; ".join(str(s) for s in shapes) + ")")
    else:
        tier3 = ("(3) exports node_shapes is not published — tier 3 degrades "
                 "to Autopilot defaults, stated as such")
    ladder = (
        "Resource-requests evidence ladder: (1) existing limits in the "
        "document -> derive requests from them; (2) HPA targets -> "
        f"back-compute; {tier3}. Every tier-2/3 estimate MUST be listed in "
        "assumptions as unverified — an estimated value is not a discovered "
        "fact.")
    if str(ctype or "").lower() == "autopilot":
        return ["exports records cluster.type 'autopilot': every container "
                "needs resource requests or the deployment fails admission. "
                + ladder]
    if ctype:
        return [f"exports records cluster.type '{ctype}': resource requests "
                "are not structurally mandatory; where you add or adjust "
                "them, use the same evidence ladder. " + ladder]
    return ["exports does not publish the target cluster type (cluster is "
            "null or typeless). If the target is Autopilot every container "
            "needs resource requests; on Standard they stay recommended. "
            "Author both branches honestly and record in assumptions which "
            "one you wrote against. " + ladder]


HOST_NAMESPACE_FLAGS = ("hostNetwork", "hostPID", "hostIPC")

# The container lists a pod spec can carry. Same list as
# transforms.CONTAINER_LIST_KEYS: a privileged initContainer is a privileged
# pod, and Autopilot rejects it exactly as it rejects a privileged container.
CONTAINER_LIST_KEYS = ("containers", "initContainers", "ephemeralContainers")


def _privileged_in(pod_spec: dict) -> bool:
    for key in CONTAINER_LIST_KEYS:
        for container in _sequence(pod_spec.get(key)):
            if _mapping(_mapping(container).get("securityContext")) \
                    .get("privileged"):
                return True
    return False


def _daemonset_host_facts(records: list) -> list:
    """(name, sorted flags) per DaemonSet that records host access — all three
    host namespaces and a privileged container in ANY of the pod spec's
    container lists. Every one of them is an Autopilot admission blocker."""
    facts = []
    for record in records:
        if record["kind"] != "DaemonSet":
            continue
        spec = _mapping(_mapping(_mapping(record["_doc"].get("spec"))
                                 .get("template")).get("spec"))
        flags = [k for k in HOST_NAMESPACE_FLAGS if spec.get(k)]
        if _privileged_in(spec):
            flags.append("privileged")
        if flags:
            facts.append((record["name"] or record["path"], sorted(flags)))
    return facts


def _pod_dns_facts(records: list) -> list:
    """The persisted per-document pod DNS facts (unit inputs.pod_dns_facts).

    Same reason routing persists ingress_facts: the validate contract has
    to compare the shipped pod specs against what the worker was briefed
    on, on whatever machine validate runs, long after the parsed documents
    are gone. Read through the one shared reader (transforms.pod_dns_nodes /
    pod_dns_fields) so the brief, the knowledge and the contract see the
    same fields. One entry per DNS-bearing pod-spec-like mapping, in
    document order, with the mapping's key path within its document so the
    contract finds the same pod spec in the shipped document even after
    every DNS field was removed from it. A document with no metadata.name
    (a `kind: List` bundle, a nameless template) is facted too, labelled by
    its stream and index: its literals count for conservation, and only the
    per-document policy check skips it, since the worker must split or name
    it and the new names cannot be known here. The list itself is ALWAYS
    written for wkld-manifests: an empty list means "nothing to translate",
    an absent key means a plan from before the field.
    """
    from servers.phases.workload.workload_translate_3 import transforms
    facts = []
    for record in records:
        for path, node in transforms.pod_dns_nodes(record["_doc"]):
            fields = transforms.pod_dns_fields(node)
            if not transforms.dns_bearing(fields):
                continue
            facts.append({
                "label": record["name"] or f"{record['path']}#{record['doc_index']}",
                "kind": record["kind"],
                "namespace": record["namespace"],
                "name": record["name"],
                "node_path": path,
                **fields,
            })
    return facts


def _pod_dns_notes(facts: list) -> list:
    """Fact lines and the contract fence — no mapping: what each fact maps
    to on GKE with Cloud DNS is the unit knowledge's (pod-dns-translation.md,
    appended to the worker prompt whenever these facts are present)."""
    if not facts:
        return []
    notes = []
    for fact in facts:
        parts = []
        if fact["dns_policy"]:
            parts.append(f"dnsPolicy {fact['dns_policy']}")
        if fact["host_network"]:
            parts.append("hostNetwork true")
        if fact["nameservers"]:
            parts.append("dnsConfig.nameservers " + ", ".join(fact["nameservers"]))
        if fact["searches"]:
            parts.append("dnsConfig.searches " + ", ".join(fact["searches"]))
        if fact["options"]:
            parts.append("dnsConfig.options " + ", ".join(
                o["name"] + (f"={o['value']}" if o["value"] is not None else "")
                for o in fact["options"]))
        if fact["host_aliases"]:
            parts.append("hostAliases " + ", ".join(
                (a["ip"] or "?") + " -> " + ",".join(a["hostnames"])
                for a in fact["host_aliases"]))
        nameless = ("" if fact["name"] else
                    " (no metadata.name: split or name this document; its "
                    "addresses and domains are held to the contract, its "
                    "dnsPolicy is not matched)")
        notes.append(f"Pod DNS facts of {fact['kind']} {fact['label']}{nameless}: "
                     + "; ".join(parts) + ".")
    notes.append(
        "The pod DNS facts above are persisted as inputs.pod_dns_facts. The "
        "unit knowledge appended to this prompt (pod-dns-translation.md) "
        "states what each maps to on GKE with Cloud DNS, conditioned on the "
        "cluster type line above. The validate gate checks the shipped pod "
        "specs against these facts: every recorded address, search domain, "
        "option and host alias is either kept or named in "
        "tradeoffs/open_questions/assumptions; dnsPolicy is unchanged except "
        "the change the knowledge's None rule permits; no nameserver, search "
        "domain, option or alias the source did not carry appears; and no "
        "node resolver or cluster DNS address is ever pinned into a pod.")
    return notes


def _residual_notes(records: list) -> list:
    residual = sorted({r["kind"] for r in records
                       if r["classification"] == "aws_coupled_or_unknown"})
    if not residual:
        return []
    notes = [
        "Residual bucket (kinds outside the closed portability table): "
        + ", ".join(residual) + ". Translate each mechanically where a "
        "mapping authority covers it (reference/api-translation.md, "
        "docs/core/glossary.md, reference/service-mapping.md); otherwise "
        "emit the source document UNCHANGED and raise an open question. "
        "Never invent a GKE equivalent, never silently drop a document."]
    leaked = sorted(set(residual) & CLUSTER_SCOPED_KINDS)
    if leaked:
        notes.append(
            "Cluster-scoped platform-owned kind(s) leaked past the scope "
            "step: " + ", ".join(leaked) + " — platform-owned per the "
            "coverage map. Advise excluding these files from the component "
            "scope (amend scope and re-plan) instead of translating them.")
    other = sorted(set(residual) & CLUSTER_SCOPED_OTHER)
    if other:
        notes.append(
            "Cluster-scoped kind(s) in scope that the coverage map does NOT "
            "assign to the platform: " + ", ".join(other) + ". They are not "
            "namespaced, so shipping them changes the whole target cluster: "
            "confirm ownership with the platform engineer before they go in "
            "a component PR, and record the answer as an open question. This "
            "list and the platform-owned one above are the workload-side "
            "cluster-scope tables — a kind absent from both is not a claim "
            "that it is namespaced.")
    return notes


def _chart_strategy_notes(carriers: list, degraded: list) -> list:
    notes = []
    if carriers:
        paths = ", ".join(c["chart_path"] for c in carriers)
        notes.append(
            f"Helm chart source(s): {paths}. Strategy is worker judgment "
            "recorded in tradeoffs: (a) edit values/templates and keep the "
            "chart, or (b) flatten the deterministic render. Under (a) the "
            "chart directory ships ONCE under this unit as the shared "
            "carrier (plan `carriers`); wkld-identity/wkld-storage record "
            "their edits as file-level diffs against it. Either way the "
            "edited chart must re-render deterministically from in-repo "
            "values alone (`helm template`, no --set) — the validate "
            "gate re-renders it; raw Go templates are never gated directly.")
    for line in degraded:
        notes.append("Open question: " + line)
    return notes


def _container_image_refs(records: list) -> list:
    """Sorted unique container image refs across the records' documents,
    walked at the same closed pod-spec paths the deterministic pass uses
    (transforms.POD_SPEC_PATHS) — the brief's literals and the pass's
    verdicts must be computed over the same fields or they drift."""
    from servers.phases.workload.workload_translate_3 import transforms
    refs = set()
    for record in records:
        doc = record["_doc"]
        path = transforms.POD_SPEC_PATHS.get(str(doc.get("kind")))
        if path is None:
            continue
        node = doc
        for key in path:
            node = _mapping(node).get(key)
        for list_key in transforms.CONTAINER_LIST_KEYS:
            for container in _sequence(_mapping(node).get(list_key)):
                ref = _mapping(container).get("image")
                if isinstance(ref, str) and ref:
                    refs.add(ref)
    return sorted(refs)


def _carrier_image_notes(records: list, exports) -> list:
    """Per-image transcription literals for a carrier unit — one disposition
    per ref, mirroring transforms.rewrite_image_references's verdicts, so a
    carrier unit and a plain unit disagree in OWNER, never in outcome."""
    refs = _container_image_refs(records)
    if not refs:
        return []
    registry = (exports or {}).get("artifact_registry")
    image_map = registry.get("image_map") if isinstance(registry, dict) else None
    if not isinstance(image_map, dict):
        return ["exports artifact_registry/image_map is unpublished: leave "
                "every image reference at its source value and raise the "
                "missing map as an open question — never guess an address."]
    notes = []
    for ref in refs:
        entry = image_map.get(ref)
        entry = entry if isinstance(entry, dict) else {}
        dest, status = entry.get("dest_ref"), entry.get("status")
        if dest and status == "replicated":
            notes.append(
                f"Transcribe into the carrier source: image '{ref}' -> "
                f"'{dest}' (exports image_map, status replicated). The "
                "rendered documents must carry the destination address.")
        elif dest:
            notes.append(
                f"Image '{ref}' maps to '{dest}' but its replication status "
                f"is '{status}', not 'replicated' — nothing is at the "
                "destination yet: leave the source value and raise the "
                "pending replication as an open question.")
        else:
            notes.append(
                f"Image '{ref}' has no usable image_map entry: leave it at "
                "its source value and raise it as an open question — a "
                "guessed address is never written.")
    return notes


def _manifests_notes(records: list, exports, carriers: list,
                     degraded: list, pod_dns_facts: list = None) -> list:
    notes = [
        "A Deployment is a Deployment on GKE: translate the scoped "
        "documents near-unchanged and change ONLY the AWS coupling points "
        "this brief names."]
    if not carriers:
        notes.append(
            "Image references are the deterministic pass — do NOT "
            "hand-rewrite image addresses (it consumes the exports "
            "image_map; a guessed address is the worst outcome).")
    else:
        notes.append(
            "Ownership split: in PLAIN manifest files image references are "
            "the deterministic pass — do NOT hand-rewrite them there. "
            "Chart/kustomize SOURCE files are the exception: the pass "
            "cannot edit them, so YOU own those rewrites — transcribe the "
            "published literals this brief lists into the sources "
            "(values/templates) so the RENDERED documents carry them. "
            "Transcribing a published literal is not invention; the "
            "validate gate re-renders the shipped sources and BLOCKS any "
            "left-over value the pass would have rewritten.")
        notes.extend(_carrier_image_notes(records, exports))
    notes.extend(_autopilot_notes(exports))
    cluster = (exports or {}).get("cluster")
    ctype = str((cluster or {}).get("type") or "").lower() \
        if isinstance(cluster, dict) else ""
    for name, flags in _daemonset_host_facts(records):
        target = ("exports records cluster.type 'autopilot': these host "
                  "flags are blocked there — record the re-design or "
                  "stay-behind decision as a tradeoff plus open question "
                  "(judgment before translation)." if ctype == "autopilot"
                  else "deployable on a Standard cluster, blocked on "
                       "Autopilot; condition the plan on the published "
                       "cluster type and record the tradeoff.")
        notes.append(f"DaemonSet '{name}' records {', '.join(flags)}: {target}")
    notes.extend(_pod_dns_notes(
        _pod_dns_facts(records) if pod_dns_facts is None else pod_dns_facts))
    if degraded:
        notes.append(
            "A render source above did not render at plan time, so its pod "
            "specs are not in pod_dns_facts. If you flatten or repair it, "
            "translate their DNS fields by the unit knowledge all the same: "
            "the validate gate reads every shipped pod spec against the closed "
            "list, and does not treat their addresses as invented.")
    facts = [f for f in (_node_placement_facts(r) for r in records) if f]
    notes.extend(_node_placement_notes(facts, exports))
    notes.extend(_residual_notes(records))
    notes.extend(_chart_strategy_notes(carriers, degraded))
    return notes


# The Karpenter placement keys the deterministic pass reads
# (workload_translate_3/transforms.KARPENTER_PLACEMENT_KEYS, restated as a
# literal so the planner stays import-light from the translate step;
# planner_test pins the two). kubernetes.io/arch is portable and not one.
KARPENTER_PLACEMENT_KEYS = ("karpenter.sh/nodepool", "karpenter.sh/provisioner-name",
                            "karpenter.sh/capacity-type", "node.kubernetes.io/instance-type")
KARPENTER_TOLERATION_PREFIX = "karpenter.sh/"
KARPENTER_POOL_KEYS = ("karpenter.sh/nodepool", "karpenter.sh/provisioner-name")


def _pod_spec_of(doc: dict):
    kind = str(doc.get("kind") or "")
    spec = _mapping(doc.get("spec"))
    if kind == "Pod":
        return spec
    if kind == "CronJob":
        return _mapping(_mapping(_mapping(_mapping(spec.get("jobTemplate")).get("spec"))
                                 .get("template")).get("spec"))
    if kind in ("Deployment", "DaemonSet", "StatefulSet", "Job"):
        return _mapping(_mapping(spec.get("template")).get("spec"))
    return {}


def _node_placement_facts(record: dict):
    """The persisted per-document Karpenter placement facts (unit
    inputs.node_placement_facts), or None when the pod spec carries none.

    Same reason as _ingress_facts: the brief conditions on them and must be
    rebuildable from the persisted plan. Filtered to the keys the
    deterministic pass reads and to karpenter.sh/ tolerations; every other
    selector and toleration is the document's own business.
    """
    spec = _pod_spec_of(record["_doc"])
    if not spec:
        return None
    selector = {k: v for k, v in _mapping(spec.get("nodeSelector")).items()
                if k in KARPENTER_PLACEMENT_KEYS}
    affinity_keys = []
    node_affinity = _mapping(_mapping(spec.get("affinity")).get("nodeAffinity"))
    for term in _sequence(_mapping(node_affinity.get(
            "requiredDuringSchedulingIgnoredDuringExecution")).get("nodeSelectorTerms")):
        for expr in _sequence(_mapping(term).get("matchExpressions")):
            expr = _mapping(expr)
            if expr.get("key") in KARPENTER_PLACEMENT_KEYS:
                affinity_keys.append({"key": expr.get("key"), "operator": expr.get("operator"),
                                      "values": _sequence(expr.get("values"))})
    tolerations = []
    for toleration in _sequence(spec.get("tolerations")):
        toleration = _mapping(toleration)
        key = toleration.get("key")
        if isinstance(key, str) and key.startswith(KARPENTER_TOLERATION_PREFIX):
            tolerations.append({"key": key, "operator": toleration.get("operator"),
                                "value": toleration.get("value"), "effect": toleration.get("effect")})
    if not (selector or affinity_keys or tolerations):
        return None
    return {"label": record["name"] or record["path"], "namespace": record["namespace"],
            "kind": record["kind"], "node_selector": selector,
            "affinity_keys": affinity_keys, "tolerations": tolerations}


def _compute_class_verdict(name, menu) -> str:
    if not isinstance(menu, list):
        return (f"selects Karpenter pool '{name}': exports compute_classes is unpublished "
                "— the deterministic pass leaves it and raises an open question; say so")
    shown = ", ".join(str(c) for c in menu)
    if name in menu:
        return (f"selects Karpenter pool '{name}', which IS a published ComputeClass "
                f"(exports compute_classes: {shown}) — the deterministic pass rewrites the "
                "key to cloud.google.com/compute-class; do not hand-edit it in plain files")
    return (f"selects Karpenter pool '{name}', ABSENT from exports compute_classes "
            f"({shown or 'empty'}) — the pass leaves it with an open question; under a NAP "
            "or Autopilot landing zone map it to the target node pool label or drop it, "
            "never invent a class name")


def _node_placement_notes(facts: list, exports) -> list:
    """One line per placement fact: the menu verdict for pool selectors, and
    what the deterministic pass does with the rest (the transform table in
    workload_translate_3/transforms.rewrite_karpenter_node_placement)."""
    menu = (exports or {}).get("compute_classes")
    notes = []
    for fact in facts:
        label = f"{fact['kind']} '{fact['label']}'"
        pools = [v for k, v in fact["node_selector"].items() if k in KARPENTER_POOL_KEYS]
        pools += [e["values"][0] for e in fact["affinity_keys"]
                  if e.get("key") in KARPENTER_POOL_KEYS and e.get("operator") == "In"
                  and len(e.get("values") or []) == 1]
        for name in pools:
            notes.append(f"{label} {_compute_class_verdict(name, menu)}.")
        other = sorted(set(k for k in fact["node_selector"] if k not in KARPENTER_POOL_KEYS)
                       | set(e["key"] for e in fact["affinity_keys"] if e.get("key") not in KARPENTER_POOL_KEYS)
                       | set(t["key"] for t in fact["tolerations"]))
        odd = [e for e in fact["affinity_keys"] if e.get("key") in KARPENTER_POOL_KEYS
               and not (e.get("operator") == "In" and len(e.get("values") or []) == 1)]
        for e in odd:
            notes.append(
                f"{label} selects a Karpenter pool through nodeAffinity {e['key']} "
                f"{e.get('operator')} {e.get('values')!r}: only `In` with one value is "
                "rewritten deterministically; the pass leaves it with an open question — "
                "decide the GKE equivalent in tradeoffs.")
        if other:
            notes.append(
                f"{label} carries Karpenter placement keys the deterministic pass handles in "
                "plain files (" + ", ".join(other) + "): capacity-type spot becomes "
                "cloud.google.com/gke-spot, on-demand becomes a gke-spot DoesNotExist affinity, "
                "karpenter.sh/* tolerations are removed, an AWS instance type is an open "
                "question — in chart or kustomize sources YOU transcribe the same rewrites.")
    return notes


def _missing_key_fields(record: dict) -> str:
    """Names the metadata field(s) actually absent — never a composite key
    with a fabricated half in it (the brief grammar's literal-truth rule)."""
    missing = [field for field in ("name", "namespace") if not record[field]]
    if len(missing) == 2:
        return "records neither metadata.name nor metadata.namespace"
    return f"records no metadata.{missing[0]}"


def _identity_notes(records: list, exports, chart_cites: list) -> list:
    bindings = (exports or {}).get("gsa_bindings")
    notes = []
    for record in records:
        annotations = _mapping(_mapping(record["_doc"].get("metadata"))
                               .get("annotations"))
        has_irsa = "eks.amazonaws.com/role-arn" in annotations
        key = f"{record['namespace']}/{record['name']}" \
            if record["namespace"] and record["name"] else None
        label = record["name"] or record["path"]
        if not has_irsa:
            notes.append(f"ServiceAccount '{label}' carries no IRSA role-arn "
                         "annotation: nothing to swap; translate unchanged.")
        elif not isinstance(bindings, dict):
            notes.append(
                f"ServiceAccount '{label}' carries an IRSA role-arn, but "
                "exports gsa_bindings is not published: leave the annotation "
                "untouched and raise the missing mapping as an open question "
                "— never invent a GSA email.")
        elif key and key in bindings:
            if (record.get("rendered_from") or {}).get("type") == "helm":
                notes.append(
                    f"exports gsa_bindings records '{key}' -> "
                    f"{bindings[key]}: this document renders from a chart, "
                    "which the deterministic pass cannot edit — transcribe "
                    "the published email into the carrier source so the "
                    "rendered ServiceAccount carries "
                    f"iam.gke.io/gcp-service-account: {bindings[key]} and "
                    "no eks.amazonaws.com/role-arn (drop the IRSA companion "
                    "annotations too). Transcribing the published literal "
                    "is not invention; the validate gate re-renders the "
                    "shipped chart and blocks a left-over role-arn.")
            else:
                notes.append(
                    f"exports gsa_bindings records '{key}' -> "
                    f"{bindings[key]}: the deterministic pass replaces "
                    "eks.amazonaws.com/role-arn with "
                    "iam.gke.io/gcp-service-account — do not hand-edit the "
                    "email.")
        elif key is None:
            notes.append(
                f"ServiceAccount '{label}' carries an IRSA role-arn but "
                f"{_missing_key_fields(record)}: the gsa_bindings key cannot "
                "be built without guessing, so no lookup was attempted. "
                "Leave the annotation untouched and raise the incomplete "
                "metadata as an open question.")
        else:
            notes.append(
                f"exports gsa_bindings has no entry for '{key}': flag it "
                "as an open question and leave the annotation untouched — "
                "never invent an email.")
    notes.append(
        "Co-existence: IRSA keeps working on the source cluster until "
        "cutover — the translated ServiceAccount targets the new cluster "
        "and must not break the source.")
    notes.append(
        "Ghost rule: where a credentialed reference this account carries "
        "(an annotation, an env var naming a bucket or queue) has no "
        "observed usage in the scoped files, translate it mechanically AND "
        "attach the no-observed-usage evidence as an open question. Never "
        "silently drop it, never silently keep it without the question.")
    for chart_path in chart_cites:
        notes.append(
            f"Documents rendered from chart {chart_path}: record edits as "
            "file-level diffs against the wkld-manifests carrier for that "
            "chart (see plan `carriers`) and cross-cite it.")
    return notes


def _class_name_verdict(class_name, menu) -> str:
    # exports fields are open data: a menu entry need not be a str, so every
    # rendering coerces (node_shapes next door already does) and membership
    # is decided on the coerced values — a stray int must not raise out of
    # the planner.
    if class_name is None:
        return ("records no storageClassName — it binds to the target "
                "default class; raise the intended class as an open question")
    if not isinstance(menu, list):
        return (f"references storageClassName '{class_name}', but exports "
                "storage_class_menu is unpublished — say so; do not guess "
                "whether the class exists on the target")
    shown = [str(m) for m in menu]
    if str(class_name) in shown:
        return (f"references storageClassName '{class_name}', present in "
                f"exports storage_class_menu ({', '.join(shown)}) — the "
                "reference keeps resolving unchanged")
    return (f"references storageClassName '{class_name}', which is ABSENT "
            f"from exports storage_class_menu ({', '.join(shown) or 'empty'})"
            " — raise it as an open question; never invent a class")


def _sts_vct_facts(manifest_records: list) -> list:
    """(sts_name, locator, [(claim_name, class_name), ...]) per StatefulSet
    with volumeClaimTemplates — the one-document-two-consumers cross-cite."""
    facts = []
    for record in manifest_records:
        if record["kind"] != "StatefulSet":
            continue
        templates = _sequence(_mapping(record["_doc"].get("spec"))
                              .get("volumeClaimTemplates"))
        claims = []
        for template in templates:
            if not isinstance(template, dict):
                continue
            claims.append((
                _mapping(template.get("metadata")).get("name"),
                _mapping(template.get("spec")).get("storageClassName")))
        if claims:
            locator = f"{record['path']}#doc{record['doc_index']}"
            facts.append((record["name"] or locator, locator, claims))
    return facts


def _transport_note(label: str, staging) -> str:
    """The conditional data-transport question, phrased identically for a
    standalone claim and for a StatefulSet's volumeClaimTemplate."""
    note = (f"If {label} holds real data at cutover it must be transported — "
            "the pipeline does not move data. Record the transport item as "
            "an open question")
    if staging:
        return note + (f"; exports designates staging_bucket {staging} as "
                       "the transport staging.")
    return note + ("; exports records no staging_bucket, so the staging "
                   "path is itself an open question.")


def _sts_cite_notes(sts_facts: list, menu, staging) -> list:
    """The StatefulSet cross-cite, with the SAME two verdicts a standalone
    PVC gets. A volumeClaimTemplate is the claim most likely to hold real
    data at cutover, so the cross-cite frames the claims — it does not
    weaken them into prose."""
    notes = []
    for sts_name, locator, claims in sts_facts:
        rendered = ", ".join(
            f"{name or '(unnamed)'} (storageClassName: {cls or 'unset'})"
            for name, cls in claims)
        notes.append(
            f"StatefulSet '{sts_name}' ({locator}, wkld-manifests unit) "
            f"declares volumeClaimTemplates: {rendered}. The document stays "
            "in the manifests unit — the unit boundary is the document; this "
            "brief cites those claims so the storage review covers them.")
        for name, cls in claims:
            label = (f"volumeClaimTemplate '{name or '(unnamed)'}' of "
                     f"StatefulSet '{sts_name}'")
            notes.append(f"{label} {_class_name_verdict(cls, menu)}.")
            notes.append(_transport_note(label, staging))
    return notes


def _storage_notes(records: list, exports, sts_facts: list,
                   chart_cites: list) -> list:
    menu = (exports or {}).get("storage_class_menu")
    staging = (exports or {}).get("staging_bucket")
    notes = [
        "The PVC claim translates near-unchanged: keep accessModes, sizes "
        "and names at their source values — this unit produces the claim "
        "only, never the volume contents."]
    for record in records:
        class_name = _mapping(record["_doc"].get("spec")) \
            .get("storageClassName")
        label = f"PVC '{record['name'] or record['path']}'"
        notes.append(f"{label} {_class_name_verdict(class_name, menu)}.")
        notes.append(_transport_note(label, staging))
    notes.extend(_sts_cite_notes(sts_facts, menu, staging))
    for chart_path in chart_cites:
        notes.append(
            f"Documents rendered from chart {chart_path}: record edits as "
            "file-level diffs against the wkld-manifests carrier for that "
            "chart (see plan `carriers`) and cross-cite it.")
    return notes


def _ingress_facts(record: dict) -> dict:
    """The persisted per-document Ingress facts (unit inputs.ingress_facts).

    Everything the routing brief conditions on lives here, because the brief
    must be REBUILDABLE from the persisted plan alone: the unpark refresh
    (refresh_parked_routing) runs long after the parsed documents are gone —
    the emitted document locators carry no spec — and it must produce the
    same brief a fresh plan would.
    """
    doc = record["_doc"]
    spec = _mapping(doc.get("spec"))
    annotations = _mapping(_mapping(doc.get("metadata")).get("annotations"))
    rules = []
    for rule in _sequence(spec.get("rules")):
        if not isinstance(rule, dict):
            continue
        paths = []
        for entry in _sequence(_mapping(rule.get("http")).get("paths")):
            if not isinstance(entry, dict):
                continue
            backend = _mapping(_mapping(entry.get("backend")).get("service"))
            port = _mapping(backend.get("port"))
            paths.append({"path": entry.get("path"),
                          "path_type": entry.get("pathType"),
                          "service": backend.get("name"),
                          "port_number": port.get("number"),
                          "port_name": port.get("name")})
        rules.append({"host": rule.get("host"), "paths": paths})
    return {
        "label": record["name"] or record["path"],
        "namespace": record["namespace"],
        "rules": rules,
        "default_backend": bool(spec.get("defaultBackend")),
        "tls": bool(spec.get("tls")),
        "ingress_class_name": spec.get("ingressClassName"),
        "annotations": sorted(str(k) for k in annotations
                              if not _is_bookkeeping_annotation(str(k))),
    }


# Tool bookkeeping an apply/render wrote, owned by nobody in the source: it
# carries no target behaviour and re-appears on its own. Everything else is
# persisted, because the closed disposition table can only be closed over
# what the facts actually record — an annotation the facts drop can never
# become the open question the brief promises it will.
_BOOKKEEPING_ANNOTATIONS = {
    "kubectl.kubernetes.io/last-applied-configuration",
    "deployment.kubernetes.io/revision",
}
_BOOKKEEPING_PREFIXES = ("meta.helm.sh/",)

# The AWS load balancer controller's sentinel: a path backed by an
# actions.<name> annotation names the action as its Service and this as its
# port name. Read by the live routing notes, never resolved as a port.
ACTION_PORT_SENTINEL = "use-annotation"

# When present, the controller treats every ImplementationSpecific path as a
# regular expression; the live note then defers to the annotation's row
# instead of offering a provisional PathPrefix.
REGEX_PATH_ANNOTATION = "alb.ingress.kubernetes.io/use-regex-path-match"


def _is_bookkeeping_annotation(key: str) -> bool:
    return (key in _BOOKKEEPING_ANNOTATIONS
            or key.startswith(_BOOKKEEPING_PREFIXES))


# The closed annotation disposition table. Every annotation found on a
# scoped Ingress gets exactly one of three dispositions — mapped / dropped
# with tradeoff / open question. The table is the `### Ingress / Gateway
# annotations` section of reference/api-translation.md, parsed at start-up
# (servers/dag/server/api_translation.py) so the document the humans read
# and the brief the worker gets are the same rows. An annotation not in
# the table is an open question BY RULE, never silently dropped. The
# deterministic strip list never touches alb.* precisely so this family
# can consume them here.


def disposition_row(key: str) -> tuple:
    """(disposition, rationale) for one annotation key actually present."""
    return _api_translation.annotation_disposition(key)


def _fact_line(fact: dict, live: bool) -> str:
    """One Ingress's routing-facts line. The parked rendering's format is
    regression-protected; the live one adds each path's pathType —
    the live mapping rules turn on it."""
    rendered = []
    for rule in fact["rules"]:
        host = rule.get("host") or "(no host)"
        for p in rule["paths"]:
            line = (f"{host} {p.get('path') or '/'} -> "
                    f"{p.get('service')}:"
                    f"{p.get('port_number') or p.get('port_name')}")
            if live:
                line += f" (pathType {p.get('path_type') or 'unset'})"
            rendered.append(line)
    return (f"Ingress '{fact['label']}' routing facts: "
            + ("; ".join(rendered) if rendered else "no rules recorded")
            + ".")


def _disposition_notes(facts: list) -> list:
    """One row per annotation key actually present, naming its carriers."""
    carriers = {}
    for fact in facts:
        for key in fact.get("annotations") or []:
            carriers.setdefault(key, []).append(fact["label"])
    notes = []
    for key in sorted(carriers):
        disposition, rationale = disposition_row(key)
        notes.append(
            f"Disposition — {key} (on Ingress "
            f"{', '.join(sorted(carriers[key]))}): {disposition} — "
            f"{rationale}.")
    return notes


def _live_fact_notes(fact: dict) -> list:
    """The per-Ingress live lines: facts plus the conditional mapping rules
    that fire only on what this document actually records."""
    label = fact["label"]
    notes = [_fact_line(fact, live=True)]
    paths = [p for rule in fact["rules"] for p in rule["paths"]]
    if any(p.get("path_type") == "ImplementationSpecific" for p in paths):
        if REGEX_PATH_ANNOTATION in (fact.get("annotations") or []):
            notes.append(
                f"Ingress '{label}' uses pathType ImplementationSpecific "
                f"under {REGEX_PATH_ANNOTATION}: when its value is true, "
                "every such path is a regular expression — raise an open "
                "question per path and never assume a PathPrefix (see that "
                "annotation's disposition row; a false value is the "
                "default and changes nothing).")
        else:
            notes.append(
                f"Ingress '{label}' uses pathType ImplementationSpecific: ALB "
                "matching semantics are not portable — raise an open "
                "question; PathPrefix may be chosen only when the value is "
                "a plain prefix, recorded as an assumption.")
    # The ALB controller's action idiom: a path whose backend "Service" is
    # the name of an actions.<name> annotation, with the sentinel port name
    # use-annotation. That is not a named port to resolve — there is no
    # Service — so it gets its own line and stays out of the named-port
    # rule, which would otherwise send the worker hunting for one.
    action_backed = sorted({str(p.get("service")) for p in paths
                            if p.get("port_name") == ACTION_PORT_SENTINEL})
    if action_backed:
        notes.append(
            f"Ingress '{label}' has path(s) backed by an ALB action "
            f"({', '.join(action_backed)}; port name "
            f"{ACTION_PORT_SENTINEL}): the backend name is the action's "
            "annotation key, not a Service — emit no backendRef named after "
            "the action; the action's own disposition row (actions.<name>, "
            "exact or prefix) says what it becomes.")
    named = sorted({str(p["port_name"]) for p in paths
                    if p.get("port_name") and not p.get("port_number")
                    and p.get("port_name") != ACTION_PORT_SENTINEL})
    if named:
        notes.append(
            f"Ingress '{label}' references named backend port(s) "
            f"({', '.join(named)}): resolve each against a Service in the "
            "component's scoped documents; unresolvable -> open question, "
            "never a guessed number.")
    if fact.get("default_backend"):
        notes.append(
            f"Ingress '{label}' declares spec.defaultBackend: emit a "
            "catch-all PathPrefix / rule ordered last, noted in tradeoffs.")
    if fact.get("tls"):
        notes.append(
            f"Ingress '{label}' declares spec.tls: certificates are "
            "platform-side — record the hosts (and any ACM ARN) as a "
            "tradeoff pointing at the platform Gateway and emit NO TLS "
            "configuration. The platform Gateway ships an HTTP listener "
            "default and records TLS as an open question — do not promise "
            "HTTPS.")
    if fact.get("ingress_class_name"):
        notes.append(
            f"Ingress '{label}' sets spec.ingressClassName "
            f"'{fact['ingress_class_name']}': dropped with tradeoff — class "
            "selection is replaced by parentRefs attachment.")
    return notes


def _routing_live_notes(facts: list, gateway: dict) -> list:
    """The live routing brief: Ingress rules become HTTPRoute objects whose
    parentRefs come verbatim from exports.gateway."""
    name, namespace = gateway.get("name"), gateway.get("namespace")
    notes = [
        "exports.gateway records "
        f"{namespace}/{name}: this unit is live. Emit one HTTPRoute "
        "(apiVersion gateway.networking.k8s.io/v1) per Ingress document by "
        "default, in the Ingress's OWN namespace, named after the Ingress; "
        "splitting per host is worker judgment recorded in tradeoffs.",
        "parentRefs: exactly [{name: "
        f"{name!r}, namespace: {namespace!r}}}] — copied verbatim from "
        "exports.gateway. These are contract values: never invent, "
        "abbreviate or 'correct' them, and never add a sectionName — "
        "listener names are unknowable until the platform publishes them.",
        "Mapping rules (deviations are tradeoffs): spec.rules[].host -> "
        "spec.hostnames; paths[].pathType Prefix -> matches[].path.type "
        "PathPrefix, Exact -> Exact; backend.service.name/port.number -> "
        "backendRefs [{name, port}].",
        "Rule precedence differs: the ALB evaluates rules in order (first "
        "match wins), Gateway API by longest path match. Where source rules "
        "overlap (a broader path listed before a narrower one), state the "
        "resulting precedence change in tradeoffs.",
    ]
    notes.extend(_cross_namespace_notes(facts, namespace))
    for fact in facts:
        notes.extend(_live_fact_notes(fact))
    notes.extend(_disposition_notes(facts))
    notes.append(
        "Co-existence: the source Ingress keeps serving until cutover — the "
        "HTTPRoute is additive; do not modify or delete the source Ingress "
        "in this unit's output.")
    return notes


# The attach-permission contract, read from its ONE product-owned
# definition in the shared exports base (the platform tenancy/gateway
# briefs and gateway_contract import the same pair): every platform-issued
# Namespace carries this label, and the shared Gateway's listeners admit
# routes from exactly that label (or from all namespaces). Imported, not
# restated — a bare literal here would silently split the platform
# Namespaces from the brief the developer worker reads if the label ever
# moved (2026-08-15 audit, family 1). exports is a light import (no GCS
# client), so the planner stays unit-testable without GCS.
GATEWAY_ACCESS_LABEL = _exports_base.GATEWAY_ACCESS_LABEL
GATEWAY_ACCESS_VALUE = _exports_base.GATEWAY_ACCESS_VALUE


def _cross_namespace_notes(facts: list, gw_namespace) -> list:
    """The attach PERMISSION contract for a cross-namespace parentRef.

    The brief above mandates an HTTPRoute in the Ingress's own namespace
    pointing at a Gateway in another one. That attachment is a product
    CONTRACT, not an unpublished unknown: the platform tenancy unit labels
    every Namespace it issues with GATEWAY_ACCESS_LABEL, and the shared
    Gateway's listeners select exactly that label (or admit all
    namespaces) — both halves machine-checked platform-side
    (gateway_contract). So the brief states the guarantee; the one
    residual open question is a namespace the platform did NOT issue,
    which lacks the label and would be accepted-and-never-attached.
    """
    foreign = sorted({str(f.get("namespace")) for f in facts
                      if f.get("namespace") and f["namespace"] != gw_namespace})
    if not foreign:
        return []
    return [
        f"Cross-namespace attach — this unit's Ingress namespace(s) "
        f"({', '.join(foreign)}) differ from the Gateway's "
        f"({gw_namespace}). The platform contract guarantees attachment: "
        "every platform-issued Namespace carries the label "
        f'{GATEWAY_ACCESS_LABEL}: "{GATEWAY_ACCESS_VALUE}", and the shared '
        "Gateway's listeners admit routes from exactly that label (or from "
        "all namespaces) — both halves are machine-checked on the platform "
        "side. If a namespace above was NOT issued by the platform tenancy "
        "unit, it lacks the label and its HTTPRoute would be accepted and "
        "never attach: raise exactly that as the open question, naming the "
        "namespace and the label. Do not add a ReferenceGrant (that "
        "governs backendRefs, not listener attachment) and do not move the "
        "HTTPRoute into the Gateway's namespace to dodge it."]


def _routing_parked_notes(facts: list, gaps: list = None) -> list:
    """The parked brief, kept for the null-gateway arm:
    the facts exist, the attach point does not (regression, not removal)."""
    notes = [
        "Routing translation is live, but this unit is parked: "
        "the shared platform Gateway is not published yet. It unparks "
        "automatically at translate entry once exports.gateway carries a "
        "complete attach point — no re-plan needed."]
    for fact in facts:
        notes.append(_fact_line(fact, live=False))
        present = list(fact.get("annotations") or [])
        if present:
            notes.append(
                f"Ingress '{fact['label']}' carries annotations "
                f"({', '.join(present)}): they belong to this family's "
                "disposition table (applied when the unit unparks) and "
                "are NOT stripped by the deterministic pass.")
    if gaps and len(gaps) < len(GATEWAY_ATTACH_FIELDS):
        notes.append(
            "exports.gateway is PARTIAL — it is published but its "
            f"{', '.join(gaps)} field(s) are null/blank. parentRefs is a "
            "verbatim contract, so an incomplete attach point cannot go "
            "live: the missing field(s) are the open question for the "
            "platform side (a Gateway manifest with no explicit "
            "metadata.namespace publishes exactly this way).")
    else:
        notes.append(
            "exports.gateway is null — the shared platform Gateway is not "
            "published. The routing facts exist; the attach point does not: "
            "this unit is parked with that open question, never skipped.")
    return notes


def _routing_notes(facts: list, exports, chart_cites: list) -> list:
    """The routing brief, derived from persisted facts + exports ONLY, so
    the unpark refresh rebuilds it byte-identical to a fresh plan's."""
    gaps = gateway_attach_gaps(exports)
    if not gaps:
        notes = _routing_live_notes(facts, (exports or {})["gateway"])
        for chart_path in chart_cites:
            notes.append(
                f"Documents rendered from chart {chart_path}: record edits "
                "as file-level diffs against the wkld-manifests carrier for "
                "that chart (see plan `carriers`) and cross-cite it.")
    else:
        notes = _routing_parked_notes(facts, gaps)
    notes.append(
        "NetworkPolicy is out of the workload pipeline's scope for now — see "
        "the coverage map's NetworkPolicy row; namespace isolation is owned "
        "by the platform side.")
    return notes


# --- plan assembly ----------------------------------------------------------

# What each family's bucket is empty OF. Family-scoped on purpose: a scope
# holding one ServiceAccount and nothing else skips wkld-manifests, and
# "no documents of any kind" would be a false claim about that plan — a
# placeholder note is a coverage claim a human ratifies, so it states only
# what its own family found.
_EMPTY_CLAIMS = {
    "wkld-manifests": ("no document of the portable manifest kinds and no "
                       "residual document"),
    "wkld-identity": "no ServiceAccount documents",
    "wkld-storage": "no PersistentVolumeClaim documents",
    "wkld-routing": "no Ingress documents",
}


def _skip_note(family: str) -> str:
    # The claim states exactly what the scoped, parsed documents lack —
    # placeholders are coverage claims a human reviews, so the wording must
    # stay literally true (the landing-zone planner's skip_note discipline).
    return (
        f"Skipped placeholder: the scoped files' parsed documents contain "
        f"{_EMPTY_CLAIMS[family]}, so there is nothing for this family to "
        "translate. If the estate disagrees, amend the component scope and "
        "re-plan; unskipping without new facts hands a worker empty inputs.")


def _unit(family: str, status: str, inputs: dict, notes: list,
          placeholder: bool = False) -> dict:
    return {
        "unit_id": family,
        "family": family,
        "title": _title(family, status),
        "status": status,  # planned | parked | skipped (done/revise are set by translate and review)
        # The status the FACTS produced, kept immutable across review edits so
        # restore_units can return a unit to it instead of promoting a parked
        # unit to planned (nothing about the facts changed in between).
        "planned_status": status,
        "placeholder": placeholder,
        "inputs": inputs,
        "notes": notes,
        "feedback": None,
        "error": None,
    }


def _chart_cites(records: list) -> list:
    return sorted({r["rendered_from"]["chart_path"] for r in records
                   if (r.get("rendered_from") or {}).get("type") == "helm"})


def _helm_accounted(chart_dir: str, text: str, members: list) -> set:
    """The in-scope files a helm render accounted for: the chart metadata and
    values helm always reads, every vendored subchart file (the parent render
    emits those), and every template named in a `# Source:` comment — helm's
    own record of which file produced which document."""
    accounted = {f for f in members
                 if os.path.basename(f) in ("Chart.yaml", "Chart.lock")
                 or os.path.basename(f).startswith("values")
                 or "/charts/" in f}
    prefix = (chart_dir + "/") if chart_dir else ""
    for match in re.finditer(r"^#\s*Source:\s*(\S+)\s*$", text or "",
                             re.MULTILINE):
        named = match.group(1).split("/", 1)
        if len(named) == 2:
            accounted.add(prefix + named[1])
    return accounted


def _collect_records(source_root, parts, renderers, plan_notes, sources,
                     carriers, degraded, accounted):
    """Renders/parses plain files and chart sources into classified doc
    records. Degradation is a note plus an open-question line, never a crash,
    never a guess."""
    records = []
    for rel in parts["plain"]:
        try:
            with open(os.path.join(source_root, rel), "r", encoding="utf-8") as f:
                text = f.read()
        except (OSError, UnicodeDecodeError) as e:
            plan_notes.append(f"{rel}: unreadable ({e}); excluded")
            continue
        accounted.add(rel)
        records.extend(_parse_stream(text, rel, None, plan_notes))
    for chart_dir in parts["charts"]:
        label = source_label(chart_dir)
        entry = {"chart_path": label,
                 "values_digest": _digest_dir(source_root, chart_dir)}
        text, error = renderers["helm"](source_root, label)
        entry["rendered"] = error is None
        sources["charts"].append(entry)
        if error is not None:
            plan_notes.append(f"{label}: helm render failed: {error}")
            degraded.append(
                f"chart {label} could not be rendered ({error}); its "
                "documents are not classified — resolve the render before "
                "translation.")
            continue
        accounted.update(_helm_accounted(
            chart_dir, text, parts["members"].get(chart_dir, [])))
        carriers.append({"chart_path": label,
                         "values_digest": entry["values_digest"],
                         "owner": "wkld-manifests"})
        records.extend(_parse_stream(
            text, label, {"type": "helm", "chart_path": label}, plan_notes))
    return records


def _prune_kustomize_bases(graphs: dict, plan_notes: list) -> list:
    """The kustomize sources to render: those no OTHER in-scope source
    reaches through its resources/bases.

    A base that an in-scope overlay includes is rendered by that overlay, so
    rendering it standalone too classifies every one of its documents a
    second time. The base is not dropped from the plan's coverage — it is
    named here, so the narrowing is visible rather than silent.
    """
    owned = {reached for rel_dir, graph in graphs.items()
             for reached in graph["dirs"]
             if reached != rel_dir and reached in graphs}
    render = [d for d in sorted(graphs) if d not in owned]
    if graphs and not render:  # a reference cycle: render the first, honestly
        render = [sorted(graphs)[0]]
    for rel_dir in sorted(owned):
        if rel_dir in render:
            continue
        owners = sorted(source_label(o) for o, g in graphs.items()
                        if o != rel_dir and rel_dir in g["dirs"])
        plan_notes.append(
            f"{source_label(rel_dir)}: rendered as a base of "
            f"{', '.join(owners)}, not separately — its documents are "
            "classified once, through the kustomization that includes it")
    return render


def _collect_kustomize(source_root, parts, renderers, plan_notes, sources,
                       degraded, accounted):
    """Kustomize leg: the transitive graph pre-scan refuses the render when
    anything in it would have to be fetched (nothing is fetched),
    bases an in-scope overlay already renders are not rendered again, and a
    local render failure degrades the same way."""
    records = []
    graphs = {d: resolve_kustomize_graph(source_root, d)
              for d in parts["kustomize"]}
    for rel_dir in _prune_kustomize_bases(graphs, plan_notes):
        label, graph = source_label(rel_dir), graphs[rel_dir]
        entry = {"kustomize_dir": label,
                 "input_digest": _digest_graph(source_root, graph["dirs"]),
                 "input_dirs": sorted(source_label(d) for d in graph["dirs"])}
        accounted.update(graph["files"])
        if graph["refusals"]:
            entry.update({"rendered": False, "remote_refs": graph["refusals"]})
            sources["kustomize"].append(entry)
            plan_notes.append(
                f"{label}: the kustomization graph names base(s) this render "
                f"cannot resolve locally ({'; '.join(graph['refusals'])}); "
                "not rendered — renders never fetch")
            degraded.append(
                f"kustomization {label} (or a base it includes) names "
                f"{'; '.join(graph['refusals'])}; the render was refused "
                "(nothing is fetched) and its documents are not classified — "
                "vendor the bases locally or exclude the directory.")
            continue
        text, error = renderers["kustomize"](source_root, label)
        entry["rendered"] = error is None
        sources["kustomize"].append(entry)
        if error is not None:
            plan_notes.append(f"{label}: kustomize render failed: {error}")
            degraded.append(
                f"kustomization {label} could not be rendered ({error}); "
                "its documents are not classified — resolve the render "
                "before translation.")
            continue
        records.extend(_parse_stream(
            text, label, {"type": "kustomize", "kustomize_dir": label},
            plan_notes))
    return records


def _dedup_by_identity(records: list, plan_notes: list) -> list:
    """One Kubernetes object -> one classified record.

    Two overlays that share a base each render that base's documents, and a
    chart pulled in from two places renders twice. The (path, doc_index)
    locator cannot see it — the paths differ — so the "every document lands
    in exactly one unit" invariant has to be enforced on the object identity
    (group, kind, namespace, name). The first record in the deterministic
    collection order wins; the rest are summarized in a plan note, never
    dropped in silence.
    """
    kept, seen, repeats = [], {}, []
    for record in records:
        identity = (api_group(record["api_version"]), record["kind"],
                    record["namespace"], record["name"])
        if not record["name"] or identity not in seen:
            if record["name"]:
                seen[identity] = record
            kept.append(record)
        else:
            repeats.append((seen[identity], record))
    if repeats:
        shown = sorted({f"{d['kind']} {d['namespace'] or '(no namespace)'}/"
                        f"{d['name']} ({d['path']} after {f['path']})"
                        for f, d in repeats})
        plan_notes.append(
            f"{len(repeats)} rendered document(s) repeat an object identity "
            "already classified from another stream and were counted once: "
            + "; ".join(shown[:10]) + (" …" if len(shown) > 10 else "")
            + ". Shared kustomize bases and multiply-included charts emit the "
            "same object more than once; the plan classifies each object once "
            "so the translate step gets one unit of work per object. If any repeat is "
            "genuinely a different object, narrow the component scope.")
    return kept


def _coverage_notes(parts: dict, accounted: set, carved: list) -> list:
    """What the renders did NOT account for, per source directory.

    `partition_sources` keeps a source dir's files out of `plain` because
    they are "consumed via render" — these two notes are what make that claim
    checkable: files the render emitted nothing for, and scope exclusions the
    render cannot honour because it is handed the whole directory.
    """
    notes = []
    for source_dir, files in sorted(parts["members"].items()):
        label = source_label(source_dir)
        missed = [f for f in files if f not in accounted]
        if missed:
            notes.append(
                f"{label}: {len(missed)} in-scope file(s) produced no "
                "document in the render and are NOT classified: "
                + ", ".join(missed[:12]) + (" …" if len(missed) > 12 else "")
                + ". A conditional template, an unreferenced resource, or a "
                "file the render does not read all look like this — confirm "
                "each is intentional rather than a silently dropped document.")
        hidden = [p for p in carved if _under(p, source_dir)]
        if hidden:
            notes.append(
                f"{label}: the confirmed scope EXCLUDES "
                + ", ".join(hidden[:12]) + (" …" if len(hidden) > 12 else "")
                + ", but helm/kubectl are handed the whole directory and read "
                "them off disk, so the render re-includes them and their "
                "documents ARE classified. The exclusion cannot be honoured "
                "inside a render source: exclude the source directory itself, "
                "or resolve it in the chart's own values, and re-plan.")
    return notes


def build_workload_plan(component: str, scope: dict, source_root: str,
                        exports, renderers: dict = None) -> dict:
    """Builds the component's workload translation plan. Pure + injectable.

    scope: the confirmed scope.json content ({"included", "excluded"} globs
    over the developer's local clone). exports: the exports.json document as
    a plain dict, or None (absent -> degraded, stamped nulls).
    renderers: {"helm": fn, "kustomize": fn} with the module functions'
    signatures; tests inject pre-rendered streams. Deterministic: same files
    + same exports -> byte-identical plan.

    Every family appears exactly once: planned when it has documents, parked
    for wkld-routing with documents while exports.gateway is null (the facts
    exist, the attach point does not), or a skipped placeholder whose
    skip_note is literally true about what the scoped documents lack.
    """
    renderers = renderers or {"helm": helm_template,
                              "kustomize": kubectl_kustomize}
    plan_notes = []
    sources = {"charts": [], "kustomize": []}
    carriers, degraded = [], []

    paths, carved, walk_notes = enumerate_scoped_files(source_root, scope)
    plan_notes.extend(walk_notes)
    parts = partition_sources(paths)
    for nested in parts["nested"]:
        plan_notes.append(
            f"{source_label(nested)}: a render source nested inside another "
            "one; the enclosing source's render emits it, so it is not "
            "rendered separately (one carrier per chart tree)")
    accounted = set()
    records = _collect_records(source_root, parts, renderers, plan_notes,
                               sources, carriers, degraded, accounted)
    records += _collect_kustomize(source_root, parts, renderers, plan_notes,
                                  sources, degraded, accounted)
    records = _dedup_by_identity(records, plan_notes)
    coverage = _coverage_notes(parts, accounted, carved)
    plan_notes.extend(coverage)
    degraded.extend(coverage)

    by_family = {family: [] for family in FAMILIES}
    for record in records:
        by_family[record["family"]].append(record)

    units = [
        _build_manifests_unit(by_family["wkld-manifests"], exports, carriers,
                              degraded),
        _build_family_unit("wkld-identity", by_family["wkld-identity"],
                           lambda recs: _identity_notes(
                               recs, exports, _chart_cites(recs))),
        _build_storage_unit(by_family["wkld-storage"],
                            by_family["wkld-manifests"], exports),
        _build_routing_unit(by_family["wkld-routing"], exports),
    ]
    return {
        "component": component,
        "units": units,
        "sources": sources,
        "carriers": carriers,
        "notes": plan_notes,
        "exports_stamp": _exports_stamp(exports, plan_notes),
    }


# Bookkeeping, not brief input: these move on every publish and say nothing
# about what a brief was derived from. `data_gate` is here for the same
# reason and one more: it moves on every operator report of a database
# landing, and leaving it in would make the gateway unpark refuse ("exports
# changed BEYOND gateway, re-plan instead") over a fact no brief was ever
# derived from. The gate that reads it is workload/datagate, which does not
# go through a brief at all.
_STAMP_EXCLUDED_FIELDS = ("generated_at", "generations", "derivation_notes",
                          "data_gate")


def non_gateway_digest(exports) -> str | None:
    """A digest over every exports field EXCEPT `gateway` and bookkeeping.

    The unpark refresh can only rebuild the wkld-routing brief — it is the
    one family whose brief is derived from persisted facts (ingress_facts)
    and exports alone; wkld-manifests persists its pod DNS facts too, but
    its brief also reads the image map, the cluster type and the parsed
    documents, and the emitted document locators carry no spec. So the
    unpark may advance the plan's
    stamp only when the gateway is the ONLY thing that moved; this digest is
    how refresh_parked_routing knows. Anything else changing means the other
    briefs are stale and the plan must be rebuilt (STATE_WKLD_REVIEW's
    on_replan edge exists for exactly that).
    """
    if not isinstance(exports, dict):
        return None
    payload = {k: v for k, v in sorted(exports.items())
               if k != "gateway" and k not in _STAMP_EXCLUDED_FIELDS}
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()


def _exports_stamp(exports, plan_notes: list) -> dict:
    if exports is None:
        plan_notes.append(
            "exports.json was not available when this plan was built: the "
            "stamp records explicit nulls — degraded but honest, never a "
            "guess (staleness cannot be cross-checked until it publishes).")
        return {"generated_at": None, "generations": None,
                "non_gateway_digest": None}
    return {"generated_at": exports.get("generated_at"),
            "generations": exports.get("generations"),
            "non_gateway_digest": non_gateway_digest(exports)}


def _emit(records: list) -> list:
    return [_strip_private(r) for r in records]


def _build_manifests_unit(records, exports, carriers, degraded) -> dict:
    if not records and not degraded:
        return _unit("wkld-manifests", "skipped",
                     {"documents": [], "pod_dns_facts": [], "pod_dns_unread": []},
                     [_skip_note("wkld-manifests")], placeholder=True)
    # A degraded render with no parsed documents still plans the unit: the
    # open question about the unrendered source is real work to review.
    facts = _pod_dns_facts(records)
    placement = [f for f in (_node_placement_facts(r) for r in records) if f]
    # `pod_dns_unread` names the render sources whose documents the plan
    # could not read: the facts are incomplete for them, and the contract
    # does not call a shipped literal invented while any is listed.
    return _unit("wkld-manifests", "planned",
                 {"documents": _emit(records), "pod_dns_facts": facts,
                  "pod_dns_unread": list(degraded),
                  "node_placement_facts": placement},
                 _manifests_notes(records, exports, carriers, degraded, facts))


def _build_family_unit(family, records, notes_fn) -> dict:
    if not records:
        return _unit(family, "skipped", {"documents": []},
                     [_skip_note(family)], placeholder=True)
    return _unit(family, "planned", {"documents": _emit(records)},
                 notes_fn(records))


def _build_storage_unit(records, manifest_records, exports) -> dict:
    sts_facts = _sts_vct_facts(manifest_records)
    if not records:
        notes = [_skip_note("wkld-storage")]
        if sts_facts:
            notes += _storage_notes([], exports, sts_facts, [])[1:]
        return _unit("wkld-storage", "skipped", {"documents": []}, notes,
                     placeholder=True)
    return _unit("wkld-storage", "planned", {"documents": _emit(records)},
                 _storage_notes(records, exports, sts_facts,
                                _chart_cites(records)))


GATEWAY_ATTACH_FIELDS = ("name", "namespace")


def gateway_attach_gaps(exports) -> list:
    """Which parentRefs contract fields exports.gateway is missing.

    [] means the gateway is publishable as a verbatim attach point. A
    non-dict gateway returns every field. The whole live brief copies
    {name, namespace} into parentRefs and forbids the worker from inventing
    or 'correcting' them, so a field that is null/blank is NOT a live
    contract: `namespace: null` is rejected by the API server, and the only
    other way out is the invention the same brief forbids. A Gateway
    manifest without an explicit metadata.namespace (routine in
    kustomize/GitOps layouts) is exactly how exports publishes one.
    """
    gateway = (exports or {}).get("gateway") if isinstance(exports, dict) \
        else None
    if not isinstance(gateway, dict):
        return list(GATEWAY_ATTACH_FIELDS)
    return [f for f in GATEWAY_ATTACH_FIELDS
            if not (isinstance(gateway.get(f), str) and gateway[f].strip())]


def _build_routing_unit(records, exports) -> dict:
    if not records:
        return _unit("wkld-routing", "skipped", {"documents": []},
                     [_skip_note("wkld-routing")], placeholder=True)
    facts = [_ingress_facts(r) for r in records]
    status = "planned" if not gateway_attach_gaps(exports) else "parked"
    return _unit("wkld-routing", status,
                 {"documents": _emit(records), "ingress_facts": facts},
                 _routing_notes(facts, exports, _chart_cites(records)))


def refresh_parked_routing(plan: dict, exports) -> tuple:
    """(new_plan, changed, refusal): unparks parked wkld-routing units once
    exports publishes a COMPLETE gateway — the translate-entry integration
    point.

    Re-derives each parked routing unit's brief through the SAME code path a
    fresh plan uses (from the ingress_facts the plan persisted) and flips it
    parked -> planned. Advancing the plan's exports_stamp is what makes the
    re-stamped plan honest for the whole component, so two things ride with
    it:

    * The unpark REFUSES when anything other than `gateway` moved in exports
      since the plan was built (non_gateway_digest). Only wkld-routing
      persists the facts its brief was derived from, so no other brief can be
      rebuilt here — advancing the stamp over a stale storage/identity brief
      would stamp it CURRENT and defeat the decision-8 gate outright. That
      case is a re-plan (STATE_WKLD_REVIEW -> on_replan -> STATE_WKLD_PLAN),
      and the refusal says so.
    * Units already `done` are demoted to `planned`. Their blobs carry the
      old generations, `done` is not in the translate step's PENDING_STATUSES,
      and validate cross-checks every done blob against the CURRENT exports —
      so leaving them `done` guarantees a failed validation with no way
      forward but a re-plan. Demoted, they fail reuse conjunct (d) and
      genuinely re-run, which is the settled staleness posture.

    Returns the plan UNTOUCHED (changed False) when there is nothing to
    unpark, the gateway is absent or partial, or a parked unit predates the
    persisted-facts plan shape (re-plan instead — rebuilding its brief
    without facts would fabricate claims). `refusal` is a reason string when
    an unpark was possible but declined, else "".
    """
    if gateway_attach_gaps(exports):
        return plan, False, ""
    target_ids = {u["unit_id"] for u in plan.get("units", [])
                  if u.get("family") == "wkld-routing"
                  and u.get("status") == "parked"
                  and (u.get("inputs") or {}).get("ingress_facts")}
    if not target_ids:
        return plan, False, ""
    stamped = (plan.get("exports_stamp") or {}).get("non_gateway_digest")
    current = non_gateway_digest(exports)
    if stamped != current:
        return plan, False, (
            "exports.json changed in fields BEYOND `gateway` since this plan "
            "was built, so the other units' briefs no longer describe the "
            "current exports and this step cannot rebuild them (only "
            "wkld-routing's brief is rebuildable from persisted facts). Unparking would "
            "re-stamp those stale briefs as current and defeat the staleness "
            "gate. Re-plan instead: approve_workload_translation("
            "action='replan') rebuilds every brief and the stamp from the "
            "current exports."
            + ("" if stamped is not None else
               " (This plan predates the non_gateway_digest stamp, so the "
               "comparison cannot be made at all.)"))
    new_units = []
    for unit in plan.get("units", []):
        if unit["unit_id"] in target_ids:
            inputs = unit.get("inputs") or {}
            refreshed = dict(unit)
            refreshed.update({
                "status": "planned", "planned_status": "planned",
                "title": _title("wkld-routing", "planned"),
                "feedback": None, "error": None,
                "notes": _routing_notes(
                    inputs["ingress_facts"], exports,
                    _chart_cites(inputs.get("documents") or []))})
            new_units.append(refreshed)
        elif unit.get("status") == "done":
            new_units.append({**unit, "status": "planned",
                              "feedback": None, "error": None})
        else:
            new_units.append(dict(unit))
    return {**plan, "units": new_units,
            "exports_stamp": {
                "generated_at": exports.get("generated_at"),
                "generations": exports.get("generations"),
                "non_gateway_digest": current}}, True, ""


def redone_at_unpark(before: dict, after: dict) -> list:
    """Unit ids the unpark demoted from done -> planned, for the report."""
    was_done = {u["unit_id"] for u in before.get("units", [])
                if u.get("status") == "done"}
    return sorted(u["unit_id"] for u in after.get("units", [])
                  if u["unit_id"] in was_done and u.get("status") == "planned")


# --- plan utilities (tool layer + review) -----------------------------------


def active_units(plan: dict) -> list:
    """Units that carry work — everything not skipped (parked counts: it is
    listed coverage awaiting its attach point, not an opt-out)."""
    return [u for u in plan.get("units", []) if u.get("status") != "skipped"]


def all_placeholders(plan: dict) -> bool:
    """True when every unit is a no-facts placeholder."""
    units = plan.get("units", [])
    return bool(units) and all(u.get("placeholder") for u in units)


def classified_documents(plan: dict) -> int:
    """How many documents the plan actually classified, across all units."""
    return sum(len((u.get("inputs") or {}).get("documents") or [])
               for u in plan.get("units", []))


def empty_plan_reason(plan: dict):
    """None when the plan classified at least one document; otherwise the
    reason the tool layer refuses to advance it.

    Keyed on the document count, not on the placeholder flags: a source that
    failed to render leaves wkld-manifests `planned` (its open question is
    real work to review) with nothing in it, so the flags alone cannot see an
    empty plan — and signing one off would hand the translate step a component with no
    documents. The two cases get different advice because they have different
    remedies.
    """
    if not plan.get("units") or classified_documents(plan) > 0:
        return None
    failed = [n for n in plan.get("notes", [])
              if "render failed" in n or "not rendered" in n]
    if failed:
        return ("every source in the confirmed scope failed to render, so no "
                "document was classified: " + "; ".join(failed)
                + ". Fix the render (or vendor the bases) and plan again — "
                "amending the scope would only hide it.")
    return ("the confirmed scope selected no classifiable document under the "
            "source root, so every unit is a no-facts placeholder.")


def set_unit_status(plan: dict, unit_ids: list, status: str,
                    feedback: str = None) -> tuple:
    """Returns (new_plan, notes). Pure: unknown unit IDs are reported, not
    fatal (the landing-zone review idiom)."""
    known = {u["unit_id"] for u in plan.get("units", [])}
    notes, new_units = [], []
    targets = set(unit_ids or [])
    for unit in plan.get("units", []):
        if unit["unit_id"] in targets:
            updated = dict(unit)
            updated["status"] = status
            if feedback is not None:
                updated["feedback"] = feedback
            new_units.append(updated)
            notes.append(f"{unit['unit_id']} -> {status}")
        else:
            new_units.append(dict(unit))
    for missing in sorted(targets - known):
        notes.append(f"'{missing}' not found in plan (ignored)")
    return {**plan, "units": new_units}, notes


def restore_units(plan: dict, unit_ids: list) -> tuple:
    """Unskip. Returns (new_plan, notes) — the only status edit that can
    make the plan LIE, so it is guarded rather than symmetric with skip.

    A placeholder unit carries no facts: restoring it hands a worker empty
    inputs, so it is REFUSED and stays skipped (amend the scope instead).
    Anything else returns to planned_status — the status the facts produced
    — so a unit the planner parked comes back parked, not promoted: nothing
    about the fact that parked it changed while it was skipped.
    """
    known = {u["unit_id"] for u in plan.get("units", [])}
    targets = set(unit_ids or [])
    notes, new_units = [], []
    for unit in plan.get("units", []):
        if unit["unit_id"] not in targets:
            new_units.append(dict(unit))
            continue
        if unit.get("placeholder"):
            new_units.append(dict(unit))
            notes.append(
                f"{unit['unit_id']} NOT restored: it is a no-facts "
                "placeholder, and unskipping it would hand a worker empty "
                "inputs. Amend the component scope and re-plan.")
            continue
        restored = dict(unit)
        restored["status"] = unit.get("planned_status") or "planned"
        new_units.append(restored)
        notes.append(f"{unit['unit_id']} -> {restored['status']}"
                     + (" (the status the facts produced; it was parked "
                        "before it was skipped)"
                        if restored["status"] == "parked" else ""))
    for missing in sorted(targets - known):
        notes.append(f"'{missing}' not found in plan (ignored)")
    return {**plan, "units": new_units}, notes


def summarize_plan(plan: dict) -> str:
    """Human-readable unit/family/status summary, placeholders and parked
    units included — coverage, not silence."""
    import json
    lines = []
    for unit in plan.get("units", []):
        line = {"unit_id": unit["unit_id"], "status": unit["status"],
                "documents": len((unit.get("inputs") or {})
                                 .get("documents") or [])}
        if unit.get("placeholder"):
            line["placeholder"] = True
        if unit.get("notes"):
            line["notes"] = unit["notes"]
        lines.append(line)
    return json.dumps({
        "component": plan.get("component"),
        "exports_stamp": plan.get("exports_stamp"),
        "carriers": plan.get("carriers", []),
        "plan_notes": plan.get("notes", []),
        "units": lines,
    }, indent=2)
