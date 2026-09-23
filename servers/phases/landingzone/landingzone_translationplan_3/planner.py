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

"""Deterministic decomposition of the discovery inventory into translation units.

Pure code, no LLM: every problem the discovery inventory surfaced becomes a
bounded, independently-translatable unit carrying exactly the inventory slice
and landing-zone decisions a translation worker needs. Unit IDs are stable so
results cache and revisions target specific units.

A unit family whose inventory section holds no facts is not silently omitted:
it appears as a single `skipped` placeholder naming the empty section, so plan
review sees what was NOT found. An extraction gap that starves a section turns
into a reviewable line instead of a unit that quietly never existed.
"""

import re

from servers.dag.server import decisions as decisions_lib
from servers.dag.server.exports import (GATEWAY_ACCESS_LABEL,
                                        GATEWAY_ACCESS_VALUE)

# The attach-permission label literal both briefs order verbatim: the gateway
# unit's `from: Selector` policy and Namespace, and every tenancy Namespace.
# One product-owned definition (exports, beside SHARED_GATEWAY_MARKER), so
# the briefs and the validate gate (gateway_contract) can never drift.
_ACCESS_LABEL_LITERAL = f'{GATEWAY_ACCESS_LABEL}: "{GATEWAY_ACCESS_VALUE}"'

# Which coverage-map rows each unit family claims to generate — the keys are
# the planner's unit kinds, the values are normalized artifact-kind row keys
# from landingzone/knowledge/coverage-map.md (normalized as
# servers/dag/server/coverage_map.normalize_kind does: backticks stripped,
# whitespace collapsed, casefolded). Written as literals so the planner stays
# import-light; the planner_test invariant binds every value to the live map,
# so a renamed row or a retired family fails a test instead of drifting.
# v1 citation is per FAMILY at the map's section granularity: every unit of a
# family carries the family's rows, and a no-facts placeholder carries the
# same rows — it is the family's explicit coverage claim over the empty
# section. Per-fact citation is deferred with the rest of fact-level coverage.
# The citations feed two consumers: the plan-time observe-only checks
# (coverage.run_coverage_checks) and the validate step's ENFORCED omission
# gate (translation_validate_3/coverage_gate.py).
FAMILY_COVERS = {
    "cluster-addons": ["cluster addon disposition (drop / built-in / reconfigure)"],
    "autoscaling": ["node auto-provisioning (karpenter replacement)"],
    # One ComputeClass per typed Karpenter NodePool; the row is sourced from
    # the typed field alone (autoscaling.karpenter_nodepools), so inventories
    # extracted before the field existed read no-facts and plan a placeholder.
    "compute-class": ["computeclass (karpenter nodepool replacement)"],
    "node-pool": ["gke node pools"],
    "workload-policy": ["privileged workload policy"],
    "host-network": ["hostnetwork workload compatibility"],
    "workload-identity": ["workload identity: gsa and iam bindings"],
    "network": ["cross-vpc connectivity and load balancers"],
    "storage": ["storageclass tier menu"],
    "gateway": ["gateway (shared entry point)"],
    "tenancy": ["namespaces", "resourcequota and limitrange"],
    # Two rows, one family: the resolver ConfigMap (k8s, Standard only) and
    # the Cloud DNS zones (terraform). Both are sourced from `cluster_dns`, so
    # both are facts-present together; a done unit satisfies the pair.
    "cluster-dns": ["cluster dns resolver configuration (stub domains, upstream nameservers)",
                    "cluster dns zones and records (hosts entries, forwarding zones)"],
}

# Which landing-zone decision ids each unit family consumes. Every unit of a
# consuming family carries `inputs.decisions = {id: token | null}` with
# exactly these keys (family-level: a non-GPU node pool carries gpu_tpu: null),
# so a test can bind family <-> decision by id. Decision-free families carry
# no `decisions` key. The cluster MODE is never read off these tokens: it is
# `decisions_lib.cluster_mode`, the one projection every reader shares
# (DESIGN §7.2; coverage-guards G0).
FAMILY_DECISIONS = {
    "cluster-dns": ("karpenter", "privileged_daemonsets", "gpu_tpu"),
    "autoscaling": ("karpenter",),
    "compute-class": ("karpenter",),
    "node-pool": ("karpenter", "gpu_tpu"),
    "workload-policy": ("privileged_daemonsets",),
    "network": ("vpc_peering",),
}


def family_decisions(kind: str, choices: dict) -> dict:
    """{id: recorded token or None} for the ids a family consumes; {} for a
    decision-free family (the caller omits the key then)."""
    return {did: choices.get(did) for did in FAMILY_DECISIONS.get(kind, ())}


# A discovery scan note saying a CoreDNS configuration was seen and NOT
# recorded — "not recorded", "was not read", "not read (…)", "is unknown" are
# the phrasings clusterdns.py uses across its unread branches (a variable, a
# file(), an expression-valued addons map, a data map that is not literal,
# an unparseable yaml_body, a Helm release).
# Two conditions, because the walk's bookkeeping ("3 YAML file(s) under Helm
# chart roots were not read for cluster DNS configuration", "a block is never
# closed … were not read") also says "not read" without any CoreDNS artifact
# having been seen — and the WARNING claims one was.
_UNREAD_PHRASE_RE = re.compile(r"not (?:recorded|read)\b|is unknown|skipped, larger than|unreadable \(")
_COREDNS_NOTE_RE = re.compile(r"coredns|node-local-dns|Corefile|configuration it carries", re.I)
_BOOKKEEPING_NOTE_RE = re.compile(r"were not read for cluster DNS configuration")


def _unread_coredns_note(note: str) -> bool:
    """A scan note saying a CoreDNS configuration was seen and NOT recorded —
    including a file the walk skipped or could not open whose path names
    coredns or node-local-dns."""
    return bool(_UNREAD_PHRASE_RE.search(note) and _COREDNS_NOTE_RE.search(note)
                and not _BOOKKEEPING_NOTE_RE.search(note))


# Scan notes that qualify a recorded text or name configuration beside it
# that was not recorded: truncated, reaching for values outside the file,
# demoted by the section cap, or unread. Relayed in the planned brief so
# "verbatim" is not overstated and the worker knows what the inputs do not
# settle.
_QUALIFYING_NOTE_RE = re.compile(
    r"truncated|also references|section is capped|on the assumption|pod settings")


def slugify(text: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", str(text).lower()).strip("-")
    return slug or "unnamed"


def _unit(unit_id: str, kind: str, title: str, inputs: dict, notes: list = None, status: str = "planned",
          placeholder: bool = False) -> dict:
    return {
        "unit_id": unit_id,
        "kind": kind,
        "title": title,
        "status": status,  # planned | skipped | done | revise | error
        # True only for the no-facts coverage placeholders. Distinguishes them
        # from real units that a landing-zone decision skipped (e.g. Autopilot
        # node pools, which carry facts and can legitimately be unskipped).
        "placeholder": placeholder,
        # The coverage-map rows this unit covers, cited by family (see
        # FAMILY_COVERS). Consumed by coverage.run_coverage_checks and the
        # validate step's coverage_gate.
        "covers": list(FAMILY_COVERS.get(kind, [])),
        "inputs": inputs,
        "notes": notes or [],
        "feedback": None,
        "error": None,
    }


def _cluster_dns_notes(sources: list, mode, mode_reason=None, scan_notes=()) -> list:
    """Notes for a planned cluster-dns unit: the facts, the mode, the fences.

    Deliberately NOT the mapping — that is `cluster-dns-translation.md`,
    attached to the worker prompt by unit kind, so it never rides into the
    plan summary, the Gate C elicitation or state.json, and an edit to it
    reaches the next worker run without a re-plan. Like every brief, the
    wording is conditioned on what the inputs record.
    """
    where = []
    for s in sources:
        if not isinstance(s, dict):
            where.append(f"an entry of unexpected shape ({type(s).__name__})")
            continue
        label = s.get("address") or s.get("path") or "?"
        where.append(f"{s.get('kind')} {s.get('name')} at {label} ({s.get('form')})")
    qualifying = [n for n in scan_notes
                  if _QUALIFYING_NOTE_RE.search(n) or _unread_coredns_note(n)]
    notes = [
        "inputs.cluster_dns.sources carries the source cluster's DNS configuration "
        "VERBATIM — " + "; ".join(where) + ". Each `text` is a CoreDNS Corefile, or the "
        "managed add-on's configuration_values — which may wrap a Corefile under its "
        "corefile key or carry only add-on settings — as the repository declares it; "
        "nothing has interpreted it. Read it as CoreDNS reads it."
        + (" The scan qualified what it recorded — inputs.scan_notes: " + " | ".join(qualifying)
           + " — so a truncated tail, a referenced value, a capped source or an unread "
           "one is not in these inputs; say so in open_questions rather than guessing "
           "what it held." if qualifying else ""),
        "The mapping to Cloud DNS for GKE arrives with this unit's translation prompt "
        "(cluster-dns-translation.md), with the output contract the validate gate "
        "checks: target shape, owner boundary, and that every IP and name in your "
        "files comes from the source text and every source IP is accounted for.",
    ]
    if mode is None and mode_reason and mode_reason.startswith("disagree"):
        notes.append(
            "inputs.cluster_mode is null because the recorded landing-zone decisions imply "
            f"different cluster modes ({mode_reason[len('disagree: '):]}). That is a "
            "design conflict for the landing zone to settle: use the Autopilot column "
            "of the mapping, name the conflict in open_questions, and do not pick a "
            "mode yourself.")
    elif mode is None:
        notes.append(
            "inputs.cluster_mode is null: no landing-zone decision recorded the cluster "
            "mode. Use the Autopilot column of the mapping and say in assumptions "
            "that the mode was not recorded.")
    else:
        notes.append(
            f"inputs.cluster_mode = {mode}: the cluster mode derived from the landing-zone "
            "decisions. Use it, not the raw landing_zone_decisions or inputs.decision, "
            "to pick the mapping column.")
    notes.append(
        "Never emit google_container_cluster, a dns_config or addons_config block, a "
        "google_dns_policy, a coredns or node-local-dns Deployment, DaemonSet or "
        "ConfigMap, or any kube-system object other than the kube-dns ConfigMap: the "
        "cluster and its DNS provider are the landing zone's, the resolver pods are "
        "GKE's. The cluster-addons unit does not translate CoreDNS either; this unit "
        "does.")
    return notes


def _compute_class_notes(pool: dict) -> list:
    """Notes for a planned compute-class unit: one ComputeClass per NodePool.

    Same grammar as _storage_notes: derived only from the typed facts of
    inputs.nodepool, literally true against them, inventing nothing. The
    mapping and the family table are NOT here — they ride with the worker
    prompt (gke-compute-classes.md, via translator.FAMILY_KNOWLEDGE) so an
    edit reaches the next run without a re-plan and the brief stays short
    enough for the plan summary and the Gate C prompt.
    """
    name = str(pool.get("name") or "?")
    capacity = [str(c) for c in pool.get("capacity_types") or []]
    families = [str(f) for f in pool.get("instance_families") or []]
    arches = [str(a) for a in pool.get("architectures") or []]
    unreduced = [str(k) for k in pool.get("requirements_unreduced") or []]
    limits = pool.get("limits") if isinstance(pool.get("limits"), dict) else {}
    taints = [t for t in pool.get("taints") or [] if isinstance(t, dict)]
    notes = [
        f"Emit exactly ONE Kubernetes ComputeClass manifest (.yaml), apiVersion "
        f"cloud.google.com/v1, metadata.name `{name}` — byte-identical to the source "
        "NodePool name, because the workload rewrite swaps `karpenter.sh/nodepool: "
        f"{name}` for `cloud.google.com/compute-class: {name}`. Cluster-scoped: no "
        "metadata.namespace. No other kind, no Terraform.",
        "spec.nodePoolAutoCreation.enabled: true and spec.whenUnsatisfiable: DoNotScaleUp "
        "(a Karpenter migration accepts Pending over an unchosen family). The mapping, "
        "the AWS-to-GCP family table and the output contract the validate step checks "
        "arrive with this unit's translation prompt (gke-compute-classes.md).",
    ]
    if capacity:
        has_spot, has_od = "spot" in capacity, "on-demand" in capacity
        if has_spot and has_od:
            rule = "spot priorities first, then an on-demand floor (`spot: false`); "
        elif has_spot:
            rule = ("spot priorities; an on-demand floor the source did not have is allowed only "
                    "as a recorded tradeoff; ")
        else:
            rule = "no priority may set `spot: true`; "
        if "reserved" in capacity:
            rule += ("a `reserved` capacity type has no ComputeClass field: route it to "
                     "open_questions; ")
        notes.append(
            "inputs.nodepool.capacity_types records " + ", ".join(capacity) + ": " + rule
            + "never introduce spot the source did not allow.")
    else:
        notes.append(
            "inputs.nodepool.capacity_types records no capacity type"
            + (" (the requirement uses an operator the merger does not reduce; see "
               "requirements_unreduced)" if "karpenter.sh/capacity-type" in unreduced else "")
            + ": no priority may set `spot: true`; state the on-demand assumption.")
    if families:
        notes.append(
            "inputs.nodepool.instance_families records " + ", ".join(families)
            + ": one priority per family, in that order, each `machineFamily` taken from "
            "the family table's row for that AWS family; name the pick in tradeoffs. A "
            "family the table does not list has no candidates: name it in open_questions "
            "and do not guess a GCE family for it. Memory-heavy families map to a general "
            "family plus `minMemoryGb`, never to a `-highmem` machineFamily value.")
    else:
        notes.append(
            "inputs.nodepool.instance_families records no family constraint: use the "
            "table's no-constraint row" + (" for " + "/".join(arches) if arches else "")
            + " and state the family choice in assumptions.")
    if arches:
        notes.append("inputs.nodepool.architectures records " + ", ".join(arches)
                     + ": every machineFamily must match one of these architectures.")
    if unreduced:
        notes.append(
            "These requirement keys use an operator the merger does not reduce to a list "
            "(" + ", ".join(unreduced) + "; the verbatim requirement is in "
            "inputs.nodepool.requirements): route each to open_questions by key, never "
            "guess what it constrains.")
    if limits:
        notes.append(
            "inputs.nodepool.limits records " + ", ".join(f"{k}={v}" for k, v in limits.items())
            + ": a ComputeClass has no limits field; state the value and its disposition "
            "(CapacityQuota on GKE 1.36.2+, or cluster NAP resource_limits) in tradeoffs — "
            "the number must appear in your files or your prose.")
    if taints:
        notes.append(
            "inputs.nodepool.taints records " + ", ".join(str(t.get("key")) for t in taints)
            + ": carry each as nodePoolConfig.taints with the same key, value and effect; a "
            "key containing kubernetes.io is refused by GKE and goes to open_questions. Every "
            "taint key must appear in your files or your prose.")
    notes.append(
        "Route to open_questions: the GKE minimum version (nodePoolAutoCreation needs "
        "1.33.3+; do not pin a version literal), the zones the class should prefer (never "
        "invent a region or zone), and CUD or reservation alignment for the families you "
        "chose. Route to tradeoffs: the disruption and weight mapping, and the limits "
        "disposition above.")
    notes.append(
        "Never emit google_container_cluster, google_container_node_pool, a "
        "PodDisruptionBudget, a CapacityQuota or any kind other than ComputeClass: the "
        "cluster is the landing zone's, node pools are the node-pool units', PDBs are the "
        "workloads'.")
    return notes


def _storage_notes(storage: dict) -> list:
    """Notes for a planned storage unit: the worker's StorageClass brief.

    The translation worker contract (TRANSLATE_RULES) stays unit-agnostic — it
    says in-cluster objects ship as Kubernetes YAML and nothing storage-specific
    — so everything StorageClass-shaped rides here, derived only from the
    discovered facts (CSI driver flags and StorageClass names). Every value the
    inventory does not state is explicitly routed to assumptions or open
    questions instead of being invented, and like skip_note the wording must
    stay literally true against the inputs. The storage dict is an open
    mapping (the schema allows extra keys and the merger preserves them), so
    no note claims a value is undiscovered outright — claims are conditioned
    on what inputs.storage records.
    """
    ebs = bool(storage.get("ebs_csi"))
    efs = bool(storage.get("efs_csi"))
    classes = [str(name) for name in storage.get("storage_classes") or []]
    notes = []
    if classes:
        notes.append(
            "Emit Kubernetes StorageClass manifests (.yaml), one per discovered "
            "StorageClass name — " + ", ".join(classes) + " — keeping each source "
            "name so existing PVC storageClassName references keep resolving."
        )
    else:
        menu = "a general-purpose class (type pd-balanced) and a performance class (type pd-ssd)"
        if efs:
            menu += ", plus a Filestore class for shared file volumes"
        notes.append(
            "inventory.storage.storage_classes names no StorageClasses; emit a "
            f"small GKE tier menu as Kubernetes StorageClass manifests (.yaml) — {menu} — "
            "and name the menu choice in tradeoffs."
        )
    if ebs:
        notes.append(
            "ebs.csi.aws.com maps to provisioner pd.csi.storage.gke.io (GKE's "
            "built-in Persistent Disk CSI driver): gp2/gp3 -> parameters type "
            "pd-balanced; io1/io2 -> type pd-ssd by default, or hyperdisk-extreme "
            "where the node pools' machine series supports it — state the choice "
            "in tradeoffs. A volume type implied only by a class name (e.g. a "
            "'gp3-' prefix) is an assumption to state, not a discovered fact."
        )
    if efs:
        notes.append(
            "efs.csi.aws.com maps to provisioner filestore.csi.storage.gke.io "
            "(Filestore CSI driver; it must be enabled on the cluster — flag that "
            "as an assumption). Filestore tier and capacity go to open_questions "
            "unless inputs.storage records them."
        )
    if not ebs:
        notes.append(
            "inventory.storage does not record ebs_csi as true. For any "
            "block-storage (PD-backed) class, use pd.csi.storage.gke.io "
            "(GKE's built-in Persistent Disk CSI driver) and state the choice "
            "in assumptions; if inputs.storage records provisioner evidence "
            "instead, translate it (ebs.csi.aws.com -> pd.csi.storage.gke.io, "
            "efs.csi.aws.com -> filestore.csi.storage.gke.io) — never copy an "
            "AWS provisioner into a GKE manifest. A volume type implied only "
            "by a class name (e.g. a 'gp3-' prefix) is an assumption to "
            "state, not a discovered fact."
        )
    notes.append(
        "Set volumeBindingMode: WaitForFirstConsumer on every class (required "
        "for correct zonal placement of PD-backed volumes; harmless for "
        "Filestore classes)."
    )
    notes.append(
        "Honor every key inputs.storage records as a discovered fact; beyond "
        "that, invent nothing — encryption and other parameters are assumptions "
        "to state or open questions to raise. If inputs.storage records no "
        "reclaimPolicy, pick one deliberately and defend it in tradeoffs."
    )
    notes.append(
        "Do not set the storageclass.kubernetes.io/is-default-class annotation "
        "unless inputs.storage records which source class was the default; "
        "otherwise raise the cluster default choice as an open question."
    )
    return notes


def _string_entries(value) -> tuple:
    """(usable strings, unusable entries) from one open list recording.

    Only a non-empty plain string is quotable into a brief. A scalar
    recording is not a list at all (iterating it would fabricate
    per-character entries), and a list item that is a mapping — the
    natural shape for an Ingress rule, {"host": ..., "path": ...} — would
    be quoted as a Python repr; both are fabrications the literal-truth
    rule forbids, so both are named for open_questions instead.
    """
    if not isinstance(value, list):
        return [], []
    usable = [v.strip() for v in value if isinstance(v, str) and v.strip()]
    unusable = [repr(v) for v in value
                if not (isinstance(v, str) and v.strip())]
    return usable, unusable


def _gateway_facts(network: dict) -> tuple:
    """(hostnames, unusable hosts, load balancers, unusable load balancers).

    The network dict is open, so either key can arrive in a shape the
    brief cannot quote; such a recording is still entry-point evidence
    (the unit gets planned) but is never enumerated — see _string_entries.
    """
    hosts, host_rest = _string_entries(network.get("ingress_hosts"))
    balancers, lb_rest = _string_entries(network.get("load_balancers"))
    return hosts, host_rest, balancers, lb_rest


# Free-text entry-point mentions in network.evidence. Word-anchored so
# "global" and "albeit" do not read as load balancers.
_ENTRY_POINT_RE = re.compile(
    r"\b(albs?|elbs?|nlbs?|ingress(?:es)?|load[ -]?balancers?)\b", re.I)


def _entry_point_evidence(network: dict) -> list:
    """network.evidence lines mentioning an entry point in free text.

    The structured keys are what plans the family; this is only the
    discovery-gap signal for the placeholder, the same arrangement the
    storage placeholder uses against inventory.addons — extraction files
    facts in the wrong section often enough that silence is the worse
    failure. Deliberately NOT a trigger: a free-text mention never plans
    a unit, it only tells the plan reviewer to rescan.
    """
    return sorted({
        e.strip() for e in (network.get("evidence") or [])
        if isinstance(e, str) and _ENTRY_POINT_RE.search(e)
    })


def _recorded_namespace_names(clusters) -> list:
    """Sorted recorded namespace names, for the gateway brief's collision
    fence: the platform namespace the gateway unit chooses must not be a
    name the tenancy side of the plan issues (one name issued by two units
    ships two Namespace manifests for one object). Derived from the same
    merge the tenancy unit is planned from (_namespace_entries), so the
    fence and the issuance can never disagree about what those names are;
    recordings without a plain string name carry no name to collide with.
    """
    merged, _, _ = _namespace_entries(clusters)
    return [entry["name"] for entry in merged]


def _gateway_notes(network: dict, recorded_namespaces: list) -> list:
    """Notes for a planned gateway unit: the shared entry-point Gateway brief.

    Same grammar as _storage_notes: derived only from the discovered facts,
    literally true against them, inventing nothing. The network dict is an
    open mapping, so every claim is conditioned on what inputs.network
    records — ingress_hosts is honored when present, and certificate/DNS
    facts are never assumed absent: they route to open_questions unless
    the inputs record them (cert and DNS stay elicitation territory by
    design). The marker note is the exports interlock:
    exports.derive_translation_fields publishes exports.gateway only from
    a marked Gateway API manifest (SHARED_GATEWAY_MARKER).

    The attach contract is deliberately NOT the worker's to design (an audit
    of an early end-to-end run showed why): the free-form "explicit cross-namespace attach policy"
    wording let a worker invent a Selector label no Namespace carried —
    the API server accepted every HTTPRoute and attached none. allowedRoutes
    is now one of exactly two shapes over the product access label, the unit
    must itself create the platform namespace its Gateway lives in (exports
    derives the attach point from unit files, not the cluster, so an
    uncreatable Gateway would otherwise publish cleanly), and
    `recorded_namespaces` (the tenancy side's merged names) fences the
    namespace choice off the names the tenancy unit issues.
    """
    hosts, host_rest, balancers, lb_rest = _gateway_facts(network)
    notes = [
        "Emit exactly ONE Kubernetes Gateway API Gateway manifest (.yaml), "
        "apiVersion gateway.networking.k8s.io/v1, in a platform-owned "
        "namespace this unit chooses and states; set metadata.namespace "
        "literally in the manifest — exports.gateway.namespace is read from "
        "that field alone, so a namespace supplied only at apply time (a "
        "kustomize overlay, kubectl -n) publishes as null and no developer "
        "HTTPRoute can attach — and record the namespace choice in tradeoffs.",
        'The manifest MUST carry gkma.dev/shared-gateway: "true" in '
        "metadata.annotations (or labels): the exports derivation publishes "
        "exports.gateway only from a Gateway carrying this marker, so an "
        "unmarked Gateway is invisible to every developer and parks their "
        "routing units.",
        "Scope, so no artifact is emitted twice: the `network` unit owns the "
        "Terraform half of the entry point (peering / NCC, load balancer and "
        "address infrastructure) and emits no Gateway API objects — do not "
        "re-emit its resources here. HTTPRoutes are the workload phase's "
        "half of this pair: emit none, not even an example. This unit "
        "emits exactly TWO Kubernetes objects — the one Gateway, and the "
        "one platform Namespace ordered below — and NO other object kind.",
    ]
    notes.append(_gateway_class_note(network, balancers, lb_rest))
    notes.extend(_gateway_listener_notes(network, hosts, host_rest))
    notes.append(
        "Do not emit certificate configuration. Record TLS termination as "
        "an open question — certificates and DNS are elicitation territory; "
        "name certificate evidence (e.g. an ACM ARN) only if inputs.network "
        "records it, never invent or assume one."
    )
    notes.append(
        "Workload HTTPRoutes attach to this Gateway from other namespaces, "
        "so EVERY listener must set allowedRoutes.namespaces explicitly, "
        "to one of exactly two policies: `from: All` — record the "
        "open-attach tradeoff and raise tightening it as an open question "
        "— or `from: Selector` with matchLabels of exactly "
        f"`{_ACCESS_LABEL_LITERAL}`, the product-owned attach-permission "
        "label every Namespace this migration issues carries. Never "
        "`from: Same` and never a selector label of your own devising: "
        "the API server accepts both, and then accepts every developer "
        "HTTPRoute without ever attaching it — a silent failure. The "
        "validate step parses the shipped policy and fails the unit on "
        "either shape."
    )
    ns_note = (
        "Emit exactly ONE Kubernetes Namespace manifest (.yaml) beside "
        "the Gateway: metadata.name byte-identical to the Gateway's "
        f"metadata.namespace, carrying the `{_ACCESS_LABEL_LITERAL}` "
        "label. No other unit issues the platform namespace, so without "
        "this manifest nothing in the PR creates it and `kubectl apply` "
        'fails with `namespaces "<name>" not found` — while exports, '
        "derived from files rather than the cluster, still publishes the "
        "attach point, so every developer HTTPRoute applies cleanly and "
        "never attaches."
    )
    if recorded_namespaces:
        ns_note += (
            " Choose a platform-owned name that is none of the recorded "
            "workload namespaces — " + ", ".join(recorded_namespaces)
            + " — those are the tenancy unit's to issue; a collision "
            "ships two Namespace manifests for one name."
        )
    notes.append(ns_note)
    return notes


def _gateway_class_note(network: dict, balancers: list, unusable: list) -> str:
    """The gatewayClassName instruction, conditioned on the LB facts."""
    note = (
        "Set gatewayClassName to one of the GKE Gateway classes — external "
        "global (gke-l7-global-external-managed), external regional "
        "(gke-l7-regional-external-managed), or internal (gke-l7-rilb) — "
        "and defend the choice in tradeoffs. The two regional classes are "
        "Envoy-based and need a REGIONAL_MANAGED_PROXY proxy-only subnet in "
        "the VPC, which is a landing-zone-owned artifact no translation unit "
        "may emit: choosing one raises that subnet as an open question for "
        "the landing zone. "
    )
    if balancers:
        note += (
            "inputs.network.load_balancers records " + ", ".join(balancers)
            + " — the entry points this Gateway replaces; a scheme implied "
            "only by a load balancer name or an ALB annotation string in "
            "the evidence is an assumption to state, not a discovered fact."
        )
        if unusable:
            note += (" These entries do not parse as load balancer names: "
                     + ", ".join(unusable) + " — route each to "
                     "open_questions instead of quoting it.")
        return note
    if network.get("load_balancers"):
        return note + (
            "inputs.network.load_balancers does not parse as a list of "
            "names; route its content to open_questions, and state the "
            "scheme you choose as an assumption."
        )
    return note + (
        "inputs.network.load_balancers names no load balancers, so no "
        "discovered fact implies a scheme; state the scheme you choose "
        "as an assumption."
    )


def _gateway_listener_notes(network: dict, hosts: list, unusable: list) -> list:
    """The listener instruction: HTTP-on-80 default, hostnames only as
    recorded — a missing or unparseable host inventory routes to
    open_questions instead of an invented hostname."""
    default = "Default to one HTTP listener on port 80. "
    if hosts:
        note = default + (
            "inputs.network.ingress_hosts records " + ", ".join(hosts)
            + " — enumerate exactly these hostnames as listener "
            "hostnames, verbatim; invent no others.")
        if unusable:
            note += (" These entries do not parse as hostnames: "
                     + ", ".join(unusable) + " — route each to "
                     "open_questions instead of quoting it.")
        return [note]
    if network.get("ingress_hosts"):
        return [default + (
            "inputs.network.ingress_hosts does not parse as a list of "
            "hostnames; leave the listener hostname unset and route its "
            "content to open_questions instead of guessing.")]
    return [default + (
        "inputs.network records no ingress_hosts entries, so leave the "
        "listener hostname unset and route the estate's hostname "
        "inventory to open_questions.")]


def _namespace_entries(clusters) -> tuple:
    """(merged entries, conflicted names, unusable reprs) from clusters[].

    Tenancy facts live per cluster (clusters[].workloads.namespaces, open
    objects {name, deployments, ...}); one target cluster receives them
    all, so the plan needs the union. Merge rule: names are stripped of
    surrounding whitespace first (never part of a DNS-1123 name — a padded
    recording is the same namespace), then deduplicate by exact name,
    first recording wins (clusters[] order, then entry order),
    identical duplicates fold silently, differing duplicates keep the
    first recording and surface the name for open_questions — synthesis
    (summing counts, unioning keys) would present arithmetic as a
    discovered fact. Entries without a plain non-empty string name —
    recordings that are not lists at all, and a workloads section that
    is not a mapping — cannot be issued or quoted (see _string_entries
    on reprs), so they are returned for the brief to route to
    open_questions. Merged entries sort by name.
    """
    kept, conflicts, unusable = {}, set(), []
    for cluster in clusters or []:
        if not isinstance(cluster, dict):
            continue
        workloads = cluster.get("workloads")
        if workloads and not isinstance(workloads, dict):
            unusable.append(repr(workloads))
            continue
        recorded = (workloads or {}).get("namespaces")
        if recorded and not isinstance(recorded, list):
            unusable.append(repr(recorded))
            continue
        for entry in recorded or []:
            name = entry.get("name") if isinstance(entry, dict) else None
            if not (isinstance(name, str) and name.strip()):
                unusable.append(repr(entry))
                continue
            # Normalization, not synthesis: surrounding whitespace is never
            # part of a DNS-1123 name, so 'acme-shop ' and 'acme-shop' are
            # one namespace. Keying the dedupe on the padded form (as
            # recorded) would issue two Namespace+ResourceQuota pairs for
            # one real namespace — one with a name no cluster accepts.
            name = name.strip()
            if entry.get("name") != name:
                entry = {**entry, "name": name}
            if name not in kept:
                kept[name] = entry
            elif kept[name] != entry:
                conflicts.add(name)
    merged = [kept[name] for name in sorted(kept)]
    return merged, sorted(conflicts), sorted(set(unusable))


# Shared by both tenancy regimes: with usable names the fence bounds the
# issuance; with none it is (with the open_questions route) the whole brief —
# an issuance instruction beside quoted raw reprs would invite guessing.
_RBAC_LIMITRANGE_NOTE = (
    "Do not emit RBAC objects (Roles, RoleBindings, ClusterRoles, "
    "ClusterRoleBindings): namespace RBAC issuance is [PLANNED] and "
    "elicitation-based — RBAC facts come from the teams, never "
    "invented from the scan. If an entry records role-like keys, "
    "raise them in open_questions rather than issuing RBAC from a "
    "scan. Do not emit LimitRange objects unless an entry records "
    "limit facts; when one does, honor it literally."
)


def _tenancy_notes(namespaces: list, conflicts: list, unusable: list) -> list:
    """Notes for a planned tenancy unit: the Namespace + ResourceQuota brief.

    Same grammar as _storage_notes: derived only from the discovered facts,
    literally true against them, inventing nothing. Issuance is
    namespace-granularity by design: multi-team
    shared namespaces are an estate reality this design accepts — acme
    runs 3 teams in acme-shop — so the brief forbids per-team splitting.
    Entries are open objects, so quota claims are conditioned on what each
    entry records; with zero quota facts the ordered starter quotas are
    object-count-only (a compute quota would make requests/limits mandatory
    while the LimitRange that could default them stays forbidden); RBAC
    stays elicitation territory ([PLANNED]). When no entry parses to a
    usable name, the brief is the open_questions route plus the
    prohibitions — no issuance or quota instruction rides beside the
    quoted raw recordings.
    """
    names = [str(e.get("name")) for e in namespaces]
    if not names:
        # No usable name: the brief hands the worker the open_questions
        # route and the prohibitions ONLY. An issuance or quota note here
        # would sit beside the raw reprs the boundary note quotes — a
        # prohibition and an invitation to guess a name from them at once.
        return [
            "inventory.clusters[].workloads.namespaces records entries, but "
            "none parses as a namespace entry with a name: emit no Namespace "
            "or ResourceQuota manifests from guesses — this unit then emits "
            "no Kubernetes object at all — and route the estate's namespace "
            "inventory to open_questions.",
            _RBAC_LIMITRANGE_NOTE,
        ] + _tenancy_boundary_notes(conflicts, unusable)
    issue_note = (
        "Issue every recorded namespace — a recorded deployment count is a "
        "fact about the namespace, not an issuance threshold")
    if any(e.get("deployments") == 0 for e in namespaces):
        issue_note += (", so the entries recording zero deployments are "
                       "issued like every other")
    notes = [
        "Per recorded namespace — " + ", ".join(names) + " — emit one "
        "Kubernetes Namespace manifest and one ResourceQuota manifest "
        "(.yaml), the quota inside that namespace: namespace-granularity "
        "issuance, one pair per name. Use each name verbatim; never "
        "invent, rename, or split one.",
        issue_note + ".",
        "Scope, so no object is emitted twice or invented: this unit emits "
        "exactly one Namespace and one ResourceQuota per recorded name — "
        "plus a LimitRange only under the recorded-limit-facts condition "
        "below — and NO other Kubernetes object kind. No NetworkPolicy "
        "(default-deny hygiene included: isolation posture is not a "
        "recorded tenancy fact, and a default-deny invented from a scan "
        "would cut east-west traffic for the migrated workloads), no "
        "ServiceAccount, no RBAC, no workload objects, and nothing in any "
        "namespace this brief does not name. The platform namespace the "
        "shared Gateway itself lives in is the `gateway` unit's to emit, "
        "never this unit's: issue ONLY the recorded names above.",
        "Quota values: where an entry records only its name and deployment "
        "count, no quota value is a discovered fact — set conservative "
        "starter quotas on OBJECT COUNTS only (counts such as pods, "
        "services, persistentvolumeclaims), listed in assumptions as "
        "unverified, or route sizing entirely to open_questions; never "
        "present a quota number as discovered, and never constrain compute "
        "(cpu or memory) from zero facts: a compute quota makes "
        "requests/limits mandatory for every pod in the namespace, and the "
        "LimitRange that would supply defaults may not be emitted without "
        "recorded limit facts. Where an entry records quota or limit keys, "
        "honor them literally instead; if a recorded quota constrains cpu "
        "or memory and the entry records no limit facts, raise in "
        "open_questions that pods declaring no requests/limits will be "
        "rejected in that namespace.",
        "Tenancy issues per namespace, never per team: a namespace shared "
        "by several teams still receives exactly ONE Namespace and ONE "
        "ResourceQuota — the shared-tenancy estate reality this design "
        "accepts. When an entry records team signals (e.g. team labels), "
        "record the shared tenancy as a tradeoff; never split a namespace "
        "by team, and never equate a namespace with a team.",
        "Label every Namespace manifest this unit emits with "
        f"`{_ACCESS_LABEL_LITERAL}`. The label is not a discovered fact "
        "and needs none: it is the attach permission the shared Gateway's "
        "listeners select on (`from: Selector`) — a developer HTTPRoute "
        "in a Namespace without it is accepted by the API server and "
        "never attaches. A label is no new object kind: the scope fence "
        "above is unchanged.",
        _RBAC_LIMITRANGE_NOTE,
    ]
    return notes + _tenancy_boundary_notes(conflicts, unusable)


def _tenancy_boundary_notes(conflicts: list, unusable: list) -> list:
    """One conditioned note per fact defect, so the brief stays literally
    true when clusters disagree or a recording arrives in an odd shape."""
    notes = []
    if conflicts:
        notes.append(
            "The inventory records these namespace names more than once "
            "with differing entries: " + ", ".join(conflicts) + " — "
            "inputs.namespaces keeps the first recording (clusters[] "
            "order); route each discrepancy to open_questions instead of "
            "merging the recordings silently."
        )
    if unusable:
        notes.append(
            "These inventory.clusters[].workloads.namespaces recordings do "
            "not parse as namespace entries (mappings with a name): "
            + ", ".join(unusable) + " — route each to open_questions "
            "instead of guessing a namespace name."
        )
    return notes


def _workload_identity_notes(irsa_bindings: list,
                             target_project: str = None) -> list:
    """Notes for a planned workload-identity unit: the GSA/KSA brief plus the
    machine-checked ksa_annotations output contract.

    target_project is the ledger's recorded gcp_project ("The GCP Project
    ID hosting target GKE fleets"), threaded through inputs.target_project.
    With it the email note pins the literal project id — restating a
    recorded ledger fact, not inventing one — and the empty-map escape is
    demoted to the genuinely-absent case. Without this threading no rule-
    following worker could EVER emit a real email: the worker payload
    carries only {unit, inputs, notes} + lz_decisions, none of which held a
    project, and the translate rules forbid inventing one — so the escape
    was the unconditional outcome in every workspace, not an edge case.

    Same grammar as _storage_notes: derived from the discovered facts,
    literally true against them, inventing nothing. inputs.irsa_bindings
    entries are the merger's promoted "namespace/serviceaccount" strings.
    The account id half of each GSA email is always the unit's own choice,
    so it can be restated literally; the project half may be genuinely
    undecided at translation time (units routinely parameterize it), and
    for that case the contract has an explicit empty-map escape — an early
    end-to-end run shipped "...@PROJECT_ID..." placeholder literals
    under a wording that assumed the project was always choosable. The
    escape is shape-valid but never silent: over recorded IRSA bindings
    the validate step raises a finding that returns the unit to review,
    where a human decides the project instead of the gap shipping quietly.

    The Kubernetes half is out of this unit's scope by design: the workload
    pipeline's wkld-identity unit owns the KSA object and its
    iam.gke.io/gcp-service-account annotation (coverage map row "KSA and
    Workload Identity annotation", k8s x workload). Until that unit existed
    the brief left the boundary unsaid, and an early end-to-end run filled
    the silence with a default-off kubernetes-provider ServiceAccount
    resource no phase owned or applied (DESIGN section 14 issue 18). The
    brief now states the boundary, and "ksa_annotations" is the handover.
    """
    return [
        "Each IRSA ServiceAccount maps to a GSA + KSA pair bound by "
        "roles/iam.workloadIdentityUser. Emit the Google half only: the "
        "service account and the binding that names the KSA principal "
        "(serviceAccount:<project>.svc.id.goog[<namespace>/<ksa-name>]) as "
        "its member.",
        "Do not emit the KSA itself or its iam.gke.io/gcp-service-account "
        "annotation — not as a kubernetes-provider resource, not as a YAML "
        "manifest, and not behind a default-off flag or variable. That half "
        "is the workload pipeline's wkld-identity unit (coverage map row "
        '"KSA and Workload Identity annotation", k8s x workload); the '
        '"ksa_annotations" output below is how this unit hands the pairing '
        "to it. A Kubernetes ServiceAccount in this unit is a second "
        "claimant on that row, not a convenience — and the validate step "
        "now rejects it in every form (YAML document or Terraform "
        "resource), so a unit that ships one comes straight back for a "
        "rewrite.",
        "Contract (machine-checked by the validate step): emit exactly one "
        'Terraform output named "ksa_annotations" whose value is a literal '
        'map from "<namespace>/<ksa-name>" to the GSA email, e.g. value = '
        '{ "acme-shop/orders" = "orders@my-project.iam.gserviceaccount.com" }. '
        "Literal quoted pairs only — no resource references, no "
        "interpolation, no for-expressions, no function calls (comments are "
        "fine). A missing or non-literal output is a validation finding "
        "that returns this unit to review.",
        _wi_email_note(target_project),
        "Map keys come from the inputs.irsa_bindings entries; every entry "
        'that parses as "namespace/serviceaccount" becomes a key verbatim, '
        "and any that do not are routed to open_questions, never into an "
        "invented key. Map values restate the exact emails of the "
        "google_service_account resources this unit itself declares — the "
        "same literal strings, not resource attribute references.",
    ] + _irsa_shape_notes(irsa_bindings)


def _wi_email_note(target_project) -> str:
    """The email-grammar note, conditioned on whether the workspace records
    a target project — the same fact-regime grammar as _storage_notes."""
    if target_project:
        return (
            "Values must be real emails in the lowercase GSA grammar "
            '"<account-id>@<project-id>.iam.gserviceaccount.com"; the '
            "validate step rejects placeholder tokens (PROJECT_ID, "
            "<project>, ${...}). inputs.target_project records "
            f"'{target_project}' — the workspace's target GCP project, a "
            "recorded ledger fact (restating it is transcription, not "
            "invention): pin exactly that project id in every email, and "
            "give any var.project_id you declare that literal as its "
            "default. Do NOT use the empty-map escape — it is only for a "
            "workspace that records no target project, and over recorded "
            "IRSA bindings it raises a validation finding that returns "
            "this unit to review.")
    return (
        "Values must be real emails in the lowercase GSA grammar "
        '"<account-id>@<project-id>.iam.gserviceaccount.com"; the validate '
        "step rejects placeholder tokens (PROJECT_ID, <project>, ${...}). "
        "Use the exact project id this unit's Terraform pins (a project "
        "variable counts only when you give it that literal default). "
        "inputs.target_project is null — this workspace records no target "
        "project — so emit value = {} and route each binding's intended "
        "account id to open_questions with the project marked undecided; "
        "never invent a placeholder email. An empty map over recorded "
        "IRSA bindings is shape-valid but raises a validation finding "
        "that returns this unit to review — a human decides the project "
        "there; it never ships silently.")


def _irsa_shape_notes(irsa_bindings: list) -> list:
    """One conditioned note per fact regime: entries that do not parse as
    "namespace/serviceaccount" are named, so the brief stays literally true
    when the promoted list carries an odd worker string."""
    malformed = [
        str(binding) for binding in irsa_bindings or []
        if not (isinstance(binding, str)
                and len(binding.split("/")) == 2 and all(binding.split("/")))
    ]
    if not malformed:
        return []
    return [
        "These inputs.irsa_bindings entries do not parse as "
        '"namespace/serviceaccount": ' + ", ".join(sorted(malformed)) + " — "
        "route each to open_questions instead of guessing a map key."
    ]


def _choices(decisions: dict) -> dict:
    """Normalizes decisions to a flat {id: choice} map.

    Accepts either a resolved form ({id: {choice, source, title}}) or an
    already-flat {id: choice} map, so the planner is decoupled from how the
    ledger happens to store the landing-zone decisions. The same unwrapping
    the registry and exports apply (decisions_lib.flat_choices).
    """
    return decisions_lib.flat_choices(decisions)


def _derived(choices: dict, triggers) -> dict:
    """plan.derived: the typed values the registry derives from the recorded
    choices and the inventory triggers (never recorded, never guessed):
    cluster_mode, karpenter_replacement, nap_enabled. Each carries the
    inventory paths it read, so a reviewer can trace it (coverage-guards R1,
    "derived defaults are typed")."""
    mode, mode_reason = decisions_lib.cluster_mode(choices, triggers)
    replacement, repl_reason = decisions_lib.karpenter_replacement(choices, triggers)
    nap, nap_reason = decisions_lib.nap_enabled(choices, triggers)
    # The trigger keys each derivation consulted, directly or through the
    # projection: the mode reads all three (gpu_tpu decides whether the
    # advisory is considered); the two Karpenter values read the projection's
    # voting triggers.
    voting = ["triggers." + did for did in decisions_lib.voting_ids()]
    advisory = ["triggers." + did for did in decisions_lib.advisory_ids()]
    return {
        "cluster_mode": {"choice": mode, "trigger_path": voting + advisory, "reason": mode_reason},
        "karpenter_replacement": {"choice": replacement, "trigger_path": voting,
                                  "reason": repl_reason},
        "nap_enabled": {"choice": nap, "trigger_path": voting, "reason": nap_reason},
    }


def _findings(choices: dict, triggers, inventory: dict) -> list:
    """plan.findings: one typed record per thing the plan reviewer must see
    beside the decisions, {kind, severity, subject, value, expected, note}.

    - decisions-disagree (conflict): the projection is None with a disagree
      reason (every considered id in `value`), or a considered advisory
      decision implies the other mode (subject = its id).
    - facts-vs-decision (conflict, as every "vs" record is under coverage-
      guards R1): the registry's recommendation (a predicate in the decision
      table that fired on typed facts) differs from the recorded choice; the
      note says whether the two imply the same cluster mode. Computed here,
      never on the best-effort coverage path; no finding when the typed
      field is absent.
    """
    findings = []
    result = decisions_lib.project(choices, triggers)
    if result["mode"] is None and str(result["reason"]).startswith("disagree"):
        findings.append({
            "kind": "decisions-disagree", "severity": "conflict", "subject": "cluster_mode",
            "value": result["reason"][len("disagree: "):], "expected": "one cluster mode",
            "note": "the recorded landing-zone decisions imply different cluster modes; "
                    "re-resolve one of them (resolve_lz_decision) and re-plan",
        })
    for did, token, implied in decisions_lib.advisory_mismatches(choices, triggers):
        findings.append({
            "kind": "decisions-disagree", "severity": "conflict", "subject": did,
            "value": token, "expected": result["mode"],
            "note": f"{did} is recorded at a choice that implies {implied} while the voting "
                    f"decisions settle on {result['mode']}; advisory, the mode stands",
        })
    replacement = decisions_lib.karpenter_replacement(choices, triggers)[0]
    pools = ((inventory.get("autoscaling") or {}).get("karpenter_nodepools")
             if isinstance(inventory.get("autoscaling"), dict) else None) or []
    if replacement == "computeclass" and not [p for p in pools if isinstance(p, dict)]:
        findings.append({
            "kind": "facts-vs-decision", "severity": "conflict", "subject": "karpenter",
            "value": choices.get("karpenter"),
            "expected": "at least one typed autoscaling.karpenter_nodepools entry",
            "note": "the recorded choice replaces Karpenter with ComputeClass but the inventory "
                    "types no NodePool, so no ComputeClass unit could be planned; re-extract with "
                    "the current schema and re-plan, or re-resolve the decision",
        })
    for did in decisions_lib.load_registry()["order"]:
        recorded = choices.get(did)
        if not isinstance(recorded, str):
            continue
        recommended, reason = decisions_lib.recommend(did, inventory)
        if recommended is None or recommended == recorded:
            continue
        rec_mode = decisions_lib.mode_of(recommended)[0]
        cur_mode = decisions_lib.mode_of(recorded)[0]
        modes = ("both imply the same cluster mode" if rec_mode == cur_mode
                 else f"the recorded choice implies {cur_mode}, the recommendation {rec_mode}")
        findings.append({
            "kind": "facts-vs-decision", "severity": "conflict", "subject": did,
            "value": recorded, "expected": recommended,
            "note": reason + f"; {modes}; the recorded choice stands unless the reviewer "
                    "re-resolves it",
        })
    return findings


def render_findings(plan: dict) -> list:
    """One line per plan.findings record for the plan-review elicitation; pure
    (no mcp import) so it is testable, and severity, not kind, drives the
    prefix so the reviewer does not learn to skip the list."""
    lines = []
    for record in plan.get("findings") or []:
        if not isinstance(record, dict):
            continue
        prefix = "CONFLICT" if record.get("severity") == "conflict" else "INFO"
        lines.append(
            f"{prefix} [{record.get('kind')}] {record.get('subject')}: recorded "
            f"{record.get('value')!s}, expected {record.get('expected')!s} — {record.get('note')}")
    return lines


def build_translation_plan(inventory: dict, decisions: dict = None,
                           target_project: str = None) -> dict:
    """Builds the translation plan from an approved discovery inventory.

    `decisions` are the landing-zone target-shape decisions
    (variables.lz_decisions, written by resolve_lz_decision), as a flat
    {id: choice} map — a resolved {id: {choice, ...}} form is also accepted.
    `target_project` is the ledger config's gcp_project ("The GCP Project
    ID hosting target GKE fleets"); it becomes the workload-identity unit's
    inputs.target_project, the fact its brief pins GSA emails to — without
    it the unit's email contract is structurally unsatisfiable (the worker
    payload holds no project and inventing one is forbidden).
    Returns {"units": [...], "decisions": {id: choice}}.
    Deterministic: same inventory + decisions + target_project always
    produce the same units in the same order.

    The landing zone owns the base VPC and the GKE cluster (its Terraform is
    designed by the landing zone phase from these same decisions and shipped in
    the translation-tail PR alongside the units), so the plan does NOT re-emit
    them. Units cover only the per-workload migration work that layers on top of
    that landing zone.

    Every unit family appears in the plan exactly once: as planned unit(s) when
    its inventory section holds facts, or as one `skipped` placeholder naming
    the empty section. The plan is therefore a coverage claim over everything
    the planner knows how to translate, not just a list of what discovery
    happened to surface.
    """
    choices = _choices(decisions)
    triggers = (inventory or {}).get("triggers")
    derived = _derived(choices, triggers)
    mode = derived["cluster_mode"]["choice"]
    mode_reason = derived["cluster_mode"]["reason"]
    # The node-pool skip follows the one projection: Autopilot skips, Standard
    # and an unresolved mode (None) plan — the pre-registry behaviour on every
    # ledger where the readers agreed (coverage-guards G0, compatibility rule).
    autopilot = mode == "autopilot"
    units = []
    used_ids = set()

    def skip_note(claim: str) -> str:
        # `claim` states exactly what the inventory lacks — placeholders are
        # coverage claims a human reviews, so the wording must stay literally
        # true even when the section holds unrelated facts (e.g. an
        # autoscaling section with cluster-autoscaler evidence but no
        # Karpenter).
        return (
            f"Skipped by default: {claim}, so there is nothing to translate. "
            "If the estate disagrees, amend discovery and re-plan; unskipping "
            "without new facts hands the translation worker empty inputs."
        )

    def unique_id(base):
        # slugify collapses case and punctuation, so distinct inventory names
        # ("gpu_pool" / "gpu-pool") can produce one id — and everything
        # downstream (result maps, unit blob paths, revision targeting) keys
        # on unit_id. Deterministic suffixing keeps every unit addressable.
        uid, n = base, 1
        while uid in used_ids:
            n += 1
            uid = f"{base}-{n}"
        used_ids.add(uid)
        return uid

    # One family owns the ComputeClass kind. Under the ComputeClass arm every
    # other unit whose worker sees the recorded choice is told so, because
    # the first harness run showed the addons and node-pool workers improvising
    # classes of their own (the contract's foreign sweep caught them).
    replacement = derived["karpenter_replacement"]["choice"]
    cc_fence = (
        "Do not emit a ComputeClass (cloud.google.com/v1) of any name: under the recorded "
        "karpenter decision the compute-class units own that kind, one per typed NodePool, "
        "and the validate step fails any other unit that ships one."
    ) if replacement == "computeclass" else None

    addons = inventory.get("addons", []) or []
    if addons:
        units.append(_unit(
            "cluster-addons",
            "cluster-addons",
            "EKS addons and platform components (drop / built-in / reconfigure on GKE)",
            {"addons": addons},
            notes=["Map each detected addon to drop / built-in / reconfigure on GKE; any addon without a confident mapping is an open question, not a guess.",
                   "A coredns addon is built-in: Cloud DNS for GKE is the landing zone's cluster DNS. Its Corefile customizations, if discovery recorded any, are the cluster-dns unit's to translate — say 'built-in, customizations handled by the cluster-dns unit' and emit nothing for it here."]
                  + ([cc_fence + " A karpenter addon is 'drop': its replacement is those units' output."] if cc_fence else []),
        ))
    else:
        units.append(_unit(
            "cluster-addons",
            "cluster-addons",
            "EKS addons and platform components (drop / built-in / reconfigure on GKE)",
            {"addons": []},
            notes=[skip_note("inventory.addons recorded no facts")],
            status="skipped",
            placeholder=True,
        ))

    # Cluster DNS: the CoreDNS configuration discovery copied verbatim. The
    # section holds facts only when a source carries text (a default add-on
    # is a scan note, not a source), so planned-iff-facts and the coverage
    # rows' facts-present verdict agree by construction. The mapping itself
    # is not in this brief: it rides with the translation prompt
    # (translator.FAMILY_KNOWLEDGE) so a mapping edit needs no re-plan.
    cluster_dns = inventory.get("cluster_dns") or {}
    # Any entry counts: the coverage gate's facts-present verdict does not
    # filter by shape either, and a planner that filtered would leave a
    # facts-present row behind a placeholder — a hard omission at validate.
    dns_sources = list(cluster_dns.get("sources") or [])
    dns_scan_notes = [n for n in inventory.get("cluster_dns_scan_notes") or []
                      if isinstance(n, str)]
    # `cluster_mode` is the projection's mode string (or null with
    # `decision_reason`); the legacy `decision` key keeps the recorded
    # karpenter token for readers of stored plans and is not the mode.
    dns_inputs = {"cluster_dns": cluster_dns, "decision": choices.get("karpenter"),
                  "cluster_mode": mode, "decision_reason": mode_reason,
                  "decisions": family_decisions("cluster-dns", choices),
                  "scan_notes": dns_scan_notes}
    if dns_sources:
        units.append(_unit(
            "cluster-dns",
            "cluster-dns",
            "Cluster DNS: CoreDNS customizations on Cloud DNS for GKE",
            dns_inputs,
            notes=_cluster_dns_notes(dns_sources, mode, mode_reason, dns_scan_notes),
        ))
    else:
        notes = [skip_note("inventory.cluster_dns recorded no source carrying a "
                           "Corefile or add-on configuration")]
        unread = [n for n in dns_scan_notes if _unread_coredns_note(n)]
        if unread:
            notes.append(
                "WARNING: the scan saw something that may carry a CoreDNS configuration "
                "and could not read it, so this absence is unverified: " + " | ".join(unread)
                + " — obtain the text (the note says where it lives) and re-scan before "
                "trusting this skip.")
        units.append(_unit(
            "cluster-dns",
            "cluster-dns",
            "Cluster DNS: CoreDNS customizations on Cloud DNS for GKE",
            {**dns_inputs, "cluster_dns": {}},
            notes=notes,
            status="skipped",
            placeholder=True,
        ))

    autoscaling = inventory.get("autoscaling", {}) or {}
    autoscaling_inputs = {"autoscaling": autoscaling, "decision": choices.get("karpenter"),
                          "decisions": family_decisions("autoscaling", choices),
                          "derived_decisions": {
                              "nap_enabled": derived["nap_enabled"],
                              "karpenter_replacement": derived["karpenter_replacement"]}}
    nodepools = [p for p in (autoscaling.get("karpenter_nodepools") or []) if isinstance(p, dict)]
    autoscaling_brief = [
        "Karpenter has no GKE equivalent; translate it per the recorded karpenter "
        "decision (inputs.decision): Autopilot removes the node layer, Standard "
        "replaces it with node auto-provisioning. Order the NAP guardrail only when "
        "inputs.derived_decisions.nap_enabled.choice is true; its reason says why."]
    if autoscaling.get("karpenter") or (inventory.get("triggers", {}) or {}).get("karpenter"):
        if replacement == "computeclass" and nodepools:
            # One-mode construct under the other arm: skipped with the facts
            # kept, exactly as node pools are under Autopilot. The coverage
            # row stays satisfied through the explicit skip. The generation
            # brief stays attached so an unskip hands the worker a real brief.
            units.append(_unit(
                "autoscaling-karpenter",
                "autoscaling",
                "Karpenter replacement (node auto-provisioning strategy)",
                autoscaling_inputs,
                notes=["Skipped: the compute-class units ("
                       + ", ".join(f"compute-class-{slugify(p.get('name', 'nodepool'))}" for p in nodepools)
                       + ") carry the Karpenter replacement under the recorded karpenter "
                       "decision (inputs.decision); no cluster-level NAP guardrail is ordered "
                       "(inputs.derived_decisions.nap_enabled.choice is false). Unskip only to "
                       "order NAP as a fallback beside the classes; the brief below then applies."]
                      + autoscaling_brief,
                status="skipped",
            ))
        else:
            notes = list(autoscaling_brief)
            if replacement == "computeclass":
                # The recorded choice names ComputeClass but no pool is typed,
                # so no class can be written: this unit stays PLANNED as the
                # safety net and says so, and the plan carries a finding, so
                # the gap is never a silent all-skipped pass at validate.
                notes.insert(0,
                    "WARNING: the recorded karpenter decision replaces Karpenter with "
                    "ComputeClass, but inventory.autoscaling.karpenter_nodepools records no "
                    "typed NodePool, so no ComputeClass unit could be planned. Re-extract with "
                    "the current schema and re-plan; until then this unit is the only Karpenter "
                    "replacement in the plan — emit nothing that assumes a class exists, and "
                    "name the gap in open_questions.")
            units.append(_unit(
                "autoscaling-karpenter",
                "autoscaling",
                "Karpenter replacement (node auto-provisioning strategy)",
                autoscaling_inputs,
                notes=notes,
            ))
    else:
        units.append(_unit(
            "autoscaling-karpenter",
            "autoscaling",
            "Karpenter replacement (node auto-provisioning strategy)",
            autoscaling_inputs,
            notes=[skip_note("inventory.autoscaling / triggers recorded no Karpenter evidence")],
            status="skipped",
            placeholder=True,
        ))

    # ComputeClass: one unit per typed Karpenter NodePool, planned only when
    # the derived Karpenter replacement is ComputeClass. Keyed on the
    # derivation, never on a token; skipped (facts kept, Gate C may unskip)
    # under every other replacement, and under an unresolved mode, where a
    # class could land on Autopilot with the wrong field set.
    cc_derived = {"karpenter_replacement": derived["karpenter_replacement"],
                  "nap_enabled": derived["nap_enabled"]}
    if not nodepools:
        notes = [skip_note("inventory.autoscaling.karpenter_nodepools recorded no typed NodePool")]
        if (inventory.get("triggers", {}) or {}).get("karpenter") or autoscaling.get("karpenter"):
            notes.append(
                "WARNING: Karpenter is recorded (autoscaling.karpenter / triggers.karpenter) but "
                "no NodePool is typed in autoscaling.karpenter_nodepools; re-extract with the "
                "current schema before trusting this skip.")
        units.append(_unit(
            "compute-classes",
            "compute-class",
            "ComputeClasses replacing Karpenter NodePools",
            {"nodepool": None, "decision": choices.get("karpenter"),
             "decisions": family_decisions("compute-class", choices),
             "derived_decisions": cc_derived, "cluster_mode": mode},
            notes=notes,
            status="skipped",
            placeholder=True,
        ))
    for pool in nodepools:
        name = pool.get("name", "nodepool")
        inputs = {"nodepool": pool, "decision": choices.get("karpenter"),
                  "decisions": family_decisions("compute-class", choices),
                  "derived_decisions": cc_derived, "cluster_mode": mode}
        if replacement == "computeclass":
            status, notes = "planned", _compute_class_notes(pool)
        elif replacement == "nap":
            status, notes = "skipped", [
                "Skipped: the recorded karpenter decision (inputs.decision) replaces Karpenter "
                "with node auto-provisioning; the NAP unit and node pools carry this pool's "
                "constraints. Unskip only after re-resolving the decision to ComputeClass."]
        elif replacement is None:
            status, notes = "skipped", [
                f"Skipped: the cluster mode is unresolved ({mode_reason}); settle the "
                "landing-zone design before a ComputeClass is emitted — a class written for "
                "an unresolved mode could land on Autopilot, where its fields are wrong."]
        elif mode == "autopilot":
            status, notes = "skipped", [
                "Skipped: Autopilot manages nodes; ComputeClass on Autopilot uses a different "
                "field set and is an open design question, not this unit's output."]
        else:
            status, notes = "skipped", [
                "Skipped: no Karpenter replacement was decided for this pool "
                f"({derived['karpenter_replacement']['reason']})."]
        units.append(_unit(
            unique_id(f"compute-class-{slugify(name)}"),
            "compute-class",
            f"ComputeClass replacing Karpenter NodePool '{name}'",
            inputs,
            notes=notes,
            status=status,
        ))

    nodegroups = inventory.get("nodegroups", []) or []
    if not nodegroups:
        units.append(_unit(
            "node-pools",
            "node-pool",
            "GKE node pools replacing EKS nodegroups",
            {"nodegroups": [], "decisions": family_decisions("node-pool", choices)},
            notes=[skip_note("inventory.nodegroups recorded no facts")],
            status="skipped",
            placeholder=True,
        ))
    for nodegroup in nodegroups:
        name = nodegroup.get("name", "nodegroup")
        status = "skipped" if autopilot else "planned"
        notes = []
        if autopilot:
            notes.append("Autopilot cluster mode: Autopilot manages nodes; express this nodegroup's requirements as workload constraints (resource requests, nodeSelectors, tolerations) instead of a node pool.")
        inputs = {"nodegroup": nodegroup, "decision": choices.get("karpenter"),
                  "decisions": family_decisions("node-pool", choices)}
        if cc_fence and not autopilot:
            notes.append(cc_fence + " This unit emits the google_container_node_pool for the "
                         "source nodegroup only.")
        if nodegroup.get("gpu"):
            inputs["accelerators"] = choices.get("gpu_tpu")
            notes.append(f"GPU nodegroup: map instance types to GKE machine types + guest accelerators per the '{choices.get('gpu_tpu')}' gpu_tpu decision; check regional accelerator availability.")
        units.append(_unit(
            unique_id(f"node-pool-{slugify(name)}"),
            "node-pool",
            f"GKE node pool replacing EKS nodegroup '{name}'",
            inputs,
            notes=notes,
            status=status,
        ))

    workloads = inventory.get("workloads", {}) or {}
    daemonsets = workloads.get("privileged_daemonsets", []) or []
    if not daemonsets:
        units.append(_unit(
            "workload-policies",
            "workload-policy",
            "Privileged workload policy strategy on GKE",
            {"privileged_daemonsets": [], "decisions": family_decisions("workload-policy", choices)},
            notes=[skip_note("inventory.workloads.privileged_daemonsets recorded no facts")],
            status="skipped",
            placeholder=True,
        ))
    for daemonset in daemonsets:
        name = daemonset.get("name", "daemonset")
        units.append(_unit(
            unique_id(f"workload-policy-{slugify(name)}"),
            "workload-policy",
            f"Privileged DaemonSet '{name}' policy strategy on GKE",
            {"daemonset": daemonset, "decision": choices.get("privileged_daemonsets"),
             "decisions": family_decisions("workload-policy", choices)},
            notes=["Privileged workloads conflict with Autopilot; per the privileged_daemonsets decision, either keep them on a Standard cluster or re-architect them."],
        ))

    if workloads.get("host_network"):
        units.append(_unit(
            "host-network-workloads",
            "host-network",
            "hostNetwork workloads compatibility on GKE",
            {"host_network": workloads.get("host_network", [])},
            notes=["hostNetwork pods interact with VPC-native networking differently on GKE; verify port collisions and NetworkPolicy behavior."],
        ))
    else:
        units.append(_unit(
            "host-network-workloads",
            "host-network",
            "hostNetwork workloads compatibility on GKE",
            {"host_network": []},
            notes=[skip_note("inventory.workloads.host_network recorded no facts")],
            status="skipped",
            placeholder=True,
        ))

    if workloads.get("irsa_bindings"):
        units.append(_unit(
            "workload-identity",
            "workload-identity",
            "IRSA bindings to GKE Workload Identity Federation",
            {"irsa_bindings": workloads.get("irsa_bindings", []),
             "target_project": target_project or None},
            notes=_workload_identity_notes(
                workloads.get("irsa_bindings", []), target_project),
        ))
    else:
        units.append(_unit(
            "workload-identity",
            "workload-identity",
            "IRSA bindings to GKE Workload Identity Federation",
            {"irsa_bindings": []},
            notes=[skip_note("inventory.workloads.irsa_bindings recorded no facts")],
            status="skipped",
            placeholder=True,
        ))

    network = inventory.get("network", {}) or {}
    if network.get("vpc_peering") or network.get("load_balancers"):
        units.append(_unit(
            "network",
            "network",
            "Cross-VPC connectivity and load balancers layered on the landing zone VPC",
            {"network": network, "decision": choices.get("vpc_peering"),
             "decisions": family_decisions("network", choices)},
            notes=[
                "The landing zone owns the base VPC; emit only the additional connectivity (peering / NCC, ingress and load balancers) that layers on top of it, per the vpc_peering decision.",
                "Cloud DNS: a private or peering zone that shares names across the peered VPCs is this unit's; a forwarding zone (a CoreDNS stub domain carried over), the kube-dns ConfigMap and anything else from the source cluster's Corefile are the cluster-dns unit's, and a google_dns_policy is nobody's (VPC-wide, one per network, landing-zone territory).",
                "This unit owns the Terraform half of that connectivity only. "
                "The shared entry point itself is the `gateway` unit's Gateway "
                "API manifest and per-workload routing is the workload phase's "
                "(coverage map, rows `Gateway (shared entry point)` and "
                "`HTTPRoute`): emit no Gateway API objects here — no Gateway, "
                "no HTTPRoute — even though the same load balancer facts are "
                "in your inputs.",
            ],
        ))
    else:
        units.append(_unit(
            "network",
            "network",
            "Cross-VPC connectivity and load balancers layered on the landing zone VPC",
            {"network": network, "decisions": family_decisions("network", choices)},
            notes=[skip_note("inventory.network recorded no cross-VPC peering or load balancers")],
            status="skipped",
            placeholder=True,
        ))

    # Planned iff network records entry-point evidence: load_balancers or
    # ingress_hosts truthy (a scalar recording still counts as evidence;
    # only its enumeration is guarded — see _gateway_facts). No heuristic
    # free-text scan of network.evidence.
    if network.get("load_balancers") or network.get("ingress_hosts"):
        # The recorded namespace names ride in the input slice: the brief's
        # collision fence names them, and the worker sees what it must not
        # issue — the tenancy side of the plan owns those names.
        recorded_ns = _recorded_namespace_names(
            inventory.get("clusters", []) or [])
        units.append(_unit(
            "gateway",
            "gateway",
            "Shared entry-point Gateway (Gateway API) on GKE",
            {"network": network, "recorded_namespaces": recorded_ns},
            notes=_gateway_notes(network, recorded_ns),
        ))
    else:
        notes = [skip_note("inventory.network recorded no load balancers or ingress hosts")]
        entry_point_evidence = _entry_point_evidence(network)
        if entry_point_evidence:
            notes.append(
                "WARNING: inventory.network records no entry point, but its "
                "evidence mentions " + "; ".join(entry_point_evidence)
                + " — evidence of an entry point this scan did not structure. "
                "Treat this as a discovery gap and rescan before approving "
                "the plan; a migration with no shared Gateway parks every "
                "workload route."
            )
        units.append(_unit(
            "gateway",
            "gateway",
            "Shared entry-point Gateway (Gateway API) on GKE",
            {"network": network},
            notes=notes,
            status="skipped",
            placeholder=True,
        ))

    storage = inventory.get("storage", {}) or {}
    if storage.get("ebs_csi") or storage.get("efs_csi") or storage.get("storage_classes"):
        units.append(_unit(
            "storage",
            "storage",
            "StorageClasses and CSI drivers on GKE",
            {"storage": storage},
            notes=_storage_notes(storage),
        ))
    else:
        notes = [skip_note("inventory.storage recorded no CSI drivers or StorageClasses")]
        csi_evidence = sorted({
            str(addon.get("name")) for addon in addons
            if "csi" in str(addon.get("name", "")).lower()
        })
        if csi_evidence:
            notes.append(
                "WARNING: inventory.storage is empty, but inventory.addons lists "
                + ", ".join(csi_evidence)
                + " — evidence of storage this scan did not structure. Treat this as a "
                "discovery gap and rescan before approving the plan."
            )
        units.append(_unit(
            "storage",
            "storage",
            "StorageClasses and CSI drivers on GKE",
            {"storage": storage},
            notes=notes,
            status="skipped",
            placeholder=True,
        ))

    # Planned iff any cluster records a truthy workloads.namespaces value —
    # entries that parse as {name: ...} mappings are issued; every other
    # recording is evidence that plans the family but routes to
    # open_questions instead of being issued (see _namespace_entries).
    ns_entries, ns_conflicts, ns_unusable = _namespace_entries(
        inventory.get("clusters", []) or [])
    if ns_entries or ns_unusable:
        units.append(_unit(
            "tenancy",
            "tenancy",
            "Namespace and ResourceQuota issuance per discovered namespace",
            {"namespaces": ns_entries},
            notes=_tenancy_notes(ns_entries, ns_conflicts, ns_unusable),
        ))
    else:
        units.append(_unit(
            "tenancy",
            "tenancy",
            "Namespace and ResourceQuota issuance per discovered namespace",
            {"namespaces": []},
            notes=[skip_note("inventory.clusters[].workloads.namespaces recorded no namespaces")],
            status="skipped",
            placeholder=True,
        ))

    return {"units": units, "decisions": dict(choices), "derived": derived,
            "findings": _findings(choices, triggers, inventory or {})}


def active_units(plan: dict) -> list:
    """Units translation will actually run — everything not skipped."""
    return [u for u in plan.get("units", []) if u.get("status") != "skipped"]


def all_placeholders(plan: dict) -> bool:
    """True when the plan holds nothing but no-facts coverage placeholders.

    Deliberately NOT the same as `not active_units(plan)`: a plan whose real
    units were all skipped by a landing-zone decision (Autopilot node pools)
    still carries facts and belongs in review, where the operator can unskip
    units — and, once one is active, decline the sign-off to revisit the
    design; only a placeholder-only plan means the inventory itself is empty
    and the fix is rediscovery.
    """
    units = plan.get("units", [])
    return bool(units) and all(u.get("placeholder") for u in units)


def set_unit_status(plan: dict, unit_ids: list, status: str, feedback: str = None) -> tuple:
    """Returns (new_plan, notes). Pure: unknown unit IDs are reported, not fatal."""
    known = {u["unit_id"] for u in plan.get("units", [])}
    notes = []
    new_units = []
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
    for missing in targets - known:
        notes.append(f"'{missing}' not found in plan (ignored)")
    return {**plan, "units": new_units}, notes


def summarize_plan(plan: dict) -> str:
    import json

    lines = []
    for unit in plan.get("units", []):
        line = {
            "unit_id": unit["unit_id"],
            "kind": unit["kind"],
            "status": unit["status"],
            "title": unit["title"],
        }
        if unit.get("placeholder"):
            line["placeholder"] = True
        if unit.get("covers"):
            line["covers"] = unit["covers"]
        if unit.get("error"):
            line["error"] = unit["error"]
        if unit.get("notes"):
            line["notes"] = unit["notes"]
        lines.append(line)
    summary = {"decisions": plan.get("decisions", {}), "units": lines}
    if plan.get("derived"):
        summary["derived"] = {k: v.get("choice") if isinstance(v, dict) else v
                              for k, v in plan["derived"].items()}
    if plan.get("findings"):
        summary["findings"] = render_findings(plan)
    coverage = plan.get("coverage")
    if coverage:
        # Compact by design: verdict counts and the check findings (which name
        # only offending rows/units). The full instantiated table stays in the
        # plan blob.
        try:
            statuses = {}
            for verdict in coverage.get("map", []):
                statuses[verdict["status"]] = statuses.get(verdict["status"], 0) + 1
            checks = coverage.get("checks", {})
            out_of_scope = checks.get("out_of_scope", {})
            summary["coverage"] = {
                "granularity": coverage.get("granularity"),
                "row_verdicts": statuses,
                "omission": checks.get("omission", []),
                "overlap": checks.get("overlap", []),
                "traceability": checks.get("traceability", {}),
                "unknown_sections": checks.get("unknown_sections", []),
                "out_of_scope_rows": {
                    owner: len(rows) for owner, rows in out_of_scope.items()
                },
            }
        except Exception:
            # A stored coverage key this code did not write (tampered or
            # half-written ledger state) must not take down the whole summary
            # — the unit lines above are still good. Deterministic: the
            # defect surfaces as data, not as an exception or a log.
            summary["coverage"] = {
                "error": "stored coverage is unreadable; re-run plan_translation to rebuild it"
            }
    return json.dumps(summary, indent=2)
