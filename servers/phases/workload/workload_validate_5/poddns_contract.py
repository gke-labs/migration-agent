"""Pod DNS contract: the deterministic half of the workload side's DNS story.

The platform side moves the cluster resolver (CoreDNS → Cloud DNS for GKE,
`cluster-dns` unit, `clusterdns_contract.py`). This module is the pod side.
A pod's own DNS settings — `dnsPolicy`, `dnsConfig` (nameservers, searches,
options), `hostAliases`, `hostNetwork` — travel with the workload manifests,
and some of their values name things that do not exist on GKE: the EKS
cluster DNS ClusterIP, the AWS VPC resolver, the EC2 search suffixes. The
translation worker decides what each becomes, from the mapping document
(`knowledge/pod-dns-translation.md`); this module knows NO mapping. It checks
three things any correct translation satisfies:

- shape: the four Kubernetes policies, the nameserver/search limits, a
  `None` policy backed by at least one nameserver;
- a closed list of addresses and suffixes that must not appear in shipped
  output whatever the mapping says: the AWS resolvers, the EKS default
  cluster DNS addresses, the GKE node resolver addresses (never pinned into
  a pod), the EC2 search suffixes;
- conservation against the facts the plan persisted (`inputs.pod_dns_facts`,
  read from the unit's own blob so the facts are the ones the worker was
  briefed on): every source nameserver, search domain, option and host
  alias is either kept in the shipped pod specs or named in the unit's
  tradeoffs/open_questions/assumptions; nothing appears that the source did
  not carry; `dnsPolicy` is unchanged, the one permitted change being
  `None` → `ClusterFirst` — which is REQUIRED when a `None` pod's nameservers
  include a cluster DNS address from the closed list, because keeping `None`
  there ships a pod that resolves no cluster name.

Conservation is unit-wide over sets, as the platform contract does: shipped
documents may lose or gain a namespace through a render or a worker edit,
so only the policy check matches documents (kind and name, plus namespace
when both sides record one), and inside the document the pod spec by the
key path the fact recorded — a pod that lost every DNS field is still that
pod, read as policy unset. A `None` pod on host networking may become
`ClusterFirstWithHostNet` instead of `ClusterFirst`, since `ClusterFirst`
silently means `Default` there. A unit whose blob carries no `pod_dns_facts`
predates the field; it is skipped with a visible note, never failed against
nothing and never silently passed. The blob's facts are also compared with
the current plan's: a reused blob translated against other facts (a source
edit followed by a re-plan with unchanged exports) is a finding, not a pass
over stale facts.

Known blind spots, recorded in DESIGN §14: an EKS cluster DNS address off
the two defaults (a non-default service range) is not on the closed list;
the VPC resolver at `.2` of the VPC CIDR cannot be recognised without the
CIDR, which the workload side does not have; a custom cluster domain under
Cloud DNS VPC scope is not projected to workloads.
"""

import ipaddress
import re

from servers.phases import k8s_manifests
from ..workload_translate_3 import transforms
from ..workload_translate_3 import translator

FAMILY = "wkld-manifests"
FACTS_KEY = "pod_dns_facts"
UNREAD_KEY = "pod_dns_unread"

DNS_POLICIES = ("Default", "ClusterFirst", "ClusterFirstWithHostNet", "None")
MAX_NAMESERVERS = 3
MAX_SEARCHES = 32
MAX_SEARCH_CHARS = 2048

# Addresses that must never sit in a shipped pod's dnsConfig.nameservers,
# with the reason the finding gives. The GKE node resolver addresses are
# listed too: a pod that pins them breaks on the next platform change and
# gains nothing, since Cloud DNS is what the node hands out anyway.
FORBIDDEN_NAMESERVERS = {
    "169.254.169.253": "the AWS VPC resolver, which does not exist on GCP",
    "10.100.0.10": "the EKS default cluster DNS address (service range 10.100.0.0/16)",
    "172.20.0.10": "the EKS default cluster DNS address (service range 172.20.0.0/16)",
    "169.254.169.254": "the GKE node metadata server; Cloud DNS is reached through the node, never pinned into a pod",
    "169.254.20.10": "the NodeLocal DNSCache address; reached through the node, never pinned into a pod",
}
CLUSTER_DNS_ADDRESSES = frozenset({"10.100.0.10", "172.20.0.10"})
FORBIDDEN_SEARCH_SUFFIXES = ("ec2.internal", "compute.internal")


# --- one node -----------------------------------------------------------------

def schema_errors(fields: dict) -> list:
    """Kubernetes-level shape errors of one pod-spec-like mapping."""
    errors = []
    policy = fields.get("dns_policy")
    nameservers = fields.get("nameservers") or []
    searches = fields.get("searches") or []
    if policy is not None and policy not in DNS_POLICIES:
        errors.append(f"dnsPolicy {policy!r} is not one of {', '.join(DNS_POLICIES)}")
    if policy == "None" and not nameservers:
        errors.append("dnsPolicy None with no dnsConfig.nameservers: the pod would have no resolver at all")
    if len(nameservers) > MAX_NAMESERVERS:
        errors.append(f"{len(nameservers)} nameservers; Kubernetes allows at most {MAX_NAMESERVERS}")
    for address in nameservers:
        try:
            ipaddress.ip_address(address)
        except ValueError:
            errors.append(f"nameserver {address!r} is not an IP address")
    if len(searches) > MAX_SEARCHES:
        errors.append(f"{len(searches)} search domains; Kubernetes allows at most {MAX_SEARCHES}")
    if sum(len(s) for s in searches) > MAX_SEARCH_CHARS:
        errors.append(f"search domains total more than {MAX_SEARCH_CHARS} characters")
    for option in fields.get("options") or []:
        if not option.get("name"):
            errors.append("a dnsConfig option has no name")
    return errors


def forbidden_errors(fields: dict) -> list:
    """The closed-list check: literals that must not ship, whatever the mapping."""
    errors = []
    for address in fields.get("nameservers") or []:
        reason = FORBIDDEN_NAMESERVERS.get(address)
        if reason:
            errors.append(f"nameserver {address} is {reason}")
    for domain in fields.get("searches") or []:
        lowered = domain.lower().rstrip(".")
        if any(lowered == suffix or lowered.endswith("." + suffix)
               for suffix in FORBIDDEN_SEARCH_SUFFIXES):
            errors.append(f"search domain {domain} is an EC2 suffix, which does not exist on GCP")
    return errors


# --- prose and literal membership ----------------------------------------------

def _prose(result: dict) -> str:
    result = result or {}
    parts = [str(result.get("tradeoffs") or "")]
    for key in ("assumptions", "open_questions"):
        parts.extend(str(x) for x in (result.get(key) or []))
    return "\n".join(parts)


def _named(literal: str, prose: str) -> bool:
    """Is the literal named in the prose, label-aligned (10.20.0.53 does
    not satisfy 10.20.0.5; corp.acme.internal does not satisfy acme.internal)?"""
    if not literal:
        return False
    # A word character, a dot, a colon or a dash right before the literal
    # would make it the tail of a longer token — except a colon, which is
    # how prose attaches a label (`nameserver:10.20.0.53`). After it, a word
    # character or a dash extends the token, and so does a dot followed by
    # one (`db.internal` does not name `db`); a sentence-final dot does not.
    pattern = r"(?<![\w.-])" + re.escape(literal) + r"(?![\w-]|\.\w)"
    return re.search(pattern, prose) is not None


def _option_text(option: dict) -> str:
    return option["name"] + ("" if option.get("value") is None else f"={option['value']}")


# --- documents ----------------------------------------------------------------

def _doc_identity(doc: dict) -> tuple:
    metadata = doc.get("metadata") if isinstance(doc.get("metadata"), dict) else {}
    return (str(doc.get("kind") or ""), metadata.get("namespace"), metadata.get("name"))


def result_documents(entry: dict) -> list:
    """[(label, doc)] parsed from a done unit's PLAIN result files. A file
    routed to a chart or kustomize carrier is a source, not a document: its
    rendered form arrives through `rendered_docs`, so reading it here too
    would count and report it twice. A file that does not parse is the
    structural gate's."""
    unit = (entry or {}).get("unit") or {}
    cites = translator.unit_render_cites(unit)
    out = []
    for f in ((entry or {}).get("result") or {}).get("files") or []:
        path = str(f.get("path") or "")
        if translator._route_chart(path, cites["helm"]) is not None \
                or translator._route_kustomize(path, cites["kustomize"]) is not None:
            continue
        try:
            docs = k8s_manifests.load_manifest_documents(str(f.get("content") or ""))
        except ValueError:
            continue
        for doc in docs:
            if isinstance(doc, dict) and doc.get("kind"):
                out.append((str(f.get("path") or "?"), doc))
    return out


def _nodes(documents: list) -> list:
    """[(label, identity, path, fields)] over every pod-spec-like mapping
    that still carries a DNS key in the shipped documents."""
    out = []
    for label, doc in documents:
        identity = _doc_identity(doc)
        for path, node in transforms.pod_dns_nodes(doc):
            out.append((label, identity, path, transforms.pod_dns_fields(node)))
    return out


def _shipped_candidates(fact: dict, documents: list):
    """([fields, ...], moved) for one fact. Each entry is a shipped pod
    spec's DNS fields for a document matching the fact's kind and name:
    exact namespace matches first, then documents recording no namespace
    (a render or an edit may drop it). `moved` names the namespace a
    same-kind-and-name document was found in when the fact's namespace is
    set and differs — a namespace change, not a missing document. A matched
    document whose recorded path no longer holds a pod spec, or holds one
    with no DNS key at all, reads as the empty fields — policy unset. Two
    source facts may share an identity (a base and an overlay both in
    scope), so the caller accepts a fact when ANY candidate satisfies it."""
    exact, loose, moved = [], [], None
    for _, doc in documents:
        kind, namespace, name = _doc_identity(doc)
        if fact.get("kind") != kind or fact.get("name") != name:
            continue
        if fact.get("namespace") is not None and namespace is not None:
            if fact["namespace"] == namespace:
                exact.append(doc)
            else:
                moved = moved or namespace
        else:
            loose.append(doc)
    fields = []
    for doc in exact + loose:
        node = transforms.node_at(doc, str(fact.get("node_path") or ""))
        fields.append(transforms.pod_dns_fields(node if node is not None else {}))
    return fields, (None if fields else moved)


def _policy_error(fact: dict, out_policy: str):
    """The policy finding text for one shipped candidate, or None when the
    candidate satisfies the fact. The rule: unchanged, except that a `None`
    pod may become `ClusterFirst` — `ClusterFirstWithHostNet` on a
    host-network pod, where `ClusterFirst` silently means `Default` — and
    MUST when its source nameservers include a cluster DNS address."""
    src_policy = fact.get("dns_policy") or "ClusterFirst"
    host_network = bool(fact.get("host_network"))
    cluster_dns_in_source = any(a in CLUSTER_DNS_ADDRESSES
                                for a in fact.get("nameservers") or [])
    # A None pod may flip to ClusterFirst; on host networking it may also
    # flip to ClusterFirstWithHostNet (ClusterFirst silently means Default
    # there). When the source nameservers included a cluster DNS address the
    # pod HAD cluster names, so the flip is required and, on host
    # networking, only the policy that keeps them will do.
    permitted = {"ClusterFirst", "ClusterFirstWithHostNet"} if host_network else {"ClusterFirst"}
    required = "ClusterFirstWithHostNet" if host_network else "ClusterFirst"
    if src_policy == "None" and cluster_dns_in_source and out_policy != required:
        return (f"make it dnsPolicy {required} (it ships {out_policy}): the source was "
                "None with the EKS cluster DNS address, so on GKE it resolves no cluster "
                "name; the other resolvers are an open question for the cluster-dns unit")
    if src_policy == out_policy:
        return None
    if src_policy == "None" and out_policy in permitted:
        return None
    return (f"dnsPolicy changed from {src_policy} to {out_policy}; the only change the "
            f"contract permits is None -> {' or '.join(sorted(permitted))}")


# --- the contract ---------------------------------------------------------------

def check_component(done_entries: list, rendered_docs: list = (),
                    plan_units: list = None) -> dict:
    """{checked, findings, advisory, skipped, incomplete} over the component's
    done units — `skipped` the units whose blob has no facts key, `incomplete`
    the units with an unread render source.

    `rendered_docs` are [(label, doc)] from the validate step's re-render
    of chart/kustomize sources; they are the wkld-manifests unit's output
    (carriers are owned by that family) and are read beside its plain files.
    `plan_units` is the CURRENT plan's unit list, for the facts-drift check.
    Shape and closed-list checks run over every done unit's output;
    conservation runs for wkld-manifests only, the family that persists the
    facts.
    """
    findings, advisory, skipped, incomplete = [], [], [], []
    checked = 0
    plan_facts = {str(u.get("unit_id")): (u.get("inputs") or {}).get(FACTS_KEY)
                  for u in plan_units or [] if isinstance(u, dict)}

    def finding(unit_id, subject, error):
        findings.append({"unit_id": unit_id, "subject": subject, "error": error})

    for entry in done_entries:
        unit = (entry or {}).get("unit") or {}
        unit_id = str(unit.get("unit_id") or unit.get("family") or "?")
        documents = result_documents(entry)
        if unit.get("family") == FAMILY:
            documents = documents + list(rendered_docs)
        nodes = _nodes(documents)
        checked += len(nodes)
        for label, identity, index, fields in nodes:
            subject = f"{identity[0]}/{identity[2] or label}"
            for error in schema_errors(fields) + forbidden_errors(fields):
                finding(unit_id, subject, error)
            if fields["host_network"] and (fields["dns_policy"] or "ClusterFirst") == "ClusterFirst":
                advisory.append({
                    "unit_id": unit_id, "subject": subject,
                    "note": "hostNetwork with dnsPolicy ClusterFirst (or unset) resolves "
                            "through the node, not the cluster; ClusterFirstWithHostNet "
                            "is the policy if the pod needs cluster names"})
        if unit.get("family") != FAMILY:
            continue

        facts = (unit.get("inputs") or {}).get(FACTS_KEY)
        if facts is None:
            skipped.append({"unit_id": unit_id,
                            "note": "the unit blob carries no pod_dns_facts (translated "
                                    "before the field existed); conservation was not "
                                    "checked — re-plan, then retranslate the unit "
                                    "(approve_workload_translation(action='retranslate')); "
                                    "a re-plan alone reuses this blob"})
            continue
        prose = _prose(entry.get("result"))
        unread = (unit.get("inputs") or {}).get(UNREAD_KEY) or []
        if unread:
            incomplete.append({"unit_id": unit_id,
                               "note": f"{len(unread)} render source(s) did not render at "
                                       "plan time, so the facts are incomplete: shipped "
                                       "addresses and domains are held to the closed list "
                                       "but not called invented"})
        current = plan_facts.get(unit_id)
        if current is not None and current != facts:
            finding(unit_id, "pod_dns_facts",
                    "retranslate this unit (approve_workload_translation("
                    "action='retranslate')): its blob was translated against pod DNS "
                    "facts that differ from the current plan's — a re-plan after a source "
                    "edit reused it, or the facts are read differently now")

        # Policy, per document. A nameless source document (a List bundle)
        # cannot be matched — the worker names or splits it — so only its
        # literals are held, below.
        for fact in facts:
            if not fact.get("name"):
                continue
            subject = f"{fact.get('kind')}/{fact.get('label')}"
            candidates, moved = _shipped_candidates(fact, documents)
            if not candidates and moved:
                finding(unit_id, subject,
                        f"shipped in namespace {moved}, not the source's "
                        f"{fact.get('namespace')}; a pod-bearing document does not move "
                        "namespace silently — keep it, or make the move a reviewed tradeoff "
                        "and re-plan")
                continue
            if not candidates:
                finding(unit_id, subject,
                        "this source document carries pod DNS settings and is not in the "
                        "shipped output under its kind and name, so its dnsPolicy cannot be "
                        "checked — a pod-bearing document is never dropped or renamed silently")
                continue
            verdicts = [_policy_error(fact, c.get("dns_policy") or "ClusterFirst")
                        for c in candidates]
            if all(verdicts):
                finding(unit_id, subject, verdicts[0])

        # Literals, unit-wide.
        out_fields = [n[3] for n in nodes]
        src = {"nameserver": set(), "search domain": set(), "option": set(),
               "host alias address": set(), "host alias hostname": set()}
        out = {k: set() for k in src}
        for fact in facts:
            src["nameserver"].update(fact.get("nameservers") or [])
            src["search domain"].update(fact.get("searches") or [])
            src["option"].update(_option_text(o) for o in fact.get("options") or [])
            for alias in fact.get("host_aliases") or []:
                if alias.get("ip"):
                    src["host alias address"].add(alias["ip"])
                src["host alias hostname"].update(alias.get("hostnames") or [])
        for fields in out_fields:
            out["nameserver"].update(fields["nameservers"])
            out["search domain"].update(fields["searches"])
            out["option"].update(_option_text(o) for o in fields["options"])
            for alias in fields["host_aliases"]:
                if alias.get("ip"):
                    out["host alias address"].add(alias["ip"])
                out["host alias hostname"].update(alias.get("hostnames") or [])
        for kind in src:
            for literal in sorted(src[kind] - out[kind]):
                name = literal.split("=", 1)[0] if kind == "option" else literal
                if not _named(name, prose):
                    finding(unit_id, f"{kind} {literal}",
                            f"the source {kind} {literal} is neither kept in the shipped pod "
                            "specs nor named in tradeoffs/open_questions/assumptions; a "
                            "value that goes away is named, never dropped silently")
            invented = out[kind] - src[kind]
            if unread:
                invented = set()  # the facts are incomplete; nothing is called invented
            if kind == "option":
                # An option is conserved by name and value; a changed value
                # reads as "not kept" above, not as an invention here.
                src_names = {o.split("=", 1)[0] for o in src[kind]}
                invented = {o for o in invented if o.split("=", 1)[0] not in src_names}
            for literal in sorted(invented):
                finding(unit_id, f"{kind} {literal}",
                        f"the shipped pod specs carry {kind} {literal}, which no source "
                        "document carried — the translation names no resolver, domain, "
                        "option or alias the source did not")

    return {"checked": checked, "findings": findings, "advisory": advisory,
            "skipped": skipped, "incomplete": incomplete}
