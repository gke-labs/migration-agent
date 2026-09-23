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

"""Target IP ranges proposed from the source address space. Pure code.

The knowledge document's baseline (nodes /22, pods /16, services /20 at
10.0.0.0/22, 10.4.0.0/16, 10.20.0.0/20) was the only default the design step
had, and it was written down before any estate was scanned — so a source VPC
at 10.0.0.0/16 or a peered 10.20.0.0/16 collided with it silently. The source
and target networks are routable to each other for as long as a migration
runs, so an overlap is a real defect, not a cosmetic one.

`propose_target_ranges` keeps each baseline block when it is clear of every
range discovery recorded, and otherwise takes the first clear aligned block
of the same size, walking 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16 and,
last, the shared address space 100.64.0.0/10. The proposal is echoed by
`resolve_lz_decision` beside the source ranges, and the design step uses it
as the default it writes into the network module and plan.md. Nothing here
decides: an `unresolved` entry in the section is a range the user still has
to supply, and the count of those rides along so the agent asks.
"""

import ipaddress
from collections import Counter

from servers.phases.discovery.discovery_init_1.addressspace import (
    PRIMARY_RANGE_ARGUMENTS, SERVICE_RANGE_ARGUMENTS, asked_arguments, entry_key, triage_unresolved)

# From landingzone/knowledge/gke-landing-zone.md §2 "Shared VPC": the three
# ranges of one regional subnet, in the order they are proposed, with the
# baseline block for each.
BASELINE = (
    ("nodes", ipaddress.ip_network("10.0.0.0/22")),
    ("pods", ipaddress.ip_network("10.4.0.0/16")),
    ("services", ipaddress.ip_network("10.20.0.0/20")),
)
# Where a replacement block is looked for, in order. Private space only: a
# range the design invents must not be one the internet routes. The shared
# address space 100.64.0.0/10 comes last: GKE accepts it for pod and service
# ranges, and a hub-and-spoke estate whose transit gateway routes summarise
# all of 10/8 and 172.16/12 has nothing else left for a pod /16.
CANDIDATE_SPACES = tuple(ipaddress.ip_network(s) for s in
                         ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "100.64.0.0/10"))
# Ranges inside the candidate spaces that are never proposed: Google Cloud
# accepts 172.17.0.0/16 as a subnet range, but its VPC documentation says
# not to use it where a product routes it inside the guest OS, as the
# default Docker bridge does.
RESERVED = tuple(ipaddress.ip_network(s) for s in ("172.17.0.0/16",))


def source_ranges(address_space: dict) -> list:
    """Every CIDR the section states, as (cidr, what it is) pairs in the
    section's order, deduplicated on the CIDR: VPC primary and secondary
    ranges, subnets, each cluster's service range and remote node and pod
    ranges, and routed destinations. A subnet inside a recorded VPC range
    adds nothing the VPC does not already say, so it is left out: the list
    is echoed on every decision call and thirty /24s would drown the ranges
    that matter. A subnet whose VPC has no stated range is kept."""
    seen, out, networks = set(), [], []

    def add(cidr, label, unless_covered=False):
        if not cidr or cidr in seen:
            return
        try:
            network = ipaddress.ip_network(cidr, strict=False)
        except ValueError:
            return
        if unless_covered and any(network.subnet_of(n) for n in networks if n.version == network.version):
            return
        seen.add(cidr)
        networks.append(network)
        out.append((cidr, label))

    section = address_space or {}
    # Two VPCs named `main`, one per environment: the place tells them apart.
    vpcs = section.get("vpcs") or []
    for vpc in vpcs:
        role = "cluster VPC" if vpc.get("cluster_vpc") else "VPC"
        add(vpc.get("cidr"), f"{role} {vpc.get('name')}" + _told_apart(vpcs, vpc)
            + (" (eksctl default)" if vpc.get("defaulted") else ""))
        for cidr in vpc.get("secondary_cidrs") or []:
            add(cidr, f"secondary range of VPC {vpc.get('name')}")
    for subnet in section.get("subnets") or []:
        tier = subnet.get("tier")
        add(subnet.get("cidr"), f"{tier} subnet {subnet.get('name')}" if tier else f"subnet {subnet.get('name')}",
            unless_covered=True)
    for cluster in section.get("clusters") or []:
        name = cluster.get("name") or cluster.get("address")
        add(cluster.get("service_ipv4_cidr"), f"service range of cluster {name}")
        for cidr in cluster.get("remote_node_cidrs") or []:
            add(cidr, f"remote node range of cluster {name}")
        for cidr in cluster.get("remote_pod_cidrs") or []:
            add(cidr, f"remote pod range of cluster {name}")
    for route in section.get("routes") or []:
        add(route.get("destination"), f"routed via {route.get('via')}")
    return out


def _place_of(entry: dict) -> str:
    # The place the harvester keys by: the directory for Terraform (a
    # module is a directory), the file for eksctl and CloudFormation.
    return entry_key(entry)[0] or entry.get("path") or ""


def _told_apart(entries: list, entry: dict) -> str:
    """How a same-named entry is told from its namesakes in the echo: by
    place when the places differ, else by address (two copies of one local
    module share a directory). "" when the name is unique."""
    name = entry.get("name")
    same = [e for e in entries if e.get("name") == name]
    if len(same) < 2:
        return ""
    if len({_place_of(e) for e in same}) > 1:
        return f" in {_place_of(entry)}"
    return f" ({entry.get('address')})" if entry.get("address") else ""


def _overlaps(block, taken) -> bool:
    return any(block.overlaps(t) for t in taken)


def _first_clear_block(prefixlen: int, taken: list):
    """The first block of size /prefixlen, aligned, in the candidate spaces
    that overlaps nothing in `taken`; None when the private space is full."""
    for space in CANDIDATE_SPACES:
        # Step block by block; the taken list is short (an estate's ranges),
        # so the walk ends within a handful of steps in practice, and each
        # overlap skips to the end of the range it hit.
        cursor = int(space.network_address)
        end = int(space.broadcast_address) + 1
        size = 2 ** (space.max_prefixlen - prefixlen)
        while cursor + size <= end:
            block = ipaddress.ip_network((cursor, prefixlen))
            hit = next((t for t in taken if block.overlaps(t)), None)
            if hit is None:
                return block
            # The end of the hit is aligned to this size already: a hit
            # smaller than the block lies inside it, a larger one is a CIDR
            # block whose size is a multiple of this one.
            cursor = max(cursor + size, int(hit.broadcast_address) + 1)
    return None


def propose_target_ranges(address_space: dict) -> dict:
    """Returns {"avoid": [(cidr, label), ...], "proposed": {"nodes": cidr,
    "pods": cidr, "services": cidr}, "moved": [kind, ...], "unresolved": n}.

    `moved` names the ranges that left the baseline because it overlapped a
    source range; `unresolved` counts the section's unresolved entries that
    can still change the proposal, each a range the user has to supply
    before it can be trusted, and `covered` the ones that cannot (a subnet
    inside a stated VPC range or of a VPC whose own range is already asked
    for, a public-endpoint allow list). A kind is null
    in `proposed` only when no candidate block is free.
    """
    avoid = source_ranges(address_space)
    # An IPv6 range never overlaps an IPv4 block (ipaddress says so), so
    # nothing filters them out here.
    taken = [ipaddress.ip_network(cidr, strict=False) for cidr, _ in avoid] + list(RESERVED)
    source_taken = list(taken)
    proposed, moved = {}, []
    for index, (kind, baseline) in enumerate(BASELINE):
        if not _overlaps(baseline, taken):
            block = baseline
        else:
            # A replacement must not evict a later baseline that is itself
            # clear: nodes moved off 10.0.0.0/22 by a 10.0.0.0/14 VPC must
            # not land inside 10.4.0.0/16 and push pods out as well.
            later = [b for _, b in BASELINE[index + 1:] if not _overlaps(b, source_taken)]
            block = _first_clear_block(baseline.prefixlen, taken + later)
        if block is not baseline:
            moved.append(kind)
        proposed[kind] = str(block) if block is not None else None
        if block is not None:
            taken.append(block)
    blocking, covered = triage_unresolved(address_space or {})
    asked = asked_arguments(address_space or {})
    vpcs = (address_space or {}).get("vpcs") or []
    clusters = (address_space or {}).get("clusters") or []

    def asked_for(entry, arguments):
        # Already a question under `unresolved`: not also "unset".
        return bool(asked.get(entry_key(entry), set()) & set(arguments))
    return {
        "avoid": avoid,
        "proposed": proposed,
        "moved": moved,
        "unresolved": len(blocking),
        "blocking": blocking,
        "covered": len(covered),
        # A VPC the files name without stating its range: IPAM-allocated, or
        # an existing VPC named by id. Not an unresolved expression, but
        # every bit as much a question — the proposal cannot be checked
        # against a range nobody wrote down.
        "unstated_vpcs": [f"{v.get('name')}{_told_apart(vpcs, v)}" for v in vpcs
                          if not v.get("cidr") and not asked_for(v, PRIMARY_RANGE_ARGUMENTS)],
        # A cluster whose VPC has no entry at all: an existing VPC reached
        # through a data source, or subnets the files do not declare. Its
        # range is the one that matters most and nobody wrote it down.
        "clusters_without_a_vpc": _clusters_without_a_vpc(address_space or {}),
        # A cluster that leaves service_ipv4_cidr unset: EKS picked a range
        # at creation, usually one of two documented defaults, and for a
        # cluster with remote networks (hybrid nodes) one clear of those
        # networks, which may be another block; the files do not say which.
        # Not avoided (that would be a guess), but said. An IPv6 cluster
        # cannot set it and its service range is an IPv6 block, so there is
        # nothing to ask.
        "clusters_with_a_defaulted_service_range": [
            (c.get("name") or c.get("address") or "cluster") + _told_apart(clusters, c)
            for c in clusters
            if not c.get("service_ipv4_cidr") and (c.get("ip_family") or "ipv4") != "ipv6"
            and not asked_for(c, SERVICE_RANGE_ARGUMENTS)],
    }


def _clusters_without_a_vpc(section: dict) -> list:
    """Clusters whose `vpc` names no recorded entry. Keyed as the harvester
    keys: an eksctl or CloudFormation cluster's VPC is in the same file (a
    Parameter named like another template's resource is not it); only a
    Terraform reference may cross directories."""
    recorded = {entry_key(v) for v in section.get("vpcs") or []}
    addresses = {v.get("address") for v in section.get("vpcs") or []}
    out = []
    for cluster in section.get("clusters") or []:
        vpc = cluster.get("vpc")
        terraform = (cluster.get("path") or "").endswith(".tf")
        found = entry_key(cluster, vpc) in recorded or (terraform and vpc in addresses)
        if not found:
            name = cluster.get("name") or cluster.get("address") or "cluster"
            out.append(f"{name} (vpc: {cluster.get('vpc') or 'not followed'})")
    return out


def describe_proposal(address_space: dict) -> str:
    """The lines `resolve_lz_decision` echoes: the source ranges, the proposed
    target ranges, and what the agent still has to ask for."""
    baseline = ", ".join(f"{kind} {block}" for kind, block in BASELINE)
    if not address_space:
        return ("source_address_space: not recorded — the inventory has no address_space "
                "section (discovery ran before the address-space scan existed, or the scan "
                "found no VPC, subnet or cluster range in the files; address_space_scan_notes "
                "says which when it is present). Ask the user for the source ranges before "
                "choosing the target ones.\n"
                f"proposed_target_ranges: {baseline} (the baseline, not checked against the source)")
    proposal = propose_target_ranges(address_space)
    if not proposal["avoid"]:
        # The section exists but states no range: a VPC named by id, an IPAM
        # pool, or only unresolved expressions. The baseline is offered with
        # the same caveat as an absent section, never as a checked proposal;
        # the questions below it are the same ones the checked echo carries.
        # Every VPC without a range, asked-for or not: this sentence names
        # the network; `vpcs_without_a_stated_range` below names only the
        # ones with no question of their own.
        rangeless = [v.get("name") for v in address_space.get("vpcs") or [] if not v.get("cidr")]
        lines = ["source_address_space: no range stated — the files name the network "
                 f"({', '.join(n for n in rangeless if n) or 'no VPC'}) without "
                 "stating its ranges, so ask the user for the source ranges before choosing the "
                 "target ones.",
                 f"proposed_target_ranges: {baseline} (the baseline, not checked against the source)"]
        return "\n".join(lines + _question_lines(proposal, unstated=False))
    lines = ["source_address_space: " + "; ".join(f"{cidr} ({label})" for cidr, label in proposal["avoid"])]
    ranges = ", ".join(f"{kind} {cidr or 'no free block'}" for kind, cidr in proposal["proposed"].items())
    lines.append(f"proposed_target_ranges: {ranges}")
    return "\n".join(lines + _question_lines(proposal))


def _question_lines(proposal: dict, unstated: bool = True) -> list:
    """What the agent still has to ask for or confirm, one line per kind,
    the same in the checked echo and the no-range one (which has already
    named the unstated VPCs in its first line, hence `unstated`)."""
    lines = []
    if unstated and proposal["unstated_vpcs"]:
        names = ", ".join(n for n in proposal["unstated_vpcs"] if n)
        lines.append(f"vpcs_without_a_stated_range: {names} — the files name this VPC (an IPAM "
                     "pool, or an existing VPC by id) without its CIDR; ask the user for it, the "
                     "proposal was not checked against it")
    if proposal["clusters_with_a_defaulted_service_range"]:
        lines.append("clusters_with_a_defaulted_service_range: "
                     + ", ".join(proposal["clusters_with_a_defaulted_service_range"])
                     + " — the files set no service_ipv4_cidr, so EKS chose one at creation: usually "
                     "10.100.0.0/16 or 172.20.0.0/16, another block for a cluster with remote "
                     "networks; not in the avoidance set, ask the user for the live value "
                     "(aws eks describe-cluster) and keep the proposal clear of it")
    if proposal["clusters_without_a_vpc"]:
        lines.append("clusters_without_a_recorded_vpc: " + "; ".join(proposal["clusters_without_a_vpc"])
                     + " — the cluster's own VPC is not among the recorded ones (an existing VPC "
                     "reached through a data source, or subnets the files do not declare); ask the "
                     "user for its range, the proposal was not checked against it")
    if proposal["moved"]:
        lines.append("moved_off_the_baseline: " + ", ".join(proposal["moved"])
                     + " (the baseline block overlapped a source range)")
    if proposal["unresolved"]:
        # The entries themselves, as the scan summary and the readiness
        # report carry them: the agent asks for what is listed and does not
        # re-derive the split from address_space.unresolved.
        entries = "; ".join(f"{e.get('address')} ({e.get('path')}) {e.get('argument')} = {e.get('expression')}"
                            for e in proposal["blocking"])
        lines.append(f"unresolved_source_ranges: {proposal['unresolved']} — ranges the files build from "
                     "something the scan does not follow; ask the user for each of these before the "
                     f"design is final: {entries}")
    if proposal["covered"]:
        lines.append(f"unresolved_but_covered: {proposal['covered']} — subnet ranges inside a "
                     "stated VPC range or of a VPC whose own range is already among the questions, "
                     "or public-endpoint allow lists; listed under address_space.unresolved for the "
                     "record, no question of their own")
    missing = [kind for kind, cidr in proposal["proposed"].items() if cidr is None]
    if missing:
        lines.append("no_free_block_for: " + ", ".join(missing) + " — every candidate space "
                     "(10/8, 172.16/12, 192.168/16, 100.64/10) overlaps a source range at that "
                     "size; ask the user which range the target may use for it")
    return lines
