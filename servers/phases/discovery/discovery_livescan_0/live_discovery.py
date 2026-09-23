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

"""Live discovery orchestrator: single round-trip AWS + in-cluster walk.

Assembles the Live IR from the three seams: the AWS cloud-plane walk
(aws_live), one in-cluster walk per reachable cluster (k8s_live via the
per-cluster get_json), and the IRSA cloud-side resolution that joins the
two (the ARNs come from the cluster, the trust policies from AWS). All AWS
and cluster access is injected — `client_factory` and `get_json_factory` —
so the orchestration is testable end to end with no boto3, no kubernetes
client, and no network.

The whole point of the design is one call: `run_live_discovery` returns the
complete IR plus the CSV tables, so the state machine step is a thin ledger
wrapper (the datascan pattern) with no fan-out of its own.

Partial results are the contract, not an error mode. A cluster whose control
plane will not answer keeps its AWS-side record with `kubernetes_error` set,
and the walk goes on — a scan that dropped 39 clusters because the 40th was
unreachable would be worse than useless.

The AWS-side record gets the same keys-only treatment the in-cluster walk
gives every object: EKS cluster tags, nodegroup labels and subnet tags are
free-form customer text, so aws_live records their key names and the
OMITTED marker for each value, before anything downstream (the in-cluster
join, the CSVs, the ledger) sees the record, and for unreachable clusters
too. The IR is one shape and one blob: no free-form value a customer
authored is in it (what survives is an identifier, a reference, an
enumeration or a scheduling and routing contract), so there is no fuller
record to keep beside it.
"""

from . import aws_live, k8s_live, live_csv

# Live IR schema version. Bumped when the IR shape changes so a consumer can
# tell which fields to expect; unrelated to the platform DAG version.
LIVE_IR_VERSION = "2.0"


def run_live_discovery(client_factory, regions, get_json_factory,
                       cluster_names=None, namespaces=None) -> dict:
    """Runs the full live walk and returns {"ir", "tables"}.

    Args:
      client_factory: (service, region) -> AWS client. From eks_auth in
        production, a fake in tests.
      regions: AWS regions to scan for EKS clusters.
      get_json_factory: (cluster_dict) -> get_json(path), the per-cluster
        control-plane reader. Raising means the cluster is unreachable; the
        walk records that and continues.
      cluster_names: optional allow-list of cluster names.
      namespaces: optional namespace filter for every in-cluster walk; empty
        walks all namespaces except the AWS system ones (see k8s_live).

    The IR carries its own provenance and every note the walk raised, so the
    ledger record says what was and was not covered without a second source.
    Its `regions` are the ones whose clusters were listed; a named region
    that could not be listed is in the summary's `regions_unreachable` and
    a note, and a scan in which none listed comes back with no regions at
    all — the tool's to refuse, not this walk's.
    """
    if not regions:
        raise ValueError("no regions to scan: name at least one AWS region")
    estate = aws_live.discover_estate(client_factory, regions, cluster_names)
    ir = {
        "live_ir_version": LIVE_IR_VERSION,
        "regions": estate["regions"],
        "clusters": estate["clusters"],
        "notes": list(estate["notes"]),
    }

    for cluster in ir["clusters"]:
        try:
            get_json = get_json_factory(cluster)
        except Exception as e:
            cluster["kubernetes_error"] = str(e)
            ir["notes"].append(
                f"cluster {cluster.get('name')}: control plane not reached — "
                f"{e}. Its AWS-side record stands; its in-cluster workloads "
                "were not collected.")
            continue
        try:
            kubernetes_ir = k8s_live.discover_cluster(get_json,
                                                      namespaces=namespaces)
        except Exception as e:
            cluster["kubernetes_error"] = str(e)
            ir["notes"].append(
                f"cluster {cluster.get('name')}: in-cluster walk failed — {e}"
                + _walk_failure_advice(e))
            continue
        finally:
            # Release the control-plane client's temp CA file if the factory
            # exposed a cleanup hook (a fake get_json in tests won't). Runs
            # whether the walk succeeded or raised.
            cleanup = getattr(get_json, "cleanup", None)
            if callable(cleanup):
                cleanup()
        cluster["kubernetes"] = kubernetes_ir
        # Per-cluster notes belong to the cluster, but also surface at the top
        # so a reader sees total coverage without descending into each cluster.
        for note in kubernetes_ir.get("notes") or []:
            ir["notes"].append(f"cluster {cluster.get('name')}: {note}")

    # IAM is global; its client goes to a region that answered — the first
    # that listed its clusters, or the first requested when none did.
    _resolve_irsa(client_factory, ir["clusters"], ir["notes"],
                  (ir["regions"] or list(regions))[0])
    tables = live_csv.render_tables(ir)
    ir["summary"] = _summary(ir, estate.get("regions_unreachable") or [])
    return {"ir": ir, "tables": tables}


def _walk_failure_advice(error) -> str:
    """The one next step a control-plane refusal calls for, or "".

    A 401 from an EKS API server has causes the error text alone ("HTTP
    401 Unauthorized") does not tell apart. When the re-mint the client
    answers a 401 with (or the one it makes before a call, past the
    refresh window) could not even sign a token, the AWS credentials
    themselves are gone — an SSO session ended, a provider's refresh
    failed — and the error carries that failure. When the re-mint
    succeeded and the fresh token was refused too, the cause is either
    the identity's mapping (mapped to nothing in that cluster: no access
    entry, no aws-auth row — a per-cluster fix the AWS-side credential
    check cannot predict) or a session credential that expired during the
    walk: presigning is offline, so a token signed with an expired session
    key is minted without complaint and refused by the cluster exactly
    like an unmapped one. The advice names both and the one call that
    tells them apart. A 403 is explained where it is detected (k8s_live
    raises it with the grant to make); anything else is left to speak for
    itself.
    """
    if getattr(error, "status", None) == 401:
        if getattr(error, "token_refresh_error", None) is not None:
            return (". No fresh token could be minted, so the AWS "
                    "credentials themselves have expired or been revoked: "
                    "refresh them (a new session, `aws sso login`) and "
                    "re-run")
        return (". A freshly minted token was refused too: either the AWS "
                "identity is not mapped in this cluster — add an EKS access "
                "entry for it (with AmazonEKSViewPolicy at cluster scope) or "
                "an aws-auth mapping to a view ClusterRole — or the session "
                "credentials it was signed with expired during the walk "
                "(`aws sts get-caller-identity` failing now says which; "
                "refresh them). Then re-run")
    return ""


def _resolve_irsa(client_factory, clusters, notes, region) -> None:
    """Attaches AWS-side role detail to each IRSA binding, in place.

    One resolution pass over the union of ARNs (a role is often shared
    across service accounts and clusters), then the result is stitched back
    onto every binding that names it. This is the join a static scan cannot
    make: the manifest has the annotation, only AWS has the trust policy.
    """
    arns = {
        binding.get("role_arn")
        for cluster in clusters
        for binding in ((cluster.get("kubernetes") or {}).get("identity")
                        or {}).get("irsa") or []
        if binding.get("role_arn")}
    if not arns:
        return
    # The account the clusters live in, read off their ARNs
    # (arn:aws:eks:REGION:ACCOUNT:cluster/NAME): a role in another account
    # cannot be read with these credentials, and is recorded as unresolved
    # rather than missing. Left unknown when the clusters span accounts.
    accounts = {(cluster.get("arn") or "").split(":")[4]
                for cluster in clusters
                if (cluster.get("arn") or "").count(":") >= 5}
    account = accounts.pop() if len(accounts) == 1 else None
    try:
        resolved = aws_live.resolve_irsa_roles(client_factory, arns,
                                               region=region, account=account)
    except Exception as e:
        # resolve_irsa_roles downgrades a per-role failure to that role's
        # `error` (exists false for a role that is gone, null for one that
        # could not be read); reaching here means the IAM client itself
        # could not be built — no credentials, no endpoint for the region.
        # Leave the bindings' ARNs standing without role detail — the
        # annotation is still the finding — rather than dropping them.
        notes.append(f"IRSA roles not resolved against AWS IAM — {e}. The "
                     "bindings are recorded from the cluster; their trust "
                     "policies and grants were not read.")
        return
    for cluster in clusters:
        for binding in ((cluster.get("kubernetes") or {}).get("identity")
                        or {}).get("irsa") or []:
            role = resolved.get(binding.get("role_arn"))
            if role:
                binding["role"] = role


def _summary(ir: dict, regions_unreachable=()) -> dict:
    """A compact, machine-and-human summary the tool response leads with.

    Aggregate counts across clusters plus the facts an operator acts on
    first: the regions and clusters that could not be read, which say how
    far this record can be trusted to be the whole estate.
    """
    totals = {}
    unreachable = []
    karpenter_clusters = 0
    for cluster in ir["clusters"]:
        if cluster.get("kubernetes_error"):
            unreachable.append(cluster.get("name"))
            continue
        kubernetes_ir = cluster.get("kubernetes") or {}
        for kind, count in (kubernetes_ir.get("counts") or {}).items():
            totals[kind] = totals.get(kind, 0) + count
        karpenter = (kubernetes_ir.get("autoscaling") or {}).get(
            "karpenter") or {}
        # NodePools are current Karpenter; a v1alpha5 estate has only
        # Provisioners, and it uses Karpenter no less for that.
        if karpenter.get("nodepools") or karpenter.get("provisioners"):
            karpenter_clusters += 1
    return {
        "regions_scanned": len(ir["regions"]),
        "regions_unreachable": list(regions_unreachable),
        "clusters_found": len(ir["clusters"]),
        "clusters_walked": len(ir["clusters"]) - len(unreachable),
        "clusters_unreachable": unreachable,
        "clusters_using_karpenter": karpenter_clusters,
        "workload_totals": totals,
        "note_count": len(ir["notes"]),
    }
