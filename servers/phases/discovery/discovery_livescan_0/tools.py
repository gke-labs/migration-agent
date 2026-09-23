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

"""MCP tool for discovery step 0 (live scan): discover_and_dump_all_clusters.

Owns the STATE_DISCOVERY_LIVE agent task — the first step of discovery, ahead
of the static IaC index. The walk itself is deterministic code
(live_discovery.py over the aws_live / k8s_live seams); this step is the
ledger and state-machine wrapper, the same division datascan keeps.

An agent task, not a server-side action, for the datascan reasons and one
more of its own: live AWS access is not guaranteed at discovery time (a
repos-only engagement, a green-field target), and an agent task can offer a
recorded skip that advances the graph, where a server action could only
fail. A failure — authentication, an exception in the walk, no requested
region that would list its clusters — leaves the state here, so an expired
credential or a mistyped region can be fixed and the call retried; a region
denied among others that listed, or a cluster that refuses the token, is a
note on a completed scan (the region named in the summary's
regions_unreachable too), and the graph moves on (a second call is refused
as out of state).

Read-only against the customer estate by construction: every AWS and cluster
operation the walk issues is a List/Describe/Get. Credentials come only from
the caller's local environment (the boto3 chain); this server never asks for
or stores one, and holds the AWS and Kubernetes client libraries' loggers
(boto3, botocore, urllib3, kubernetes) at INFO while it authenticates and
walks, releasing them after, so a presigning trace cannot reach a DEBUG root
logger.

Safe to write to the workspace-readable bucket by construction too: no
free-form value a customer authored is in the record. Every object the walk
persists is projected (projection.py) to its structure and the strings a
migration reproduces literally — identifiers and references, images,
classes, ports, drivers, the scheduling and routing contracts (selectors,
taints and tolerations, requests and limits, hostnames, route paths), an
exact-key list of platform labels, class/mode annotations and StorageClass
parameters — and every other string, whatever its key, is the `<omitted>`
marker: a Secret's or ConfigMap's values, an env literal, a command token,
a custom annotation or label value, a Karpenter userData script, an AWS
tag. Nothing is parsed or classified, so there is no credential
shape to miss; the walk carries the keys and the shape a migration
reproduces, and the values stay in the estate.

The IR's shape is a contract, servers/dag/server/schema/live_discovery.json
(live_schema.py), persisted once under the schema's name with one CSV per
table beside it. The IR is code-authored, so a schema violation is a bug in
the walk, not in the estate: the tool keeps the scan (minutes of work, and
the operator's only copy) and records the violation as a coverage note in
the IR itself rather than refusing to save it — a reader keyed on the schema
sees exactly which section to distrust, and the state still advances.
"""

import asyncio
import json
import logging
from typing import List, Optional

from google.api_core import exceptions
from mcp.server.fastmcp import Context

from servers.dag.state_management import (
    get_bucket_name,
    load_dag,
    authorize_and_rehydrate,
)
import servers.dag.state_management as state_mgr

from . import eks_auth, live_discovery, live_schema

logger = logging.getLogger("migration-dag")

# Where the live scan's outputs live. A prefix, not a single blob: the IR
# (named for its schema, server/schema/live_discovery.json) and one CSV
# per table, all served to the Review UI together.
LIVE_PREFIX = "platform/discovery/live/"
LIVE_IR_BLOB = LIVE_PREFIX + live_schema.SCHEMA_NAME

# The platform graph's state record: read under a generation at the start
# of the call, re-read before the prefix is touched, advanced under the
# same generation at the end.
_STATE_BLOB = "platform/onboarding/state.json"


async def discover_and_dump_all_clusters(
    regions: Optional[List[str]] = None,
    cluster_names: Optional[List[str]] = None,
    namespaces: Optional[List[str]] = None,
    aws_profile: Optional[str] = None,
    skip: bool = False,
    skip_reason: Optional[str] = None,
    ctx: Context = None,
) -> str:
    """
    Discovers the live EKS estate from AWS in a single call: the cloud plane
    (clusters, node groups and ASGs, VPC/subnets/security groups, load
    balancers, IRSA roles) and each reachable cluster's in-cluster estate
    (Deployments and the other workload kinds, owner-less pods, HPAs,
    Karpenter node pools, nodes, PVCs, PVs and StorageClasses,
    Services/Ingress plus Gateway API / Istio / Traefik routing, ConfigMaps,
    Secrets, and external secret references), each object projected to
    its key names and structural fields — no free-form value a customer
    authored is written. Writes a Live IR and CSV tables to the ledger.

    This is the operational reality the migration reconciles the static
    Terraform/GitOps sources against. It is read-only against AWS, and uses
    only the credentials already in your local environment.

    Args:
        regions: AWS regions to scan for EKS clusters (e.g. ["us-east-1",
            "eu-west-1"]). Required unless skipping — scanning every region
            is slow and usually wrong, so the operator names the ones in use.
        cluster_names: optional filter; omit to take every cluster in the
            scanned regions. A named cluster found in no region is reported.
        namespaces: optional namespace filter for the in-cluster walk. Omit
            to walk every namespace except the AWS system ones (kube-system,
            kube-public, kube-node-lease, amazon-cloudwatch,
            aws-observability, amazon-guardduty) — EKS plumbing that does
            not migrate. Naming a namespace includes it, system or not.
        aws_profile: optional named profile from the local AWS config; omit
            to use the default credential chain (env vars, SSO, default
            profile).
        skip: set true only when there is no live AWS access at discovery
            time (a repos-only engagement, or a green-field target with no
            source cluster). Advances to the static index, recording that the
            live estate was not scanned. skip_reason is then required.
        skip_reason: why the live scan was skipped — persisted with the
            inventory so an empty Live IR reads as a decision, not a gap.

    On success the graph advances to the static IaC index (STATE_DISCOVERY);
    a region denied among others that listed (named in the summary's
    regions_unreachable) or a cluster that refused the token is a coverage
    note on that success, not a reason to call again — the state has moved,
    and a second call is refused. Only a failure (authentication, no
    requested region reachable to authenticate against, an exception in
    the walk, no requested region listable at all) leaves the state here
    for a retry.
    """
    logger.info(
        "discover_and_dump_all_clusters called "
        f"(regions={regions!r}, cluster_names={cluster_names!r}, "
        f"namespaces={namespaces!r}, aws_profile={aws_profile!r}, "
        f"skip={skip!r}).")
    try:
        state_dict, generation, config = authorize_and_rehydrate(None)
    except Exception as e:
        return f"ERROR: {e}"

    current_state = state_dict["current_state"]
    if current_state != "STATE_DISCOVERY_LIVE":
        return f"ERROR: Invalid state for discover_and_dump_all_clusters: {current_state}"

    variables = state_dict["variables"]
    bucket_name = get_bucket_name(config["ledger_uri"])
    bucket = state_mgr.gcs_client.bucket(bucket_name)
    try:
        platform_dag = load_dag(bucket, "platform_dag.json")
    except Exception as e:
        return f"ERROR: {e}"
    try:
        next_state = platform_dag["states"][current_state][
            "transitions"]["on_tool_call_received"]
    except (KeyError, TypeError):
        return (f"ERROR: The ledger's platform DAG has no "
                f"on_tool_call_received transition for {current_state}. The "
                "graph in GCS may predate this step; run upgrade_ledger_dags "
                "to bring it to the bundled version, then retry.")

    if skip:
        if not (skip_reason and skip_reason.strip()):
            return ("ERROR: skip=True needs a skip_reason. An empty Live IR "
                    "with no reason cannot be told apart from a scan that "
                    "found nothing — say why there is no live estate to scan "
                    "(e.g. 'repos-only engagement', 'green-field target').")
        # A skip must not leave an earlier scan's record standing under the
        # live prefix: a reader would take it for this run's estate. The
        # graph moves first, under the generation check: only the caller
        # whose skip actually landed removes anything, so a skip that lost
        # a race to a concurrent scan cannot delete that scan's results.
        error = await _write_state(
            bucket, state_dict, generation, current_state, next_state,
            variables, live_summary=None, skip_reason=skip_reason.strip())
        if error:
            return error
        try:
            removed = await asyncio.to_thread(_clear_live_prefix, bucket)
        except Exception as e:   # noqa: BLE001 — the skip has landed
            # The graph has moved; the leftovers are the only thing wrong,
            # and the caller can act on them by hand. A PrefixClearError
            # carries the count already removed, reported as such so the
            # message matches the bucket; any other failure removed nothing.
            leftover = (str(e) if isinstance(e, PrefixClearError)
                        else f"{type(e).__name__}: {e}")
            return _skip_response(next_state, skip_reason.strip(),
                                  removed=getattr(e, "removed", 0),
                                  leftover=leftover,
                                  found=getattr(e, "found", None))
        return _skip_response(next_state, skip_reason.strip(), removed)

    if not regions:
        return ("ERROR: No regions to scan. Ask the platform engineer which "
                "AWS region(s) the EKS estate runs in and call "
                "discover_and_dump_all_clusters(regions=[...]). If there is no "
                "live AWS access for this migration, call it with skip=True "
                "and a skip_reason instead.")
    if any(not isinstance(region, str) or not region.strip()
           for region in regions):
        return ("ERROR: Every region must be a non-empty AWS region name "
                f"(us-east-1, eu-west-1, …); got {regions!r}.")

    missing = eks_auth.missing_dependencies()
    if missing:
        return ("ERROR: Live discovery needs the optional packages "
                f"{', '.join(missing)}, which are not installed in this "
                f"server's environment. Install them (pip install "
                f"{' '.join(missing)}) and retry, or call with skip=True and a "
                "skip_reason to continue with the static sources only.")

    # Credentials are verified before the walk (one STS call) and the walk
    # follows, both in one worker thread off the event loop: an unreachable
    # STS endpoint or an SSO refresh would otherwise hold every other
    # request of this server for botocore's connect timeout times its
    # retries, and the walk itself is minutes of blocking network I/O
    # (boto3 and the kubernetes client are synchronous) — the discipline
    # translation_translate_1 already keeps for its blocking work. One
    # thread rather than two because the hold on the client libraries'
    # loggers (eks_auth._hold_library_logs_at_info, taken when the first
    # AWS client is built) must be released by the code that knows the
    # walk is over, and only the thread does: a cancelled request ends
    # this coroutine at once while the thread keeps signing requests.
    identity, result, failure = await asyncio.to_thread(
        _authenticate_and_walk, aws_profile, regions, cluster_names,
        namespaces)
    if failure:
        stage, e = failure
        if stage == "authenticate":
            not_enabled = (
                " An InvalidClientTokenId from every requested region is "
                "also what STS answers in a region the account has not "
                "enabled; check the regions are opted in (aws account "
                "get-region-opt-status)."
                if eks_auth.aws_error_code(e) == "InvalidClientTokenId" else "")
            unreached = getattr(e, "unreachable", None) or []
            not_asked = (
                f" {len(unreached)} requested region(s) could not be reached "
                "at all and gave no verdict: " + "; ".join(unreached)
                + "; check those names." if unreached else "")
            return ("ERROR: Could not authenticate to AWS with the local "
                    f"credentials: {e}. Check that credentials are present "
                    "(aws sts get-caller-identity), refresh SSO if it expired, or "
                    "pass aws_profile=<name>." + not_enabled + not_asked
                    + " The state stays here; retry when fixed.")
        if stage == "regions":
            return ("ERROR: No requested region could be reached to "
                    f"authenticate against: {e}. Check the region names (a "
                    "mistyped one has no STS endpoint) and the network path "
                    "to AWS. Nothing was written and the state stays here; "
                    "retry when fixed.")
        return ("ERROR: Live discovery failed after authentication: "
                f"{e}. The state stays here; retry when fixed.")

    ir = result["ir"]
    if not ir["regions"]:
        # No region listed its clusters: every name mistyped, or
        # eks:ListClusters denied everywhere. An empty estate written as a
        # completed scan would read as "nothing runs there", and the only
        # way back would be a platform reset — a failure to retry, not a
        # coverage note.
        return ("ERROR: Live discovery could not list EKS clusters in any "
                f"requested region ({', '.join(regions)}): "
                + "; ".join(ir["notes"])
                + ". Nothing was written and the state stays here. Check "
                "the region names and that eks:ListClusters is permitted, "
                "then retry; if there is genuinely no live access, call "
                "with skip=True and a skip_reason instead.")
    ir["scanned_by"] = identity.get("arn")
    await asyncio.to_thread(_note_schema_violation, ir)
    began = []      # _persist appends "clear" once the prefix is touched
    try:
        await asyncio.to_thread(_persist, bucket, ir, result["tables"],
                                generation, began)
    except StateMovedError as e:
        return ("ERROR: Concurrent update conflict: another writer moved the "
                f"migration state while this scan ran ({e}). Nothing was "
                f"written or removed under {LIVE_PREFIX}; what that writer "
                f"recorded is the graph's. If it still stands at "
                f"{current_state}, re-run discover_and_dump_all_clusters; if "
                "it has moved on, the live estate is that writer's.")
    except PrefixClearError as e:
        # The clear runs first and the IR goes first within it, so
        # nothing of this scan was written; what stands under the prefix
        # is the earlier scan's, less what was removed.
        logger.exception("Clearing the Live prefix failed")
        gone = (f"{e.removed} of its {e.found} object(s) removed"
                if e.found is not None else "its listing failed, nothing removed")
        return ("ERROR: Live discovery ran but its results could not be "
                f"saved: clearing the earlier estate under {LIVE_PREFIX} "
                f"stopped part-way ({e}), {gone}. Nothing of this scan was "
                "written and the state stays here; what remains under the "
                "prefix is the earlier scan's. Retry once the bucket answers.")
    except Exception as e:
        logger.exception("Persisting the Live IR failed")
        if "clear" not in began:
            # The re-read of state.json itself failed: nothing was touched.
            return ("ERROR: Live discovery ran but its results could not be "
                    f"saved ({type(e).__name__}: {e}). Nothing was cleared or "
                    f"written under {LIVE_PREFIX} — the earlier estate stands "
                    "— and the state has not moved. Re-run "
                    "discover_and_dump_all_clusters once the bucket answers.")
        return ("ERROR: Live discovery ran but its results could not be "
                f"saved ({type(e).__name__}: {e}). The earlier estate under "
                f"{LIVE_PREFIX} was cleared before the writes began, so what "
                "stands there is this scan's tables without its IR, and "
                "the state has not moved. Re-run "
                "discover_and_dump_all_clusters once the bucket answers: the "
                "rescan clears the prefix before it writes.")

    error = await _write_state(
        bucket, state_dict, generation, current_state, next_state,
        variables, live_summary=ir["summary"], skip_reason=None)
    if error:
        return error
    return _scan_response(next_state, ir["summary"], ir["notes"])


def _authenticate_and_walk(profile, regions, cluster_names, namespaces):
    """Authenticates, walks, and releases the client libraries' loggers when
    the walk is over — in the calling thread, whichever way it ends.

    Returns `(identity, result, failure)`: `failure` is None, or
    `("authenticate", error)`, `("regions", error)` or `("walk", error)`,
    the three being reported differently. A failure is returned rather
    than raised because the
    caller awaits this in asyncio.to_thread, and a cancelled request
    detaches from the thread without stopping it: anything the coroutine
    did in a `finally` would run while the walk was still signing
    requests, and releasing the hold then would put botocore's DEBUG
    trace of those signatures in the server log. Released here, the hold
    ends with the last request the walk makes."""
    try:
        try:
            client_factory, identity = _authenticate(profile, regions)
        except RegionsUnreachable as e:
            return None, None, ("regions", e)
        except Exception as e:   # noqa: BLE001 — reported, not raised
            return None, None, ("authenticate", e)
        try:
            result = live_discovery.run_live_discovery(
                client_factory,
                regions,
                lambda cluster: eks_auth.make_get_json(cluster, client_factory),
                cluster_names=cluster_names,
                namespaces=namespaces)
        except Exception as e:   # noqa: BLE001 — reported, not raised
            logger.exception("Live discovery failed")
            return identity, None, ("walk", e)
        return identity, result, None
    finally:
        eks_auth.release_library_logs()


class RegionsUnreachable(Exception):
    """No requested region answered the STS preflight: every endpoint failed
    to resolve or connect (a mistyped name, no network path to AWS)."""


class CredentialRefused(Exception):
    """Every requested region that could be reached answered the STS
    preflight with `InvalidClientTokenId` — the credential's verdict, raised
    from the first refusal so eks_auth.aws_error_code reads the code through
    the cause — while `unreachable` names the requested regions that could
    not be asked at all, as RegionsUnreachable would name them, so the report
    names both and a mistyped region is not lost behind the refusal."""

    def __init__(self, refusal, unreachable):
        super().__init__(str(refusal))
        self.unreachable = list(unreachable)


def _authenticate(profile, regions):
    """Builds the AWS seam and resolves the caller's identity — the one STS
    call before the walk, so a missing or expired credential is one clear
    error rather than a note on every region. Asked in the first requested
    region, where the scan is about to go (a credential in another
    partition cannot reach sts.us-east-1). A region whose endpoint cannot
    be reached at all — a mistyped `eu-west1` has none — is not the
    credential's failure (eks_auth.is_unreachable_endpoint), and the next
    region is asked instead, so the typo stays the coverage note the walk
    makes of it wherever it stands in the list; a region that answers
    `InvalidClientTokenId` is asked past the same way — STS in a region
    the account has not opted into says exactly that of a valid key
    (eks_auth.aws_error_code) — and the answer is the credential's verdict
    only when no region authenticates (CredentialRefused, naming beside the
    refusal the regions that could not be asked at all); when no region can
    be reached the failure is the regions' (RegionsUnreachable). Blocking
    (botocore), so the caller runs it off the event loop."""
    client_factory = eks_auth.make_client_factory(profile_name=profile)
    unreachable, refused = [], []
    for region in regions:
        try:
            identity = eks_auth.caller_identity(client_factory, region=region)
        except Exception as e:   # noqa: BLE001 — sorted by kind, re-raised
            if eks_auth.is_unreachable_endpoint(e):
                unreachable.append(f"{region} ({e})")
                continue
            if eks_auth.aws_error_code(e) == "InvalidClientTokenId":
                refused.append(e)
                continue
            raise
        return client_factory, identity
    if refused:
        raise CredentialRefused(refused[0], unreachable) from refused[0]
    raise RegionsUnreachable("; ".join(unreachable))


async def _write_state(bucket, state_dict, generation, current_state,
                       next_state, variables, live_summary, skip_reason):
    """Records the outcome in variables and advances the graph under the
    generation check. Returns an error string on a conflict, or on a write
    the bucket refused, else None.

    Shared by the skip path and the scanned path. On the scanned path it
    runs once the results are persisted — _persist re-read the state's
    generation just before touching the prefix, so a conflict here means
    the graph moved in the moment between that re-read and this write:
    the scan stands but the graph did not move, and the message says so.
    On the skip path it runs BEFORE the prefix is cleared, so a conflict
    removes nothing."""
    variables["live_discovery"] = {
        "status": "skipped" if skip_reason else "completed",
        "skip_reason": skip_reason,
        "summary": live_summary,
    }
    state_dict["history"].append(
        f"Transitioned {current_state} -> {next_state} via "
        "discover_and_dump_all_clusters"
        + (" (skipped)" if skip_reason else ""))
    state_dict["current_state"] = next_state
    try:
        bucket.blob(_STATE_BLOB).upload_from_string(
            json.dumps(state_dict, indent=2), content_type="application/json",
            if_generation_match=generation)
    except exceptions.PreconditionFailed:
        if skip_reason:
            return ("ERROR: Concurrent update conflict saving the migration "
                    "state. Nothing was scanned or removed. Re-run "
                    "discover_and_dump_all_clusters.")
        return ("ERROR: Concurrent update conflict saving the migration state: "
                "another writer moved the graph first. The Live IR was "
                f"written under {LIVE_PREFIX}, but the graph did not advance "
                f"on it. If it still stands at {current_state}, re-run "
                "discover_and_dump_all_clusters. If it has moved on, what "
                "that writer recorded is the graph's: after a skip, these "
                "objects are not its estate — delete them by hand, or a later "
                "scan clears the prefix before it writes; after another scan "
                "of the same clusters, what stands under the prefix is this "
                "scan's estate, put there after its clear removed the "
                "winner's, and it serves the later steps as the winner's "
                "would.")
    except Exception as e:   # noqa: BLE001 — reported to the caller, not raised
        logger.exception("Saving the migration state failed")
        if skip_reason:
            return ("ERROR: Saving the migration state failed "
                    f"({type(e).__name__}: {e}). Nothing was scanned or "
                    "removed and the state stays here. Retry once the bucket "
                    "answers.")
        return ("ERROR: Saving the migration state failed "
                f"({type(e).__name__}: {e}). The Live IR was written under "
                f"{LIVE_PREFIX}, but the graph did not advance on it and the "
                f"state stays at {current_state}. Retry "
                "discover_and_dump_all_clusters once the bucket answers; the "
                "rescan clears the prefix before it writes.")
    return None


def _skip_response(next_state, skip_reason, removed, leftover=None,
                   found=None) -> str:
    """The skip's response. `removed` objects of an earlier scan were
    deleted; `leftover` is the error that stopped the clear, if one did,
    and `found` how many objects the listing saw (None when the listing
    itself failed, so the message does not assert objects it never saw)."""
    if leftover and found is None:
        problem = (f"Whether an earlier scan left objects under {LIVE_PREFIX} "
                   f"could not be checked ({leftover}); anything there is not "
                   "this run's estate.")
    elif leftover:
        problem = ((f"The other {found - removed} object(s) of that scan"
                    if removed else
                    f"An earlier scan's {found} object(s)")
                   + f" under {LIVE_PREFIX} could not be removed ({leftover}); "
                   "they are not this run's estate.")
    else:
        problem = ""
    return (f"SUCCESS: Live discovery skipped — {skip_reason}. The "
            "migration proceeds on the static sources only; a later "
            "reconciliation has no live estate to diff against.\n"
            + (f"An earlier scan's {removed} object(s) under {LIVE_PREFIX} "
               "were removed"
               + ("" if problem else
                  " — everything the one listing of the prefix, made behind "
                  "the state write, found; a scan still writing as this skip "
                  "landed reports its own leftovers")
               + ".\n" if removed else "")
            + (problem + " Delete them by hand, or a later scan clears the "
               "prefix before it writes.\n" if problem else "")
            + f"Current State: {next_state}. "
            "Next: call discover_configuration_files() to index the "
            "static IaC sources.")


def _scan_response(next_state, live_summary, notes) -> str:
    summary = dict(live_summary)
    unreachable = summary.get("clusters_unreachable") or []
    lines = [
        f"SUCCESS: Live discovery complete — {summary['clusters_found']} "
        f"cluster(s) across {summary['regions_scanned']} region(s), "
        f"{summary['clusters_walked']} walked in full.",
    ]
    if unreachable:
        lines.append(f"{len(unreachable)} could not be reached and hold their "
                     f"AWS-side record only: {', '.join(unreachable)}.")
    regions_unreachable = summary.get("regions_unreachable") or []
    if regions_unreachable:
        lines.append(
            f"{len(regions_unreachable)} requested region(s) could not be "
            "listed and are not in this record: "
            f"{', '.join(regions_unreachable)}. The graph has moved on; they "
            "can be added only by resetting the platform journey and "
            "scanning again.")
    if summary.get("clusters_using_karpenter"):
        lines.append(f"{summary['clusters_using_karpenter']} cluster(s) use "
                     "Karpenter node pools.")
    return (
        "\n".join(lines)
        + "\n" + json.dumps(summary, indent=2)
        + (("\n\nCoverage notes (relay verbatim):\n- "
            + "\n- ".join(notes)) if notes else "")
        + f"\n\nCurrent State: {next_state}. "
        "Next: call discover_configuration_files() to index the static IaC "
        "sources; the live estate is recorded for the reconciliation step.")


def _note_schema_violation(ir: dict) -> None:
    """Holds the IR to its schema; a violation becomes a note, not a refusal.

    The IR is produced by deterministic code, so a violation means the walk
    and the contract have drifted — an agent bug, and one the operator can
    do nothing about mid-engagement. Dropping the scan over it would lose
    the only record of the estate; the note travels inside the IR, so every
    reader (the tool response, the Review UI, the reconciliation) sees it.
    """
    try:
        live_schema.validate_live_ir(ir)
    except ValueError as e:
        logger.warning("Live IR failed schema validation: %s", e)
        notes = ir.setdefault("notes", [])
        notes.append(
            f"Live IR does not match its schema ({live_schema.SCHEMA_NAME}) "
            f"— {e}. The scan is recorded in full; a reader keyed on the "
            "schema may not read that section. Report this as an agent bug.")
        if isinstance(ir.get("summary"), dict):
            ir["summary"]["note_count"] = len(notes)
    except Exception as e:  # noqa: BLE001 — saving the scan outranks validating it
        # The validator itself could not run: schema file missing, jsonschema
        # absent, or a malformed schema. That is never a reason to drop the
        # only record of the estate — note it and let _persist proceed.
        logger.warning("Live IR schema validation could not run: %s", e)
        notes = ir.setdefault("notes", [])
        notes.append(
            f"Live IR schema validation could not run "
            f"({live_schema.SCHEMA_NAME}) — {e}. The scan is recorded in "
            "full. Report this as an agent bug.")
        if isinstance(ir.get("summary"), dict):
            ir["summary"]["note_count"] = len(notes)


class StateMovedError(RuntimeError):
    """The migration state is no longer at the generation this scan read:
    another writer moved the graph while the walk ran."""


def _persist(bucket, ir: dict, tables: dict, generation,
             began: list = None) -> None:
    """Writes one CSV per table and last the Live IR under LIVE_PREFIX —
    once the state is confirmed still at `generation`. `began`, when given,
    receives "clear" the moment the prefix is touched, so a caller reporting
    a failure can say whether anything was.

    The IR goes under the schema's name and is written LAST: a reader that
    finds it can trust the tables beside it are this run's. Whatever an
    earlier run left under the prefix goes FIRST — after a rewind, a stale
    IR beside this run's fresh tables would read as one run — so a failure
    midway leaves tables with no IR, never an IR over the wrong tables. And
    before any of it, state.json is re-read: a generation other than the
    one this scan started from means another writer (a concurrent scan, or
    a skip) moved the graph during the walk, and this scan lost — it raises
    StateMovedError and touches nothing, so the winner's record is not
    replaced by the loser's. What remains is the moment between this
    re-read and the advance in _write_state; a writer landing in it leaves
    this run's objects under the prefix under the other's state, and
    _write_state's conflict message says so.
    """
    state_blob = bucket.blob(_STATE_BLOB)
    try:
        state_blob.reload()
    except exceptions.NotFound as e:
        # state.json gone (a reset deleted it during the walk) is a move
        # too: the generation the scan read no longer stands, so nothing
        # is cleared or written. Any other failure to re-read is reported
        # as the save failure it is, by the caller.
        raise StateMovedError(
            "state.json is gone: it was deleted while this scan ran") from e
    if state_blob.generation != generation:
        raise StateMovedError(
            f"state.json is at generation {state_blob.generation}, the scan "
            f"read it at {generation}")
    if began is not None:
        began.append("clear")
    _clear_live_prefix(bucket)
    for name, csv_text in tables.items():
        bucket.blob(f"{LIVE_PREFIX}{name}.csv").upload_from_string(
            csv_text, content_type="text/csv")
    bucket.blob(LIVE_IR_BLOB).upload_from_string(
        json.dumps(ir, indent=2, sort_keys=True),
        content_type="application/json")


class PrefixClearError(RuntimeError):
    """Clearing LIVE_PREFIX stopped part-way: `removed` objects are gone,
    `found` is how many the listing saw (None when the listing itself
    failed), `cause` is the error the next delete (or the listing) raised."""

    def __init__(self, removed: int, cause: Exception, found=None):
        super().__init__(f"{type(cause).__name__}: {cause}")
        self.removed = removed
        self.found = found
        self.cause = cause


def _clear_live_prefix(bucket) -> int:
    """Deletes whatever an earlier scan left under LIVE_PREFIX and returns
    the count removed, so neither a skip nor a rescan is read together with
    that scan's estate. The IR goes first: a failure part-way then leaves
    tables with no IR, never an IR standing over tables that are gone —
    the same invariant _persist keeps by writing it last.
    An object the listing saw that is gone by the time it is reached (a
    concurrent skip or scan cleared it) counts as removed — the invariant is
    the empty prefix, not who emptied it; the delete names the listed
    generation (the client passes it), so an object rewritten between the
    listing and its turn answers NotFound too — the generation listed is
    gone — and is counted with the removed, while what stands is the
    other writer's to answer for (its own re-read of the state decides
    whether what it wrote is the graph's). A failure raises
    PrefixClearError carrying the count already removed and the count
    listed, so the caller's message matches the bucket."""
    removed = 0
    found = None
    try:
        blobs = sorted(bucket.list_blobs(prefix=LIVE_PREFIX),
                       key=lambda blob: blob.name != LIVE_IR_BLOB)
        found = len(blobs)
        for blob in blobs:
            try:
                blob.delete()
            except exceptions.NotFound:
                pass    # gone since the listing: a concurrent clear's work
            removed += 1
    except Exception as e:   # noqa: BLE001 — re-raised with the counts
        raise PrefixClearError(removed, e, found) from e
    return removed


def register(mcp):
    """Register this step's tools on the FastMCP server."""
    mcp.tool()(discover_and_dump_all_clusters)
