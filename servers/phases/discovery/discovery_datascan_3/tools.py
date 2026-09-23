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

"""MCP tools for discovery step 3 (data scan): managed data services and
the cluster DNS configuration.

Owns the STATE_DISCOVERY_DATA_SCAN agent task. The harvests themselves are
pure deterministic code (discovery_init_1/datastores.py and
discovery_init_1/clusterdns.py); this step is the ledger and state-machine
wrapper around them.

An agent task rather than a server-side action, for two reasons the image
scan does not have. The harvest reads a local checkout, and the image scan
gets to assume one exists because it runs inside the same call that cloned
it — this step does not, so a session resuming the migration on another
workstation has nothing to read and must be able to say so and ask. And a
failure has to be recoverable: an agent task that errors leaves the state
where it is, so the next session is still told there is work to do, whereas
a server-side action's failure edge would walk the graph on and leave the
section empty with nothing recording why.

The path comes from the user when the recorded one is gone (the
`plan_workload_translation` pattern). Re-cloning it instead would risk
scanning a different commit than the operator scoped — see DESIGN.md
issue 26.
"""

import json
import logging
import os
from typing import Optional

from google.api_core import exceptions
from mcp.server.fastmcp import Context

from servers.dag.state_management import (
    get_bucket_name,
    load_dag,
    load_inventory,
    save_inventory,
    authorize_and_rehydrate,
)
import servers.dag.state_management as state_mgr

from ..discovery_init_1 import addressspace, clusterdns, consumers, datastores
from ..discovery_init_1 import overrides as overrides_lib
from ..discovery_init_1.files import validate_explicit_root
# The corrections object belongs to the review step, which writes it; this
# step reads it, because a re-scan is exactly when the corrections have to be
# put back.
from ..discovery_datareview_3.tools import (
    OVERRIDES_BLOB, load_overrides, save_scan_baseline)

logger = logging.getLogger("migration-dag")


async def scan_data_dependencies(
    source_root: Optional[str] = None, ctx: Context = None,
) -> str:
    """
    Records the managed data services (databases, object storage, caches,
    queues, streams, secret and parameter stores) the workloads depend on,
    and the cluster DNS configuration (the CoreDNS Corefile and add-on
    settings) verbatim.

    Deterministic: it reads Terraform declarations from the confirmed
    in-scope files and writes them to the inventory's data_dependencies
    section. No LLM is involved, and nothing outside the checkout is read.
    A data service the files reach only by a literal ARN — a bucket another
    repository provisions, granted to a role here — is recorded too, marked
    'referenced', with the ARN as its address.
    The same call copies every CoreDNS artifact it finds into
    cluster_dns without interpreting it, and records the source network
    address space — VPC and subnet CIDRs, each cluster's service range and
    hybrid remote ranges, peered and routed ranges — in address_space, from
    Terraform, eksctl and CloudFormation declarations; a range the files do
    not state is listed as unresolved, never guessed.

    Args:
        source_root: path to your local source checkout. Only needed when the
            path discovery recorded is gone — resuming on another workstation,
            typically. Omit it otherwise and the recorded one is reused.

    Each entry records what the service is, which files declared it, and what
    the migration should do with it: migrate, rebuild, replatform, escalate
    or undecided. Only 'migrate' should later gate a workload.
    """
    logger.info(f"scan_data_dependencies called (source_root={source_root!r}).")
    try:
        state_dict, generation, config = authorize_and_rehydrate(None)
    except Exception as e:
        return f"ERROR: {e}"

    current_state = state_dict["current_state"]
    if current_state != "STATE_DISCOVERY_DATA_SCAN":
        return f"ERROR: Invalid state for scan_data_dependencies: {current_state}"

    variables = state_dict["variables"]
    if source_root:
        # Same guard discover_configuration_files applies: a relative path
        # resolves against the server's cwd, and this one is written into the
        # scope that extraction and the exports derivation both read.
        try:
            source_root = validate_explicit_root(source_root)
        except ValueError as e:
            return f"ERROR: {e}"
    root_dir = source_root or variables.get("discovery_root_dir")
    if not root_dir or not os.path.isdir(root_dir):
        recorded = variables.get("discovery_root_dir")
        return (
            "ERROR: No local source checkout to scan"
            + (f" (recorded path '{recorded}' is not on this machine)." if recorded
               else ".")
            + " Ask the user for the path of their checkout of "
            + f"{variables.get('source_repo_url') or 'the source repository'} "
            + f"at branch {variables.get('source_branch') or '(configured)'}, "
            "then call scan_data_dependencies(source_root=<that path>). The "
            "server does not re-clone here: the branch may have moved since "
            "the scope was confirmed, and scanning a different commit than "
            "the one the operator reviewed would be silent."
        )

    scope = variables.get("discovery_scope")
    if not scope:
        return ("ERROR: No confirmed discovery scope. Run discover_configuration_files "
                "and confirm_discovery_scope first.")

    bucket_name = get_bucket_name(config["ledger_uri"])
    bucket = state_mgr.gcs_client.bucket(bucket_name)
    try:
        platform_dag = load_dag(bucket, "platform_dag.json")
    except Exception as e:
        return f"ERROR: {e}"

    # What earlier reviews decided. Read before the harvest and replayed
    # inside it: the section is rebuilt from the checkout every run, so a
    # correction that is not replayed is a correction that was thrown away.
    try:
        overrides, _ = load_overrides(bucket)
    except Exception as e:
        # The step's contract is that a failure returns an ERROR string and
        # leaves the state here to be retried. A transient 503 on this read
        # would otherwise raise out of the tool, which reports as a broken tool
        # call rather than as work still to do — and this is the only ledger
        # read in the step that was outside a try.
        return (f"ERROR: Could not read {OVERRIDES_BLOB}, which holds the "
                f"corrections this scan has to replay: {e}. The scan did not "
                "run; try again.")
    if overrides is None:
        return (f"ERROR: {OVERRIDES_BLOB} exists but is not valid JSON. It "
                "holds every correction the data review recorded, and scanning "
                "without it would silently drop them — repair or remove the "
                "object, then re-run the scan.")

    try:
        inventory, inv_generation = load_inventory(bucket)
        if inventory is None:
            return "ERROR: No inventory found; run discover_configuration_files first."
        # A re-run replaces the section wholesale — rediscovery semantics, the
        # same as the image scan. Merging would keep entries whose files the
        # operator has since excluded.
        inventory["data_dependencies"] = []
        # The notes describe *this* scan — which files its scope excluded, what
        # it could not read. Keeping the previous run's would leave two
        # contradictory counts side by side, and a re-scan that excludes
        # nothing would leave the old "2 files not scanned" standing as the
        # durable record.
        inventory["data_dependency_scan_notes"] = []
        harvest = datastores.harvest_datastores(
            inventory, root_dir, scope, overrides)
        notes, workloads = harvest.notes, harvest.workloads
        # The cluster DNS configuration rides the same walk and the same
        # rediscovery semantics: the section is rebuilt from the checkout,
        # and its notes describe this run. Copied, never interpreted — the
        # landing-zone planner's cluster-dns unit hands the text to a
        # translation worker and clusterdns_contract.py checks what it wrote
        # against the same text (DESIGN §7.1). Chart roots are
        # skipped by name: a template is not parseable YAML and nothing
        # renders charts for this scan. A failure inside this harvest must not
        # take the data services down with it — the data section gates a
        # pipeline, this one is translation input whose absence the plan's
        # placeholder reports — so it is demoted to a persisted note rather
        # than an ERROR that leaves the step unrunnable.
        chart_roots = [t.get("root") for t in inventory.get("render_targets") or []
                       if isinstance(t, dict) and t.get("type") == "helm"]
        try:
            dns = clusterdns.harvest_cluster_dns(root_dir, scope, chart_roots)
            inventory["cluster_dns"] = dns.section
            inventory["cluster_dns_scan_notes"] = dns.notes
        except Exception as e:
            logger.exception("Cluster DNS harvest failed")
            inventory["cluster_dns"] = {}
            inventory["cluster_dns_scan_notes"] = [
                f"the cluster DNS scan failed ({e.__class__.__name__}: {e}); "
                "nothing was recorded — the CoreDNS configuration, if any, is "
                "unknown until the scan is fixed and re-run"]
        # The source address space rides the same walk, with the same
        # rediscovery semantics and the same demotion of a failure to a note:
        # the landing-zone design reads it to propose ranges that do not
        # overlap the source, and an empty section there means the design
        # asks the user, which is the pre-existing behaviour, not a wrong one.
        try:
            space = addressspace.harvest_address_space(root_dir, scope, chart_roots)
            inventory["address_space"] = space.section
            inventory["address_space_scan_notes"] = space.notes
        except Exception as e:
            logger.exception("Address space harvest failed")
            inventory["address_space"] = {}
            inventory["address_space_scan_notes"] = [
                f"the address-space scan failed ({e.__class__.__name__}: {e}); "
                "nothing was recorded — the source VPC and subnet ranges are "
                "unknown until the scan is fixed and re-run"]
        # Before the save, not after: inventory.source is the provenance record
        # issue 26 refers to, and leaving it naming the previous workstation
        # makes it disagree with the scope.
        if isinstance(inventory.get("source"), dict):
            inventory["source"]["root_dir"] = root_dir
        save_inventory(bucket, inventory, inv_generation)
        # What the Terraform says on its own, before a single correction was
        # replayed over it, plus the two facts `note_unattributed` needs. The
        # review rebuilds the corrected section from exactly this — the same
        # inputs this scan used — so the section a reviewer approves is the
        # section the next scan produces, by construction rather than by
        # argument. See DESIGN.md issue 31.
        save_scan_baseline(bucket, {
            "data_dependencies": harvest.scanned,
            "truncated": harvest.truncated,
            "excluded": harvest.excluded,
            # Which corrections this scan already replayed, so the review
            # can say how many were recorded SINCE it rather than in total —
            # the ones inside these counts are not stale. Fingerprints rather
            # than a count: supersession retires records, so the store shrinks
            # as often as it grows and arithmetic on its size underflows.
            "corrections_replayed": [
                overrides_lib.fingerprint(record)
                for record in ((overrides or {}).get("overrides") or [])],
        })
    except exceptions.PreconditionFailed:
        return "ERROR: Concurrent update conflict writing the inventory."
    except Exception as e:
        logger.exception("Data dependency scan failed")
        return f"ERROR: Data dependency scan failed: {e}"

    # Only advance once the scan is persisted. An error above leaves the state
    # here, which is what makes the step retryable and resumable.
    variables["discovery_root_dir"] = root_dir
    # Extraction and the exports derivation read the root from the scope, not
    # from discovery_root_dir. Updating only the latter meant a resumed session
    # could supply a path here, advance, and then have run_discovery_extraction
    # fail on the previous workstation's path with no way back — the recovery
    # path this step exists for, wedging one state later.
    if isinstance(scope, dict):
        scope["root_dir"] = root_dir
    # The workloads this walk saw, whether or not they attributed anything.
    # The review ranks them when it asks who owns an entry nothing reached;
    # holding them here rather than re-walking at review time keeps the
    # candidates and the entries describing the same commit.
    variables["data_dependency_workloads"] = workloads
    # Workloads whose configuration states a recorded entry's name exactly.
    # The review offers them first, with the reason; the scan never attaches
    # on a name (inferred.py).
    variables["data_dependency_hints"] = harvest.hints
    # A sign-off is on a section, and this run just replaced it. The
    # corrections survive a re-scan by design; the approval must not, or a
    # reviewer's yes would carry silently onto entries they never saw.
    variables.pop("data_dependency_review", None)
    state_def = platform_dag["states"][current_state]
    next_state = state_def["transitions"]["on_tool_call_received"]
    state_dict["history"].append(
        f"Transitioned {current_state} -> {next_state} via scan_data_dependencies")
    state_dict["current_state"] = next_state

    try:
        bucket.blob("platform/onboarding/state.json").upload_from_string(
            json.dumps(state_dict, indent=2), content_type="application/json",
            if_generation_match=generation)
    except exceptions.PreconditionFailed:
        return ("ERROR: Concurrent update conflict saving the migration state. "
                "The scan results were written but the graph did not advance, and "
                "the recorded checkout path was not saved either — call "
                f"scan_data_dependencies(source_root='{root_dir}') again with that "
                "same path; the re-run replaces the results.")

    found = inventory["data_dependencies"]
    gating = [d for d in found if d.get("disposition") == "migrate"]
    escalations = [d for d in found if d.get("disposition") == "escalate"]
    # Known from an ARN and declared nowhere in the scanned files. Counted on
    # its own because these are the ones that get forgotten: nothing in the
    # repository owns them, so no plan, runbook or teardown will mention them
    # unless this section does.
    # The estate's own replicas are answered, not known only from an ARN.
    referenced = [d for d in found
                  if d.get("detection") == "referenced" and not datastores.is_replica(d)]
    # Guesses from a configuration key's name and value. Each is a question
    # for the review — confirm or dismiss — and none gates anything until
    # answered.
    guesses = [d for d in found if d.get("detection") == "inferred"]
    # An entry nothing references is the one a human has to act on: it is a real
    # data service that no workload in the Terraform claims, so the gate cannot
    # place it. Counted separately from `found` so it cannot be read as noise.
    #
    # Keyed on the note the harvester wrote, NOT on an empty consumers list.
    # When part of the Terraform could not be read, the harvester deliberately
    # declines to say "nothing references this" and says "unknown" instead;
    # counting empty lists here would re-assert the very claim it refused to
    # make, and the two would reach the user side by side.
    unattributed = [d for d in found
                    if consumers.UNATTRIBUTED_NOTE in (d.get("notes") or [])]
    unknown = [d for d in found
               if consumers.TRUNCATED_NOTE in (d.get("notes") or [])]
    summary = {
        "found": len(found),
        "needing_migration": len(gating),
        "needing_an_escalation_decision": len(escalations),
        "known_only_from_an_arn_or_endpoint": len(referenced),
        "guesses_needing_a_yes_or_no": len(guesses),
        "unattributed_to_any_workload": len(unattributed),
        "consumer_unknown_unreadable_terraform": len(unknown),
        "by_service": {},
        # Identity only, for every entry — the full records are in the
        # inventory, and the step after this one is the context-expensive part
        # of discovery. Escalations are named too: they are what a customer has
        # to decide about. Consumers are reduced to their workload names for
        # the same reason: enough to see the shape, not the whole record.
        "entries": [
            dict({k: d.get(k) for k in ("service", "identifier", "disposition",
                                        "detection")},
                 # "name (kind)": the instructions ask the agent for a table
                 # of which workloads use each service, and a Secret named
                 # catalog-db is not a Deployment. Deduplicated because two
                 # blocks can deploy the same release from different files.
                 consumers=list(dict.fromkeys(
                     f"{c.get('workload')} ({c.get('kind')})"
                     for c in d.get("consumers") or [])))
            for d in found
        ],
        "scan_notes": notes,
        # Identity only, as above: which artifacts carry a Corefile and where.
        # The text itself is in the inventory for the translation worker; the
        # notes say what was seen but not recorded (a default add-on, a
        # Corefile behind a variable, a chart) and are the agent's to relay.
        "cluster_dns": {
            "sources_with_text": [
                {"kind": s.get("kind"), "name": s.get("name"),
                 "where": s.get("address") or s.get("path"),
                 "form": s.get("form")}
                for s in (inventory.get("cluster_dns") or {}).get("sources") or []],
            "scan_notes": inventory.get("cluster_dns_scan_notes") or [],
        },
        # The ranges themselves, not just counts: they are short, and the
        # instructions have the agent put them to the user beside the
        # unresolved ones, which are the ranges only the user can supply.
        "address_space": _address_space_summary(inventory),
    }
    for entry in found:
        service = entry.get("service", "other")
        summary["by_service"][service] = summary["by_service"].get(service, 0) + 1

    return (
        f"SUCCESS: {len(found)} managed data service(s) recorded — "
        f"{len(gating)} to migrate, {len(escalations)} needing an escalation "
        f"decision, {len(unattributed)} not attributable to a workload from the "
        f"Terraform alone"
        + (f", {len(unknown)} unknown because part of the Terraform could not "
           f"be read" if unknown else "")
        + (f", {len(referenced)} known only from an ARN or endpoint and not "
           f"matched to a declaration here" if referenced else "")
        + (f", {len(guesses)} guessed from a configuration key and awaiting a "
           f"yes or no" if guesses else "")
        + f"; {len(summary['cluster_dns']['sources_with_text'])} cluster DNS "
        "configuration source(s) recorded verbatim"
        + f"; {summary['address_space']['vpcs_with_a_cidr']} VPC range(s), "
        f"{summary['address_space']['subnets_with_a_cidr']} subnet range(s) and "
        f"{len(summary['address_space']['unresolved'])} unresolved range(s) recorded "
        "for the address space"
        + (f" ({summary['address_space']['unresolved_but_covered']} more unresolved but "
           "settled by a stated VPC range, by the VPC's own question or not a routing range, "
           "so no question of their own)"
           if summary['address_space']['unresolved_but_covered'] else "")
        + ".\n" + json.dumps(summary, indent=2)
        + f"\n\nCurrent State: {next_state}. "
        # Derived from the graph, not hardcoded: a workspace bootstrapped
        # before v2.8 keeps its own copy of the DAG until join_ledger
        # --reconfigure, and naming a tool its graph does not reach would stop
        # the run.
        + ("Next: put this section to the user and correct it with them — "
           "list_data_dependencies() prints it — then call "
           "confirm_data_dependencies() for the sign-off."
           if next_state == "STATE_DISCOVERY_DATA_REVIEW"
           else "Next: call run_discovery_extraction().")
    )


def _address_space_summary(inventory: dict) -> dict:
    """The recorded ranges, reduced to what the agent relays: every VPC and
    cluster range as recorded, subnets counted (a list of thirty /24s is the
    inventory's to hold, not the conversation's), and the unresolved entries
    that can change the target ranges in full — each is a question for the
    user. The ones a stated VPC range, the VPC's own question or a
    public-endpoint allow list already settles are counted, not asked:
    the same split the landing-zone proposal makes (addressspace.triage_unresolved),
    so the user is not asked here for a value the design would never need."""
    section = inventory.get("address_space") or {}
    vpcs = section.get("vpcs") or []
    subnets = section.get("subnets") or []
    blocking, covered = addressspace.triage_unresolved(section)
    return {
        "vpcs": [
            # With address and path: two environments both declare `main`.
            {"name": v.get("name"), "address": v.get("address"), "path": v.get("path"),
             "cidr": v.get("cidr"),
             "secondary_cidrs": v.get("secondary_cidrs") or [],
             "cluster_vpc": v.get("cluster_vpc"), "form": v.get("form"),
             **({"defaulted": True} if v.get("defaulted") else {})}
            for v in vpcs],
        "vpcs_with_a_cidr": sum(1 for v in vpcs if v.get("cidr")),
        "subnets_with_a_cidr": sum(1 for s in subnets if s.get("cidr")),
        "clusters": [
            {"name": c.get("name"), "address": c.get("address"), "path": c.get("path"), "vpc": c.get("vpc"),
             "service_ipv4_cidr": c.get("service_ipv4_cidr"),
             "public_access_cidrs": c.get("public_access_cidrs") or [],
             "remote_node_cidrs": c.get("remote_node_cidrs") or [],
             "remote_pod_cidrs": c.get("remote_pod_cidrs") or []}
            for c in section.get("clusters") or []],
        "routes": [f"{r.get('destination')} via {r.get('via')}"
                   for r in section.get("routes") or []],
        "unresolved": [
            # With the path: two directories can declare the same address,
            # and the user has to be told which one the question is about.
            {"address": u.get("address"), "path": u.get("path"), "argument": u.get("argument"),
             "expression": u.get("expression")}
            for u in blocking],
        "unresolved_but_covered": len(covered),
        "scan_notes": inventory.get("address_space_scan_notes") or [],
    }


def register(mcp):
    """Register this step's tools on the FastMCP server."""
    mcp.tool()(scan_data_dependencies)
