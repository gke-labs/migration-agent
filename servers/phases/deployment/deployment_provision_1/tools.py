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

"""MCP tools for deployment step 1 (provision): the image destination.

Owns the STATE_DEPLOYMENT_INIT agent task. prepare_image_deployment moves the
graph into the provisioning segment and the dispatch loop does the rest: the
provision_artifact_registry action (actions.py) resolves the destination
registry, creates it when missing, and the walk parks at
STATE_DEPLOYMENT_DATA_MIGRATION — the step the deployment segment waits in for
the work this server cannot do, and NOT the terminal. See instructions.md in
this folder for the agent-facing playbook.

mark_replication_complete is the follow-through for the self-service leg: the
runbook runs outside the server, so completion is a fact only the user holds.
The tool records that assertion into the inventory (replication.py owns the
semantics) and republishes the deployment exports so image_map flips to
`replicated`. No DAG transition: it runs at STATE_DEPLOYMENT_DATA_MIGRATION,
which the segment does not leave until every copy is reported or abandoned.
SEGMENT_RUN and CONFIRMED_FROM below are the two constants that distinction
changed.
"""

import json
import logging
import os
from datetime import datetime, timezone

from google.api_core import exceptions
from mcp.server.fastmcp import Context

from servers.dag.dispatch import run_dispatch_loop
from servers.dag.server import exports as exports_lib
from servers.dag.state_management import (
    authorize_and_rehydrate,
    get_bucket_name,
    load_dag,
)
import servers.dag.state_management as state_mgr

from .. import actions, replication

logger = logging.getLogger("migration-dag")


def deployment_export_inputs(variables: dict, inventory: dict) -> tuple:
    """(planned_refs, cluster_scan) for the exports deployment derivation.

    planned_refs re-derives the self-service destination plan (persisted
    outcomes carry no destination; the plan is deterministic). cluster_scan
    is the literal google_container_cluster scan of the target clone, or
    None when no clone is on disk. Shared with refresh_exports.
    """
    inventory = inventory or {}
    ecr_images = [image for image in inventory.get("images") or []
                  if (image or {}).get("registry") == "ecr"]
    destinations = variables.get("artifact_registry_destinations") or []
    dest_url = next((d.get("url") for d in destinations
                     if isinstance(d, dict) and d.get("url")), None)
    planned_refs = (replication.planned_destination_refs(dest_url, ecr_images)
                    if dest_url and ecr_images else {})
    clone_dir = variables.get("target_clone_path")
    cluster_scan = (actions.scan_container_clusters(clone_dir)
                    if clone_dir and os.path.isdir(clone_dir) else None)
    return planned_refs, cluster_scan


# Where the deployment walk parks with provisioning and replication behind it,
# and so where the deployment exports are published from. This USED to be the
# terminal, and the reader below said "== STATE_DEPLOYMENT_COMPLETED" and meant
# "the segment has run"; inserting a step made those two different things, and
# anything keyed on the old parking spot broke quietly.
#
# The step and not the terminal, because these fields — the registry, the
# cluster, the workload pool — are what an application team needs to plan and
# translate, and they are ready the moment provisioning finishes. Publishing at
# the terminal instead would hold every developer behind a database migration
# that has nothing to do with them and takes weeks.
SEGMENT_RUN = ("STATE_DEPLOYMENT_DATA_MIGRATION",)

# Where a human can assert something. NOT the terminal: a terminal state means
# the workflow is finished, and a tool that accepts calls there says the
# opposite. Everything the server cannot observe for itself — self-service
# image copies, database migrations — is confirmed from the one step that waits
# for it.
#
# The two sets coincide today and are still two names, because they answer
# different questions: publishing at a terminal would be harmless, accepting an
# assertion there would not.
CONFIRMED_FROM = ("STATE_DEPLOYMENT_DATA_MIGRATION",)


def redraw_the_data_migration_worklist(bucket) -> str:
    """The data migration step's runbook, after an image outcome changed.

    Unconfirmed image copies are half of what holds that step open, and its
    runbook — the state's primary review-UI artifact — lists them. These two
    tools are the only things that settle one, so without this the artifact
    went on naming a copy that had already been reported or abandoned, and
    disagreed with the gate one click away. The listing is the only other
    writer and the step's instructions tell the agent not to re-list, so the
    stale window is as long as the databases take.

    Best effort, and only while the step is live — at the terminal the
    document is the record of what the close left behind and nothing rewrites
    it. That test belongs to the callee, which re-reads the state: this
    module's `state_dict` was rehydrated at the top of the call and another
    session may have closed the step since.
    """
    # Function-level: that package imports the review step, which is a longer
    # chain than this module needs at import time.
    from servers.phases.deployment.deployment_datamigration_2.tools import (
        refresh_runbook_from_ledger)
    return refresh_runbook_from_ledger(bucket)


def _closed_under_us() -> str:
    """The callee's marker string, for callers that must stop instructing."""
    from servers.phases.deployment.deployment_datamigration_2.tools import (
        CLOSED_UNDER_US)
    return CLOSED_UNDER_US


def _nothing_left_at_the_step(bucket) -> bool:
    """Whether settling that copy left the step with nothing outstanding.

    An image copy can be the LAST thing holding the step open — an estate
    whose databases are all settled, or which never owed one. These two tools
    are then the moment the gate opens, and the step's own argument is that
    the moment has to be named where the operator is looking: the instructions
    tell the agent to hand control back and not to re-list, so nothing else
    will say it.
    """
    from servers.phases.deployment import datamigration as dm
    from servers.phases.deployment.deployment_datamigration_2.tools import (
        load_migrations)
    try:
        inventory, _ = state_mgr.load_inventory(bucket)
        document, _ = load_migrations(bucket)
    except Exception:
        return False
    if inventory is None or document is None:
        return False
    entries = inventory.get("data_dependencies") or []
    return not dm.outstanding(entries, document) and not dm.unconfirmed_copies(
        inventory)


def _freeze_warning(bucket, taken: bool = False) -> str:
    """`closing_freezes_in_flight` for the two image tools, or "".

    These are two of the six sites that offer the close, and the only two that
    offer it in the shape an earlier review named: an image copy as the LAST thing
    holding the step open. Nothing about that shape says a database cutover is
    not running — the two populations are independent — so the cost is the one
    every other site states, and the instructions tell the agent to hand back
    control here rather than re-list, which makes this the surface the
    operator acts on.

    Silent on any failure, like `_nothing_left_at_the_step` beside it: the line
    it accompanies is already withheld when either object cannot be read.

    `taken` switches to the past tense, for the arm where another session
    closed the step under this call. There the offer is suppressed and the
    cost has already been paid, so the conditional sentence would read as a
    warning there is still time to act on.
    """
    from servers.phases.deployment import datamigration as dm
    from servers.phases.deployment.deployment_datamigration_2.tools import (
        load_migrations)
    try:
        inventory, _ = state_mgr.load_inventory(bucket)
        document, _ = load_migrations(bucket)
    except Exception:
        return ""
    if inventory is None or document is None:
        return ""
    render = (dm.closed_over_in_flight if taken
              else dm.closing_freezes_in_flight)
    return render(inventory.get("data_dependencies") or [], document)


def publish_completion_exports(bucket, variables: dict, ledger_project) -> str:
    """The deployment-completion exports hook: reads the ledger inventory
    (replication outcomes live there), assembles the derived inputs, and
    publishes best-effort. Returns "" or a warning line.

    Carries `refresh_exports`' degraded-input discipline, because
    mark_replication_complete can run weeks later on a different platform
    engineer's machine: with the recorded target clone absent, `cluster`
    derives to null, and a wholesale republish would overwrite the literals
    provisioning published with nulls and bump generations.deployment. Only
    the image map is republished in that case (the sole field the flip
    changes); an unreadable inventory recomputes image_map and node_shapes to
    nothing, so nothing is published at all.
    """
    inventory, degraded = None, None
    try:
        inventory, _ = state_mgr.load_inventory(bucket)
    except Exception as e:
        logger.error(f"Could not read the inventory for the exports hook: {e}")
        return (f"WARNING: the inventory blob is unreadable here ({e}), so "
                "exports.json was left untouched rather than republished from "
                "nothing; refresh_exports retries the publish.")
    clone_dir = variables.get("target_clone_path")
    if clone_dir and not os.path.isdir(clone_dir):
        degraded = "the target clone is not on this machine"
    planned_refs, cluster_scan = deployment_export_inputs(variables, inventory or {})
    return exports_lib.publish_deployment_exports(
        bucket, variables, inventory or {}, ledger_project, planned_refs,
        cluster_scan, degraded=degraded)


async def prepare_image_deployment(ctx: Context = None) -> str:
    """Provisions the Artifact Registry destination for the migrated images.

    Takes no arguments. The server resolves the destination (the landing-zone
    design's registry when one is declared, a deterministic default
    otherwise), checks it exists, and creates it when it does not. Failures
    are reported, never blocking.
    """
    logger.info("prepare_image_deployment called.")
    try:
        state_dict, generation, config = authorize_and_rehydrate(None)
    except Exception as e:
        return f"ERROR: {e}"

    current_state = state_dict["current_state"]
    if current_state != "STATE_DEPLOYMENT_INIT":
        return f"ERROR: Invalid state for prepare_image_deployment: {current_state}"

    bucket_name = get_bucket_name(config["ledger_uri"])
    bucket = state_mgr.gcs_client.bucket(bucket_name)
    try:
        platform_dag = load_dag(bucket, "platform_dag.json")
    except Exception as e:
        return f"ERROR: {e}"

    state_def = platform_dag["states"][current_state]
    next_state = state_def["transitions"]["on_tool_call_received"]
    state_dict["history"].append(
        f"Transitioned {current_state} -> {next_state} via prepare_image_deployment")
    state_dict["current_state"] = next_state

    transitions_run, message, error = await run_dispatch_loop(
        ctx, state_dict, platform_dag, config, "Image deployment preparation started.")
    if error:
        return error

    # Publish the deployment slice of exports.json when the walk parks at the
    # data migration step — replication run, self-service runbook,
    # or declined alike. Best-effort: a failure warns, never blocks.
    #
    # The condition is a SET rather than the terminal, and that is not
    # cosmetic. These fields (the registry, the cluster, the workload pool) are
    # what an application team needs to plan and translate, and they are ready
    # the moment provisioning finishes. Keying the publish on the terminal
    # meant that inserting a step before it held every developer's inputs
    # behind a database migration that has nothing to do with them and takes
    # weeks. Provisioning being done is the fact worth publishing on.
    exports_note = ""
    if state_dict["current_state"] in SEGMENT_RUN:
        warning = publish_completion_exports(
            bucket, state_dict["variables"], config.get("gcp_project"))
        exports_note = (f"\n- {warning}" if warning else
                        f"\n- Exports: gs://{bucket_name}/"
                        f"{exports_lib.EXPORTS_BLOB} refreshed (deployment fields)")

    data = json.dumps(state_dict, indent=2)
    try:
        bucket.blob("platform/onboarding/state.json").upload_from_string(
            data, content_type="application/json", if_generation_match=generation)
    except exceptions.PreconditionFailed:
        return "ERROR: Concurrent update conflict. Your changes were not saved."

    return (
        f"Image deployment preparation finished.\n"
        f"- Transition log: {', '.join(transitions_run) or 'none'}\n"
        f"- Current State: {state_dict['current_state']}\n"
        f"- {message}"
        f"{exports_note}"
    )


async def mark_replication_complete(refs: list[str] = None,
                                    digests: dict[str, str] = None,
                                    destinations: dict[str, str] = None,
                                    ctx: Context = None) -> str:
    """Records that self-service image copies have been completed.

    The self-service replication leg ends with a runbook the user runs with
    their own credentials — the server never sees those copies happen, so
    the inventory keeps `status: "self_service"` and the exports image_map
    never flips to `replicated`. Call this only after the user states the
    runbook (or a manual equivalent) has been run; it is their assertion
    that is recorded (`verified_by: "user_asserted"`). All arguments are
    optional:

    - refs: source image references to mark; omit to mark every entry with
      a self-service or failed replication outcome (bulk).
    - digests: {source ref -> destination content digest}, recorded
      verbatim when supplied — e.g. from
      `skopeo inspect --format '{{.Digest}}' docker://<destination ref>`,
      run by the user — and never invented.
    - destinations: {source ref -> destination ref pushed to}, needed only
      when the runbook carried an <AR_DESTINATION> placeholder.

    Refreshes the deployment slice of exports.json so consumers see the
    flip. Platform engineers and admins only; no DAG transition.
    """
    logger.info("mark_replication_complete called.")
    try:
        state_dict, _, config = authorize_and_rehydrate(None)
    except Exception as e:
        return f"ERROR: {e}"

    current_state = state_dict["current_state"]
    bucket_name = get_bucket_name(config["ledger_uri"])
    bucket = state_mgr.gcs_client.bucket(bucket_name)
    if current_state not in CONFIRMED_FROM:
        # Both populations and both exits. This and `_open`'s refusal are the
        # two state messages for the same step, and each used to give a
        # different half-account of what it waits on — the one an operator met
        # depending on which tool they reached for first.
        return (f"ERROR: Invalid state for mark_replication_complete: "
                f"{current_state}. Self-service copies are confirmed from "
                "STATE_DEPLOYMENT_DATA_MIGRATION, the step the deployment "
                "segment waits in for the work it cannot observe. It does not "
                "close until every planned or failed container image copy has "
                "been confirmed or abandoned, AND every data service graded "
                "'migrate' has been reported migrated or excused with "
                "'keep-in-aws'.")

    try:
        inventory, generation = state_mgr.load_inventory(bucket)
    except Exception as e:
        return f"ERROR: Could not read the inventory: {e}"
    if not inventory:
        return ("ERROR: No discovery inventory exists in this ledger; "
                "there is nothing to mark.")

    variables = state_dict["variables"]
    dest_url = next(
        (d.get("url") for d in variables.get("artifact_registry_destinations")
         or [] if isinstance(d, dict) and d.get("url")), None)
    marked, updated, already, problems = replication.mark_self_service_complete(
        inventory, refs, digests, destinations, dest_url)

    exports_note = "exports.json unchanged (nothing was marked)"
    if marked or updated:
        try:
            state_mgr.save_inventory(bucket, inventory, generation)
        except exceptions.PreconditionFailed:
            return ("ERROR: Concurrent inventory update conflict; nothing "
                    "was recorded. Re-run mark_replication_complete.")
        except Exception as e:
            return (f"ERROR: Saving the replication statuses failed ({e}); "
                    "nothing was recorded.")
        warning = publish_completion_exports(
            bucket, variables, config.get("gcp_project"))
        total = sum(1 for i in inventory.get("images") or []
                    if (i.get("replication") or {}).get("status") == "replicated")
        exports_note = warning or (
            f"Exports: gs://{bucket_name}/{exports_lib.EXPORTS_BLOB} refreshed "
            f"— image_map now lists {total} replicated image(s)")

    with_digest = sum(1 for r in marked if (digests or {}).get(r))
    lines = [f"Marked {len(marked)} image(s) replicated on your assertion"
             f" ({with_digest} with a recorded content digest):"]
    lines += [f"- {r}" for r in marked]
    if not marked:
        lines = ["Nothing marked." if (problems or already or updated) else
                 "Nothing to mark: no self-service or failed replication "
                 "outcomes remain in the inventory."]
    if updated:
        lines.append("Already replicated, record improved with the value(s) "
                     f"you supplied: {', '.join(updated)}")
    if already:
        lines.append(f"Already replicated (left as recorded): "
                     f"{', '.join(already)}")
    for problem in problems:
        lines.append(f"Not marked — {problem}")
    lines.append(exports_note)
    worklist_warning = ""
    if marked or updated:
        worklist_warning = redraw_the_data_migration_worklist(bucket)
        if worklist_warning:
            lines.append(worklist_warning)
        if worklist_warning == _closed_under_us():
            # The other side of the same fact. These two tools are the fifth
            # and sixth sites that report the close has been TAKEN, and an earlier
            # fix wired only the three in the step's own module — the same two
            # tools an earlier review found missing from the OFFER side, for
            # the same reason.
            was_frozen = _freeze_warning(bucket, taken=True)
            if was_frozen:
                lines.append(was_frozen)
        elif _nothing_left_at_the_step(bucket):
            lines.append(
                "Nothing is outstanding now: complete_data_migration() closes "
                "the step.")
            frozen = _freeze_warning(bucket)
            if frozen:
                lines.append(frozen)
    # Not once the step has closed under this call: a re-call is a call that
    # refuses, and `redraw_the_data_migration_worklist` has just said so.
    if (marked and with_digest < len(marked)
            and worklist_warning != _closed_under_us()):
        lines.append(
            "Tip: to record verified digests, have the user run "
            "`skopeo inspect --format '{{.Digest}}' docker://<destination>` "
            "and re-call with digests={source ref: digest}.")
    return "\n".join(lines)


async def abandon_image_replication(refs: list[str], reason: str,
                                    ctx: Context = None) -> str:
    """
    Records that these container images will not be copied to Artifact Registry.

    The counterpart of recording a data service `keep-in-aws`: a decision, not
    a failure. Use it when a copy failed and nobody is going to chase it, or a
    self-service runbook is not going to be run — the image stays in ECR and
    stops being work the deployment step waits for.

    It is not free, and the reason is required because of that. A workload
    whose image stays in ECR keeps pulling from ECR: the reference is left
    untouched by workload translation (nothing is at the Artifact Registry
    address), so the cluster needs pull credentials for ECR and pays
    cross-cloud egress on every pull that misses the node cache.

    Args:
        refs: source image references to abandon, as they appear in the
            inventory and the runbook.
        reason: why. Recorded verbatim and shown to whoever reads the ledger
            next.

    Only a planned or failed copy can be abandoned. Naming an abandoned ref in
    mark_replication_complete's `refs` re-opens the decision if the image is
    copied after all. Platform engineers and admins only; no DAG transition.
    """
    logger.info(f"abandon_image_replication called (refs={refs!r}).")
    if not refs:
        return ("ERROR: name the image reference(s) to abandon in refs=[...]. "
                "There is no bulk form: abandoning is a per-image decision.")
    if not (reason or "").strip():
        return ("ERROR: reason= is required. An abandoned image leaves a "
                "workload pulling from ECR across clouds, and the next reader "
                "has to be able to tell that from an oversight.")
    try:
        state_dict, _, config = authorize_and_rehydrate(None)
    except Exception as e:
        return f"ERROR: {e}"
    bucket_name = get_bucket_name(config["ledger_uri"])
    bucket = state_mgr.gcs_client.bucket(bucket_name)
    if state_dict["current_state"] not in CONFIRMED_FROM:
        # The third of the three state messages for this step, and the
        # tersest: it named the state and nothing about what the state waits
        # on. All three now give the same complete answer.
        return (f"ERROR: Invalid state for abandon_image_replication: "
                f"{state_dict['current_state']}. Replication decisions are "
                "recorded from STATE_DEPLOYMENT_DATA_MIGRATION, which the "
                "deployment segment parks in until every planned or failed "
                "container image copy has been confirmed or abandoned, AND "
                "every data service graded 'migrate' has been reported "
                "migrated or excused with 'keep-in-aws'.")
    try:
        inventory, generation = state_mgr.load_inventory(bucket)
    except Exception as e:
        return f"ERROR: Could not read the inventory: {e}"
    if not inventory:
        return ("ERROR: No discovery inventory exists in this ledger; there "
                "is nothing to abandon.")

    abandoned, problems = replication.abandon_copies(
        inventory, refs, reason.strip(),
        state_mgr.get_authenticated_user_email(),
        datetime.now(timezone.utc).isoformat())
    if abandoned:
        try:
            state_mgr.save_inventory(bucket, inventory, generation)
        except exceptions.PreconditionFailed:
            return ("ERROR: the inventory changed while this call was being "
                    "written. Nothing was saved — try again.")
        except Exception as e:
            return f"ERROR: Could not record the decision: {e}"

    lines = []
    if abandoned:
        # Republished, for the same reason mark_replication_complete does it:
        # the decision changed image_map, and exports.json is the only thing a
        # developer can read. `abandoned` is not admitted to the map, so the
        # entry drops out and workload translation says "no image_map entry;
        # left untouched" — which is true. Without this it keeps telling them
        # to "finish the replication" for a copy that was cancelled.
        warning = publish_completion_exports(
            bucket, state_dict["variables"], config.get("gcp_project"))
        lines.append(
            f"{len(abandoned)} image(s) will not be copied: "
            + ", ".join(abandoned) + f". Reason recorded: {reason.strip()}")
        lines.append(warning if warning else
                     "Exports refreshed: the abandoned image(s) leave the "
                     "image map, so workload translation stops asking anyone "
                     "to finish the copy.")
        lines.append(
            "Tell the user: these stay in ECR. Workload translation leaves "
            "their references untouched, so the cluster needs ECR pull "
            "credentials and pays cross-cloud egress on every pull that "
            "misses the node cache.")
        worklist_warning = redraw_the_data_migration_worklist(bucket)
        if worklist_warning:
            lines.append(worklist_warning)
        if worklist_warning == _closed_under_us():
            # The other side of the same fact. These two tools are the fifth
            # and sixth sites that report the close has been TAKEN, and an earlier
            # fix wired only the three in the step's own module — the same two
            # tools an earlier review found missing from the OFFER side, for
            # the same reason.
            was_frozen = _freeze_warning(bucket, taken=True)
            if was_frozen:
                lines.append(was_frozen)
        elif _nothing_left_at_the_step(bucket):
            lines.append(
                "Nothing is outstanding now: complete_data_migration() closes "
                "the step.")
            frozen = _freeze_warning(bucket)
            if frozen:
                lines.append(frozen)
    for problem in problems:
        lines.append(f"- {problem}")
    # No empty-case fallback: `refs` is refused above when empty, and
    # `abandon_copies` puts every ref into `abandoned` or `problems`, so
    # `lines` cannot be empty here. A branch that looks reachable and is not
    # is worse than no branch — the same ground an earlier fix removed one on.
    return "\n".join(lines)


def register(mcp):
    """Register this step's tools on the FastMCP server."""
    mcp.tool()(prepare_image_deployment)
    mcp.tool()(mark_replication_complete)
    mcp.tool()(abandon_image_replication)
