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

"""MCP tools for landing zone design: open the workspace, resolve the four
target-shape decisions, and finalize.

Owns the STATE_LZ_DESIGN agent task — the merged landing-zone entry step. The
first resolve_lz_decision call allocates the target clone workspace (branch
uuid, branch name and clone path) so the agent has somewhere to write the HCL;
finalize_landing_zone_design records the design and moves straight to the
translation plan. Terraform validation and the PR happen at the END of the
phase, over the generated code, not here. See instructions.md in this folder.
"""

import json
import logging
import uuid

from google.api_core import exceptions
from mcp.server.fastmcp import Context

from servers.dag.dispatch import run_dispatch_loop
from servers.dag.server import decisions as decisions_lib
from servers.dag.state_management import (
    authorize_and_rehydrate,
    get_bucket_name,
    load_dag,
)
import servers.dag.state_management as state_mgr

from ..workspace import target_clone_path
from . import ranges

logger = logging.getLogger("migration-dag")

# The four target-shape decisions and their choice sets (DESIGN.md §7.2) are
# read out of knowledge/gke-landing-zone.md by the decision registry
# (servers/dag/server/decisions.py) — one table for the humans and the
# server, the way blocker_criteria.py reads the assessment taxonomy.


async def resolve_lz_decision(decision_id: str, choice: str, ctx: Context = None) -> str:
    """Resolves a target configuration decision for a specific discovery trigger.

    Args:
        decision_id: The trigger name: 'karpenter', 'privileged_daemonsets', 'gpu_tpu', 'vpc_peering'.
        choice: The selected GKE configuration option.
    """
    logger.info(f"resolve_lz_decision called: {decision_id} -> {choice}")

    valid_choices = decisions_lib.choices()
    if decision_id not in valid_choices:
        return f"ERROR: Invalid decision_id: '{decision_id}'."
    if choice not in valid_choices[decision_id]:
        return f"ERROR: Invalid choice '{choice}' for decision '{decision_id}'. Valid choices: {valid_choices[decision_id]}"

    try:
        state_dict, generation, config = authorize_and_rehydrate(None)
    except Exception as e:
        return f"ERROR: {e}"

    if state_dict["current_state"] != "STATE_LZ_DESIGN":
        return f"ERROR: Invalid state for resolve_lz_decision: {state_dict['current_state']}"

    # The design step is the phase entry, so the first decision resolved is
    # where the target clone workspace is allocated: the agent needs the branch
    # and clone path before it writes any HCL. Idempotent — re-entry after a
    # rejection or a failed validation keeps the existing clone.
    if "lz_branch_uuid" not in state_dict["variables"]:
        branch_uuid = str(uuid.uuid4())
        state_dict["variables"]["lz_branch_uuid"] = branch_uuid
        state_dict["variables"]["lz_branch_name"] = f"migration/gke-landing-zone-{branch_uuid}"
        target_dir = target_clone_path(branch_uuid)
        state_dict["variables"]["target_clone_path"] = target_dir
        logger.debug(f"Allocated landing zone workspace. Branch: migration/gke-landing-zone-{branch_uuid}, Path: {target_dir}")

    if "lz_decisions" not in state_dict["variables"]:
        state_dict["variables"]["lz_decisions"] = {}

    state_dict["variables"]["lz_decisions"][decision_id] = choice

    bucket_name = get_bucket_name(config["ledger_uri"])
    bucket = state_mgr.gcs_client.bucket(bucket_name)
    data = json.dumps(state_dict, indent=2)
    try:
        blob_state = bucket.blob("platform/onboarding/state.json")
        blob_state.upload_from_string(data, content_type="application/json", if_generation_match=generation)
    except exceptions.PreconditionFailed:
        return "ERROR: Concurrent update conflict. Your changes were not saved."

    # Echo the workspace coordinates on every success. The design step allocates
    # the target clone workspace on the first decision, and the instructions have
    # the agent read these values back before writing any HCL — so the tool that
    # allocates them must also report them, on the first call and every re-entry.
    # The source address space and the target ranges proposed from it ride
    # the same echo: the ranges are design input the way the clone path is,
    # and the baseline the knowledge document states was written before any
    # estate was scanned. Pure code (ranges.py); the agent reads the values
    # back rather than choosing its own.
    variables = state_dict["variables"]
    reply = (
        f"SUCCESS: Decision for '{decision_id}' resolved to '{choice}'.\n"
        f"lz_branch_uuid: {variables['lz_branch_uuid']}\n"
        f"lz_branch_name: {variables['lz_branch_name']}\n"
        f"target_clone_path: {variables['target_clone_path']}\n"
        + ranges.describe_proposal(_address_space(variables, bucket))
    )
    # The agent learns which regime it is in BEFORE finalize refuses: without
    # usable triggers every recorded choice votes, so a recorded default must
    # agree in mode with the deliberate picks (coverage-guards G0 step 3).
    if not _triggers_usable(variables):
        reply += ("\nWARNING: the discovery inventory carries no usable `triggers`, so "
                  "every recorded choice counts as deliberate; a defaulted decision must "
                  "agree in cluster mode with the ones you chose, or finalize will refuse.")
    return reply


def _triggers_usable(variables: dict) -> bool:
    inventory = variables.get("discovery_inventory") or {}
    triggers = inventory.get("triggers") if isinstance(inventory, dict) else None
    return decisions_lib.triggers_available(triggers)


def _address_space(variables: dict, bucket) -> dict:
    """The inventory's address_space section: from the ledger blob, which a
    scope amendment's rescan rewrites, else from the copy extraction stored
    in the state variables. Empty when neither has one — the echo then says
    so rather than failing the decision."""
    try:
        inventory, _ = state_mgr.load_inventory(bucket)
    except Exception as e:
        logger.warning(f"Could not read the inventory for the address space: {e}")
        inventory = None
    if not isinstance(inventory, dict) or "address_space" not in inventory:
        inventory = variables.get("discovery_inventory")
    section = (inventory or {}).get("address_space")
    return section if isinstance(section, dict) else {}


async def finalize_landing_zone_design(ctx: Context = None) -> str:
    """Signals that the landing zone design generation is complete and transitions to validation."""
    logger.info("finalize_landing_zone_design called.")
    try:
        state_dict, generation, config = authorize_and_rehydrate(None)
    except Exception as e:
        return f"ERROR: {e}"

    if state_dict["current_state"] != "STATE_LZ_DESIGN":
        return f"ERROR: Invalid state for finalize_landing_zone_design: {state_dict['current_state']}"

    # All four target-shape decisions must be on record: the translation
    # planner reads them, and an unrecorded decision is one the design never
    # considered. resolve_lz_decision is repeatable, so re-run it for any named
    # below before finalizing again.
    decisions = state_dict["variables"].get("lz_decisions", {})
    missing = [r for r in decisions_lib.choices() if r not in decisions]
    if missing:
        return f"ERROR: Cannot finalize. Missing trigger decisions: {', '.join(missing)}"

    # Refusal at record time (coverage-guards G0 step 3): two recorded choices
    # that imply different cluster modes are a design conflict every reader
    # downstream would have to guess about. Name the ids; resolve_lz_decision
    # overwrites, so recovery is one call per named id and finalize again.
    triggers = (state_dict["variables"].get("discovery_inventory") or {}).get("triggers") \
        if isinstance(state_dict["variables"].get("discovery_inventory"), dict) else None
    mode, reason = decisions_lib.cluster_mode(decisions, triggers)
    if mode is None and str(reason).startswith("disagree"):
        named = reason[len("disagree: "):]
        regime = ("" if _triggers_usable(state_dict["variables"]) else
                  " (triggers unavailable: every recorded choice votes; record a "
                  "mode-consistent choice, or re-extract so the triggers exist)")
        return ("ERROR: Cannot finalize. The recorded decisions imply different cluster "
                f"modes: {named}{regime}. Re-resolve one of them with resolve_lz_decision "
                "so they agree, then finalize again.")

    bucket_name = get_bucket_name(config["ledger_uri"])
    bucket = state_mgr.gcs_client.bucket(bucket_name)
    try:
        platform_dag = load_dag(bucket, "platform_dag.json")
    except Exception as e:
        return f"ERROR: {e}"

    current_state = state_dict["current_state"]
    state_def = platform_dag["states"][current_state]
    next_state = state_def["transitions"]["on_tool_call_received"]

    state_dict["history"].append(f"Transitioned {current_state} -> {next_state} via finalize_landing_zone_design")
    state_dict["current_state"] = next_state

    transitions_run, message, error = await run_dispatch_loop(
        ctx, state_dict, platform_dag, config, "Landing zone design finalized.")
    if error:
        return error

    # Save back to GCS
    data = json.dumps(state_dict, indent=2)
    try:
        blob_state = bucket.blob("platform/onboarding/state.json")
        blob_state.upload_from_string(data, content_type="application/json", if_generation_match=generation)
    except exceptions.PreconditionFailed:
        return "ERROR: Concurrent update conflict. Your changes were not saved."

    output = f"Landing Zone Design Finalized:\n- Transition log: {', '.join(transitions_run) or 'none'}\n- Current State: {state_dict['current_state']}\n- Message: {message}\n"
    if state_dict["current_state"] == "STATE_LZ_TRANSLATION_PLAN":
        output += ("\nSUCCESS: Landing zone design recorded — nothing validated or "
                   "pushed yet. The phase concludes by agreeing the translation "
                   "plan — call get_next_stage to continue.")
    return output


def register(mcp):
    """Register this step's tools on the FastMCP server."""
    mcp.tool()(resolve_lz_decision)
    mcp.tool()(finalize_landing_zone_design)
