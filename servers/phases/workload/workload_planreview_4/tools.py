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

"""MCP tools for workload plan review: the human sign-off on the plan.

Owns the STATE_WKLD_PLAN_REVIEW agent task (the landing-zone Gate C analog,
landingzone_planreview_4). update_workload_plan shapes the unit list (pure
variables/plan update, no transition); confirm_workload_plan raises THE
plan-approval elicitation in-call — approve parks the component at the
graph tail, reject returns to STATE_WKLD_PLAN. Both claimant-enforced.
"""

import json
import logging
from typing import List, Optional

from google.api_core import exceptions
from mcp.server.fastmcp import Context
from pydantic import BaseModel, Field

from servers.dag.state_management import (
    get_bucket_name,
    load_dag,
    authorize_and_rehydrate_workload,
)
import servers.dag.state_management as state_mgr
from servers.dag.dispatch import run_elicitation

from ..workload_plan_2 import planner

logger = logging.getLogger("migration-dag")

REVIEW_STATE = "STATE_WKLD_PLAN_REVIEW"


class WorkloadPlanApprovalSchema(BaseModel):
    approved: bool = Field(
        description="Approve the workload translation plan — the four unit "
                    "families with their statuses (planned, parked, "
                    "placeholder) and fact-derived briefs? Approving fixes "
                    "what the pipeline will translate for this component; "
                    "declining returns to planning."
    )


def _write_state(bucket, component, state_dict, generation, config):
    """One precondition-guarded state write; returns an error string or None."""
    try:
        bucket.blob(f"workloads/{component}/state.json").upload_from_string(
            json.dumps(state_dict, indent=2), content_type="application/json",
            if_generation_match=generation)
    except exceptions.PreconditionFailed:
        return "ERROR: Concurrent update conflict. Your changes were not saved."
    except exceptions.Forbidden:
        return state_mgr.workload_write_denied(config, component)
    return None


def _write_plan_blob(bucket, component, plan, config):
    """Keeps plan.json consistent with variables.workload_plan on review
    edits. Best-effort precondition (re-read generation each time)."""
    blob = bucket.blob(f"workloads/{component}/plan.json")
    try:
        blob.reload()
        plan_generation = blob.generation
    except exceptions.NotFound:
        plan_generation = 0
    try:
        blob.upload_from_string(
            json.dumps(plan, indent=2), content_type="application/json",
            if_generation_match=plan_generation)
    except exceptions.PreconditionFailed:
        return ("ERROR: Concurrent update conflict writing plan.json. "
                "Please retry.")
    except exceptions.Forbidden:
        return state_mgr.workload_write_denied(config, component)
    return None


async def update_workload_plan(
    skip: Optional[List[str]] = None,
    unskip: Optional[List[str]] = None,
    ctx: Context = None,
) -> str:
    """Skips or restores plan units before approval. Repeatable; no DAG advance.

    Args:
        skip: unit_ids the developer considers out of scope (they stay
            visible in the plan as skipped — coverage, not deletion).
        unskip: previously skipped unit_ids to restore. A unit returns to
            the status the FACTS produced, so a parked unit comes back
            parked. A placeholder unit carries no facts and is refused
            outright — amend the scope and re-plan instead.
    """
    logger.info(f"update_workload_plan called. skip={skip} unskip={unskip}")
    try:
        state_dict, generation, config = authorize_and_rehydrate_workload(None)
    except Exception as e:
        return f"ERROR: {e}"

    if state_dict["current_state"] != REVIEW_STATE:
        return (f"ERROR: Invalid state for update_workload_plan: "
                f"{state_dict['current_state']}")
    plan = state_dict["variables"].get("workload_plan")
    if not plan:
        return "ERROR: No workload plan found. Run plan_workload_translation first."

    notes = []
    if skip:
        plan, skip_notes = planner.set_unit_status(plan, skip, "skipped")
        notes.extend(skip_notes)
    if unskip:
        plan, unskip_notes = planner.restore_units(plan, unskip)
        notes.extend(unskip_notes)
    state_dict["variables"]["workload_plan"] = plan
    state_dict["history"].append(
        f"Workload plan updated: {'; '.join(notes) or 'no changes'}")

    component = config["component"]
    bucket = state_mgr.gcs_client.bucket(get_bucket_name(config["ledger_uri"]))
    error = _write_state(bucket, component, state_dict, generation, config)
    if error:
        return error
    error = _write_plan_blob(bucket, component, plan, config)
    if error:
        return error
    return (
        f"Plan updated ({'; '.join(notes) or 'no changes'}).\n"
        f"{planner.summarize_plan(plan)}\n"
        "Call update_workload_plan again to adjust, or confirm_workload_plan "
        "to raise the sign-off."
    )


def _plan_summary_suffix(plan: dict) -> str:
    """One line per unit for the elicitation prompt — parked and placeholder
    units included (coverage, not silence). Braces escaped: ids are data."""
    parts = []
    for unit in plan.get("units", []):
        token = f"{unit['unit_id']}: {unit['status']}"
        if unit.get("placeholder"):
            token += " (placeholder)"
        parts.append(token)
    return (" Units — " + "; ".join(parts) + ".") \
        .replace("{", "{{").replace("}", "}}")


async def confirm_workload_plan(ctx: Context = None) -> str:
    """Raises the plan-approval elicitation and acts on the user's answer.

    Call after summarizing the plan. The elicitation is the decision — do
    not ask in chat first and do not call again to act on the answer.
    Approve -> the component parks at the graph tail awaiting the next
    milestone; reject -> back to STATE_WKLD_PLAN for a re-plan.
    """
    logger.info("confirm_workload_plan called.")
    try:
        state_dict, generation, config = authorize_and_rehydrate_workload(None)
    except Exception as e:
        return f"ERROR: {e}"

    current = state_dict["current_state"]
    if current != REVIEW_STATE:
        return f"ERROR: Invalid state for confirm_workload_plan: {current}"
    plan = state_dict["variables"].get("workload_plan")
    if not plan:
        return "ERROR: No workload plan found. Run plan_workload_translation first."
    if not planner.active_units(plan):
        return (
            "ERROR: every unit is skipped — nothing would be translated. "
            "Unskip the units that are actually in scope with "
            "update_workload_plan(unskip=[...]). Placeholder units carry no "
            "facts: for those, amend the component scope and re-plan "
            "instead of unskipping."
        )

    component = config["component"]
    bucket = state_mgr.gcs_client.bucket(get_bucket_name(config["ledger_uri"]))
    try:
        dag = load_dag(bucket, f"workloads/{component}/dag.json")
    except Exception as e:
        return f"ERROR: {e}"
    state_def = dag["states"][current]
    elicit_def = {**state_def,
                  "prompt_template": state_def.get("prompt_template", "")
                  + _plan_summary_suffix(plan)}
    try:
        approved, _ = await run_elicitation(
            ctx, current, elicit_def, WorkloadPlanApprovalSchema, state_dict,
            config)
    except Exception as e:
        return (f"ERROR: Could not raise the plan approval elicitation "
                f"(is this a live MCP session?): {e}")

    if not approved:
        dest = state_def["transitions"]["on_reject"]
        state_dict["history"].append(
            "Workload plan declined by the user; returning to planning")
        state_dict["history"].append(
            f"Transitioned {current} -> {dest} via confirm_workload_plan")
        state_dict["current_state"] = dest
        error = _write_state(bucket, component, state_dict, generation, config)
        if error:
            return error
        return (
            f"Plan not approved; back to planning. Current State: {dest}.\n"
            "Re-run plan_workload_translation after amending the scope or "
            "the source checkout — the plan is deterministic, so an "
            "unchanged input produces the same plan."
        )

    dest = state_def["transitions"]["on_tool_call_received"]
    active = planner.active_units(plan)
    state_dict["history"].append(
        f"Workload plan approved ({len(active)} active unit(s) of "
        f"{len(plan.get('units', []))})")
    state_dict["history"].append(
        f"Transitioned {current} -> {dest} via confirm_workload_plan")
    state_dict["current_state"] = dest
    error = _write_state(bucket, component, state_dict, generation, config)
    if error:
        return error
    return (
        f"SUCCESS: Workload plan approved for component '{component}' "
        f"({len(active)} active unit(s)). Current State: {dest}.\n"
        "Call get_next_stage for the current step's instructions and report "
        "its outcome to the user."
    )


def register(mcp):
    """Register this step's tools on the FastMCP server."""
    mcp.tool()(update_workload_plan)
    mcp.tool()(confirm_workload_plan)
