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

"""MCP tools for landing zone step 4 (plan review): client sign-off on the units.

Owns the STATE_LZ_TRANSLATION_PLAN_REVIEW agent task. The client skips units that
should not be translated (or restores them) and signs off before any
translation spend.
"""

import json
import logging

from pydantic import BaseModel, Field
from typing import List, Optional

from google.api_core import exceptions
from mcp.server.fastmcp import Context

from servers.dag.state_management import (
    get_bucket_name,
    load_dag,
    authorize_and_rehydrate,
)
import servers.dag.state_management as state_mgr
from servers.dag.dispatch import run_elicitation

from ..landingzone_translationplan_3 import coverage
from ..landingzone_translationplan_3 import planner

logger = logging.getLogger("migration-dag")


class PlanApprovalSchema(BaseModel):
    approved: bool = Field(
        description="Approve the landing zone plan — the target-shape decisions, the "
                    "landing-zone Terraform draft, and the translation unit list? "
                    "Approving fixes what translation will generate; validation and "
                    "the PR happen after generation. Declining returns to landing "
                    "zone design."
    )


async def update_translation_plan(
    skip: Optional[List[str]] = None,
    unskip: Optional[List[str]] = None,
    ctx: Context = None,
) -> str:
    """Skips or restores translation units before translation. Repeatable; no DAG advance.

    Args:
        skip: unit_ids the client considers out of scope for translation.
        unskip: previously skipped unit_ids to restore.
    """
    logger.info(f"update_translation_plan called. skip={skip} unskip={unskip}")
    try:
        state_dict, generation, config = authorize_and_rehydrate(None)
    except Exception as e:
        return f"ERROR: {e}"

    if state_dict["current_state"] != "STATE_LZ_TRANSLATION_PLAN_REVIEW":
        return f"ERROR: Invalid state for update_translation_plan: {state_dict['current_state']}"

    plan = state_dict["variables"].get("translation_plan")
    if not plan:
        return "ERROR: No translation plan found. Run plan_translation first."

    notes = []
    if skip:
        plan, skip_notes = planner.set_unit_status(plan, skip, "skipped")
        notes.extend(skip_notes)
    if unskip:
        plan, unskip_notes = planner.set_unit_status(plan, unskip, "planned")
        notes.extend(unskip_notes)
    # The overlap check counts active units, so skips/unskips change its
    # result: recompute the stored findings rather than letting the sign-off
    # read stale ones. No-op for a plan persisted before coverage existed.
    # Guarded like plan_translation's attach_coverage: a defect in stored
    # coverage data must not block the skip/unskip path (the sign-off then
    # reads the last computed checks).
    try:
        plan = coverage.refresh_checks(plan)
    except Exception as e:
        logger.error(f"Coverage check refresh failed; plan update continues: {e!r}")

    state_dict["variables"]["translation_plan"] = plan
    state_dict["history"].append(f"Translation plan updated: {'; '.join(notes) or 'no changes'}")

    bucket_name = get_bucket_name(config["ledger_uri"])
    bucket = state_mgr.gcs_client.bucket(bucket_name)
    data = json.dumps(state_dict, indent=2)
    try:
        blob_state = bucket.blob("platform/onboarding/state.json")
        blob_state.upload_from_string(data, content_type="application/json", if_generation_match=generation)
    except exceptions.PreconditionFailed:
        return "ERROR: Concurrent update conflict. Your changes were not saved."

    return (
        f"Plan updated ({'; '.join(notes) or 'no changes'}).\n"
        f"{planner.summarize_plan(plan)}\n"
        "Call update_translation_plan again to adjust further, or confirm_translation_plan to start translating."
    )


async def confirm_translation_plan(ctx: Context = None) -> str:
    """Submits the landing zone plan for the user's sign-off and closes the phase.

    Call after the plan is shaped (update_translation_plan). The server then
    asks the user directly (an elicitation) to approve the landing zone plan —
    the target-shape decisions, the Terraform draft, and the unit list. That
    answer, not this call, is the decision: approving closes the landing zone
    and hands the plan to translation; declining returns to landing zone
    design. Do not call it again to act on the user's answer.
    """
    logger.info("confirm_translation_plan called.")
    try:
        state_dict, generation, config = authorize_and_rehydrate(None)
    except Exception as e:
        return f"ERROR: {e}"

    current_state = state_dict["current_state"]
    if current_state != "STATE_LZ_TRANSLATION_PLAN_REVIEW":
        return f"ERROR: Invalid state for confirm_translation_plan: {current_state}"

    plan = state_dict["variables"].get("translation_plan")
    if not plan:
        return "ERROR: No translation plan found. Run plan_translation first."

    active = planner.active_units(plan)
    if not active:
        # The decline route (the on_reject elicitation) is only raised further
        # down, once at least one unit is active — so the remedy must say
        # "unskip first", not "decline now".
        return (
            "ERROR: Every unit is skipped — nothing to translate. Unskip the units you "
            "skipped during review that are actually in scope. Units skipped by a "
            "landing-zone decision (e.g. Autopilot node pools) carry facts: unskip them "
            "to translate anyway — or, to revisit the design instead, unskip one and "
            "then decline at the sign-off prompt this tool raises once a unit is "
            "active. Units the planner skipped by default mark inventory sections with "
            "no facts: for those, fix discovery (rescan or amend the scope) and re-plan "
            "instead of unskipping."
        )

    bucket_name = get_bucket_name(config["ledger_uri"])
    bucket = state_mgr.gcs_client.bucket(bucket_name)
    try:
        platform_dag = load_dag(bucket, "platform_dag.json")
    except Exception as e:
        return f"ERROR: {e}"

    state_def = platform_dag["states"][current_state]
    for key in ("on_approve", "on_reject"):
        if key not in state_def.get("transitions", {}):
            return f"ERROR: The platform DAG in this ledger predates the plan approval ({key} missing). Re-run bootstrapping to refresh it."

    # One elicitation approves the whole landing zone plan: decisions, the
    # Terraform draft, and the unit list (braces escaped — ids are data).
    decisions = state_dict["variables"].get("lz_decisions", {})
    summary = (
        f" This plan has {len(active)} unit(s) to translate"
        + (f" ({len(plan['units']) - len(active)} skipped)" if len(plan['units']) != len(active) else "")
        + "; decisions: "
        + (", ".join(f"{k}={v}" for k, v in sorted(decisions.items())) or "none recorded")
        + "."
    )
    # Coverage findings ride into the sign-off prompt as counts and offending
    # names only (never the instantiated table). Overlap and traceability are
    # WARNING-grade — the human weighs them — but an OMISSION named here is
    # the same condition the validate step enforces (coverage_gate), so the
    # prompt is the early warning for a later hard failure.
    # update_translation_plan keeps the stored checks current across
    # skips/unskips. Guarded like that tool's refresh_checks: corrupt stored
    # coverage must not block the sign-off itself — the elicitation then goes
    # out without the coverage sentence.
    try:
        checks = (plan.get("coverage") or {}).get("checks")
        if checks is not None:
            problems = coverage.findings(checks)
            if problems:
                summary += (
                    f" Coverage (v1, section granularity): {len(problems)} warning(s) — "
                    + "; ".join(problems)
                    + ". Omitted rows fail the validate step unless its "
                    "UNENFORCED_ROWS pin excuses them (coverage_gate)."
                )
            else:
                summary += " Coverage checks (v1, section granularity): clean."
    except Exception as e:
        logger.error(f"Coverage findings failed; sign-off proceeds without them: {e!r}")
    # plan.findings (decisions that disagree, facts that argue against a
    # recorded choice) are computed inside build_translation_plan and must
    # reach the reviewer: rendered OUTSIDE the best-effort coverage try, so a
    # render failure is an ERROR return, never a silently dropped line.
    try:
        finding_lines = planner.render_findings(plan)
    except Exception as e:
        return f"ERROR: Could not render the plan findings for the sign-off: {e}"
    if finding_lines:
        summary += " Findings: " + " | ".join(finding_lines) + "."
    summary = summary.replace("{", "{{").replace("}", "}}")
    elicit_def = {**state_def, "prompt_template": state_def.get("prompt_template", "") + summary}
    try:
        approved, _ = await run_elicitation(
            ctx, current_state, elicit_def, PlanApprovalSchema, state_dict, config
        )
    except Exception as e:
        return f"ERROR: Could not raise the plan approval elicitation (is this a live MCP session?): {e}"

    if not approved:
        dest = state_def["transitions"]["on_reject"]
        state_dict["history"].append("Landing zone plan declined by the user; returning to design")
        state_dict["history"].append(f"Transitioned {current_state} -> {dest} via confirm_translation_plan")
        state_dict["current_state"] = dest
        data = json.dumps(state_dict, indent=2)
        try:
            bucket.blob("platform/onboarding/state.json").upload_from_string(
                data, content_type="application/json", if_generation_match=generation)
        except exceptions.PreconditionFailed:
            return "ERROR: Concurrent update conflict. Your changes were not saved."
        return (
            f"Plan not approved; back to landing zone design.\n"
            f"Current State: {dest}"
        )

    next_state = state_def["transitions"]["on_approve"]
    state_dict["history"].append(f"Landing zone plan approved ({len(active)} active unit(s))")
    state_dict["history"].append(f"Transitioned {current_state} -> {next_state} via confirm_translation_plan")
    state_dict["current_state"] = next_state

    data = json.dumps(state_dict, indent=2)
    try:
        blob_state = bucket.blob("platform/onboarding/state.json")
        blob_state.upload_from_string(data, content_type="application/json", if_generation_match=generation)
    except exceptions.PreconditionFailed:
        return "ERROR: Concurrent update conflict. Your changes were not saved."

    return (
        f"SUCCESS: Landing zone plan approved ({len(active)} units to translate, "
        f"{len(plan['units']) - len(active)} skipped). Current State: {next_state}.\n"
        "The landing zone phase is closed; nothing was validated or pushed — that "
        "happens after generation."
    )


def register(mcp):
    """Register this step's tools on the FastMCP server."""
    mcp.tool()(update_translation_plan)
    mcp.tool()(confirm_translation_plan)
