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

"""MCP tools for translation step 2 (human review): review the code + tradeoffs.

Owns the STATE_TRANSLATION_REVIEW agent task. The client reviews each unit's
generated code (Terraform and/or Kubernetes manifests) and tradeoffs; they can approve everything, send specific units back
with feedback (request_unit_revision), or retranslate the lot.
"""

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

# The plan object is authored by the landing zone's translation-plan step;
# review actions mutate unit statuses through the same pure helpers.
from servers.phases.landingzone.landingzone_translationplan_3 import planner
from ..translation_translate_1.tools import UNIT_BLOB_PREFIX

logger = logging.getLogger("migration-dag")


async def get_translation_results(unit_id: str = "", ctx: Context = None) -> str:
    """Retrieves a lean review summary for every unit, or for one unit.

    The generated code and the full tradeoffs prose are deliberately NOT
    returned here — the human reads those from the unit blobs in the ledger
    (platform/translation/units/). This tool gives the agent only what it
    needs to run the review
    conversation: each unit's status, the files it produced (paths, not bodies),
    and the assumptions and open questions the reviewer must weigh in on.

    Args:
        unit_id: empty for every unit's summary; a unit_id for that one unit's
            status, file list, assumptions, and open questions.
    """
    logger.info(f"get_translation_results called (unit_id={unit_id!r}).")
    try:
        state_dict, _, config = authorize_and_rehydrate(None)
    except Exception as e:
        return f"ERROR: {e}"

    plan = state_dict["variables"].get("translation_plan")
    if not plan:
        return "ERROR: No translation plan found."

    if not unit_id:
        return (
            f"Current State: {state_dict['current_state']}\n"
            f"{planner.summarize_plan(plan)}\n\n"
            "The code and tradeoffs for each unit are in the ledger's unit blobs "
            "(platform/translation/units/) — call get_translation_results(unit_id=...) "
            "for a unit's assumptions and open questions to discuss with the user."
        )

    known = {u["unit_id"] for u in plan["units"]}
    if unit_id not in known:
        return f"ERROR: Unknown unit_id '{unit_id}'. Known: {', '.join(sorted(known))}"

    bucket_name = get_bucket_name(config["ledger_uri"])
    bucket = state_mgr.gcs_client.bucket(bucket_name)
    try:
        payload = json.loads(bucket.blob(f"{UNIT_BLOB_PREFIX}/{unit_id}.json").download_as_text())
    except exceptions.NotFound:
        return f"ERROR: No result persisted for '{unit_id}' yet (status: " + next(
            (u["status"] for u in plan["units"] if u["unit_id"] == unit_id), "?"
        ) + ")."
    except Exception as e:
        return f"ERROR: Failed to read result for '{unit_id}': {e}"

    unit = payload.get("unit", {})
    result = payload.get("result") or {}
    files = result.get("files", [])
    # The plan is authoritative: a review action (revise, skip) updates only
    # the plan, while the blob's status is frozen at the moment its run
    # finished — report the live status, and flag any divergence.
    plan_status = next((u["status"] for u in plan["units"] if u["unit_id"] == unit_id), "?")
    output = [f"# Unit {unit_id}", f"- status: {plan_status}"]
    if unit.get("status") and unit.get("status") != plan_status:
        output.append(f"- persisted result: {unit['status']} (run {payload.get('run', '?')} — "
                      "superseded by the review action above)")
    if unit.get("error"):
        output.append(f"- error: {unit['error']}")
    output.append(
        f"- Files: {len(files)} — {', '.join(f['path'] for f in files) or '(none)'} "
        "(read the code and tradeoffs from the unit blob in the ledger)"
    )
    if result.get("assumptions"):
        output.append("## Assumptions the reviewer must verify\n"
                      + "\n".join(f"- {a}" for a in result["assumptions"]))
    if result.get("open_questions"):
        output.append("## Open questions for the client\n"
                      + "\n".join(f"- {q}" for q in result["open_questions"]))
    return "\n\n".join(output)


async def request_unit_revision(unit_ids: List[str], feedback: str, ctx: Context = None) -> str:
    """Sends specific units back for retranslation with reviewer feedback.

    Args:
        unit_ids: the units to redo.
        feedback: what the reviewer wants changed — passed verbatim to the
            translation worker, which must address it in its tradeoffs.
    """
    logger.info(f"request_unit_revision called for {unit_ids}")
    if not unit_ids or not feedback.strip():
        return "ERROR: Both unit_ids and feedback are required."

    try:
        state_dict, generation, config = authorize_and_rehydrate(None)
    except Exception as e:
        return f"ERROR: {e}"

    current_state = state_dict["current_state"]
    if current_state != "STATE_TRANSLATION_REVIEW":
        return f"ERROR: Invalid state for request_unit_revision: {current_state}"

    plan = state_dict["variables"].get("translation_plan")
    if not plan:
        return "ERROR: No translation plan found."

    statuses = {u["unit_id"]: u["status"] for u in plan["units"]}
    unknown = [u for u in unit_ids if u not in statuses]
    if unknown:
        return f"ERROR: Unknown unit_ids: {', '.join(unknown)}. Known: {', '.join(sorted(statuses))}"
    blocked = [u for u in unit_ids if statuses[u] == "skipped"]
    if blocked:
        return (
            f"ERROR: Skipped units cannot be revised: {', '.join(blocked)}. "
            "Skipped units are excluded from translation by the client's plan sign-off."
        )

    bucket_name = get_bucket_name(config["ledger_uri"])
    bucket = state_mgr.gcs_client.bucket(bucket_name)
    try:
        platform_dag = load_dag(bucket, "platform_dag.json")
    except Exception as e:
        return f"ERROR: {e}"

    state_def = platform_dag["states"][current_state]
    if "on_revise_units" not in state_def.get("transitions", {}):
        return "ERROR: The platform DAG in this ledger predates unit revisions. Use approve_translation(action='retranslate')."

    plan, notes = planner.set_unit_status(plan, unit_ids, "revise", feedback=feedback)
    dest = state_def["transitions"]["on_revise_units"]

    state_dict["variables"]["translation_plan"] = plan
    state_dict["history"].append(f"Units sent back for revision: {'; '.join(notes)}")
    state_dict["history"].append(f"Transitioned {current_state} -> {dest} via request_unit_revision")
    state_dict["current_state"] = dest

    data = json.dumps(state_dict, indent=2)
    try:
        blob_state = bucket.blob("platform/onboarding/state.json")
        blob_state.upload_from_string(data, content_type="application/json", if_generation_match=generation)
    except exceptions.PreconditionFailed:
        return "ERROR: Concurrent update conflict. Your changes were not saved."

    return (
        f"SUCCESS: {len(unit_ids)} unit(s) marked for revision. Current State: {dest}.\n"
        "Next: call run_translation() — only the revised units are retranslated, with your feedback in the prompt."
    )


async def approve_translation(action: str = "approve", feedback: str = "", ctx: Context = None) -> str:
    """Approves the translation results, or sends everything back for retranslation.

    Args:
        action: 'approve' to accept the unit results and move to terraform
            validation (requires every non-skipped unit to be done), or
            'retranslate' to redo every non-skipped unit.
        feedback: optional overall feedback applied when retranslating.
    """
    logger.info(f"approve_translation called with action: {action}")
    if action not in ("approve", "retranslate"):
        return (
            f"ERROR: Invalid action '{action}'. Use 'approve' or 'retranslate' "
            "(or request_unit_revision / skip_translation_units for specific units)."
        )
    try:
        state_dict, generation, config = authorize_and_rehydrate(None)
    except Exception as e:
        return f"ERROR: {e}"

    current_state = state_dict["current_state"]
    if current_state != "STATE_TRANSLATION_REVIEW":
        return f"ERROR: Invalid state for approve_translation: {current_state}"

    plan = state_dict["variables"].get("translation_plan")
    if not plan:
        return "ERROR: No translation plan found."

    bucket_name = get_bucket_name(config["ledger_uri"])
    bucket = state_mgr.gcs_client.bucket(bucket_name)
    try:
        platform_dag = load_dag(bucket, "platform_dag.json")
    except Exception as e:
        return f"ERROR: {e}"

    state_def = platform_dag["states"][current_state]
    transition_key = "on_approve" if action == "approve" else "on_retranslate"
    if transition_key not in state_def.get("transitions", {}):
        return f"ERROR: Invalid action '{action}' for state {current_state}"

    if action == "approve":
        not_done = [u["unit_id"] for u in plan["units"] if u["status"] not in ("done", "skipped")]
        if not_done:
            return (
                f"ERROR: Cannot approve — units not done: {', '.join(not_done)}. "
                "Send them back with request_unit_revision, or exclude them with "
                "skip_translation_units if they should not block approval."
            )
    else:
        active = [u["unit_id"] for u in plan["units"] if u["status"] != "skipped"]
        if not active:
            return (
                "ERROR: Cannot retranslate — every unit is skipped, so a re-run "
                "would have nothing to do. Approve to finish the phase instead."
            )
        plan, _ = planner.set_unit_status(plan, active, "revise", feedback=feedback or None)
        state_dict["variables"]["translation_plan"] = plan

    dest = state_def["transitions"][transition_key]
    state_dict["history"].append(f"Transitioned {current_state} -> {dest} via approve_translation({action})")
    state_dict["current_state"] = dest

    data = json.dumps(state_dict, indent=2)
    try:
        blob_state = bucket.blob("platform/onboarding/state.json")
        blob_state.upload_from_string(data, content_type="application/json", if_generation_match=generation)
    except exceptions.PreconditionFailed:
        return "ERROR: Concurrent update conflict. Your changes were not saved."

    if action == "approve":
        return (
            f"Current State: {dest}\nSUCCESS: Unit review approved. "
            "Next: call run_generated_validation() — the generated code is "
            "terraform-validated (with an auto-fix pass), then the user ships "
            "it as a PR from the final approval."
        )
    return f"Current State: {dest}\nAll active units marked for retranslation. Call run_translation()."


async def skip_translation_units(unit_ids: List[str], reason: str, ctx: Context = None) -> str:
    """Excludes units from translation during review, recording the client's reason.

    The escape hatch for a unit that persistently fails or that the client
    decides not to migrate after seeing the results: skipped units no longer
    block approve_translation. Also callable from STATE_TRANSLATION_RUNNING,
    so a unit that keeps failing every run can be excluded without the phase
    wedging (run_translation only advances once a unit succeeds). No DAG
    transition.

    Args:
        unit_ids: the units to exclude.
        reason: why the client is excluding them (recorded in the unit notes).
    """
    logger.info(f"skip_translation_units called for {unit_ids}")
    if not unit_ids or not reason.strip():
        return "ERROR: Both unit_ids and reason are required."

    try:
        state_dict, generation, config = authorize_and_rehydrate(None)
    except Exception as e:
        return f"ERROR: {e}"

    current_state = state_dict["current_state"]
    if current_state not in ("STATE_TRANSLATION_REVIEW", "STATE_TRANSLATION_RUNNING"):
        return f"ERROR: Invalid state for skip_translation_units: {current_state}"

    plan = state_dict["variables"].get("translation_plan")
    if not plan:
        return "ERROR: No translation plan found."

    known = {u["unit_id"] for u in plan["units"]}
    unknown = [u for u in unit_ids if u not in known]
    if unknown:
        return f"ERROR: Unknown unit_ids: {', '.join(unknown)}. Known: {', '.join(sorted(known))}"

    plan, notes = planner.set_unit_status(plan, unit_ids, "skipped")
    for unit in plan["units"]:
        if unit["unit_id"] in set(unit_ids):
            unit["notes"] = list(unit.get("notes", [])) + [f"Skipped in review: {reason}"]

    state_dict["variables"]["translation_plan"] = plan
    state_dict["history"].append(f"Units skipped in review ({reason}): {'; '.join(notes)}")

    bucket_name = get_bucket_name(config["ledger_uri"])
    bucket = state_mgr.gcs_client.bucket(bucket_name)
    data = json.dumps(state_dict, indent=2)
    try:
        blob_state = bucket.blob("platform/onboarding/state.json")
        blob_state.upload_from_string(data, content_type="application/json", if_generation_match=generation)
    except exceptions.PreconditionFailed:
        return "ERROR: Concurrent update conflict. Your changes were not saved."

    remaining = [u["unit_id"] for u in plan["units"] if u["status"] not in ("done", "skipped")]
    output = f"SUCCESS: Skipped {', '.join(unit_ids)}. Still in {current_state}.\n"
    output += (
        f"Units still blocking approval: {', '.join(remaining)}." if remaining
        else "All remaining units are done — approve_translation(action='approve') is now available."
    )
    return output


def register(mcp):
    """Register this step's tools on the FastMCP server."""
    mcp.tool()(get_translation_results)
    mcp.tool()(request_unit_revision)
    mcp.tool()(skip_translation_units)
    mcp.tool()(approve_translation)
