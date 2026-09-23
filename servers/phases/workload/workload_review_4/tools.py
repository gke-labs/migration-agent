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

"""MCP tools for the workload unit review (STATE_WKLD_REVIEW).

The translation_humanreview_2 tool set, component-scoped: the developer
reviews each unit's generated YAML and tradeoffs from the ledger blobs and
either approves toward validation, sends specific units back with feedback
(only revised units re-run — reuse conjunct b), skips units, or retranslates
the lot. Every state-mutating tool here is claimant-enforced;
the read-only summary is not, matching the scope step's browse rule.

Parked units (wkld-routing awaiting exports.gateway) are neither revisable
nor blocking: no worker ran for them, and approval carries them forward as
listed coverage — the ship elicitation and the PR name them as parked.
"""

import json
import logging
from typing import List

from google.api_core import exceptions
from mcp.server.fastmcp import Context

from servers.dag.state_management import (
    get_bucket_name,
    load_dag,
    authorize_and_rehydrate_workload,
)
import servers.dag.state_management as state_mgr

from ..workload_plan_2 import planner
from ..workload_translate_3.tools import unit_blob_path

logger = logging.getLogger("migration-dag")

REVIEW_STATE = "STATE_WKLD_REVIEW"


def _write_state(bucket, component, state_dict, generation, config):
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
    """Keeps plan.json consistent with variables.workload_plan on edits."""
    try:
        bucket.blob(f"workloads/{component}/plan.json").upload_from_string(
            json.dumps(plan, indent=2), content_type="application/json")
    except exceptions.Forbidden:
        return state_mgr.workload_write_denied(config, component)
    except Exception as e:
        logger.error(f"Failed to persist plan blob: {e}")
    return None


async def get_workload_results(unit_id: str = "", ctx: Context = None) -> str:
    """A lean review summary for every unit of this component, or one unit.

    The generated YAML and the tradeoffs prose are deliberately NOT returned
    here — the human reads those from the unit blobs in the ledger
    (workloads/<component>/units/). This returns each unit's status, its file
    list, and the assumptions/open questions the reviewer must weigh in on.

    Args:
        unit_id: empty for the plan-wide summary; a unit_id for that unit's
            status, files, assumptions and open questions.
    """
    logger.info(f"get_workload_results called (unit_id={unit_id!r}).")
    try:
        state_dict, _, config = authorize_and_rehydrate_workload(
            None, enforce_claimant=False)
    except Exception as e:
        return f"ERROR: {e}"
    component = config["component"]
    plan = state_dict["variables"].get("workload_plan")
    if not plan:
        return "ERROR: No workload plan found for this component."

    if not unit_id:
        return (
            f"Current State: {state_dict['current_state']}\n"
            f"{planner.summarize_plan(plan)}\n\n"
            f"Code and tradeoffs per unit are in workloads/{component}/units/"
            " — call get_workload_results(unit_id=...) for a unit's "
            "assumptions and open questions to discuss with the user.")

    known = {u["unit_id"] for u in plan["units"]}
    if unit_id not in known:
        return (f"ERROR: Unknown unit_id '{unit_id}'. "
                f"Known: {', '.join(sorted(known))}")
    bucket = state_mgr.gcs_client.bucket(get_bucket_name(config["ledger_uri"]))
    try:
        payload = json.loads(bucket.blob(
            unit_blob_path(component, unit_id)).download_as_text())
    except exceptions.NotFound:
        return (f"ERROR: No result persisted for '{unit_id}' yet (status: "
                + next((u["status"] for u in plan["units"]
                        if u["unit_id"] == unit_id), "?") + ").")
    except Exception as e:
        return f"ERROR: Failed to read result for '{unit_id}': {e}"

    unit = payload.get("unit", {})
    result = payload.get("result") or {}
    files = result.get("files", [])
    plan_status = next((u["status"] for u in plan["units"]
                        if u["unit_id"] == unit_id), "?")
    output = [f"# Unit {unit_id}", f"- status: {plan_status}"]
    if unit.get("status") and unit.get("status") != plan_status:
        output.append(f"- persisted result: {unit['status']} (run "
                      f"{payload.get('run', '?')} — superseded by the review "
                      "action above)")
    if unit.get("error"):
        output.append(f"- error: {unit['error']}")
    output.append(
        f"- Files: {len(files)} — "
        f"{', '.join(f['path'] for f in files) or '(none)'} "
        "(read the code and tradeoffs from the unit blob in the ledger)")
    if result.get("assumptions"):
        output.append("## Assumptions the reviewer must verify\n"
                      + "\n".join(f"- {a}" for a in result["assumptions"]))
    if result.get("open_questions"):
        output.append("## Open questions for the client\n"
                      + "\n".join(f"- {q}" for q in result["open_questions"]))
    return "\n\n".join(output)


async def request_workload_unit_revision(unit_ids: List[str], feedback: str,
                                         ctx: Context = None) -> str:
    """Sends specific units back for retranslation with reviewer feedback.

    Only the revised units re-run (reuse conjunct b: 'revise' invalidates a
    persisted blob for exactly those units).

    Args:
        unit_ids: the units to redo.
        feedback: what must change — passed verbatim to the worker.
    """
    logger.info(f"request_workload_unit_revision called for {unit_ids}")
    if not unit_ids or not feedback.strip():
        return "ERROR: Both unit_ids and feedback are required."
    try:
        state_dict, generation, config = authorize_and_rehydrate_workload(None)
    except Exception as e:
        return f"ERROR: {e}"
    if state_dict["current_state"] != REVIEW_STATE:
        return (f"ERROR: Invalid state for request_workload_unit_revision: "
                f"{state_dict['current_state']}")
    plan = state_dict["variables"].get("workload_plan")
    if not plan:
        return "ERROR: No workload plan found."

    statuses = {u["unit_id"]: u["status"] for u in plan["units"]}
    unknown = [u for u in unit_ids if u not in statuses]
    if unknown:
        return (f"ERROR: Unknown unit_ids: {', '.join(unknown)}. "
                f"Known: {', '.join(sorted(statuses))}")
    blocked = [u for u in unit_ids if statuses[u] in ("skipped", "parked")]
    if blocked:
        return (
            f"ERROR: These units cannot be revised: {', '.join(blocked)}. "
            "Skipped units were excluded by sign-off; parked units never ran "
            "a worker (their attach point is not published yet), so there is "
            "no attempt to revise.")

    component = config["component"]
    bucket = state_mgr.gcs_client.bucket(get_bucket_name(config["ledger_uri"]))
    try:
        dag = load_dag(bucket, f"workloads/{component}/dag.json")
    except Exception as e:
        return f"ERROR: {e}"
    state_def = dag["states"][REVIEW_STATE]
    if "on_revise_units" not in state_def.get("transitions", {}):
        return ("ERROR: this component's graph copy predates unit revisions. "
                "Re-join to upgrade it, or use "
                "approve_workload_translation(action='retranslate').")

    plan, notes = planner.set_unit_status(plan, unit_ids, "revise",
                                          feedback=feedback)
    dest = state_def["transitions"]["on_revise_units"]
    state_dict["variables"]["workload_plan"] = plan
    state_dict["history"].append(
        f"Units sent back for revision: {'; '.join(notes)}")
    state_dict["history"].append(
        f"Transitioned {REVIEW_STATE} -> {dest} via "
        "request_workload_unit_revision")
    state_dict["current_state"] = dest
    error = _write_state(bucket, component, state_dict, generation, config)
    if error:
        return error
    error = _write_plan_blob(bucket, component, plan, config)
    if error:
        return error
    return (
        f"SUCCESS: {len(unit_ids)} unit(s) marked for revision. "
        f"Current State: {dest}.\n"
        "Next: run_workload_translation() — ONLY the revised units re-run "
        "(persisted blobs of untouched units are reused), with your feedback "
        "in the prompt.")


async def skip_workload_units(unit_ids: List[str], reason: str,
                              ctx: Context = None) -> str:
    """Excludes units from translation during review, recording the reason.

    The escape hatch for a unit that persistently fails or that the
    developer decides not to migrate after seeing results: skipped units no
    longer block approval. Callable from STATE_WKLD_REVIEW and
    STATE_WKLD_TRANSLATE (a unit failing every run must not wedge the
    phase). No DAG transition.

    Args:
        unit_ids: the units to exclude.
        reason: why (recorded in the unit notes).
    """
    logger.info(f"skip_workload_units called for {unit_ids}")
    if not unit_ids or not reason.strip():
        return "ERROR: Both unit_ids and reason are required."
    try:
        state_dict, generation, config = authorize_and_rehydrate_workload(None)
    except Exception as e:
        return f"ERROR: {e}"
    current = state_dict["current_state"]
    if current not in (REVIEW_STATE, "STATE_WKLD_TRANSLATE"):
        return f"ERROR: Invalid state for skip_workload_units: {current}"
    plan = state_dict["variables"].get("workload_plan")
    if not plan:
        return "ERROR: No workload plan found."
    known = {u["unit_id"] for u in plan["units"]}
    unknown = [u for u in unit_ids if u not in known]
    if unknown:
        return (f"ERROR: Unknown unit_ids: {', '.join(unknown)}. "
                f"Known: {', '.join(sorted(known))}")

    plan, notes = planner.set_unit_status(plan, unit_ids, "skipped")
    for unit in plan["units"]:
        if unit["unit_id"] in set(unit_ids):
            unit["notes"] = list(unit.get("notes", [])) \
                + [f"Skipped in review: {reason}"]
    state_dict["variables"]["workload_plan"] = plan
    state_dict["history"].append(
        f"Units skipped in review ({reason}): {'; '.join(notes)}")

    component = config["component"]
    bucket = state_mgr.gcs_client.bucket(get_bucket_name(config["ledger_uri"]))
    error = _write_state(bucket, component, state_dict, generation, config)
    if error:
        return error
    error = _write_plan_blob(bucket, component, plan, config)
    if error:
        return error
    remaining = [u["unit_id"] for u in plan["units"]
                 if u["status"] not in ("done", "skipped", "parked")]
    output = f"SUCCESS: Skipped {', '.join(unit_ids)}. Still in {current}.\n"
    output += (
        f"Units still blocking approval: {', '.join(remaining)}."
        if remaining else
        "All remaining active units are done — "
        "approve_workload_translation(action='approve') is now available.")
    return output


async def approve_workload_translation(action: str = "approve",
                                       feedback: str = "",
                                       ctx: Context = None) -> str:
    """Approves the unit results toward validation, retranslates the lot, or
    re-plans the component against the current exports.

    Args:
        action: 'approve' (Gate D analog — requires every unit that is not
            skipped or parked to be done, and at least one done unit) moves
            to STATE_WKLD_VALIDATE; 'retranslate' sends every active unit
            back to STATE_WKLD_TRANSLATE; 'replan' returns to
            STATE_WKLD_PLAN so plan_workload_translation can rebuild the
            briefs AND the exports_stamp from the CURRENT exports.json —
            the only way to clear a validate staleness finding, since
            retranslating re-stamps every blob with the plan's frozen
            generations and would keep failing the cross-check.
        feedback: optional overall feedback applied when retranslating.
    """
    logger.info(f"approve_workload_translation called with action: {action}")
    if action not in ("approve", "retranslate", "replan"):
        return (f"ERROR: Invalid action '{action}'. Use 'approve', "
                "'retranslate' or 'replan' (or "
                "request_workload_unit_revision / skip_workload_units for "
                "specific units).")
    try:
        state_dict, generation, config = authorize_and_rehydrate_workload(None)
    except Exception as e:
        return f"ERROR: {e}"
    if state_dict["current_state"] != REVIEW_STATE:
        return (f"ERROR: Invalid state for approve_workload_translation: "
                f"{state_dict['current_state']}")
    plan = state_dict["variables"].get("workload_plan")
    if not plan:
        return "ERROR: No workload plan found."

    component = config["component"]
    bucket = state_mgr.gcs_client.bucket(get_bucket_name(config["ledger_uri"]))
    try:
        dag = load_dag(bucket, f"workloads/{component}/dag.json")
    except Exception as e:
        return f"ERROR: {e}"
    state_def = dag["states"][REVIEW_STATE]
    transition_key = {"approve": "on_approve", "retranslate": "on_retranslate",
                      "replan": "on_replan"}[action]
    if transition_key not in state_def.get("transitions", {}):
        return (f"ERROR: Invalid action '{action}' for state {REVIEW_STATE} "
                "in this component's graph copy. Re-join to upgrade it.")

    if action == "replan":
        pass
    elif action == "approve":
        not_done = [u["unit_id"] for u in plan["units"]
                    if u["status"] not in ("done", "skipped", "parked")]
        if not_done:
            return (
                f"ERROR: Cannot approve — units not done: "
                f"{', '.join(not_done)}. Send them back with "
                "request_workload_unit_revision, or exclude them with "
                "skip_workload_units if they should not block approval.")
        if not any(u["status"] == "done" for u in plan["units"]):
            return ("ERROR: Cannot approve — no unit is done, so validation "
                    "would have nothing to gate. Run "
                    "run_workload_translation first.")
    else:
        active = [u["unit_id"] for u in plan["units"]
                  if u["status"] not in ("skipped", "parked")]
        if not active:
            return ("ERROR: Cannot retranslate — every unit is skipped or "
                    "parked, so a re-run would have nothing to do.")
        plan, _ = planner.set_unit_status(plan, active, "revise",
                                          feedback=feedback or None)
        state_dict["variables"]["workload_plan"] = plan

    dest = state_def["transitions"][transition_key]
    state_dict["history"].append(
        f"Transitioned {REVIEW_STATE} -> {dest} via "
        f"approve_workload_translation({action})")
    state_dict["current_state"] = dest
    error = _write_state(bucket, component, state_dict, generation, config)
    if error:
        return error
    if action == "replan":
        return (
            f"Current State: {dest}\nRe-planning: call "
            "plan_workload_translation() to rebuild the units and the "
            "exports_stamp from the CURRENT exports.json. Blobs stamped "
            "against the old generations are refused by reuse conjunct (d), "
            "so every affected unit re-translates against the fresh facts.")
    if action != "approve":
        error = _write_plan_blob(bucket, component, plan, config)
        if error:
            return error
        return (f"Current State: {dest}\nAll active units marked for "
                "retranslation. Call run_workload_translation().")
    return (
        f"Current State: {dest}\nSUCCESS: Unit review approved.\n"
        "Next: call run_workload_validation() — the materialized output is "
        "manifest-gated (structural + re-render, nothing else), then the "
        "ship elicitation and the per-component PR follow in the same call.")


def register(mcp):
    """Register this step's tools on the FastMCP server."""
    mcp.tool()(get_workload_results)
    mcp.tool()(request_workload_unit_revision)
    mcp.tool()(skip_workload_units)
    mcp.tool()(approve_workload_translation)
