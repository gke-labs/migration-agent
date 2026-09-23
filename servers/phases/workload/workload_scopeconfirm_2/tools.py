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

"""MCP tools for workload step 2: the human sign-off on the component scope.

Owns the STATE_WKLD_SCOPE_CONFIRM agent task. The approval is an in-call
elicitation over the state's prompt_template (the submit_assessment pattern):
the user's answer IS the decision. Approve persists
workloads/<component>/scope.json and follows the graph forward (v0.2: to
STATE_WKLD_PLAN); decline follows on_reject back to scope selection and
persists nothing.

Also the EARLY half of the data-dependency gate (`workload/datagate`). The
AWS data services this component uses are named in the elicitation notice,
before the developer commits to the scope — and the answer is allowed to be
"proceed anyway", because everything between here and the pull request stays
correct while a database is still moving. The refusal is at the ship gate in
workload_validate_5.
"""

import json
import logging
from datetime import datetime, timezone

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
from servers.dag.server import exports as exports_lib

from .. import datagate

logger = logging.getLogger("migration-dag")

CONFIRM_STATE = "STATE_WKLD_SCOPE_CONFIRM"


def _data_advisory(bucket, resolution: dict) -> str:
    """The data-dependency notice for the sign-off elicitation, or "".

    Best-effort by construction: this is the WARN half of the gate, and a
    developer must not be stopped from confirming a scope because a
    read failed. Every failure mode — 403, absent object, malformed
    document — degrades to no notice, and the ship gate is where an
    unreadable exports.json becomes a refusal.
    """
    try:
        exports_doc, _ = exports_lib.load_exports(bucket)
    except Exception as e:
        logger.warning(f"data advisory: exports.json unreadable ({e})")
        return ""
    try:
        result = datagate.verdict(exports_doc,
                                  resolution.get("resolved_paths"),
                                  (exports_doc or {}).get("component_seed_index"))
        return datagate.advisory(result)
    except Exception as e:
        logger.error(f"data advisory: could not evaluate the data gate ({e})")
        return ""


class WorkloadScopeApprovalSchema(BaseModel):
    approved: bool = Field(
        description="Approve the proposed component file scope? Approving "
                    "persists it to the ledger; declining returns to scope "
                    "selection."
    )


async def confirm_workload_scope(ctx: Context = None) -> str:
    """Raises the scope sign-off elicitation and acts on the user's answer.

    Call after summarizing the proposal. The elicitation is the decision —
    do not ask in chat first and do not call again to act on the answer.
    Approve -> scope.json persisted, graph follows on_tool_call_received.
    Decline -> back to STATE_WKLD_SCOPE; nothing is persisted.
    """
    logger.info("confirm_workload_scope called.")
    try:
        state_dict, generation, config = authorize_and_rehydrate_workload(None)
    except Exception as e:
        return f"ERROR: {e}"

    current = state_dict["current_state"]
    if current != CONFIRM_STATE:
        return f"ERROR: Invalid state for confirm_workload_scope: {current}"

    variables = state_dict["variables"]
    # The component authorize_and_rehydrate_workload authorized — never the
    # raw ledger variables, so the claimant check and every write below
    # cannot name different components.
    component = config["component"]
    bucket = state_mgr.gcs_client.bucket(get_bucket_name(config["ledger_uri"]))
    try:
        dag = load_dag(bucket, f"workloads/{component}/dag.json")
    except Exception as e:
        return f"ERROR: {e}"
    state_def = dag["states"][current]

    # Computed from the DRAFT resolution, before the answer: the point of
    # warning early is that the developer reads it while deciding.
    advisory = _data_advisory(bucket,
                              variables.get("workload_scope_resolution") or {})

    try:
        approved, _ = await run_elicitation(
            ctx, current, state_def, WorkloadScopeApprovalSchema, state_dict,
            config, notice=advisory or None)
    except Exception as e:
        return f"ERROR: Could not raise the approval elicitation (is this a live MCP session?): {e}"

    state_blob = bucket.blob(f"workloads/{component}/state.json")

    if not approved:
        dest = state_def["transitions"]["on_reject"]
        state_dict["history"].append(
            "Scope proposal declined by the user; returning to scope selection")
        state_dict["history"].append(
            f"Transitioned {current} -> {dest} via confirm_workload_scope")
        state_dict["current_state"] = dest
        try:
            state_blob.upload_from_string(
                json.dumps(state_dict, indent=2), content_type="application/json",
                if_generation_match=generation)
        except exceptions.PreconditionFailed:
            return "ERROR: Concurrent update conflict. Your changes were not saved."
        except exceptions.Forbidden:
            return state_mgr.workload_write_denied(config, component)
        return (
            f"Scope not approved; nothing was persisted. Current State: {dest}.\n"
            "Adjust the proposal with update_workload_scope before submitting again."
        )

    resolution = variables.get("workload_scope_resolution") or {}
    draft = variables.get("workload_scope") or {}
    scope_doc = {
        "component": component,
        "included": draft.get("included") or [],
        "excluded": draft.get("excluded") or [],
        "resolved_paths": resolution.get("resolved_paths"),
        "degraded": bool(resolution.get("degraded")),
        "exports_generated_at": resolution.get("exports_generated_at"),
        "exports_generations": resolution.get("exports_generations"),
        "confirmed_by": state_mgr.get_authenticated_user_email(),
        "confirmed_at": datetime.now(timezone.utc).isoformat(),
    }
    scope_blob = bucket.blob(f"workloads/{component}/scope.json")
    try:
        scope_blob.reload()
        scope_generation = scope_blob.generation
    except exceptions.NotFound:
        scope_generation = 0
    try:
        scope_blob.upload_from_string(
            json.dumps(scope_doc, indent=2), content_type="application/json",
            if_generation_match=scope_generation)
    except exceptions.PreconditionFailed:
        return ("ERROR: Concurrent update conflict writing scope.json. "
                "Please call confirm_workload_scope again.")
    except exceptions.Forbidden:
        return state_mgr.workload_write_denied(config, component)

    dest = state_def["transitions"]["on_tool_call_received"]
    state_dict["history"].append(
        f"Scope confirmed by {scope_doc['confirmed_by']} "
        f"({'degraded' if scope_doc['degraded'] else str(len(scope_doc['resolved_paths'] or [])) + ' file(s)'})")
    if advisory:
        state_dict["history"].append(
            "Scope confirmed over an open data-dependency advisory "
            f"({advisory.splitlines()[0]})")
    state_dict["history"].append(
        f"Transitioned {current} -> {dest} via confirm_workload_scope")
    state_dict["current_state"] = dest
    try:
        state_blob.upload_from_string(
            json.dumps(state_dict, indent=2), content_type="application/json",
            if_generation_match=generation)
    except exceptions.PreconditionFailed:
        return ("ERROR: Concurrent update conflict recording the transition. "
                "scope.json was written; call confirm_workload_scope again.")
    except exceptions.Forbidden:
        return (state_mgr.workload_write_denied(config, component)
                + "\n(scope.json was written; once the grant lands, call "
                "confirm_workload_scope again to record the transition.)")

    detail = ("degraded=true, resolved_paths=null"
              if scope_doc["degraded"]
              else f"{len(scope_doc['resolved_paths'] or [])} resolved file(s)")
    return (
        f"SUCCESS: Scope for component '{component}' confirmed and persisted "
        f"to workloads/{component}/scope.json ({detail}).\n"
        + (f"{advisory}\n" if advisory else "")
        + f"Current State: {dest}.\n"
        "Call get_next_stage for the current step's instructions."
    )


def register(mcp):
    """Register this step's tools on the FastMCP server."""
    mcp.tool()(confirm_workload_scope)
