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

"""MCP tool for workload planning: build the component's translation plan.

Owns the STATE_WKLD_PLAN agent task. The pure core (planner.py) reads the
scoped files of the developer's LOCAL clone — the component manifests are
the fact source; this module never reads platform/* and
consumes exports.json read-only (absent -> the plan degrades honestly).
Claimant-enforced via authorize_and_rehydrate_workload.
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
    authorize_and_rehydrate_workload,
)
import servers.dag.state_management as state_mgr
from servers.dag.server import exports as exports_lib

from . import planner

logger = logging.getLogger("migration-dag")

PLAN_STATE = "STATE_WKLD_PLAN"


def _load_scope(bucket, component):
    """(scope doc, error). scope.json is the planning precondition: absent
    means the scope step never completed for this component."""
    blob = bucket.blob(f"workloads/{component}/scope.json")
    try:
        return json.loads(blob.download_as_text()), None
    except exceptions.NotFound:
        return None, (
            f"ERROR: workloads/{component}/scope.json is absent — the "
            "component has no confirmed scope. Complete the scope step "
            "(STATE_WKLD_SCOPE: update_workload_scope + "
            "submit_workload_scope + confirm_workload_scope) before "
            "planning; if this state was reached by a graph upgrade, re-run "
            "the scope flow from a fresh join.")


def _resolve_source_root(source_root, variables):
    """(root, error). The scope step records no checkout-location variable, so the
    explicit argument is authoritative; a root persisted by a previous
    plan call is the fallback for re-planning."""
    root = source_root or variables.get("workload_source_root")
    if not root:
        return None, (
            "ERROR: pass source_root=<path to your local source checkout>. "
            "The planner reads the component's scoped files from YOUR clone "
            "(the component manifests are the fact source); the ledger does "
            "not record its location.")
    root = os.path.abspath(os.path.expanduser(root))
    if not os.path.isdir(root):
        return None, f"ERROR: source_root '{root}' is not a directory."
    return root, None


async def plan_workload_translation(
    source_root: Optional[str] = None, ctx: Context = None,
) -> str:
    """Builds the component's translation plan and submits it for review.

    Args:
        source_root: path to your local source checkout (the clone whose
            files the confirmed scope selects). Required on the first call;
            later calls may omit it to reuse the recorded one.

    Reads the confirmed scope + exports.json (read-only; absent -> the plan
    is stamped with nulls, degraded but honest), runs the pure planner, and
    persists the plan to workloads/<component>/plan.json and the component
    variables. A plan that classified NO document is refused with the advice
    its cause calls for (fix the render, or amend the scope).
    """
    logger.info(f"plan_workload_translation called. source_root={source_root}")
    try:
        state_dict, generation, config = authorize_and_rehydrate_workload(None)
    except Exception as e:
        return f"ERROR: {e}"

    if state_dict["current_state"] != PLAN_STATE:
        return (f"ERROR: Invalid state for plan_workload_translation: "
                f"{state_dict['current_state']}")

    variables = state_dict["variables"]
    component = config["component"]  # the authorized component, always
    bucket = state_mgr.gcs_client.bucket(get_bucket_name(config["ledger_uri"]))

    scope_doc, scope_error = _load_scope(bucket, component)
    if scope_error:
        return scope_error
    root, root_error = _resolve_source_root(source_root, variables)
    if root_error:
        return root_error

    # A 403 here is an IAM defect (the conditional developer read grant on
    # exports.json is missing), not "exports not published": name the remedy
    # instead of raising a traceback into the MCP loop.
    try:
        exports_doc, _ = exports_lib.load_exports(bucket)
    except exceptions.Forbidden:
        return ("ERROR: this session was denied reading exports.json (403). "
                "The ledger is missing the conditional exports read grant — "
                "ask an admin to re-run provision_ledger_iam (registering "
                "any member re-runs it). Nothing is persisted; the "
                "component stays at PLAN.")
    # The planner degrades in-band by design, but it walks a developer's
    # local filesystem: an unreadable directory or an exotic OS error is not
    # a modelled degradation. Surfacing it as an ERROR string keeps the tool
    # contract (never raise into the MCP loop) and leaves the component at
    # PLAN, where re-planning is the documented recovery.
    try:
        plan = planner.build_workload_plan(
            component, scope_doc, root, exports_doc)
    except Exception as e:
        logger.exception("build_workload_plan failed")
        return (f"ERROR: the planner could not read the scoped files under "
                f"'{root}': {type(e).__name__}: {e}. Nothing is persisted; "
                "the component stays at STATE_WKLD_PLAN. Check the path and "
                "its permissions, then call plan_workload_translation again.")

    # Keyed on documents CLASSIFIED, not on the placeholder flags: a plan
    # whose only source failed to render has real units carrying zero
    # documents, and advancing that to review would review nothing.
    empty_reason = planner.empty_plan_reason(plan)
    if empty_reason:
        return (
            f"ERROR: {empty_reason} Nothing is persisted. Check source_root "
            f"points at the right clone ('{root}'), or amend the scope "
            "(reject at the next gate or re-join and redo the scope step) "
            "and plan again.\n" + planner.summarize_plan(plan))
    return await _persist_plan(bucket, component, state_dict, generation,
                               variables, plan, root, config)


async def _persist_plan(bucket, component, state_dict, generation, variables,
                        plan, root, config):
    """plan.json first, then the state transition — a failure between the
    two leaves the component still at PLAN, and re-planning overwrites."""
    plan_blob = bucket.blob(f"workloads/{component}/plan.json")
    try:
        plan_blob.reload()
        plan_generation = plan_blob.generation
    except exceptions.NotFound:
        plan_generation = 0
    try:
        plan_blob.upload_from_string(
            json.dumps(plan, indent=2), content_type="application/json",
            if_generation_match=plan_generation)
    except exceptions.PreconditionFailed:
        return ("ERROR: Concurrent update conflict writing plan.json. "
                "Please call plan_workload_translation again.")
    except exceptions.Forbidden:
        return state_mgr.workload_write_denied(config, component)

    try:
        dag = load_dag(bucket, f"workloads/{component}/dag.json")
    except Exception as e:
        return f"ERROR: {e}"
    dest = dag["states"][PLAN_STATE]["transitions"]["on_tool_call_received"]

    variables["workload_plan"] = plan
    variables["workload_source_root"] = root
    statuses = ", ".join(f"{u['unit_id']}={u['status']}"
                         for u in plan["units"])
    state_dict["history"].append(f"Workload plan built ({statuses})")
    state_dict["history"].append(
        f"Transitioned {PLAN_STATE} -> {dest} via plan_workload_translation")
    state_dict["current_state"] = dest
    try:
        bucket.blob(f"workloads/{component}/state.json").upload_from_string(
            json.dumps(state_dict, indent=2), content_type="application/json",
            if_generation_match=generation)
    except exceptions.PreconditionFailed:
        return ("ERROR: Concurrent update conflict recording the transition. "
                "plan.json was written; call plan_workload_translation again.")
    except exceptions.Forbidden:
        return (state_mgr.workload_write_denied(config, component)
                + "\n(plan.json was written; once the grant lands, call "
                "plan_workload_translation again to record the transition.)")

    return (
        f"SUCCESS: Workload plan built for component '{component}' and "
        f"persisted to workloads/{component}/plan.json.\n"
        f"{planner.summarize_plan(plan)}\n"
        f"Current State: {dest}.\n"
        "Next: walk the user through the unit summary (planned, parked AND "
        "placeholder units), then adjust with update_workload_plan or "
        "confirm with confirm_workload_plan — the elicitation is the "
        "decision."
    )


def register(mcp):
    """Register this step's tools on the FastMCP server."""
    mcp.tool()(plan_workload_translation)
