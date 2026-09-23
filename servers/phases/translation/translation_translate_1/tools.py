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

"""MCP tools for translation step 1 (translate): one subagent per unit.

Owns the STATE_TRANSLATION_RUNNING agent task. run_translation fans the
confirmed plan's units out to translation workers; each produces Terraform
and/or Kubernetes manifests plus a tradeoffs write-up, persisted per unit to the ledger. Units already
'done' are not re-translated unless a reviewer marked them 'revise'.
"""

import asyncio
import json
import logging
import os
import time

from google.api_core import exceptions
from mcp.server.fastmcp import Context

from servers.dag.state_management import (
    get_bucket_name,
    load_dag,
    authorize_and_rehydrate,
)
import servers.dag.state_management as state_mgr

from . import translator

logger = logging.getLogger("migration-dag")

UNIT_BLOB_PREFIX = "platform/translation/units"
PLAN_BLOB = "platform/translation/plan.json"
# A fan-out can run for many minutes; the lease stops a second run_translation
# from double-translating while one is in flight, and expires so a crashed run
# never wedges the step.
LEASE_SECONDS = float(os.environ.get("GKE_AGENTIC_MIGRATION_TRANSLATE_LEASE", "3600"))
# Live-progress blob: written before the fan-out so a progress reader (the
# review UI, once it lands) can show units landing one by one instead of
# nothing until the whole (multi-minute) batch completes.
PROGRESS_BLOB = "platform/translation/translation-progress.json"


def _unit_blob_body(unit: dict, result, error, run: int) -> str:
    """Serializes one unit's ledger blob (the review UI's per-unit artifact).

    Carries the post-run status so a persisted blob IS the unit finishing, the
    reviewer feedback that shaped the attempt, and the run token so the progress
    endpoint counts only units produced by THIS run (a revised unit's stale blob
    from a prior run has an older token and correctly reads as not-yet-done).
    """
    if error is None:
        payload = {"unit": {**unit, "status": "done", "error": None},
                   "result": result, "run": run}
    else:
        payload = {"unit": {**unit, "status": "error", "error": error},
                   "result": None, "run": run}
    return json.dumps(payload, indent=2)


STATE_BLOB = "platform/onboarding/state.json"


async def run_translation(ctx: Context = None) -> str:
    """Translates every pending unit of the confirmed plan into code.

    One small-context subagent per unit produces the Terraform and/or
    Kubernetes manifest files, a tradeoffs
    write-up, assumptions, and open questions; results persist to the ledger
    at platform/translation/units/<unit_id>.json. Advances the DAG to
    STATE_TRANSLATION_REVIEW once at least one active unit is done; failed
    units stay marked 'error' for the reviewer to retry or skip.
    """
    logger.info("run_translation called.")

    auth_error = translator.check_llm_auth()
    if auth_error:
        return f"ERROR: {auth_error}"

    try:
        state_dict, generation, config = authorize_and_rehydrate(None)
    except Exception as e:
        return f"ERROR: {e}"

    if state_dict["current_state"] != "STATE_TRANSLATION_RUNNING":
        return f"ERROR: Invalid state for run_translation: {state_dict['current_state']}"

    plan = state_dict["variables"].get("translation_plan")
    if not plan or not plan.get("units"):
        return "ERROR: No translation plan found. Run plan_translation first."

    pending = [u for u in plan["units"] if u["status"] in ("planned", "revise", "error")]
    already_done = [u for u in plan["units"] if u["status"] == "done"]
    if not pending and not already_done:
        return "ERROR: No active units to translate (everything is skipped). Adjust the plan first."

    bucket_name = get_bucket_name(config["ledger_uri"])
    bucket = state_mgr.gcs_client.bucket(bucket_name)

    # Claim this run before any fan-out. Two run_translation calls can both
    # pass the RUNNING check above; without a claim both fan out and both
    # stamp unit blobs, clobbering artifacts a reviewer may already be
    # reading. Two protections layer here:
    # - the lease: a fresh translation_run_active marker means a fan-out is
    #   in flight (the state generation is otherwise stable for the whole
    #   multi-minute run), so a later call refuses instead of translating
    #   everything a second time. A crashed run's lease expires.
    # - the CAS write: two calls racing to claim at the same state generation
    #   still produce exactly one winner.
    # The claimed, persisted, monotonic counter (not a clock, PID, or the
    # best-effort progress blob) is the run token stamped into every unit +
    # progress blob, so it survives restarts and strictly increases across
    # runs — a revised unit's stale blob always carries an older token.
    lease = state_dict["variables"].get("translation_run_active")
    if isinstance(lease, dict):
        age = time.time() - float(lease.get("claimed_at") or 0)
        if 0 <= age < LEASE_SECONDS:
            return (
                f"ERROR: Translation run {lease.get('run')} appears to be in "
                f"flight (claimed {int(age)}s ago). Not starting a second "
                f"fan-out — wait for it to finish, or retry after the lease "
                f"expires ({int(LEASE_SECONDS)}s) if it crashed."
            )
    prev_run = int(state_dict["variables"].get("translation_run", 0))
    run_token = prev_run + 1
    state_dict["variables"]["translation_run"] = run_token
    state_dict["variables"]["translation_run_active"] = {
        "run": run_token, "claimed_at": time.time(),
    }
    try:
        bucket.blob(STATE_BLOB).upload_from_string(
            json.dumps(state_dict, indent=2),
            content_type="application/json",
            if_generation_match=generation,
        )
    except exceptions.PreconditionFailed:
        return (
            "ERROR: Another translation run is in progress, or the workspace "
            "changed since this run started. Not starting a second fan-out — "
            "re-run run_translation once the other run finishes."
        )
    except Exception as e:
        return f"ERROR: Failed to claim the translation run: {e}"

    # Publish the live-progress blob and persist each unit the moment its worker
    # finishes, so the review UI fills in during the fan-out (which runs for
    # minutes) instead of showing nothing until the whole batch completes. The
    # run token distinguishes this run's blobs from a revised unit's stale blob.
    try:
        bucket.blob(PROGRESS_BLOB).upload_from_string(
            json.dumps({
                "run": run_token,
                "total_units": len(pending) + len(already_done),
                "reused": len(already_done),
                "pending_ids": sorted(u["unit_id"] for u in pending),
            }, indent=2),
            content_type="application/json",
        )
    except Exception as e:
        logger.warning(f"Failed to persist translation progress: {e}")

    persisted = set()

    async def persist_unit(unit, result, error):
        # upload_from_string is blocking; off-load it so a worker finishing does
        # not stall the event loop and hold up every other unit landing live.
        unit_id = unit["unit_id"]
        blob = bucket.blob(f"{UNIT_BLOB_PREFIX}/{unit_id}.json")
        await asyncio.to_thread(
            blob.upload_from_string,
            _unit_blob_body(unit, result, error, run_token),
            content_type="application/json",
        )
        persisted.add(unit_id)

    outcome = {"results": {}, "errors": {}}
    if pending:
        outcome = await translator.translate_all(
            pending, plan.get("decisions"), on_result=persist_unit
        )

    # The fan-out can run for minutes. Re-read the ledger before writing the
    # plan and state so a run that lost the race (another session already
    # advanced the state, possibly through review and approval) discards its
    # results instead of applying them over artifacts a human has seen. The
    # per-unit blobs were already written incrementally above.
    try:
        state_dict, generation, config = authorize_and_rehydrate(None)
    except Exception as e:
        return f"ERROR: Translation finished but the ledger could not be re-read: {e}"

    if state_dict["current_state"] != "STATE_TRANSLATION_RUNNING":
        return (
            f"ERROR: Another session advanced the state to {state_dict['current_state']} "
            "while translation ran. This run's results were not applied to the plan or state."
        )
    if int(state_dict["variables"].get("translation_run", 0)) != run_token:
        return (
            f"ERROR: Translation run {state_dict['variables'].get('translation_run')} "
            "superseded this one while it ran (e.g. after a lease expiry). This run's "
            "results were not applied to the plan or state."
        )

    plan = state_dict["variables"].get("translation_plan") or plan

    errors = dict(outcome["errors"])
    for unit in plan["units"]:
        unit_id = unit["unit_id"]
        if unit_id in outcome["results"]:
            result = outcome["results"][unit_id]
            if unit_id not in persisted:
                # Belt-and-suspenders: the on_result callback already wrote this
                # blob the moment the worker finished; re-persist only if that
                # incremental write failed, so a persist error still marks the
                # unit and never silently loses a successful translation.
                try:
                    bucket.blob(f"{UNIT_BLOB_PREFIX}/{unit_id}.json").upload_from_string(
                        _unit_blob_body(unit, result, None, run_token),
                        content_type="application/json",
                    )
                except Exception as e:
                    errors[unit_id] = f"translated but failed to persist: {e}"
                    unit["status"] = "error"
                    unit["error"] = errors[unit_id]
                    continue
            unit["status"] = "done"
            unit["error"] = None
            unit["feedback"] = None
        elif unit_id in errors:
            unit["status"] = "error"
            unit["error"] = errors[unit_id]
            if unit_id not in persisted:
                # Same fallback for a failed unit's error blob, so it still
                # surfaces in the review UI (and counts as finished in progress).
                try:
                    bucket.blob(f"{UNIT_BLOB_PREFIX}/{unit_id}.json").upload_from_string(
                        _unit_blob_body(unit, None, errors[unit_id], run_token),
                        content_type="application/json",
                    )
                except Exception as e:
                    logger.error(f"Failed to persist error blob for {unit_id}: {e}")

    state_dict["variables"]["translation_plan"] = plan
    done = [u for u in plan["units"] if u["status"] == "done"]

    if not done:
        # Nothing reviewable: persist the per-unit error statuses for
        # visibility but stay in STATE_TRANSLATION_RUNNING so a retry is safe.
        state_dict["variables"].pop("translation_run_active", None)
        state_dict["history"].append("run_translation: no units completed; staying in STATE_TRANSLATION_RUNNING")
        data = json.dumps(state_dict, indent=2)
        try:
            bucket.blob("platform/onboarding/state.json").upload_from_string(
                data, content_type="application/json", if_generation_match=generation
            )
        except exceptions.PreconditionFailed:
            pass
        return (
            f"ERROR: No unit completed ({json.dumps(errors)[:500]}). "
            "State unchanged — fix the cause and re-run run_translation."
        )

    try:
        platform_dag = load_dag(bucket, "platform_dag.json")
    except Exception as e:
        return f"ERROR: {e}"

    current_state = state_dict["current_state"]
    state_def = platform_dag["states"][current_state]
    next_state = state_def["transitions"]["on_tool_call_received"]

    state_dict["variables"].pop("translation_run_active", None)
    state_dict["history"].append(f"Transitioned {current_state} -> {next_state} via run_translation")
    state_dict["current_state"] = next_state

    try:
        bucket.blob(PLAN_BLOB).upload_from_string(json.dumps(plan, indent=2), content_type="application/json")
    except Exception as e:
        logger.error(f"Failed to persist plan blob: {e}")

    data = json.dumps(state_dict, indent=2)
    try:
        blob_state = bucket.blob("platform/onboarding/state.json")
        blob_state.upload_from_string(data, content_type="application/json", if_generation_match=generation)
    except exceptions.PreconditionFailed:
        return "ERROR: Concurrent update conflict. Your changes were not saved."

    failed = [u for u in plan["units"] if u["status"] == "error"]
    skipped = [u for u in plan["units"] if u["status"] == "skipped"]
    summary = (
        f"SUCCESS: Translation run complete. Current State: {next_state}.\n"
        f"- Units: {len(done)} done, {len(failed)} failed, {len(skipped)} skipped\n"
        f"- The generated code is in the ledger under {UNIT_BLOB_PREFIX}/, not in this summary.\n"
    )
    if failed:
        summary += (
            f"- Failed: {', '.join(u['unit_id'] for u in failed)} — the reviewer can send these back "
            "with request_unit_revision, skip them with skip_translation_units, or re-run after fixing the cause.\n"
        )
    summary += (
        "Next: get_translation_results() for the per-unit status, assumptions, and open questions "
        "to discuss with the user — the human reads the actual code and tradeoffs from the unit blobs."
    )
    return summary


def register(mcp):
    """Register this step's tools on the FastMCP server."""
    mcp.tool()(run_translation)
