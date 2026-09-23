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

"""MCP tools for landing zone step 3 (translation plan): decompose the inventory into units.

Owns the STATE_LZ_TRANSLATION_PLAN agent task — the landing zone's closing
step: with the target shape decided and its Terraform PR'd, the phase ends by
agreeing WHAT the translation phase must generate. Decomposition is
deterministic (planner.py); no LLM is involved until the translate step.
"""

import json
import logging

from google.api_core import exceptions
from mcp.server.fastmcp import Context

from servers.dag.state_management import (
    get_bucket_name,
    load_dag,
    authorize_and_rehydrate,
)
import servers.dag.state_management as state_mgr

from . import coverage
from . import planner

logger = logging.getLogger("migration-dag")

PLAN_BLOB = "platform/translation/plan.json"


async def plan_translation(ctx: Context = None) -> str:
    """Decomposes the approved discovery inventory into translation units.

    Each unit is a bounded, independently-translatable problem (node pool,
    autoscaling strategy, workload policy, network, gateway, storage,
    tenancy, workload identity, cluster DNS) carrying its inventory slice and the
    relevant landing-zone decisions. The plan also carries the artifact
    coverage map instantiated against the inventory (v1, section
    granularity) when instantiation succeeds — a coverage defect logs and
    planning continues without the `coverage` key: each unit cites the map
    rows it covers, and any omission/overlap/traceability findings ride
    along as WARNING-grade review material here (the omission half is
    re-checked as an ENFORCED gate at the validate step). Advances the DAG
    to STATE_LZ_TRANSLATION_PLAN_REVIEW where the client reviews and signs
    off on the plan.
    """
    logger.info("plan_translation called.")
    try:
        state_dict, generation, config = authorize_and_rehydrate(None)
    except Exception as e:
        return f"ERROR: {e}"

    if state_dict["current_state"] != "STATE_LZ_TRANSLATION_PLAN":
        return f"ERROR: Invalid state for plan_translation: {state_dict['current_state']}"

    inventory = state_dict["variables"].get("discovery_inventory")
    if not inventory:
        return "ERROR: No discovery inventory found in the ledger. Complete discovery before planning translation."

    # The landing-zone step resolves the four target-shape decisions
    # (resolve_lz_decision) and finalize_landing_zone_design refuses to
    # complete without all of them, so by this state the map is complete.
    decisions = state_dict["variables"].get("lz_decisions", {})
    # The ledger's recorded target project ("The GCP Project ID hosting
    # target GKE fleets") rides into the workload-identity unit's inputs:
    # the GSA email contract needs a literal project id, the worker payload
    # otherwise carries none, and the translate rules forbid inventing one.
    plan = planner.build_translation_plan(
        inventory, decisions, target_project=config.get("gcp_project"))
    if planner.all_placeholders(plan):
        # Placeholder-only means the inventory itself holds no translatable
        # facts. Stop here (nothing is persisted) — the fix is rediscovery,
        # never unskipping empty placeholders. A plan whose real units were
        # skipped by a landing-zone decision (Autopilot node pools) is NOT
        # this case: it carries facts and proceeds to review, where the
        # operator can unskip units or decline the sign-off to revisit the
        # design.
        return (
            "ERROR: Every unit family came back as a no-facts placeholder — the "
            "inventory holds nothing to translate, so there is nothing to plan. Review "
            "the skip reasons below, then fix discovery (rescan or amend the scope) and "
            "re-run plan_translation. Do not proceed by unskipping placeholders.\n"
            + planner.summarize_plan(plan)
        )

    # Instantiate the artifact coverage map against the inventory (v1, section
    # granularity) and check the plan's citations against it. Findings are
    # WARNING-grade material for plan review, never a refusal here — so a
    # defect in the coverage computation itself must not block planning
    # either. Enforcement happens at the validate step (coverage_gate), which
    # recomputes verdicts from the live map rather than trusting this attach.
    try:
        plan = coverage.attach_coverage(plan, inventory)
    except Exception as e:
        logger.error(f"Coverage instantiation failed; planning continues without it: {e!r}")

    bucket_name = get_bucket_name(config["ledger_uri"])
    bucket = state_mgr.gcs_client.bucket(bucket_name)
    try:
        platform_dag = load_dag(bucket, "platform_dag.json")
    except Exception as e:
        return f"ERROR: {e}"

    current_state = state_dict["current_state"]
    state_def = platform_dag["states"][current_state]
    if "on_tool_call_received" not in state_def.get("transitions", {}):
        return "ERROR: The platform DAG in this ledger predates the translation phase. Re-run bootstrapping to refresh it."
    next_state = state_def["transitions"]["on_tool_call_received"]

    try:
        bucket.blob(PLAN_BLOB).upload_from_string(
            json.dumps(plan, indent=2), content_type="application/json"
        )
    except Exception as e:
        logger.error(f"Failed to persist translation plan blob: {e}")

    state_dict["variables"]["translation_plan"] = plan
    state_dict["history"].append(f"Transitioned {current_state} -> {next_state} via plan_translation")
    state_dict["current_state"] = next_state

    data = json.dumps(state_dict, indent=2)
    try:
        blob_state = bucket.blob("platform/onboarding/state.json")
        blob_state.upload_from_string(data, content_type="application/json", if_generation_match=generation)
    except exceptions.PreconditionFailed:
        return "ERROR: Concurrent update conflict. Your changes were not saved."

    return (
        f"SUCCESS: Translation plan created ({len(plan['units'])} units). Current State: {next_state}.\n"
        f"{planner.summarize_plan(plan)}\n\n"
        "Next: review the plan with the user — update_translation_plan(skip=[...], unskip=[...]) "
        "to adjust, then confirm_translation_plan() to start translating."
    )


def register(mcp):
    """Register this step's tools on the FastMCP server."""
    mcp.tool()(plan_translation)
