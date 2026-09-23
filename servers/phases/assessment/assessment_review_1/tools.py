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

"""MCP tools for assessment step 1: one review from extraction to blockers.

Owns the STATE_ASSESSMENT agent task — the single human review of what
discovery produced. Extraction already generated the inventory and the
readiness report; here the agent presents both, agrees the blocker list with
the user, and submits. The server then asks the user directly (an
elicitation): accepting records the blockers and moves on — blocker
resolution when any were found, landing zone design otherwise — and declining
returns the DAG to STATE_DISCOVERY for a fresh scan. Scope corrections loop
back to extraction without a decision.
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
from pydantic import BaseModel, Field

from servers.dag.dispatch import run_elicitation
from servers.dag.server.blocker_criteria import (
    load_blocker_categories,
    normalize_category,
    display_category,
)

# The scope algebra is discovery's; this step only re-applies it during review.
from servers.phases.discovery.discovery_scope_2 import scope as scope_lib

logger = logging.getLogger("migration-dag")


class AssessmentReviewSchema(BaseModel):
    approved: bool = Field(
        description="Accept the discovery inventory, readiness report, and agreed "
                    "blocker list? Accepting records the blockers and moves on — "
                    "blocker resolution if any were recorded, landing zone design "
                    "otherwise. Declining re-runs the discovery scan."
    )

# Fields the agent supplies. owner/target_close_date are deliberately absent:
# they are set by a human in the next state, through assign_blocker_owner.
_REQUIRED_BLOCKER_FIELDS = ("id", "title", "category", "rationale", "resolution_path")


def validate_blockers(blockers) -> list:
    """Checks a submitted blocker list. Returns a list of human-readable errors.

    The category check is what ties this to the knowledge base: the taxonomy comes
    from the Step 4 table the agent was handed, so the server and the agent are
    reading the same document. The resolution-path check mirrors that document's
    own Validation section — "Every blocker has a resolution path. None is left as
    'TBD'."
    """
    if not isinstance(blockers, list):
        return ["blockers must be a list"]

    try:
        known = load_blocker_categories()
    except Exception as e:
        return [f"cannot load the blocker taxonomy: {e}"]

    # Compare categories by meaning, not markup: the Step 4 table carries markdown
    # backticks and free spacing that a submitted category need not reproduce.
    known_normalized = {normalize_category(c) for c in known}
    known_display = sorted(display_category(c) for c in known)

    errors = []
    seen = set()

    for i, blocker in enumerate(blockers):
        where = f"blocker[{i}]"
        if not isinstance(blocker, dict):
            errors.append(f"{where}: must be an object")
            continue

        for field in _REQUIRED_BLOCKER_FIELDS:
            value = blocker.get(field)
            if not isinstance(value, str) or not value.strip():
                errors.append(f"{where}: '{field}' is required and must be a non-empty string")

        blocker_id = blocker.get("id")
        if isinstance(blocker_id, str) and blocker_id:
            if blocker_id in seen:
                errors.append(f"{where}: duplicate blocker id '{blocker_id}'")
            seen.add(blocker_id)

        category = blocker.get("category")
        if isinstance(category, str) and category.strip() and normalize_category(category) not in known_normalized:
            errors.append(
                f"{where}: category '{category}' is not in the Step 4 blocker table of the "
                f"assessment knowledge document. Use one of: {'; '.join(known_display)}"
            )

        resolution = blocker.get("resolution_path")
        if isinstance(resolution, str) and resolution.strip().upper().startswith("TBD"):
            errors.append(
                f"{where}: resolution_path is 'TBD'. Every blocker needs a real resolution "
                "path — see the Validation section of the assessment knowledge document."
            )

        workloads = blocker.get("affected_workloads", [])
        if workloads is not None and not isinstance(workloads, list):
            errors.append(f"{where}: 'affected_workloads' must be a list when present")

    return errors


def normalize_blockers(blockers: list) -> list:
    """Returns the stored form of each blocker, with the assignment fields empty."""
    normalized = []
    for blocker in blockers:
        entry = {
            "id": blocker["id"],
            "title": blocker["title"],
            "category": blocker["category"],
            "affected_workloads": blocker.get("affected_workloads") or [],
            "rationale": blocker["rationale"],
            "resolution_path": blocker["resolution_path"],
            "owner": None,
            "target_close_date": None,
        }
        normalized.append(entry)
    return normalized


async def get_discovery_inventory(include_inventory: bool = True, ctx: Context = None) -> str:
    """Retrieves the discovered EKS inventory (and readiness report, if any) from the state ledger.

    Args:
        include_inventory: pass False to fetch only the readiness report —
            re-reading the report should not drag the full inventory JSON
            back into context.
    """
    logger.info("get_discovery_inventory called.")
    try:
        state_dict, _, config = authorize_and_rehydrate(None)
    except Exception as e:
        return f"ERROR: {e}"

    output = f"Current State: {state_dict['current_state']}\n"
    if include_inventory:
        inventory = state_dict["variables"].get("discovery_inventory")
        inventory_str = json.dumps(inventory, indent=2) if inventory is not None else "{}"
        output += f"Inventory:\n{inventory_str}"

    report_blob_path = state_dict["variables"].get("discovery_report_blob")
    if report_blob_path:
        bucket_name = get_bucket_name(config["ledger_uri"])
        bucket = state_mgr.gcs_client.bucket(bucket_name)
        try:
            report = bucket.blob(report_blob_path).download_as_text()
            output += f"\n\nReadiness report:\n{report}"
        except Exception as e:
            output += f"\n\n(Readiness report at {report_blob_path} could not be read: {e})"

    return output


async def amend_discovery_scope(
    include: list = None,
    exclude: list = None,
    ctx: Context = None,
) -> str:
    """Amends the discovery scope during review and re-runs extraction on the delta.

    Use when the user spots something missing (include: extra files or
    directories) or something that should not have been analyzed (exclude)
    while reviewing the inventory. Transitions back to STATE_DISCOVERY_DATA_SCAN;
    cached fragments make the re-run cheap — only new or changed chunks are
    extracted.

    Args:
        include: paths (absolute, or relative to the scoped root_dir) or
            previously excluded patterns to bring into scope.
        exclude: paths, directory prefixes, or globs to remove from scope.
    """
    logger.info(f"amend_discovery_scope called. include={include} exclude={exclude}")
    if not include and not exclude:
        return "ERROR: Nothing to amend — pass include and/or exclude."

    try:
        state_dict, generation, config = authorize_and_rehydrate(None)
    except Exception as e:
        return f"ERROR: {e}"

    current_state = state_dict["current_state"]
    if current_state != "STATE_ASSESSMENT":
        return f"ERROR: Invalid state for amend_discovery_scope: {current_state}"

    current_scope = state_dict["variables"].get("discovery_scope")
    if not current_scope:
        return (
            "ERROR: No discovery scope found, so there is nothing to amend. Submit "
            "the assessment to proceed, or re-run discovery from the start to rebuild "
            "the scope."
        )

    bucket_name = get_bucket_name(config["ledger_uri"])
    bucket = state_mgr.gcs_client.bucket(bucket_name)
    try:
        platform_dag = load_dag(bucket, "platform_dag.json")
    except Exception as e:
        return f"ERROR: {e}"

    state_def = platform_dag["states"][current_state]
    if "on_amend_scope" not in state_def.get("transitions", {}):
        return (
            "ERROR: The platform DAG in this ledger predates scope amendments, so "
            "amendment is unavailable here. Submit the assessment to proceed, or "
            "re-run discovery from the start on an up-to-date ledger."
        )

    new_scope, notes = scope_lib.apply_scope_update(current_scope, exclude, include)
    dest = state_def["transitions"]["on_amend_scope"]

    state_dict["variables"]["discovery_scope"] = new_scope
    state_dict["history"].append(f"Scope amended in review: {'; '.join(notes) or 'no changes'}")
    state_dict["history"].append(f"Transitioned {current_state} -> {dest} via amend_discovery_scope")
    state_dict["current_state"] = dest

    data = json.dumps(state_dict, indent=2)
    try:
        blob_state = bucket.blob("platform/onboarding/state.json")
        blob_state.upload_from_string(data, content_type="application/json", if_generation_match=generation)
    except exceptions.PreconditionFailed:
        return "ERROR: Concurrent update conflict. Your changes were not saved."

    # Derived from the ledger's own graph: a workspace bootstrapped before v2.7
    # goes straight to extraction, and naming a tool its graph cannot reach
    # would stop the run.
    if dest == "STATE_DISCOVERY_DATA_SCAN":
        next_line = ("Next: call scan_data_dependencies() to re-record the data "
                     "services against the corrected scope. The corrections an "
                     "earlier review recorded are replayed over the new scan, "
                     "but the sign-off is not — the review runs again before "
                     "extraction")
    else:
        next_line = "Next: call run_discovery_extraction()"
    return (
        f"SUCCESS: Scope amended ({'; '.join(notes) or 'no changes'}). "
        f"Current State: {dest}.\n"
        + next_line
        + " — cached fragments are reused, so only the scope delta is extracted, "
          "then review resumes with the updated inventory."
    )


async def submit_assessment(blockers: list = None, ctx: Context = None) -> str:
    """Submits the reviewed assessment for the user's sign-off and advances the DAG.

    Call after the user has reviewed the inventory and readiness report and you
    have agreed the blocker list with them. The server then asks the user
    directly (an elicitation) — that answer, not this call, is the decision.
    Accepting records the blockers and moves on: blocker resolution when any
    were found, landing zone design otherwise. Declining returns the DAG to
    STATE_DISCOVERY for a fresh scan. Do not call it again to act on the
    user's answer.

    Args:
        blockers: Structured blockers agreed during the review. Each needs id,
            title, category (verbatim from the Step 4 table of the assessment
            knowledge document), rationale, and resolution_path. Omit owner and
            target_close_date; a human assigns those in the next state.
    """
    logger.info("submit_assessment called.")
    try:
        state_dict, generation, config = authorize_and_rehydrate(None)
    except Exception as e:
        return f"ERROR: {e}"

    current_state = state_dict["current_state"]
    if current_state != "STATE_ASSESSMENT":
        return f"ERROR: Invalid state for submit_assessment: {current_state}"

    blockers = blockers or []
    errors = validate_blockers(blockers)
    if errors:
        return "ERROR: Blocker list rejected:\n- " + "\n- ".join(errors)

    bucket_name = get_bucket_name(config["ledger_uri"])
    bucket = state_mgr.gcs_client.bucket(bucket_name)
    try:
        platform_dag = load_dag(bucket, "platform_dag.json")
    except Exception as e:
        return f"ERROR: {e}"

    state_def = platform_dag["states"][current_state]
    for key in ("on_reject", "on_blockers_found", "on_no_blockers"):
        if key not in state_def.get("transitions", {}):
            return f"ERROR: The platform DAG in this ledger predates submit_assessment ({key} missing). Re-run bootstrapping to refresh it."

    # The decision is the user's, taken here and only here: one elicitation
    # covers what used to be the review step plus the assessment gate. The
    # prompt names what approval commits to — the validated blocker list and
    # the branch the graph will take (braces escaped: the template renders
    # with str.format, and a blocker id may contain them).
    summary = (
        f" This submission records {len(blockers)} blocker(s)"
        + (": " + ", ".join(b["id"] for b in blockers) if blockers else "")
        + "; approving proceeds to "
        + ("blocker resolution." if blockers else "landing zone design.")
    ).replace("{", "{{").replace("}", "}}")
    elicit_def = {**state_def, "prompt_template": state_def.get("prompt_template", "") + summary}
    try:
        approved, _ = await run_elicitation(
            ctx, current_state, elicit_def, AssessmentReviewSchema, state_dict, config
        )
    except Exception as e:
        return f"ERROR: Could not raise the approval elicitation (is this a live MCP session?): {e}"

    if not approved:
        dest = state_def["transitions"]["on_reject"]
        discarded = (
            f"; {len(blockers)} submitted blocker(s) not stored: "
            + ", ".join(b["id"] for b in blockers)
        ) if blockers else ""
        state_dict["history"].append(
            f"Assessment declined by the user{discarded}; re-running discovery"
        )
        state_dict["history"].append(f"Transitioned {current_state} -> {dest} via submit_assessment")
        state_dict["current_state"] = dest
        data = json.dumps(state_dict, indent=2)
        try:
            bucket.blob("platform/onboarding/state.json").upload_from_string(
                data, content_type="application/json", if_generation_match=generation
            )
        except exceptions.PreconditionFailed:
            return "ERROR: Concurrent update conflict. Your changes were not saved."
        return (
            f"Inventory not approved; discovery will be scanned again.\n"
            + (f"The {len(blockers)} submitted blocker(s) were NOT stored — re-propose "
               "them at the next review if they still apply.\n" if blockers else "")
            + f"Current State: {dest}"
        )

    # The transition is computed from the list, not chosen by the agent: whether
    # the migration owes anyone a blocker resolution is a fact about the review.
    stored = normalize_blockers(blockers)
    transition_key = "on_blockers_found" if stored else "on_no_blockers"
    dest = state_def["transitions"][transition_key]

    state_dict["variables"]["blockers"] = stored
    state_dict["history"].append(
        f"Assessment approved with {len(stored)} blocker(s) via submit_assessment"
    )
    state_dict["history"].append(f"Transitioned {current_state} -> {dest}")
    state_dict["current_state"] = dest

    data = json.dumps(state_dict, indent=2)
    try:
        bucket.blob("platform/onboarding/state.json").upload_from_string(
            data, content_type="application/json", if_generation_match=generation
        )
    except exceptions.PreconditionFailed:
        return "ERROR: Concurrent update conflict. Your changes were not saved."

    output = (
        f"Assessment approved. Blockers recorded: {len(stored)}\n"
        f"Current State: {state_dict['current_state']}\n"
    )
    if stored:
        output += (
            "All blockers need an owner and a target close date before landing zone "
            "design unlocks. Call list_blockers to see the checklist."
        )
    else:
        output += "No blockers found; proceeding straight to landing zone design."
    return output


def register(mcp):
    """Register this step's tools on the FastMCP server."""
    mcp.tool()(get_discovery_inventory)
    mcp.tool()(amend_discovery_scope)
    mcp.tool()(submit_assessment)
