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

"""MCP tools for assessment step 2: giving every blocker an owner and a date.

Owns the STATE_BLOCKER_RESOLUTION agent task. This is the gate the scope document
asks for — landing zone design does not unlock until every blocker in the
readiness report has both an owner and a target close date. The check lives here
rather than in the agent's instructions because an instruction is a request and a
transition guard is not.

See instructions.md in this folder for the agent-facing playbook.
"""

import datetime
import json
import logging

import yaml
from google.api_core import exceptions
from mcp.server.fastmcp import Context

from servers.dag.state_management import (
    get_bucket_name,
    load_dag,
    authorize_and_rehydrate,
)
import servers.dag.state_management as state_mgr
from servers.dag.dispatch import run_dispatch_loop

logger = logging.getLogger("migration-dag")

STATE = "STATE_BLOCKER_RESOLUTION"


def is_outstanding(blocker: dict) -> bool:
    """A blocker is outstanding until it has both an owner and a target close date."""
    return not (blocker.get("owner") and blocker.get("target_close_date"))


def parse_close_date(value) -> tuple[str, str]:
    """Validates a target close date. Returns (iso_date, error); one is always empty.

    Past dates are refused. A date that has already gone by is either a typo or a
    relative date resolved against the wrong day, and both produce a blocker that
    reads as owned but is not actually scheduled.
    """
    if not isinstance(value, str) or not value.strip():
        return "", "target_close_date is required, as YYYY-MM-DD."
    try:
        parsed = datetime.date.fromisoformat(value.strip())
    except ValueError:
        return "", f"target_close_date '{value}' is not a valid YYYY-MM-DD date."
    if parsed < datetime.date.today():
        return "", (
            f"target_close_date '{parsed.isoformat()}' is in the past. If you resolved a "
            "relative date such as 'this Friday', re-resolve it against today."
        )
    return parsed.isoformat(), ""


def registered_emails(bucket) -> set:
    """Every email named anywhere in the workspace registry's role lists."""
    try:
        registry = yaml.safe_load(bucket.blob("workspace_registry.yaml").download_as_text()) or {}
    except exceptions.NotFound:
        raise RuntimeError("workspace registry not found in the ledger bucket")
    emails = set()
    for members in (registry.get("roles") or {}).values():
        for email in members or []:
            emails.add(email)
    return emails


def format_checklist(blockers: list) -> str:
    """Renders the blocker checklist, outstanding entries first."""
    if not blockers:
        return "No blockers were recorded in the readiness report."

    outstanding = [b for b in blockers if is_outstanding(b)]
    resolved = [b for b in blockers if not is_outstanding(b)]

    lines = [f"{len(outstanding)} of {len(blockers)} blocker(s) still need an owner and a date.", ""]
    for blocker in outstanding:
        lines.append(f"[ ] {blocker['id']} — {blocker['title']}")
        lines.append(f"      category: {blocker['category']}")
        if blocker.get("affected_workloads"):
            lines.append(f"      affects: {', '.join(blocker['affected_workloads'])}")
        lines.append(f"      resolution: {blocker['resolution_path']}")
        if blocker.get("owner"):
            lines.append(f"      owner: {blocker['owner']} (no target close date yet)")
        elif blocker.get("target_close_date"):
            lines.append(f"      target close date: {blocker['target_close_date']} (no owner yet)")
        lines.append("")
    for blocker in resolved:
        lines.append(
            f"[x] {blocker['id']} — {blocker['title']} "
            f"(owner {blocker['owner']}, due {blocker['target_close_date']})"
        )
    return "\n".join(lines).rstrip()


def apply_gate(state_dict: dict, platform_dag: dict) -> str:
    """Moves the graph on if the gate is clear. Returns the destination state.

    Called with the graph sitting on STATE_BLOCKER_RESOLUTION, whether it arrived
    there from an assignment or by looping back through registration. Recomputing
    from the stored list — rather than trusting whichever path got here — is what
    makes the gate hard to get wrong.
    """
    blockers = state_dict["variables"].get("blockers") or []
    outstanding = [b for b in blockers if is_outstanding(b)]

    transition_key = "on_blockers_outstanding" if outstanding else "on_all_blockers_owned"
    dest = platform_dag["states"][STATE]["transitions"][transition_key]

    if dest != STATE:
        state_dict["history"].append(f"Transitioned {STATE} -> {dest}")
        state_dict["current_state"] = dest
    return dest


async def list_blockers(ctx: Context = None) -> str:
    """Lists the blockers from the readiness report and which still need an owner."""
    logger.info("list_blockers called.")
    try:
        state_dict, _, _ = authorize_and_rehydrate(None)
    except Exception as e:
        return f"ERROR: {e}"

    blockers = state_dict["variables"].get("blockers") or []
    return (
        f"Current State: {state_dict['current_state']}\n\n"
        f"{format_checklist(blockers)}"
    )


async def assign_blocker_owner(
    blocker_id: str, owner_email: str, target_close_date: str, ctx: Context = None
) -> str:
    """Assigns an owner and a target close date to one blocker.

    Landing zone design unlocks only once every blocker has both. If the owner is
    not registered in the workspace ledger, this raises a question about which
    team they are on, registers them, and then applies the assignment.

    Args:
        blocker_id: The blocker's id, e.g. "B-001".
        owner_email: The owner's email address. Must be a person the user named.
        target_close_date: The agreed target close date as YYYY-MM-DD. Resolve any
            relative date the user gave against today before calling.
    """
    logger.info(f"assign_blocker_owner called for {blocker_id} -> {owner_email}")
    try:
        state_dict, generation, config = authorize_and_rehydrate(None)
    except Exception as e:
        return f"ERROR: {e}"

    current_state = state_dict["current_state"]
    if current_state != STATE:
        return f"ERROR: Invalid state for assign_blocker_owner: {current_state}"

    blockers = state_dict["variables"].get("blockers") or []
    blocker = next((b for b in blockers if b.get("id") == blocker_id), None)
    if blocker is None:
        known = ", ".join(b.get("id", "?") for b in blockers) or "none"
        return f"ERROR: No blocker with id '{blocker_id}'. Known blockers: {known}"

    owner_email = (owner_email or "").strip()
    if not owner_email:
        return "ERROR: owner_email is required. Ask the user who should own this blocker."

    close_date, date_error = parse_close_date(target_close_date)
    if date_error:
        return f"ERROR: {date_error}"

    bucket_name = get_bucket_name(config["ledger_uri"])
    bucket = state_mgr.gcs_client.bucket(bucket_name)
    try:
        platform_dag = load_dag(bucket, "platform_dag.json")
        known_emails = registered_emails(bucket)
    except Exception as e:
        return f"ERROR: {e}"

    if owner_email in known_emails:
        blocker["owner"] = owner_email
        blocker["target_close_date"] = close_date
        state_dict["history"].append(
            f"Blocker {blocker_id} assigned to {owner_email}, target close date {close_date}"
        )
        message = f"{blocker_id} assigned to {owner_email}, target close date {close_date}."
    else:
        # Parked rather than applied: the assignment only becomes real once the
        # user has said which team this person is on and the ledger has granted
        # them access. Both live past an elicitation the user can decline.
        state_dict["variables"]["pending_owner"] = {
            "blocker_id": blocker_id,
            "email": owner_email,
            "target_close_date": close_date,
        }
        # STATE_CONFIRM_NEW_MEMBER's prompt_template reads this variable by name.
        state_dict["variables"]["pending_owner_email"] = owner_email
        # Drop any answer left over from a previous owner, so a round that fails
        # to reach the user cannot be registered against a stale team choice.
        (state_dict["variables"].get("elicitation_responses") or {}).pop(
            "STATE_CONFIRM_NEW_MEMBER", None)

        dest = platform_dag["states"][STATE]["transitions"]["on_unregistered_owner"]
        state_dict["history"].append(
            f"Blocker owner '{owner_email}' is not registered in the workspace"
        )
        state_dict["history"].append(f"Transitioned {STATE} -> {dest}")
        state_dict["current_state"] = dest

        _, message, error = await run_dispatch_loop(
            ctx, state_dict, platform_dag, config,
            f"Registering {owner_email}.")
        if error:
            return error

        state_dict["variables"].pop("pending_owner_email", None)
        state_dict["variables"].pop("pending_owner", None)

    if state_dict["current_state"] == STATE:
        apply_gate(state_dict, platform_dag)

    data = json.dumps(state_dict, indent=2)
    try:
        blob_state = bucket.blob("platform/onboarding/state.json")
        blob_state.upload_from_string(
            data, content_type="application/json", if_generation_match=generation
        )
    except exceptions.PreconditionFailed:
        return "ERROR: Concurrent update conflict. Your changes were not saved."

    remaining = [b for b in state_dict["variables"].get("blockers") or [] if is_outstanding(b)]
    output = f"{message}\nCurrent State: {state_dict['current_state']}\n"
    if remaining:
        output += (
            f"{len(remaining)} blocker(s) still need an owner and a date: "
            f"{', '.join(b['id'] for b in remaining)}"
        )
    else:
        output += "Every blocker now has an owner and a target close date. Landing zone design is unlocked."
    return output


def register(mcp):
    """Register this step's tools on the FastMCP server."""
    mcp.tool()(list_blockers)
    mcp.tool()(assign_blocker_owner)
