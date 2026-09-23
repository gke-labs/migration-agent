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

"""MCP tools for discovery step 2 (scope): human-controlled scope of extraction.

Owns the STATE_DISCOVERY_SCOPING agent task. The client removes files or
directories they consider out of scope (update_discovery_scope, repeatable)
and then signs off (confirm_discovery_scope) before any LLM extraction spend.
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

from . import scope as scope_lib

logger = logging.getLogger("migration-dag")

MANIFEST_BLOB = "platform/discovery/manifest.json"


def _load_manifest(bucket):
    try:
        return json.loads(bucket.blob(MANIFEST_BLOB).download_as_text())
    except exceptions.NotFound:
        return None
    except Exception as e:
        logger.warning(f"Could not read discovery manifest: {e}")
        return None


async def update_discovery_scope(
    exclude: Optional[List[str]] = None,
    include: Optional[List[str]] = None,
    ctx: Context = None,
) -> str:
    """Adjusts the discovery scope before extraction. Repeatable; does not advance the DAG.

    Args:
        exclude: paths, directory prefixes (e.g. 'vendor/'), or globs
            (e.g. '**/test_*.yaml') to remove from scope, relative to root_dir.
        include: entries to bring (back) into scope — an excluded pattern to
            undo, a path that overrides a broader exclude, or an extra
            file/directory (absolute, or relative to root_dir) to add.
    """
    logger.info(f"update_discovery_scope called. exclude={exclude} include={include}")
    try:
        state_dict, generation, config = authorize_and_rehydrate(None)
    except Exception as e:
        return f"ERROR: {e}"

    if state_dict["current_state"] != "STATE_DISCOVERY_SCOPING":
        return f"ERROR: Invalid state for update_discovery_scope: {state_dict['current_state']}"

    current_scope = state_dict["variables"].get("discovery_scope")
    if not current_scope:
        return "ERROR: No discovery scope found. Run discover_configuration_files first."

    new_scope, notes = scope_lib.apply_scope_update(current_scope, exclude, include)
    state_dict["variables"]["discovery_scope"] = new_scope
    state_dict["history"].append(f"Scope updated: {'; '.join(notes) or 'no changes'}")

    bucket_name = get_bucket_name(config["ledger_uri"])
    bucket = state_mgr.gcs_client.bucket(bucket_name)

    data = json.dumps(state_dict, indent=2)
    try:
        blob_state = bucket.blob("platform/onboarding/state.json")
        blob_state.upload_from_string(data, content_type="application/json", if_generation_match=generation)
    except exceptions.PreconditionFailed:
        return "ERROR: Concurrent update conflict. Your changes were not saved."

    manifest = _load_manifest(bucket)
    output = f"Scope updated ({'; '.join(notes) or 'no changes'}).\n"
    if manifest:
        output += f"Effective scope:\n{scope_lib.summarize_scope(manifest, new_scope)}\n"
    output += "Call update_discovery_scope again to adjust further, or confirm_discovery_scope to proceed."
    return output


async def confirm_discovery_scope(ctx: Context = None) -> str:
    """Records the user's sign-off on the discovery scope and advances to the data-dependency scan."""
    logger.info("confirm_discovery_scope called.")
    try:
        state_dict, generation, config = authorize_and_rehydrate(None)
    except Exception as e:
        return f"ERROR: {e}"

    current_state = state_dict["current_state"]
    if current_state != "STATE_DISCOVERY_SCOPING":
        return f"ERROR: Invalid state for confirm_discovery_scope: {current_state}"

    scope = state_dict["variables"].get("discovery_scope")
    if not scope:
        return "ERROR: No discovery scope found. Run discover_configuration_files first."

    bucket_name = get_bucket_name(config["ledger_uri"])
    bucket = state_mgr.gcs_client.bucket(bucket_name)
    try:
        platform_dag = load_dag(bucket, "platform_dag.json")
    except Exception as e:
        return f"ERROR: {e}"

    state_def = platform_dag["states"][current_state]
    next_state = state_def["transitions"]["on_tool_call_received"]
    state_dict["history"].append(f"Transitioned {current_state} -> {next_state} via confirm_discovery_scope")
    state_dict["current_state"] = next_state

    data = json.dumps(state_dict, indent=2)
    try:
        blob_state = bucket.blob("platform/onboarding/state.json")
        blob_state.upload_from_string(data, content_type="application/json", if_generation_match=generation)
    except exceptions.PreconditionFailed:
        return "ERROR: Concurrent update conflict. Your changes were not saved."

    manifest = _load_manifest(bucket)
    output = f"SUCCESS: Scope confirmed. Current State: {next_state}.\n"
    if manifest:
        output += f"Confirmed scope:\n{scope_lib.summarize_scope(manifest, scope)}\n"
    # Derived from the graph, not hardcoded: a workspace bootstrapped before
    # v2.7 keeps its own copy of the DAG until join_ledger --reconfigure, and
    # naming a tool its graph does not reach would stop the run.
    if next_state == "STATE_DISCOVERY_DATA_SCAN":
        output += ("Next: call scan_data_dependencies() to record the managed "
                   "data services the workloads depend on. The user then "
                   "reviews who uses each of them, and extraction follows "
                   "that review.")
    else:
        output += ("Next: call run_discovery_extraction() to extract the inventory "
                   "from the in-scope files.")
    return output


def register(mcp):
    """Register this step's tools on the FastMCP server."""
    mcp.tool()(update_discovery_scope)
    mcp.tool()(confirm_discovery_scope)
