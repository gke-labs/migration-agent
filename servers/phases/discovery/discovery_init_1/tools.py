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

"""MCP tools for discovery step 1 (collect): index the local IaC sources.

Owns the STATE_DISCOVERY agent task in the platform DAG. The scan returns a
lightweight manifest (paths, sizes, kinds) — file contents are never returned
here. The manifest is persisted to the ledger and an all-inclusive scope is
initialized, then the dispatch loop drains the image-scan segment: literal
image references and render targets are collected into the inventory
(images.py), and rendering Helm charts / Kustomize roots is offered via
elicitation and run locally on approval (render.py). The graph then parks at
STATE_DISCOVERY_SCOPING, where discovery_scope_2 lets the client trim or
extend the scope before any extraction spend.
"""

import json
import logging
import os
import re

from google.api_core import exceptions
from mcp.server.fastmcp import Context

from servers.dag.dispatch import run_dispatch_loop
from servers.dag.state_management import (
    get_bucket_name,
    load_dag,
    authorize_and_rehydrate,
)
import servers.dag.state_management as state_mgr
from servers.dag.server import git_client

from .files import index_configuration_files, validate_explicit_root

logger = logging.getLogger("migration-dag")

MANIFEST_BLOB = "platform/discovery/manifest.json"

# Server-managed source checkouts, one per workspace. Discovery re-clones on
# every call (shallow), and extraction later reads file contents from here.
# Nothing here is authoritative — the remote repo and the branch recorded at
# onboarding are — so an install that finds this directory empty or moved
# re-clones into it rather than failing.
CHECKOUTS_DIR = os.path.expanduser("~/.gke-agentic-migration/checkouts")


def checkout_configured_source(variables: dict, config: dict) -> str:
    """Clones the source repository configured at onboarding and returns the
    directory to index (the clone joined with the configured source_path)."""
    url = variables.get("source_repo_url")
    branch = variables.get("source_branch")
    if not url or not branch:
        raise ValueError(
            "No source repository configured in the workspace. Run "
            "configure_repositories first, or pass root_dir explicitly."
        )
    workspace = re.sub(
        r"[^A-Za-z0-9._-]", "_", config.get("workspace_name") or "workspace"
    )
    target = os.path.join(CHECKOUTS_DIR, workspace)
    git_client.clone_repository(url, branch, target)
    source_path = (variables.get("source_path") or "/").strip("/")
    root = os.path.join(target, source_path) if source_path else target
    if not os.path.isdir(root):
        raise ValueError(
            f"Configured source_path '{variables.get('source_path')}' does not "
            f"exist in the cloned repository {url}@{branch}."
        )
    return root


async def discover_configuration_files(root_dir: str = "", ctx: Context = None) -> str:
    """
    Indexes the source EKS estate's IaC files (.tf/.tfvars/.yaml/.yml/.json)
    and returns a manifest of paths, sizes, and kinds — not file contents.

    Call with NO root_dir: the server clones the source repository configured
    via configure_repositories (at its branch and source_path) and indexes
    that clone. Pass root_dir (an absolute path) only to index a local
    checkout the user explicitly named; relative paths, the filesystem root,
    and home directories are refused.

    Persists the manifest to the ledger, initializes an all-inclusive scope,
    and builds the container image inventory: literal image references are
    scanned immediately, and if Helm charts or Kustomize roots are found the
    user is asked (elicitation) to approve rendering them locally ('helm
    template' / 'kubectl kustomize'; no cluster access). The DAG then parks
    at STATE_DISCOVERY_SCOPING where the client can remove or add paths
    before extraction.
    """
    logger.info(f"discover_configuration_files called (root_dir={root_dir!r})")
    try:
        state_dict, generation, config = authorize_and_rehydrate(None)
    except Exception as e:
        return f"ERROR: {e}"

    if state_dict["current_state"] != "STATE_DISCOVERY":
        return f"ERROR: Invalid state for discover_configuration_files: {state_dict['current_state']}"

    variables = state_dict.get("variables") or {}
    if root_dir:
        try:
            root_dir = validate_explicit_root(root_dir)
        except ValueError as e:
            return f"ERROR: {e}"
        source_note = f"explicit local checkout {root_dir}"
    else:
        try:
            root_dir = checkout_configured_source(variables, config)
        except Exception as e:
            return f"ERROR: {e}"
        source_note = (
            f"server clone of {variables.get('source_repo_url')}"
            f"@{variables.get('source_branch')}"
            f" (source_path '{variables.get('source_path') or '/'}')"
        )

    try:
        manifest = index_configuration_files(root_dir)
    except Exception as e:
        return f"ERROR running discovery: {e}"

    bucket_name = get_bucket_name(config["ledger_uri"])
    bucket = state_mgr.gcs_client.bucket(bucket_name)
    try:
        platform_dag = load_dag(bucket, "platform_dag.json")
    except Exception as e:
        return f"ERROR: {e}"

    try:
        bucket.blob(MANIFEST_BLOB).upload_from_string(
            json.dumps(manifest, indent=2), content_type="application/json"
        )
    except Exception as e:
        return f"ERROR: Failed to persist discovery manifest to ledger: {e}"

    current_state = state_dict["current_state"]
    state_def = platform_dag["states"][current_state]
    next_state = state_def["transitions"]["on_tool_call_received"]

    state_dict["variables"]["discovery_scope"] = {
        "root_dir": manifest["root_dir"],
        "excluded": [],
        "included": [],
    }
    # The image scan and render actions read the source from here — always the
    # directory just indexed, never a repo-relative source_path.
    state_dict["variables"]["discovery_root_dir"] = root_dir
    # A restart (e.g. the assessment gate's on_reject) must not leave the
    # rejected run's results looking current while the re-scan is under way.
    state_dict["variables"].pop("discovery_inventory", None)
    state_dict["variables"].pop("discovery_report_blob", None)
    state_dict["history"].append(f"Transitioned {current_state} -> {next_state} via discover_configuration_files")
    state_dict["current_state"] = next_state

    # Drain the image-scan segment: scan_image_references runs server-side,
    # the render approval is elicited, and the loop parks at the next agent
    # task (STATE_DISCOVERY_SCOPING). A decline drains on through the
    # mark-declined mutation in this same call — the dispatch loop does not
    # stop on a reject leg until it reaches an agent task (dispatch.py).
    transitions_run, message, error = await run_dispatch_loop(
        ctx, state_dict, platform_dag, config, "Discovery manifest recorded.")
    if error:
        return error

    data = json.dumps(state_dict, indent=2)
    try:
        blob_state = bucket.blob("platform/onboarding/state.json")
        blob_state.upload_from_string(data, content_type="application/json", if_generation_match=generation)
    except exceptions.PreconditionFailed:
        return "ERROR: Concurrent update conflict. Your changes were not saved."

    by_kind = {}
    for f in manifest["files"]:
        by_kind[f["kind"]] = by_kind.get(f["kind"], 0) + 1

    summary = {
        "root_dir": manifest["root_dir"],
        "file_count": len(manifest["files"]),
        "total_bytes": manifest["total_bytes"],
        "files_by_kind": by_kind,
        "skipped": manifest["skipped"],
        "files": manifest["files"],
    }
    output = (
        f"Discovery manifest — indexed {source_note} (no file contents were loaded):\n"
        + json.dumps(summary, indent=2)
        + "\n\n--- Image inventory ---\n"
        + f"- Transition log: {', '.join(transitions_run) or 'none'}\n"
        + f"- Message: {message}\n"
        + f"\nCurrent State: {state_dict['current_state']}."
    )
    if state_dict["current_state"] == "STATE_DISCOVERY_SCOPING":
        output += (
            " Next: review the scope with the user — "
            "update_discovery_scope(exclude=[...], include=[...]) to adjust, then "
            "confirm_discovery_scope() to proceed to the data-dependency scan."
        )
    else:
        output += " Next: call get_next_stage to continue."
    return output


def register(mcp):
    """Register this step's tools on the FastMCP server."""
    mcp.tool()(discover_configuration_files)
