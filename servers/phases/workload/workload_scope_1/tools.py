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

"""MCP tools for workload step 1 (scope): agree the component's file scope.

Owns the STATE_WKLD_SCOPE agent task. Facts come ONLY from the exports
component_seed_index (D2/D3) — this module never reads platform/*. Every
state-mutating tool goes through authorize_and_rehydrate_workload (the one
claimant check, D11); browsing is read-only and skips the claimant check.
"""

import json
import logging
from typing import List, Optional

from google.api_core import exceptions
from mcp.server.fastmcp import Context

from servers.dag.state_management import (
    get_bucket_name,
    load_dag,
    authorize_and_rehydrate_workload,
)
import servers.dag.state_management as state_mgr
from servers.dag.server import exports as exports_lib
from servers.phases import scope_algebra

from . import seed as seed_lib

logger = logging.getLogger("migration-dag")

EMPTY_DRAFT = {"root_dir": "", "included": [], "excluded": []}

SCOPE_STATE = "STATE_WKLD_SCOPE"
CONFIRM_STATE = "STATE_WKLD_SCOPE_CONFIRM"


def load_seed(bucket) -> tuple:
    """(exports doc, seed index, degraded_reason). degraded_reason is None
    when a non-empty component_seed_index is available. A 403 on
    exports.json is an IAM defect (the conditional developer read grant is
    missing), named as such — never conflated with "not published yet"."""
    try:
        doc, _ = exports_lib.load_exports(bucket)
    except exceptions.Forbidden:
        return None, None, (
            "this session was DENIED reading exports.json (403). That is an "
            "IAM defect, not pipeline timing: exports.json is a root object "
            "the developer grant must cover, so this ledger is missing the "
            "conditional exports read grant — ask an admin to re-run "
            "provision_ledger_iam (registering any member re-runs it), then "
            "retry.")
    if doc is None:
        return None, None, (
            "exports.json has not been published to this ledger yet — the "
            "platform side has not published, or discovery has not run. "
            "That is a pipeline-timing fact, not an error.")
    index = doc.get("component_seed_index")
    if not index:
        return doc, None, (
            "exports.json is published but its component_seed_index is null "
            "or empty — discovery has not derived it yet. That is a "
            "pipeline-timing fact, not an error.")
    return doc, index, None


def _degraded_browse_text(reason: str) -> str:
    return (
        f"DEGRADED MODE: {reason}\n"
        "Nothing can be browsed or verified against the seed. Include/exclude "
        "globs you record with update_workload_scope will be stored against "
        "your own source clone UNVERIFIED, and submit_workload_scope will "
        "persist the scope with resolved_paths=null and degraded=true.\n"
        "Reminder: platform/* is not readable by developers — exports.json is "
        "the only platform-to-developer channel. Do not guess at seed "
        "contents; agree the globs with the user from their clone."
    )


def _state_blob(bucket, component):
    return bucket.blob(f"workloads/{component}/state.json")


def _entry_line(path: str, entry: dict) -> str:
    kinds = ", ".join(entry.get("kinds") or []) or "-"
    namespaces = ", ".join(entry.get("namespaces") or []) or "-"
    labels = ", ".join(entry.get("team_labels") or []) or "-"
    line = f"- {path} | kinds: {kinds} | namespaces: {namespaces} | team_labels: {labels}"
    warn = seed_lib.cluster_scoped_in(entry)
    if warn:
        line += (f" [WARNING: cluster-scoped kind(s) {', '.join(warn)} — "
                 "platform-owned; advise excluding this file]")
    return line


async def browse_component_seed(
    path_glob: Optional[str] = None,
    namespace: Optional[str] = None,
    team_label: Optional[str] = None,
    token: Optional[str] = None,
    limit: int = 50,
    ctx: Context = None,
) -> str:
    """Browses the exports component seed index. Read-only; no transition.

    Output order: the estate terrain note first, then (only on an unfiltered
    call) the labeled component-id token guess, then the filtered entries.
    Filters AND-combine: path_glob (exact/prefix/glob), namespace and
    team_label (exact), token (substring across path/namespaces/labels).
    """
    logger.info(f"browse_component_seed called. path_glob={path_glob} "
                f"namespace={namespace} team_label={team_label} token={token}")
    try:
        state_dict, _, config = authorize_and_rehydrate_workload(
            None, enforce_claimant=False)
    except Exception as e:
        return f"ERROR: {e}"

    current = state_dict["current_state"]
    if current not in (SCOPE_STATE, CONFIRM_STATE):
        return f"ERROR: Invalid state for browse_component_seed: {current}"

    bucket = state_mgr.gcs_client.bucket(get_bucket_name(config["ledger_uri"]))
    _, index, degraded_reason = load_seed(bucket)
    if degraded_reason:
        return _degraded_browse_text(degraded_reason)

    component = config["component"]  # the authorized component
    out = [seed_lib.terrain_note(index)]

    if not any([path_glob, namespace, team_label, token]):
        matched, note = seed_lib.token_guess(index, component)
        out.append("")
        out.append(note)
        if matched:
            sample = ", ".join(matched[:10])
            more = f" (+{len(matched) - 10} more)" if len(matched) > 10 else ""
            out.append(f"  guess candidates: {sample}{more}")

    entries = seed_lib.filter_seed(index, path_glob, namespace, team_label, token)
    shown = sorted(entries.items())[:max(limit, 0)]
    out.append("")
    out.append(f"Entries: {len(entries)} matched of {len(index)} in the seed "
               f"(showing {len(shown)}):")
    out.extend(_entry_line(path, entry) for path, entry in shown)
    if len(entries) > len(shown):
        out.append(f"... and {len(entries) - len(shown)} more — narrow the filters.")
    return "\n".join(out)


async def update_workload_scope(
    include: Optional[List[str]] = None,
    exclude: Optional[List[str]] = None,
    ctx: Context = None,
) -> str:
    """Adjusts the component scope draft. Repeatable; does not advance the DAG.

    Args:
        include: patterns selecting files INTO the component (exact paths,
            directory prefixes like 'charts/orders/', or globs). A component
            scope selects from nothing — only included files are in scope.
        exclude: patterns carving files back out of the included selection.
    """
    logger.info(f"update_workload_scope called. include={include} exclude={exclude}")
    try:
        state_dict, generation, config = authorize_and_rehydrate_workload(None)
    except Exception as e:
        return f"ERROR: {e}"

    if state_dict["current_state"] != SCOPE_STATE:
        return f"ERROR: Invalid state for update_workload_scope: {state_dict['current_state']}"

    variables = state_dict["variables"]
    draft = variables.get("workload_scope") or dict(EMPTY_DRAFT)
    # root_dir stays "": seed paths are repo-relative; nothing indexes extras.
    # include_first: a component scope selects from nothing (the inverted
    # algebra of seed.resolve_component_scope), so an include that un-excludes
    # a pattern must ALSO land in `included` — an un-exclude alone would
    # select nothing while this tool reported success.
    new_scope, notes = scope_algebra.apply_scope_update(
        draft, exclude, include, include_first=True)
    variables["workload_scope"] = new_scope
    state_dict["history"].append(
        f"Workload scope updated: {'; '.join(notes) or 'no changes'}")

    # The component authorize_and_rehydrate_workload authorized — never the
    # raw ledger variables, so the claimant check and the write path cannot
    # name different components.
    component = config["component"]
    bucket = state_mgr.gcs_client.bucket(get_bucket_name(config["ledger_uri"]))
    try:
        _state_blob(bucket, component).upload_from_string(
            json.dumps(state_dict, indent=2), content_type="application/json",
            if_generation_match=generation)
    except exceptions.PreconditionFailed:
        return "ERROR: Concurrent update conflict. Your changes were not saved."
    except exceptions.Forbidden:
        return state_mgr.workload_write_denied(config, component)

    _, index, degraded_reason = load_seed(bucket)
    if degraded_reason:
        count_line = ("In-scope count: unverifiable (degraded — no seed index; "
                      "patterns are recorded against your clone unverified).")
    else:
        resolved = seed_lib.resolve_component_scope(list(index.keys()), new_scope)
        count_line = (f"In-scope against the seed: {len(resolved)} of "
                      f"{len(index)} file(s).")
    return (
        f"Scope updated ({'; '.join(notes) or 'no changes'}).\n"
        f"Included: {new_scope['included']}\nExcluded: {new_scope['excluded']}\n"
        f"{count_line}\n"
        "Call update_workload_scope again to adjust, or submit_workload_scope "
        "to propose this scope for sign-off."
    )


async def submit_workload_scope(ctx: Context = None) -> str:
    """Proposes the drafted scope and advances to the sign-off step.

    Non-degraded: the draft is resolved against the seed index and an EMPTY
    resolution is refused (an empty scope is never advanced — D13's honesty
    rule). Degraded (no usable seed): at least one include pattern is
    required; the resolution stays null.
    """
    logger.info("submit_workload_scope called.")
    try:
        state_dict, generation, config = authorize_and_rehydrate_workload(None)
    except Exception as e:
        return f"ERROR: {e}"

    if state_dict["current_state"] != SCOPE_STATE:
        return f"ERROR: Invalid state for submit_workload_scope: {state_dict['current_state']}"

    variables = state_dict["variables"]
    component = config["component"]  # the authorized component (see update)
    draft = variables.get("workload_scope") or dict(EMPTY_DRAFT)
    bucket = state_mgr.gcs_client.bucket(get_bucket_name(config["ledger_uri"]))
    doc, index, degraded_reason = load_seed(bucket)

    if degraded_reason:
        if not draft.get("included"):
            return (f"ERROR: Degraded mode ({degraded_reason}) — at least one "
                    "include pattern is required before submitting. Record the "
                    "component's paths with update_workload_scope first.")
        resolution = {"resolved_paths": None, "degraded": True,
                      "exports_generated_at": None, "exports_generations": None}
        summary = ("Degraded submission: patterns recorded unverified "
                   f"(includes: {draft['included']}, excludes: {draft['excluded']}).")
    else:
        resolved = seed_lib.resolve_component_scope(list(index.keys()), draft)
        if not resolved:
            return (
                f"ERROR: The proposed scope resolves to 0 of the {len(index)} "
                "seed file(s) — an empty scope is not advanced (stating this "
                "honestly rather than pretending no candidates exist). "
                f"Includes: {draft.get('included')}; excludes: "
                f"{draft.get('excluded')}. Adjust with update_workload_scope."
            )
        resolution = {"resolved_paths": resolved, "degraded": False,
                      "exports_generated_at": doc.get("generated_at"),
                      "exports_generations": doc.get("generations")}
        summary = (f"Resolved {len(resolved)} of {len(index)} seed file(s) "
                   f"into the component scope.")
    variables["workload_scope"] = draft
    variables["workload_scope_resolution"] = resolution

    try:
        dag = load_dag(bucket, f"workloads/{component}/dag.json")
    except Exception as e:
        return f"ERROR: {e}"
    next_state = dag["states"][SCOPE_STATE]["transitions"]["on_tool_call_received"]
    state_dict["history"].append(
        f"Transitioned {SCOPE_STATE} -> {next_state} via submit_workload_scope")
    state_dict["current_state"] = next_state

    try:
        _state_blob(bucket, component).upload_from_string(
            json.dumps(state_dict, indent=2), content_type="application/json",
            if_generation_match=generation)
    except exceptions.PreconditionFailed:
        return "ERROR: Concurrent update conflict. Your changes were not saved."
    except exceptions.Forbidden:
        return state_mgr.workload_write_denied(config, component)

    return (
        f"SUCCESS: Scope proposed for component '{component}'. {summary}\n"
        f"Current State: {next_state}.\n"
        "Next: summarize the proposal to the user and call "
        "confirm_workload_scope — the sign-off elicitation is the decision."
    )


def register(mcp):
    """Register this step's tools on the FastMCP server."""
    mcp.tool()(browse_component_seed)
    mcp.tool()(update_workload_scope)
    mcp.tool()(submit_workload_scope)
