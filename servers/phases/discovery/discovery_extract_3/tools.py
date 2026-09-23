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

"""MCP tools for discovery step 3 (extract): map-reduce inventory extraction.

Owns the STATE_DISCOVERY_RUNNING agent task. run_discovery_extraction is the
server-side pipeline (index -> chunk -> small-context LLM workers -> merge ->
report -> persist); write_discovery_inventory is the manual path for an agent
that assembled the inventory itself. Both advance the DAG to
STATE_ASSESSMENT, the combined review of everything discovery produced.
"""

import hashlib
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
from servers.dag.server import exports as exports_lib

from ..discovery_init_1.files import index_configuration_files
from ..discovery_scope_2 import scope as scope_lib
from . import extractor, merger, reporter
from .chunker import chunk_manifest, read_chunk_contents

logger = logging.getLogger("migration-dag")

FRAGMENT_PREFIX = "platform/discovery/fragments"
INVENTORY_BLOB = "platform/discovery/inventory.json"


def extraction_contract(schema: dict) -> str:
    """Digest of what extraction was asked to produce.

    The rules and the schema are both prompt material, so a persisted
    fragment is only reusable while both are unchanged: reusing one
    extracted under an older contract would assert that the newer fields
    were asked for and found absent. Stored inside each fragment blob (not
    in the blob path) so the frontend's flat fragment lookup keeps working;
    a mismatch re-extracts the chunk in place.
    """
    material = extractor.EXTRACT_SYSTEM_RULES + json.dumps(schema, sort_keys=True)
    return hashlib.sha1(material.encode("utf-8")).hexdigest()[:12]
REPORT_BLOB = "platform/discovery/readiness-report.md"
# Read by the frontend's extraction_progress endpoint to drive the live bar.
PROGRESS_BLOB = "platform/discovery/extraction-progress.json"

# Inventory sections owned by the deterministic scan pipeline
# (discovery_init_1), which runs before extraction — the schema's passthrough
# sections. Both writers below rebuild the blob wholesale, so these sections
# must be carried over from the ledger or an extraction re-run (e.g. after
# amend_scope) would silently drop the image inventory and its provenance.
# data_dependencies belongs here, not to LLM enrichment: the facts are exact
# strings (resource types, module sources) and the workload data gate holds a
# developer's pipeline on them, which mixed scanner/model provenance could not
# support. This tuple is the only thing that actually enforces that — it is
# applied after the model's inventory arrives and before it is saved, so the
# scanned section always wins.
SCAN_OWNED_KEYS = ("images", "render_targets", "source", "schema_version",
                   "generated_at", "data_dependencies",
                   "data_dependency_scan_notes", "cluster_dns",
                   "cluster_dns_scan_notes", "address_space",
                   "address_space_scan_notes")

# The schema loader lives in state_management, which owns the path and the
# validation that reads it. This step used to open the file itself; another
# copy of "where the inventory contract lives" is one too many, and this one
# was a copy. `landingzone_translationplan_3/coverage.py` still keeps its own
# — pre-existing, and not this change's to move.
load_inventory_schema = state_mgr.load_inventory_schema


def _publish_data_gate(bucket, inventory: dict) -> str:
    """Publishes the exports `data_gate` slice for the just-persisted
    inventory. Best-effort; "" on success, else a warning for the response.

    Here rather than inside `publish_discovery_exports` because that function
    lives in `servers/dag/server`, which does not import phase code, and the
    slice is a join with the deployment phase's outcome store keyed by that
    phase's own identity function. Extraction is the earliest point the
    section is both final (the review has signed it off) and persisted, and
    it is the point a developer's scope step first has anything to read.

    Function-level import: the deployment step imports the discovery review
    module, so a top-level import here risks closing the cycle.
    """
    try:
        from servers.phases.deployment.deployment_datamigration_2.tools import (
            publish_data_gate_from_ledger)
        return publish_data_gate_from_ledger(bucket, inventory)
    except Exception as e:
        logger.error(f"exports.json data_gate publication failed: {e}")
        return (f"WARNING: the exports data_gate slice was not published "
                f"({e}); developer ship gates read its last published value "
                "until refresh_exports runs.")


def carry_scan_sections(bucket, inventory: dict) -> None:
    """Copies the image-scan passthrough sections from the ledger inventory
    into `inventory` in place. Best-effort: a missing or unreadable blob means
    there is nothing to carry."""
    try:
        existing, _ = state_mgr.load_inventory(bucket)
    except Exception as e:
        logger.error(f"Could not read existing inventory to carry image sections: {e}")
        return
    for key in SCAN_OWNED_KEYS:
        if existing and existing.get(key) is not None:
            inventory[key] = existing[key]


async def run_discovery_extraction(ctx: Context = None) -> str:
    """Runs the full discovery extraction pipeline over the confirmed scope.

    Uses the human-confirmed discovery scope (root_dir, exclusions, additions)
    from the scoping step. Chunks the in-scope IaC sources, extracts an
    inventory fragment per chunk with small-context LLM workers (resumable:
    chunks with fragments already in the ledger are skipped), merges fragments
    deterministically, generates the readiness report, persists everything to
    the ledger, and advances the DAG to STATE_ASSESSMENT for the combined review.
    """
    logger.info("run_discovery_extraction called.")

    auth_error = extractor.check_llm_auth()
    if auth_error:
        return f"ERROR: {auth_error}"

    try:
        state_dict, generation, config = authorize_and_rehydrate(None)
    except Exception as e:
        return f"ERROR: {e}"

    if state_dict["current_state"] != "STATE_DISCOVERY_RUNNING":
        return f"ERROR: Invalid state for run_discovery_extraction: {state_dict['current_state']}"

    scope = state_dict["variables"].get("discovery_scope")
    if not scope:
        return "ERROR: No discovery scope found. Run discover_configuration_files first."

    effective_root = scope["root_dir"]
    try:
        manifest = index_configuration_files(effective_root)
    except Exception as e:
        return f"ERROR: Failed to index {effective_root}: {e}"

    kept, removed = scope_lib.filter_files(manifest["files"], scope)
    extras, scope_notes = scope_lib.index_included_paths(scope, index_configuration_files)
    known = {f["path"] for f in kept}
    kept.extend(f for f in extras if f["path"] not in known)
    manifest = {**manifest, "files": kept}

    if not manifest["files"]:
        return (
            f"ERROR: No configuration files in scope under {effective_root} "
            f"({len(removed)} excluded by scope). Adjust the scope and retry."
        )

    schema = load_inventory_schema()
    chunks = chunk_manifest(manifest)

    bucket_name = get_bucket_name(config["ledger_uri"])
    bucket = state_mgr.gcs_client.bucket(bucket_name)

    # Resume: reuse fragments already persisted for identical chunks — but
    # only those produced under the current extraction contract.
    contract = extraction_contract(schema)
    fragments = {}
    pending = []
    for chunk in chunks:
        blob = bucket.blob(f"{FRAGMENT_PREFIX}/{chunk['chunk_id']}.json")
        try:
            stored = json.loads(blob.download_as_text())
        except exceptions.NotFound:
            pending.append(chunk)
            continue
        except Exception:
            pending.append(chunk)
            continue
        # pop: the marker is cache bookkeeping, not a fact for the merger.
        if isinstance(stored, dict) and stored.pop("_contract", None) == contract:
            fragments[chunk["chunk_id"]] = stored
        else:
            pending.append(chunk)

    persisted = set()

    def persist_fragment(chunk_id, fragment):
        bucket.blob(f"{FRAGMENT_PREFIX}/{chunk_id}.json").upload_from_string(
            json.dumps({**fragment, "_contract": contract}, indent=2),
            content_type="application/json"
        )
        persisted.add(chunk_id)

    # Publish the chunk set before fanning out so the frontend can watch the
    # bar fill fragment by fragment (reused chunks show done immediately; the
    # pending ones land as their workers finish). Best-effort — a failed write
    # only costs the live view, not the extraction.
    try:
        bucket.blob(PROGRESS_BLOB).upload_from_string(
            json.dumps({
                "chunk_ids": [c["chunk_id"] for c in chunks],
                "total_chunks": len(chunks),
                "reused": len(chunks) - len(pending),
            }),
            content_type="application/json",
        )
    except Exception as e:
        logger.warning(f"Failed to persist extraction progress: {e}")

    chunks_with_content = [(c, read_chunk_contents(c, effective_root)) for c in pending]
    result = await extractor.extract_all(
        chunks_with_content, schema, on_fragment=persist_fragment
    )
    errors = result["errors"]

    for chunk_id, fragment in result["fragments"].items():
        fragments[chunk_id] = fragment
        if chunk_id in persisted:
            continue  # already written the moment its worker finished
        blob = bucket.blob(f"{FRAGMENT_PREFIX}/{chunk_id}.json")
        try:
            blob.upload_from_string(
                json.dumps({**fragment, "_contract": contract}, indent=2),
                content_type="application/json")
        except Exception as e:
            logger.error(f"Failed to persist fragment {chunk_id}: {e}")

    if not fragments:
        return (
            "ERROR: Extraction produced no fragments "
            f"({len(errors)} chunk failures: {json.dumps(errors)[:500]}). State unchanged — retry is safe."
        )

    ordered = [fragments[chunk_id] for chunk_id in sorted(fragments)]
    inventory = merger.merge_fragments(ordered)
    carry_scan_sections(bucket, inventory)
    inventory["extraction"] = {
        "chunks_total": len(chunks),
        "chunks_reused": len(chunks) - len(pending),
        "chunks_failed": len(errors),
        "errors": errors,
    }

    report_error = None
    report = ""
    try:
        report = await reporter.generate_report(inventory, errors)
    except Exception as e:
        report_error = str(e)
        logger.error(f"Readiness report generation failed: {e}")

    try:
        bucket.blob(INVENTORY_BLOB).upload_from_string(
            json.dumps(inventory, indent=2), content_type="application/json"
        )
        if report:
            bucket.blob(REPORT_BLOB).upload_from_string(report, content_type="text/markdown")
    except Exception as e:
        return f"ERROR: Failed to persist inventory to ledger: {e}"

    # Publish/refresh exports.json — the platform→developer channel — now
    # that the inventory this derivation reads is persisted. Best-effort: a
    # failed publish warns in the response, never fails the discovery step.
    exports_warning = exports_lib.publish_discovery_exports(
        bucket, state_dict["variables"], inventory, scope_lib.filter_files)
    data_warning = _publish_data_gate(bucket, inventory)
    if data_warning:
        exports_warning = (exports_warning + " " + data_warning
                           if exports_warning else data_warning)

    # Advance the DAG and store the inventory in the state variables.
    try:
        platform_dag = load_dag(bucket, "platform_dag.json")
    except Exception as e:
        return f"ERROR: {e}"

    current_state = state_dict["current_state"]
    state_def = platform_dag["states"][current_state]
    next_state = state_def["transitions"]["on_tool_call_received"]

    state_dict["variables"]["discovery_inventory"] = inventory
    state_dict["variables"]["discovery_report_blob"] = REPORT_BLOB if report else None
    state_dict["history"].append(f"Transitioned {current_state} -> {next_state} via run_discovery_extraction")
    state_dict["current_state"] = next_state

    data = json.dumps(state_dict, indent=2)
    try:
        blob_state = bucket.blob("platform/onboarding/state.json")
        blob_state.upload_from_string(data, content_type="application/json", if_generation_match=generation)
    except exceptions.PreconditionFailed:
        return "ERROR: Concurrent update conflict. Your changes were not saved."

    summary = (
        f"SUCCESS: Discovery extraction complete. Current State: {next_state}.\n"
        f"- Scope: {len(manifest['files'])} files in scope, {len(removed)} excluded"
        + (f" ({'; '.join(scope_notes)})" if scope_notes else "")
        + "\n"
        f"- Chunks: {len(chunks)} total, {len(chunks) - len(pending)} reused, {len(errors)} failed\n"
        f"- Triggers: {json.dumps(inventory['triggers'])}\n"
        f"- Inventory: gs://{bucket_name}/{INVENTORY_BLOB}\n"
    )
    if report:
        summary += f"- Readiness report: gs://{bucket_name}/{REPORT_BLOB}\n"
    else:
        summary += f"- Readiness report FAILED ({report_error}); inventory is still valid.\n"
    if exports_warning:
        summary += f"- {exports_warning}\n"
    else:
        summary += f"- Exports: gs://{bucket_name}/{exports_lib.EXPORTS_BLOB} refreshed (discovery fields)\n"
    if errors:
        summary += f"- Failed chunks (retry by re-running this tool): {', '.join(sorted(errors))}\n"
    return summary


async def write_discovery_inventory(inventory_json: dict, ctx: Context = None) -> str:
    """Writes an agent-assembled EKS discovery inventory to the ledger and advances the state machine.

    Manual alternative to run_discovery_extraction for small estates the agent
    analyzed itself. Valid only in STATE_DISCOVERY_RUNNING.
    """
    logger.info("write_discovery_inventory called.")
    try:
        state_dict, generation, config = authorize_and_rehydrate(None)
    except Exception as e:
        return f"ERROR: {e}"

    if state_dict["current_state"] != "STATE_DISCOVERY_RUNNING":
        return f"ERROR: Invalid state for write_discovery_inventory: {state_dict['current_state']}"

    bucket_name = get_bucket_name(config["ledger_uri"])
    bucket = state_mgr.gcs_client.bucket(bucket_name)
    carry_scan_sections(bucket, inventory_json)
    # The derived NodePool lists are the merger's on every path: an agent-
    # assembled inventory gets the same capacity_types / instance_families /
    # architectures its requirements imply, never the agent's own reading.
    merger.derive_nodepool_summaries(inventory_json)
    state_dict["variables"]["discovery_inventory"] = inventory_json
    try:
        platform_dag = load_dag(bucket, "platform_dag.json")
    except Exception as e:
        return f"ERROR: {e}"

    current_state = state_dict["current_state"]
    state_def = platform_dag["states"][current_state]
    next_state = state_def["transitions"]["on_tool_call_received"]

    state_dict["history"].append(f"Transitioned {current_state} -> {next_state} via write_discovery_inventory")
    state_dict["current_state"] = next_state

    message = "Discovery inventory saved to ledger."

    try:
        bucket.blob(INVENTORY_BLOB).upload_from_string(
            json.dumps(inventory_json, indent=2), content_type="application/json"
        )
    except Exception as e:
        logger.error(f"Failed to persist inventory blob: {e}")

    # Same best-effort exports publication as run_discovery_extraction: this
    # is the other inventory-persist path, so the channel refreshes here too.
    exports_warning = exports_lib.publish_discovery_exports(
        bucket, state_dict["variables"], inventory_json, scope_lib.filter_files)
    if exports_warning:
        message += f" {exports_warning}"
    else:
        message += " exports.json refreshed (discovery fields)."
    data_warning = _publish_data_gate(bucket, inventory_json)
    if data_warning:
        message += f" {data_warning}"

    data = json.dumps(state_dict, indent=2)
    try:
        blob_state = bucket.blob("platform/onboarding/state.json")
        blob_state.upload_from_string(data, content_type="application/json", if_generation_match=generation)
    except exceptions.PreconditionFailed:
        return "ERROR: Concurrent update conflict. Your changes were not saved."

    return f"SUCCESS: Discovery inventory saved. Current State: {state_dict['current_state']}. Message: {message}"


def register(mcp):
    """Register this step's tools on the FastMCP server."""
    mcp.tool()(run_discovery_extraction)
    mcp.tool()(write_discovery_inventory)
