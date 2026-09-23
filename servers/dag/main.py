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

import asyncio
import functools
import inspect
import json
import logging
import os
import sys
import time
import urllib.request
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional

# We use the official Anthropic Python MCP SDK.
from mcp.server.fastmcp import FastMCP, Context
from mcp.types import CreateMessageRequestParams
from google.cloud import storage
from google.cloud import securesourcemanager_v1
from google.api_core import exceptions
import google.auth
from google.auth.transport.requests import Request
import yaml

# Ensure the repository root is importable so the phase packages (servers/phases/discovery/,
# servers/phases/translation/) and the servers.* package resolve regardless of entrypoint.
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

# Local client imports
# The repo root must be on sys.path for the `servers.dag.*` imports below to
# resolve when the server is launched by an MCP client from an arbitrary cwd.
# TODO: Remove this once the server is packaged as a proper Python package.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from server import exports as exports_lib
from server import git_client
from server import ssm_client
from server import ledger_iam
from server import ledger_admin
from server.dag_validation import validate_dag, DagValidationError
from server import workload_join as workload_join_lib
from server.blocker_criteria import (
    load_blocker_categories,
    validate_blocker_criteria,
    BlockerCriteriaError,
)
from server.api_translation import validate_api_translation
from server.coverage_map import validate_coverage_map
from server.computeclass_families import validate_family_table
from server.decisions import validate_decisions
from servers.phases.translation.translation_translate_1 import translator
from servers.phases.workload.workload_translate_3 import translator as wkld_translator
from servers.dag.state_management import get_bucket_name, LEDGER_CONFIG_PATH, load_dag, authorize_and_rehydrate
import servers.dag.state_management as state_mgr
import frontend_launcher

# The elicitation/mutation walk over the graph. Lives outside this file because the
# phase packages imported below need it, and they cannot import back into main.
# main supplies the mutation half via set_mutation_runner, further down.
import servers.dag.dispatch as dispatch
from servers.dag.dispatch import (
    ApprovalSchema,
    SSMApprovalSchema,
    LZApprovalSchema,
    NewMemberSchema,
    render_prompt,
    run_elicitation,
    run_dispatch_loop,
)

# Phase packages: each phase folder owns the tools and agent instructions for
# its DAG steps (e.g. servers/phases/discovery/discovery_init_1/). See <phase>/README.md.
import servers.phases.discovery as discovery_phase
import servers.phases.translation as translation_phase
import servers.phases.assessment as assessment_phase
import servers.phases.landingzone as landingzone_phase
import servers.phases.deployment as deployment_phase
import servers.phases.workload as workload_phase
import servers.phases.bootstrap as bootstrap_phase
from servers.phases.landingzone import actions as landingzone_actions
from servers.phases.deployment import actions as deployment_actions
from servers.phases.workload.workload_validate_5 import actions as workload_actions

# Enable logging
logging.basicConfig(level=logging.DEBUG, stream=sys.stderr)
logger = logging.getLogger("migration-dag")

# Log to file in addition to stderr
try:
    fh = logging.FileHandler("/tmp/mcp_server.log")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(fh)
    logger.debug("File logger initialized at /tmp/mcp_server.log")
except Exception as e:
    logger.error(f"Failed to initialize file logger: {e}")

# Globals
current_state = "STATE_CONFIGURATION_GATHERING"
bootstrap_dag = None

# Phase knowledge documents already delivered to the agent in this server session.
# Deliberately process-scoped rather than persisted to the ledger: a durable record
# would outlive the context window that received the document, leaving the server
# refusing to re-send material the agent no longer holds. The process and the client
# context are lost together, so the process is the honest scope.
knowledge_served = set()

def init():
    global bootstrap_dag
    try:
        dir_path = os.path.dirname(os.path.realpath(__file__))
        file_path = os.path.join(dir_path, "bootstrap_dag.json")
        with open(file_path, "r") as f:
            bootstrap_dag = json.load(f)
        logger.debug(f"Loaded bootstrap DAG from {file_path}")
    except Exception as e:
        logger.error(f"Failed to load bootstrap DAG: {e}")
        raise

    # Validate at start-up so a malformed graph or an unresolvable content path
    # is a refusal to start, not a silently empty payload mid-migration.
    validate_dag(bootstrap_dag, "bootstrap_dag.json", REPO_ROOT)

    # The platform graph ships alongside the server and is what join_ledger
    # --reconfigure uploads, so it is validated here too rather than only when
    # a ledger session first reads it back.
    platform_path = os.path.join(dir_path, "platform_dag.json")
    try:
        with open(platform_path, "r") as f:
            validate_dag(json.load(f), "platform_dag.json", REPO_ROOT)
    except DagValidationError:
        raise
    except Exception as e:
        logger.error(f"Failed to load bundled platform DAG for validation: {e}")
        raise

    # The developer graph ships alongside the server too: join_ledger copies
    # it to workloads/<component>/dag.json at component initialization, so a
    # malformed third graph must be a start-up failure like the other two.
    developer_path = os.path.join(dir_path, "developer_dag.json")
    try:
        with open(developer_path, "r") as f:
            developer_dag = json.load(f)
        validate_dag(developer_dag, "developer_dag.json", REPO_ROOT)
    except DagValidationError:
        raise
    except Exception as e:
        logger.error(f"Failed to load bundled developer DAG for validation: {e}")
        raise

    # The upgrade table is part of the graph contract: a version bump without
    # its STATE_MAPPINGS entry would pass validation and every FRESH join, and
    # only explode on the first re-join of an existing component — discovered
    # post-release by locked-out developers. A missing entry is therefore a
    # refusal to start, exactly like a malformed graph.
    developer_version = str(developer_dag.get("version"))
    if developer_version not in workload_join_lib.STATE_MAPPINGS:
        raise DagValidationError(
            f"developer_dag.json version {developer_version!r} has no "
            "STATE_MAPPINGS entry in server/workload_join.py; every version "
            "bump must declare its state mapping (DESIGN 6.3)")

    # The blocker taxonomy is parsed out of the assessment knowledge document
    # rather than restated here, so a broken table has to be a refusal to start:
    # degrading to an empty taxonomy would leave the blocker gate accepting
    # anything.
    validate_blocker_criteria(REPO_ROOT)

    # Same arrangement for the artifact coverage map (landing-zone knowledge):
    # the ownership boundary the humans review is the one the machine reads,
    # and a table broken by an edit is a start-up failure, not a silently
    # empty map.
    validate_coverage_map(REPO_ROOT)
    # The landing-zone decision table and the ComputeClass family table are
    # machine-read the same way; a bad row refuses start-up like a bad map row.
    validate_decisions(REPO_ROOT)
    validate_family_table(REPO_ROOT)

    # And for the Ingress annotation disposition table (reference/
    # api-translation.md): the workload routing brief is built from it, so
    # a table broken by an edit would demote every annotation to the generic
    # open question instead of refusing to start.
    validate_api_translation(REPO_ROOT)

    # And for the knowledge documents delivered per translation-unit kind
    # (translator.FAMILY_KNOWLEDGE): a worker handed a raw Corefile with no
    # mapping would improvise one, which is the failure the document exists
    # to prevent.
    translator.validate_family_knowledge()
    # Same check for the workload side's per-fact documents
    # (wkld_translator.FACT_KNOWLEDGE): a worker handed a pod's dnsConfig
    # with no mapping would keep or drop its addresses by guess.
    wkld_translator.validate_fact_knowledge()


    # Debug credentials
    try:
        home = os.environ.get("HOME")
        logger.debug(f"DEBUG: HOME={home}")
        logger.debug(f"DEBUG: GOOGLE_APPLICATION_CREDENTIALS={os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')}")
        if home:
            adc_path = os.path.join(home, ".config/gcloud/application_default_credentials.json")
            logger.debug(f"DEBUG: ADC file exists at {adc_path}: {os.path.exists(adc_path)}")
    except Exception as e:
        logger.debug(f"DEBUG: Failed to log environment: {e}")

# Initialize FastMCP Server
mcp = FastMCP("gke-agentic-migration")
init()

# Register phase-owned tools. Each phase folder wires up its own steps so this
# file only carries bootstrapping/onboarding concerns.
discovery_phase.register(mcp)
translation_phase.register(mcp)
assessment_phase.register(mcp)
landingzone_phase.register(mcp)
deployment_phase.register(mcp)
workload_phase.register(mcp)
bootstrap_phase.register(mcp)

# Re-export phase step tools so existing callers (and tests) can keep
# referencing them through this module.
from servers.phases.discovery.discovery_livescan_0.tools import discover_and_dump_all_clusters
from servers.phases.discovery.discovery_init_1.tools import discover_configuration_files
from servers.phases.discovery.discovery_scope_2.tools import update_discovery_scope, confirm_discovery_scope
from servers.phases.discovery.discovery_datascan_3.tools import scan_data_dependencies
from servers.phases.discovery.discovery_datareview_3.tools import (
    list_data_dependencies, reject_data_consumer, attach_data_consumer,
    annotate_data_dependency, confirm_data_dependency, dismiss_data_dependency,
    add_data_dependency, confirm_data_dependencies)
from servers.phases.discovery.discovery_extract_3.tools import run_discovery_extraction, write_discovery_inventory
from servers.phases.assessment.assessment_review_1.tools import get_discovery_inventory, amend_discovery_scope, submit_assessment
from servers.phases.assessment.assessment_blockers_2.tools import list_blockers, assign_blocker_owner
from servers.phases.landingzone.landingzone_design_2.tools import resolve_lz_decision, finalize_landing_zone_design
from servers.phases.landingzone.landingzone_translationplan_3.tools import plan_translation
from servers.phases.landingzone.landingzone_planreview_4.tools import update_translation_plan, confirm_translation_plan
from servers.phases.translation.translation_translate_1.tools import run_translation
from servers.phases.translation.translation_translate_1.tools import UNIT_BLOB_PREFIX
from servers.phases.translation.translation_humanreview_2.tools import get_translation_results, request_unit_revision, skip_translation_units, approve_translation
from servers.phases.translation.translation_validate_3.tools import run_generated_validation
from servers.phases.translation.translation_validate_3 import ksa_contract
from servers.phases.deployment.deployment_provision_1.tools import deployment_export_inputs
from servers.phases.discovery.discovery_scope_2 import scope as discovery_scope_lib

# Image scan/render actions are phase-owned; the engine dispatches to them so
# any drain loop landing on the discovery image states works (main -> phase is
# the existing import direction, no cycle).
from servers.phases.discovery.discovery_init_1 import render as discovery_render

# --- Helper Functions ---

def read_repo_file(rel_path: str, what: str) -> Optional[str]:
    """Reads a repo-root-relative file, refusing any path that escapes REPO_ROOT.

    Returns None (and logs) on a confinement violation or read error, so a
    tampered graph cannot make the server read arbitrary local files back to
    the agent.
    """
    path = os.path.normpath(os.path.join(REPO_ROOT, rel_path))
    if not path.startswith(REPO_ROOT + os.sep):
        logger.error(f"Refusing to read {what} outside the repository: {rel_path}")
        return None

    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except OSError as e:
        logger.error(f"Failed to read {what} {rel_path}: {e}")
        return None

def load_phase_knowledge(state_def: dict) -> str:
    """Loads the phase knowledge documents a DAG state declares, skipping any
    already delivered in this server session.

    States name the documents they need via a "knowledge" array of file names
    resolved within the state's phase, e.g. phase "assessment" plus
    "migration-assessment.md" -> servers/phases/assessment/knowledge/migration-assessment.md.
    The array declares a requirement, not a request: a step cannot fail to
    receive what it names, but it also gets nothing an earlier step already
    pulled in.
    """
    docs = state_def.get("knowledge")
    phase = state_def.get("phase")
    if not docs or not phase:
        return ""

    # Both components are bare names, checked here rather than relying on the
    # REPO_ROOT confinement below to catch a traversal several segments deep.
    if os.path.basename(phase) != phase or phase in (os.curdir, os.pardir):
        logger.error(f"Ignoring knowledge for malformed phase name: {phase}")
        return ""

    sections = []
    for doc in docs:
        # Names are resolved within the phase's knowledge/ directory; a name
        # carrying path separators is a graph authoring error, not a lookup.
        if os.path.basename(doc) != doc:
            logger.error(f"Ignoring knowledge entry with a path separator: {doc}")
            continue

        key = f"{phase}/{doc}"
        if key in knowledge_served:
            logger.debug(f"Knowledge already served this session, skipping: {key}")
            continue

        content = read_repo_file(
            os.path.join("servers", "phases", phase, "knowledge", doc), "phase knowledge"
        )
        if content is None:
            continue

        knowledge_served.add(key)
        name = doc[:-3] if doc.endswith(".md") else doc
        sections.append(f"\n--- Phase knowledge: {name} ---\n{content}")

    return "".join(sections)

def load_step_instructions(state_def: dict) -> str:
    """Loads the step instructions markdown referenced by a DAG state, if any.

    States in the DAG reference their phase step folder via an "instructions"
    field holding a repo-root-relative path (e.g.
    "servers/phases/discovery/discovery_init_1/instructions.md").
    """
    rel_path = state_def.get("instructions")
    if not rel_path:
        return ""

    content = read_repo_file(rel_path, "step instructions")
    if content is None:
        return ""

    return f"\n--- Step instructions ---\n{content}"

def build_stage_payload(state_name: str, state_def: dict) -> str:
    """Assembles the get_next_stage response: the state line, then any
    undelivered phase knowledge, then the step instructions.

    Pure text concatenation — no templating and no variable interpolation.
    Anything state-dependent the instructions need is either already in the
    ledger variables the agent can query, or is passed as a tool argument.
    """
    return (
        f"Current state: {state_name}. Type: {state_def['type']}."
        + load_phase_knowledge(state_def)
        + load_step_instructions(state_def)
    )

# --- MCP Tool Handlers ---

@mcp.tool()
def get_next_stage() -> str:
    """Get the next stage of the DAG"""
    global current_state, bootstrap_dag
    
    if state_mgr.active_config_path():
        logger.debug("Active ledger config found. Rehydrating state from GCS.")
        try:
            cached = state_mgr.read_local_config()
        except Exception as e:
            return f"ERROR: Failed to read the session cache: {e}"

        if cached.get("resolved_role") == "developers":
            # Developer routing: the session's
            # component graph, from the ledger copy, claimant enforced.
            try:
                state_dict, _, config = state_mgr.authorize_and_rehydrate_workload(None)
                bucket = state_mgr.gcs_client.bucket(get_bucket_name(config["ledger_uri"]))
                component = config["component"]
                developer_dag = load_dag(bucket, f"workloads/{component}/dag.json")
            except Exception as e:
                return f"ERROR: Failed to rehydrate ledger state: {e}"

            cur_state = state_dict["current_state"]
            state_def = developer_dag["states"].get(cur_state)
            if not state_def:
                return f"ERROR: Unknown state in developer DAG for component '{component}': {cur_state}"
            # No review-frontend announcement on the developer path: the UI is
            # hardwired to the platform pipeline (platform_dag.json,
            # platform/*), which a developer's ledger IAM cannot read — and a
            # de-escalated admin must not have platform state rendered into a
            # developer session either. A workload-aware view is [PLANNED].
            return build_stage_payload(cur_state, state_def)

        try:
            state_dict, _, config = authorize_and_rehydrate(None)
            bucket_name = get_bucket_name(config["ledger_uri"])
            bucket = state_mgr.gcs_client.bucket(bucket_name)
            platform_dag = load_dag(bucket, "platform_dag.json")
        except Exception as e:
            return f"ERROR: Failed to rehydrate ledger state: {e}"

        cur_state = state_dict["current_state"]
        state_def = platform_dag["states"].get(cur_state)
        if not state_def:
            return f"ERROR: Unknown state in platform DAG: {cur_state}"
        return build_stage_payload(cur_state, state_def) + frontend_launcher.announcement()
    else:
        logger.debug("No active ledger config. Using in-memory bootstrap DAG.")
        if bootstrap_dag is None:
            raise ValueError("bootstrap DAG not initialized")

        state_def = bootstrap_dag["states"].get(current_state)
        if not state_def:
            raise ValueError(f"unknown state: {current_state}")

        return build_stage_payload(current_state, state_def)

@mcp.tool()
def bootstrap_migration() -> str:
    """Unconditionally reset the state machine to the initial state of the bootstrapping DAG"""
    global current_state, bootstrap_dag
    logger.debug("bootstrap_migration called")

    if bootstrap_dag is None:
        raise ValueError("bootstrap DAG not initialized")

    # Remove the scoped cache and the legacy fallback: leaving the legacy file
    # behind would make this session silently rejoin the old workspace.
    for cfg_path in (state_mgr.scoped_config_path(), state_mgr.LEDGER_CONFIG_PATH):
        if os.path.exists(cfg_path):
            try:
                os.remove(cfg_path)
                logger.debug(f"Removed active ledger config at {cfg_path} during bootstrap reset.")
            except Exception as e:
                logger.error(f"Failed to remove active ledger config: {e}")

    # Rewinding the graph replays steps the agent has already seen, so the
    # knowledge they declare must be deliverable again.
    knowledge_served.clear()
    # The previous bootstrap's workspace is no longer this session's to administer.
    bootstrap_phase.clear_bootstrapped()

    current_state = bootstrap_dag["start_state"]
    logger.debug(f"DAG state reset to {current_state}")
    return f"DAG state reset to {current_state}"

@mcp.tool()
async def join_ledger(ledger_uri: str, role: Optional[str] = None, reconfigure: bool = False,
                      component: Optional[str] = None, reclaim_component: bool = False) -> str:
    """Join an existing workspace ledger and rehydrate the execution state.

    Args:
        ledger_uri: The GCS ledger bucket URI (e.g. gs://migration-ledger)
        role: Optional role to assume (to de-escalate access)
        reconfigure: Reset the DAG state back to config gathering (admins only)
        component: For developer joins — the workload component id to claim
            (a lowercase slug, e.g. 'orders-component'). Required when the
            resolved role is developer; the session routes to that
            component's graph.
        reclaim_component: Deliberate takeover of a mid-flight component
            claimed by someone else (recorded in the component history).
    """
    logger.info(f"join_ledger called with URI: {ledger_uri}, role: {role}, reconfigure: {reconfigure}, "
                f"component: {component}, reclaim_component: {reclaim_component}")
    
    user_email = state_mgr.get_authenticated_user_email()
    bucket_name = get_bucket_name(ledger_uri)
    
    if not state_mgr.gcs_client:
        # To download the workspace registry, we must instantiate a GCS client.
        # Since we do not know the actual project ID yet, we use a placeholder project
        # to satisfy the Client constructor. GCS does not validate the billing project ID
        # for simple object reads unless Requester Pays is enabled.
        project_id = os.environ.get("GOOGLE_CLOUD_PROJECT") or "gke-migration-placeholder-project"
        logger.debug(f"Initializing GCS client in join_ledger with project: {project_id}")
        state_mgr.gcs_client = storage.Client(project=project_id)
        
    bucket = state_mgr.gcs_client.bucket(bucket_name)
    blob_registry = bucket.blob("workspace_registry.yaml")
    
    try:
        registry_data = blob_registry.download_as_text()
    except exceptions.NotFound:
        return f"ERROR: Workspace registry not found in the ledger bucket '{ledger_uri}'. Make sure bootstrapping was run."
    except Exception as e:
        return f"ERROR: Failed to read registry from GCS: {e}"
        
    registry = yaml.safe_load(registry_data)
    workspace_name = registry.get("workspace_name")
    gcp_project = registry.get("gcp_project")
    
    # Resolve user role
    registered_role = None
    roles = registry.get("roles", {})
    for role_name, emails in roles.items():
        if user_email in emails:
            registered_role = role_name
            break
            
    if not registered_role:
        return f"ERROR: Access Denied. User '{user_email}' is not registered in this workspace."
        
    if registered_role == "platform_engineers":
        registered_role = "platform"
        
    resolved_role = registered_role
    if role:
        if role == "platform_engineers":
            role = "platform"
        try:
            state_mgr.check_role_compatibility(role, registered_role)
            resolved_role = role
        except PermissionError as e:
            return f"ERROR: {e}"
        except ValueError as e:
            return f"ERROR: {e}"
            
    if resolved_role == "developers":
        # The developer path: claim a component and route this session to
        # its per-component graph. Registered developers land here, as do
        # admins/platform engineers de-escalating via role="developers".
        return await _join_developer(
            ledger_uri, bucket_name, workspace_name, gcp_project, user_email,
            reconfigure, component, reclaim_component)

    if resolved_role not in ["admins", "platform"]:
        return f"ERROR: Access Denied. Role '{resolved_role}' is not authorized to execute platform onboarding."

    if component or reclaim_component:
        return ("ERROR: component/reclaim_component apply to developer joins only. "
                "De-escalate with role='developers' to claim a component.")

    # Cache session locally
    state_mgr.write_local_config(ledger_uri, resolved_role, workspace_name, gcp_project)

    # Spawn/probe the review frontend off-thread: announcement() does blocking
    # socket + HTTP probes and waits up to ~20s for the server to come up, which
    # would otherwise stall this async handler's event loop.
    frontend_note = await asyncio.to_thread(frontend_launcher.announcement, force=True)

    if resolved_role == "admins" and not reconfigure:
         return (
             f"Successfully joined ledger '{ledger_uri}'.\n"
             f"Workspace: {workspace_name}\n"
             f"Project: {gcp_project}\n"
             f"Assumed Role: admin\n"
             f"Status: Workspace joined successfully. No admin onboarding tasks pending."
             + frontend_note
         )
    
    state_blob_path = "platform/onboarding/state.json"
    state_mgr.gcs_client = storage.Client(project=gcp_project)
    bucket = state_mgr.gcs_client.bucket(bucket_name)
    blob_state = bucket.blob(state_blob_path)
    
    try:
        blob_state.reload()
        state_exists = True
    except exceptions.NotFound:
        state_exists = False
        
    if not state_exists:
        if reconfigure:
            return "ERROR: Onboarding state file does not exist, cannot reconfigure."
            
        try:
            platform_dag = load_dag(bucket, "platform_dag.json")
        except ValueError as e:
            return f"ERROR: {e}"
            
        new_state = {
            "current_state": platform_dag["start_state"],
            "history": [],
            "variables": {}
        }
        data = json.dumps(new_state, indent=2)
        try:
            blob_state.upload_from_string(data, content_type="application/json", if_generation_match=0)
            logger.info("Initialized platform onboarding state in GCS.")
            current_dag_state = platform_dag["start_state"]
        except exceptions.PreconditionFailed:
            blob_state.reload()
            state_data = blob_state.download_as_text()
            state_dict = json.loads(state_data)
            current_dag_state = state_dict["current_state"]
    else:
        state_data = blob_state.download_as_text()
        state_dict = json.loads(state_data)
        
        if reconfigure:
            if resolved_role != "admins":
                return "ERROR: Access Denied: Only administrators are authorized to reset onboarding configurations."
                
            try:
                write_platform_state_machine(ledger_uri, workspace_name)
                platform_dag = load_dag(bucket, "platform_dag.json")
            except Exception as e:
                return f"ERROR: Failed to update platform DAG: {e}"
                
            state_dict["current_state"] = platform_dag["start_state"]
            data = json.dumps(state_dict, indent=2)
            try:
                blob_state.upload_from_string(data, content_type="application/json", if_generation_match=blob_state.generation)
                logger.info("Reset platform onboarding state in GCS.")
                current_dag_state = platform_dag["start_state"]
            except exceptions.PreconditionFailed:
                return "ERROR: Concurrent update conflict during reconfiguration reset. Please try again."
        else:
            current_dag_state = state_dict["current_state"]
            
    return (
        f"Successfully joined ledger '{ledger_uri}'.\n"
        f"Workspace: {workspace_name}\n"
        f"Project: {gcp_project}\n"
        f"Assumed Role: {resolved_role}\n"
        f"Current Onboarding State: {current_dag_state}"
        + frontend_note
    )


def _load_bundled_developer_dag() -> tuple[dict, str]:
    """(parsed graph, raw text) of the bundled developer_dag.json."""
    path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "developer_dag.json")
    with open(path, "r") as f:
        raw = f.read()
    return json.loads(raw), raw


async def _join_developer(ledger_uri: str, bucket_name: str, workspace_name: str,
                          gcp_project: str, user_email: str, reconfigure: bool,
                          component: Optional[str], reclaim_component: bool) -> str:
    """The developer leg of join_ledger: component init, claim, upgrade, routing cache.

    Kept out of join_ledger so the platform/admin path stays untouched. The
    claim mechanics live in _claim_component; the pure decisions live in
    server/workload_join.py.
    """
    if reconfigure:
        return ("ERROR: reconfigure is an admin-only operation on the platform "
                "graph; a developer join cannot use it.")
    if not component:
        return ("ERROR: A developer join claims a component — pass "
                f"component=<id>. Naming rule: {workload_join_lib.SLUG_RULE}. "
                "After joining you can browse the component seed index with "
                "browse_component_seed to agree the component's files.")
    slug_error = workload_join_lib.validate_component_id(component)
    if slug_error:
        return f"ERROR: {slug_error}"

    state_mgr.gcs_client = storage.Client(project=gcp_project)
    bucket = state_mgr.gcs_client.bucket(bucket_name)
    bundled_dag, bundled_raw = _load_bundled_developer_dag()

    try:
        status_line, current_state, notes, new_claim = _claim_component(
            bucket, bundled_dag, bundled_raw, component, user_email, reclaim_component)
    except exceptions.Forbidden:
        # The managed-folder binding is missing (D11): joined-but-blocked is
        # NOT an error, and the cache IS written — a re-join after the grant
        # resumes cleanly from this working directory.
        state_mgr.write_local_config(ledger_uri, "developers", workspace_name,
                                     gcp_project, component=component)
        step = workload_join_lib.admin_binding_step(bucket_name, component, user_email)
        return (
            f"Joined ledger '{ledger_uri}' as developer for component "
            f"'{component}', but ledger writes are blocked: GCS denied access "
            f"to workloads/{component}/ (claim status: blocked on "
            f"managed-folder binding).\n{step}\n"
            "Re-run join_ledger once the grant is in place; the claim resumes cleanly."
        )
    except ValueError as e:
        return f"ERROR: {e}"

    if status_line == "refused":
        # notes[-1] names the current claimant and the reclaim path. The
        # session cache is NOT written: this session holds no claim.
        return f"ERROR: {notes[-1]}"

    # No review-frontend spawn/announcement for developer sessions: the UI
    # renders the platform pipeline only (see the get_next_stage developer
    # branch for the full rationale).
    state_mgr.write_local_config(ledger_uri, "developers", workspace_name,
                                 gcp_project, component=component)

    lines = [
        f"Successfully joined ledger '{ledger_uri}'.",
        f"Workspace: {workspace_name}",
        f"Project: {gcp_project}",
        "Assumed Role: developer",
        f"Component: {component}",
        f"Claim: {status_line}",
        f"Current Component State: {current_state}",
    ]
    lines.extend(f"Note: {n}" for n in notes)
    if new_claim:
        # Reported on every new claim: the server cannot verify the
        # managed-folder grant exists, so the step rides with the claim (D11).
        lines.append(workload_join_lib.admin_binding_step(
            bucket_name, component, user_email))
    return "\n".join(lines)


def _read_plan_guard(bucket, component: str):
    """The has_unparkable_units guard fact: a parked unit in
    workloads/<component>/plan.json AND a published attach point in
    exports.json. None when the plan is absent or unreadable — the pure
    applier then keeps the state and appends the visible warning (never
    re-enter blindly). Unreadable exports is not unknowable: no exports means
    no attach point, so nothing can unpark and the guard is False."""
    return ledger_admin.read_plan_guard(bucket, component)


def _claim_component(bucket, bundled_dag: dict, bundled_raw: str, component: str,
                     email: str, reclaim: bool) -> tuple[str, str, list, bool]:
    """Initializes or claims workloads/<component>/, all under preconditions.

    Returns (status_line, current_state, notes, new_claim). status_line is
    "refused" with the refusal message in notes[-1] when a mid-flight claim
    is rejected. One retry on 412 (re-read, re-decide); the second conflict
    aborts. exceptions.Forbidden propagates to the caller (the
    blocked-on-binding response).
    """
    state_path = f"workloads/{component}/state.json"
    dag_path = f"workloads/{component}/dag.json"
    for attempt in (1, 2):
        blob_state = bucket.blob(state_path)
        try:
            blob_state.reload()
            state_dict = json.loads(blob_state.download_as_text())
            generation = blob_state.generation
        except exceptions.NotFound:
            state_dict, generation = None, None

        if state_dict is None:
            # Fresh component: copy the bundled graph (create-only, swallow
            # the 412 — the registry-write idiom), then seed the state.
            try:
                bucket.blob(dag_path).upload_from_string(
                    bundled_raw, content_type="application/json", if_generation_match=0)
            except exceptions.PreconditionFailed:
                pass
            seeded = workload_join_lib.initial_state_dict(bundled_dag, component, email)
            try:
                blob_state.upload_from_string(
                    json.dumps(seeded, indent=2), content_type="application/json",
                    if_generation_match=0)
                return "new claim", seeded["current_state"], [], True
            except exceptions.PreconditionFailed:
                continue  # the initial-claim race is real: re-read, take the existing path

        # Existing component: upgrade the graph copy if the bundle is newer,
        # then run the claim decision.
        notes = []
        try:
            copy_dag = json.loads(bucket.blob(dag_path).download_as_text())
        except exceptions.NotFound:
            copy_dag = {"version": "0.0"}
            notes.append("the ledger dag.json copy was missing; restored from the bundle")

        # The claim is classified against the state AS STORED, before any
        # mapping runs. A guarded hop can change the classification of the
        # very population it targets — the v0.4 DONE -> TRANSLATE re-entry
        # turns a shipped ("done", handover) component into a "midflight"
        # one, so a second developer was refused with "mid-flight and claimed
        # by X" for work nobody was doing, instead of taking the deliberate
        # handover path decide_claim spells out for a terminal component.
        state_class = workload_join_lib.classify_state(bundled_dag, state_dict)
        recorded = ((state_dict.get("variables") or {}).get("claim") or {}).get("claimant")
        decision, message = workload_join_lib.decide_claim(
            recorded, email, state_class, reclaim)

        # Guard facts are read from the persisted plan.json + exports.json
        # ONLY when a guarded mapping entry is actually consulted for this
        # state (the v0.4 DONE -> TRANSLATE re-entry for unparkable units).
        guard_facts, reentered = None, False
        if workload_join_lib.needs_guard_facts(bundled_dag, copy_dag, state_dict):
            guard_facts = {"has_unparkable_units":
                           _read_plan_guard(bucket, component)}
        state_dict, upgraded, upgrade_notes = workload_join_lib.upgrade_component_state(
            bundled_dag, copy_dag, state_dict, guard_facts)
        notes.extend(upgrade_notes)
        if upgraded:
            notes.append(
                f"developer DAG upgraded v{copy_dag.get('version')} -> "
                f"v{bundled_dag.get('version')}")
        else:
            # No version hop: the bundled version's guarded entries are still
            # standing re-entry rules, re-evaluated on every join.
            state_dict, reentered, reentry_notes = \
                workload_join_lib.apply_standing_guards(
                    bundled_dag, state_dict, guard_facts)
            notes.extend(reentry_notes)

        # The claim mutation applies only when allowed; the graph upgrade is
        # orthogonal and persists even on a refusal — otherwise a future
        # version bump would leave an upgraded dag.json beside an unmapped
        # state.json (the mapping only runs while copy version < bundled).
        new_claim = decision in ("allow", "takeover") and recorded != email
        if new_claim:
            variables = state_dict.setdefault("variables", {})
            variables.setdefault("component", component)
            variables["claim"] = {"claimant": email,
                                  "claimed_at": datetime.now(timezone.utc).isoformat()}
            history_line = (message if decision == "takeover" else
                            f"Component {component} claimed by {email} ({message})")
            state_dict.setdefault("history", []).append(history_line)

        if new_claim or upgraded or reentered:
            try:
                blob_state.upload_from_string(
                    json.dumps(state_dict, indent=2), content_type="application/json",
                    if_generation_match=generation)
            except exceptions.PreconditionFailed:
                if attempt == 1:
                    continue  # re-read and re-decide once
                raise ValueError(
                    "Concurrent update conflict while recording the claim. "
                    "Please retry join_ledger.")

        if upgraded:
            # dag.json is overwritten strictly AFTER the mapped state.json
            # commits. The upgrade only runs while copy_version <
            # bundled_version, so the reverse order would let a failure
            # between the two writes strand the component: a new-version copy
            # beside an unmapped state that no later join could ever repair.
            # State-first, any failure here leaves the copy old and the next
            # join re-runs the whole upgrade (re-mapping an already-mapped
            # state is a no-op plus a duplicate history line). Deterministic
            # bundled bytes: concurrent upgraders write the same content, so
            # this write carries no precondition.
            bucket.blob(dag_path).upload_from_string(
                bundled_raw, content_type="application/json")

        if decision == "refuse":
            return "refused", state_dict["current_state"], notes + [message], False
        status_line = (f"takeover from {recorded}" if decision == "takeover"
                       else message)
        return status_line, state_dict["current_state"], notes, new_claim

    raise ValueError(
        "Concurrent update conflict while recording the claim. Please retry join_ledger.")

@mcp.tool()
async def configure_repositories(
    ctx: Context,
    source_repo_url: str,
    source_branch: str,
    source_path: str,
    target_branch: str,
    target_path: str = "/",
    target_repo_url: Optional[str] = None,
    ssm_instance: Optional[str] = None,
    ssm_location: Optional[str] = None,
    ssm_repository: Optional[str] = None
) -> str:
    """Configures source/target repository parameters and runs state verification checks."""
    logger.info("configure_repositories called.")
    
    if not source_repo_url or not source_branch or not target_branch:
        return "ERROR: Missing required configuration parameters."
        
    if not target_repo_url and not (ssm_instance and ssm_location and ssm_repository):
        return "ERROR: Must provide either target_repo_url or all SSM details (ssm_instance, ssm_location, ssm_repository)."

    try:
        state_dict, generation, config = authorize_and_rehydrate(None)
    except PermissionError as e:
        return f"ERROR: {e}"
    except ValueError as e:
        return f"ERROR: {e}"
        
    project_id = config["gcp_project"]
    
    if ssm_instance and ssm_location and ssm_repository and not target_repo_url:
        client = ssm_client.SSMClient()
        try:
            repo = client.get_repository_by_details(project_id, ssm_location, ssm_instance, ssm_repository)
            if repo:
                target_repo_url = repo.uris.git_https
                logger.debug(f"Resolved SSM repository Git URL dynamically: {target_repo_url}")
        except Exception as e:
            return f"ERROR: Failed to query SSM repository details: {e}"
            
    state_dict["variables"].update({
        "source_repo_url": source_repo_url,
        "source_branch": source_branch,
        "source_path": source_path or "",
        "target_repo_url": target_repo_url,
        "target_branch": target_branch,
        "target_path": target_path or "",
        "ssm_instance": ssm_instance,
        "ssm_location": ssm_location,
        "ssm_repository": ssm_repository
    })
    
    bucket_name = get_bucket_name(config["ledger_uri"])
    bucket = state_mgr.gcs_client.bucket(bucket_name)
    try:
        platform_dag = load_dag(bucket, "platform_dag.json")
    except ValueError as e:
        return f"ERROR: {e}"
        
    current_state = state_dict["current_state"]
    
    if current_state == "STATE_CONFIGURE_REPOSITORIES":
        state_def = platform_dag["states"][current_state]
        next_state = state_def["transitions"]["on_tool_call_received"]
        
        state_dict["history"].append(f"Transitioned {current_state} -> {next_state} via configure_repositories")
        state_dict["current_state"] = next_state
        
    elif current_state == "STATE_CREATE_SSM_REPOSITORY":
        pass
    else:
        return f"ERROR: Invalid state for configure_repositories: {current_state}"
    
    transitions_run, message, error = await run_dispatch_loop(
        ctx, state_dict, platform_dag, config, "Repository configuration saved.")
    if error:
        return error

    # Save back to GCS
    bucket_name = get_bucket_name(config["ledger_uri"])
    bucket = state_mgr.gcs_client.bucket(bucket_name)
    blob_state = bucket.blob("platform/onboarding/state.json")
    
    data = json.dumps(state_dict, indent=2)
    try:
        blob_state.upload_from_string(data, content_type="application/json", if_generation_match=generation)
    except exceptions.PreconditionFailed:
        return "ERROR: Concurrent update conflict during repository configuration. Your changes were not saved."
        
    output = f"Onboarding State Update:\n- Transition log: {', '.join(transitions_run) or 'none'}\n- Current State: {state_dict['current_state']}\n- Message: {message}\n"
    if state_dict["current_state"] == "STATE_COMPLETED":
        output += "\nSUCCESS: Platform onboarding completed successfully!"
    elif state_dict["current_state"] == "STATE_CONFIGURE_REPOSITORIES":
        output += "\nFAILURE: Configuration validation failed. Please review error messages and try again."
        
    return output

# --- Internal Mutation Evaluators ---

def run_internal_mutation(action: str, variables: dict, config: dict) -> tuple[str, str]:
    logger.info(f"Running internal mutation action: {action}")
    if action == "verify_repositories":
        return action_verify_repositories(variables, config)
    elif action == "check_ssm_registration":
        return action_check_ssm_registration(variables, config)
    elif action == "create_ssm_repository":
        return action_create_ssm_repository(variables, config)
    elif action == "register_ledger_member":
        return action_register_ledger_member(variables, config)
    # Phase-owned actions. The engine dispatches to them so the drain loop can
    # cross a phase's internal states without main.py knowing what they do.
    elif action in landingzone_actions.ACTIONS:
        return landingzone_actions.ACTIONS[action](variables, config)
    elif action in discovery_render.ACTIONS:
        return discovery_render.ACTIONS[action](variables, config)
    elif action in deployment_actions.ACTIONS:
        return deployment_actions.ACTIONS[action](variables, config)
    elif action in workload_actions.ACTIONS:
        return workload_actions.ACTIONS[action](variables, config)
    else:
        raise ValueError(f"Unknown internal mutation action: {action}")

# The dispatch walk owns the graph but not the actions, which reach out to SSM,
# git and terraform and so belong here. Handing it the dispatcher closes the loop
# without main and dispatch importing each other.
dispatch.set_mutation_runner(run_internal_mutation)

def action_verify_repositories(variables: dict, config: dict) -> tuple[str, str]:
    source_url = variables.get("source_repo_url")
    source_branch = variables.get("source_branch")
    source_path = variables.get("source_path")
    target_url = variables.get("target_repo_url")
    target_branch = variables.get("target_branch")
    target_path = variables.get("target_path")
    
    # Path disjoint validation
    if source_url == target_url and source_branch == target_branch:
        s_path = source_path.strip("/")
        t_path = target_path.strip("/")
        if s_path == t_path or s_path.startswith(t_path + "/") or t_path.startswith(s_path + "/"):
            return "on_failure", "Source and Target repository paths must be disjoint if sharing branch."
            
    logger.debug(f"Verifying source repository paths: {source_url} [{source_branch}] -> {source_path}")
    source_exists = git_client.verify_git_path_exists(source_url, source_branch, source_path)
    if not source_exists:
        return "on_failure", f"Source repository directory '{source_path}' does not exist on branch '{source_branch}'."
        
    write_ok = False
    if target_url:
        # The probe commit is made as the user, like the PR commits.
        user_email = config.get("user_email")
        if not user_email:
            return "on_failure", (
                "Internal error: no caller identity on the session config (the "
                "session resolver always sets one), so the write check cannot "
                "make its probe commit as you. Nothing was checked.")
        logger.debug(f"Verifying target repository write access: {target_url} [{target_branch}]")
        write_ok = git_client.verify_git_write_permission(
            target_url, target_branch, user_email=user_email)
    else:
        logger.debug("Target repository URL is None. Skipping write access check (SSM repository will be created).")
        
    variables["target_write_permission_verified"] = write_ok
    return "on_success", "Source paths and target connection verified."

def action_check_ssm_registration(variables: dict, config: dict) -> tuple[str, str]:
    ssm_instance = variables.get("ssm_instance")
    ssm_location = variables.get("ssm_location")
    ssm_repository = variables.get("ssm_repository")
    project_id = config["gcp_project"]
    
    client = ssm_client.SSMClient()
    
    if ssm_instance and ssm_location and ssm_repository:
        try:
            repo = client.get_repository_by_details(project_id, ssm_location, ssm_instance, ssm_repository)
            if repo:
                variables["target_repo_url"] = repo.uris.git_https
                return "on_success", "Target SSM repository exists."
            else:
                inst = client.get_instance_by_details(project_id, ssm_location, ssm_instance)
                variables["ssm_instance_needed"] = (inst is None or inst.state != securesourcemanager_v1.Instance.State.ACTIVE)
                return "on_ssm_creation_required", "Target SSM repository must be created. Requests approval."
        except Exception as e:
            return "on_failure", f"Failed to check SSM repository: {e}"
            
    target_url = variables.get("target_repo_url")
    if not target_url:
        return "on_failure", "Target repository URL or SSM details not provided."
        
    try:
        comps = ssm_client.parse_ssm_url(target_url)
        is_ssm = True
        logger.debug(f"Target repository {target_url} is a Secure Source Manager repository.")
    except ValueError:
        is_ssm = False
        logger.debug(f"Target repository {target_url} is a standard Git repository (non-SSM).")
        
    if is_ssm:
        try:
            repo = client.get_repository_by_details(comps["project"], comps["location"], comps["instance"], comps["repository"])
            if repo:
                variables["target_repo_url"] = repo.uris.git_https
                return "on_success", "Target SSM repository exists."
            else:
                inst = client.get_instance_by_details(comps["project"], comps["location"], comps["instance"])
                variables["ssm_instance_needed"] = (inst is None or inst.state != securesourcemanager_v1.Instance.State.ACTIVE)
                return "on_ssm_creation_required", "Target SSM repository must be created. Requests approval."
        except Exception as e:
            return "on_failure", f"Failed to check SSM repository: {e}"
    else:
        logger.debug("Standard Git repository detected. Skipping SSM checks.")
        return "on_success", "Standard Git repository ready for verification."

def clean_up_lro_variables(variables: dict):
    variables.pop("ssm_creation_lro", None)
    variables.pop("ssm_creation_type", None)
    variables.pop("ssm_creation_start_time", None)
    variables.pop("ssm_polling_instance", None)

def action_create_ssm_repository(variables: dict, config: dict) -> tuple[str, str]:
    ssm_instance = variables.get("ssm_instance")
    ssm_location = variables.get("ssm_location")
    ssm_repository = variables.get("ssm_repository")
    project_id = config["gcp_project"]
    
    if not (ssm_instance and ssm_location and ssm_repository):
        target_url = variables.get("target_repo_url")
        if not target_url:
            return "on_failure", "Target URL not set for repository creation."
        try:
            comps = ssm_client.parse_ssm_url(target_url)
            project_id = comps["project"]
            ssm_location = comps["location"]
            ssm_instance = comps["instance"]
            ssm_repository = comps["repository"]
        except ValueError as e:
            return "on_failure", f"Invalid target repository URL: {e}"
            
    start_time = variables.get("ssm_creation_start_time")
    if not start_time:
        start_time = time.time()
        variables["ssm_creation_start_time"] = start_time
        
    creation_timeout = config.get("ssm_creation_timeout", 10800.0)
    elapsed = time.time() - start_time
    if elapsed > creation_timeout:
        timeout_min = int(creation_timeout / 60)
        logger.error(f"SSM creation timed out after {timeout_min} minutes. Elapsed: {elapsed:.1f}s")
        clean_up_lro_variables(variables)
        return "on_failure", f"Secure Source Manager creation timed out after {timeout_min} minutes."

    client = ssm_client.SSMClient()
    instance_url = f"https://console.cloud.google.com/secure-source-manager/locations/{ssm_location}/instances/{ssm_instance}?project={project_id}"
    repo_url = f"https://console.cloud.google.com/secure-source-manager/locations/{ssm_location}/instances/{ssm_instance}/repositories/{ssm_repository}?project={project_id}"
    
    poll_timeout = config.get("ssm_poll_timeout", 30.0)
    poll_interval = config.get("ssm_poll_interval", 5.0)
    start_poll_time = time.time()
    
    just_triggered_instance = False
    just_triggered_repo = False

    try:
        while True:
            if variables.get("ssm_instance_needed", False):
                if variables.get("ssm_polling_instance", False):
                    # We are polling instance state directly (no LRO or lost LRO)
                    inst = client.get_instance_by_details(project_id, ssm_location, ssm_instance)
                    if inst and inst.state == securesourcemanager_v1.Instance.State.ACTIVE:
                        logger.info(f"SSM instance '{ssm_instance}' became ACTIVE.")
                        variables["ssm_instance_needed"] = False
                        variables.pop("ssm_polling_instance", None)
                        continue
                    elif inst and inst.state == securesourcemanager_v1.Instance.State.CREATING:
                        # Still creating, keep polling
                        pass
                    else:
                        clean_up_lro_variables(variables)
                        state_name = inst.state.name if inst else "None"
                        return "on_failure", f"GSSM instance is in unusable state: {state_name}"
                    # Otherwise still creating, continue to poll sleep
                else:
                    lro_name = variables.get("ssm_creation_lro")
                    if not lro_name:
                        # Check if instance already exists in CREATING state
                        inst = client.get_instance_by_details(project_id, ssm_location, ssm_instance)
                        if inst:
                            if inst.state == securesourcemanager_v1.Instance.State.ACTIVE:
                                variables["ssm_instance_needed"] = False
                                continue
                            elif inst.state == securesourcemanager_v1.Instance.State.CREATING:
                                logger.info(f"SSM instance '{ssm_instance}' already exists and is CREATING. Starting direct state polling.")
                                variables["ssm_polling_instance"] = True
                                continue
                            else:
                                return "on_failure", f"SSM instance exists in unexpected state: {inst.state.name}"
                        
                        # Trigger creation
                        lro_name = client.trigger_create_instance_by_details(project_id, ssm_location, ssm_instance)
                        variables["ssm_creation_lro"] = lro_name
                        variables["ssm_creation_type"] = "instance"
                        logger.info(f"Started GSSM instance creation LRO: {lro_name}")
                        just_triggered_instance = True
                    else:
                        # Poll LRO
                        done, err_msg, _ = client.get_operation_status(ssm_location, lro_name)
                        if done:
                            if err_msg:
                                clean_up_lro_variables(variables)
                                return "on_failure", f"GSSM instance creation failed: {err_msg}"
                            
                            logger.info(f"SSM instance '{ssm_instance}' created successfully.")
                            variables.pop("ssm_creation_lro", None)
                            variables.pop("ssm_creation_type", None)
                            variables["ssm_instance_needed"] = False
                            continue
            else:
                # 2. Repository creation step
                lro_name = variables.get("ssm_creation_lro")
                if not lro_name:
                    lro_name = client.trigger_create_repository_by_details(project_id, ssm_location, ssm_instance, ssm_repository)
                    variables["ssm_creation_lro"] = lro_name
                    variables["ssm_creation_type"] = "repository"
                    logger.info(f"Started GSSM repository creation LRO: {lro_name}")
                    just_triggered_repo = True
                else:
                    done, err_msg, _ = client.get_operation_status(ssm_location, lro_name)
                    if done:
                        if err_msg:
                            clean_up_lro_variables(variables)
                            return "on_failure", f"GSSM repository creation failed: {err_msg}"
                        
                        repo = client.get_repository_by_details(project_id, ssm_location, ssm_instance, ssm_repository)
                        if not repo:
                            clean_up_lro_variables(variables)
                            return "on_failure", f"GSSM repository '{ssm_repository}' created but could not retrieve details."
                            
                        variables["target_repo_url"] = repo.uris.git_https
                        clean_up_lro_variables(variables)
                        return "on_success", "SSM Repository created successfully."

            # If we reached here, the active operation is still pending.
            # Check if we have spent our poll_timeout during this execution.
            elapsed_poll = time.time() - start_poll_time
            if elapsed_poll >= poll_timeout:
                creation_type = variables.get("ssm_creation_type", "resource")
                lro_name = variables.get("ssm_creation_lro")
                if variables.get("ssm_polling_instance", False):
                    logger.info(f"GSSM instance provisioning still pending (polling state directly). Returning to caller after {elapsed_poll:.1f}s.")
                    return "on_pending", f"GSSM instance is still provisioning. Track progress in GCP Console: {instance_url}"
                elif just_triggered_instance:
                    return "on_pending", f"Started GSSM instance creation. LRO: {lro_name}. Track progress in GCP Console: {instance_url}"
                elif just_triggered_repo:
                    return "on_pending", f"GSSM instance is active. Started GSSM repository creation. LRO: {lro_name}. Track progress in GCP Console: {repo_url}"
                else:
                    url = repo_url if creation_type == "repository" else instance_url
                    logger.info(f"GSSM {creation_type} LRO {lro_name} still pending. Returning to caller after {elapsed_poll:.1f}s.")
                    return "on_pending", f"GSSM {creation_type} is still provisioning. LRO: {lro_name}. Track progress in GCP Console: {url}"

            logger.info(f"GSSM LRO still pending. Sleeping {poll_interval}s before next poll...")
            just_triggered_instance = False
            just_triggered_repo = False
            time.sleep(poll_interval)
            
    except Exception as e:
        logger.exception("Failed during GSSM creation LRO process")
        clean_up_lro_variables(variables)
        return "on_failure", f"SSM repository creation failed: {e}"

# --- Existing Bootstrapping Handlers ---

@mcp.tool()
async def update_orchestrator_task(dummy: str = "") -> str:
    """Update the status of an orchestrator task"""
    return "task updated"

@mcp.tool()
async def get_orchestrator_state(dummy: str = "") -> str:
    """Get the current orchestrator state"""
    return "state retrieved"

@mcp.tool()
async def acquire_lock(dummy: str = "") -> str:
    """Acquire workspace lock"""
    return "lock acquired"

@mcp.tool()
async def release_lock(dummy: str = "") -> str:
    """Release workspace lock"""
    return "lock released"

@mcp.tool()
async def refresh_exports(ctx: Context = None) -> str:
    """Recomputes every derivable exports.json field and republishes it.

    exports.json at the ledger root is the single platform→developer channel.
    The completion hooks publish it as pipeline stages finish; this tool is
    the retry path when a hook's publish failed, and the delivery path for
    late-arriving platform outputs — canonically the shared platform
    Gateway for a workspace that translated before the gateway unit family
    entered the plan: once its marked manifest ships in a
    translation unit, this tool publishes the exports gateway field.
    Derivation is
    deterministic parsing of the persisted artifacts; the generation counter
    of a source only advances when its derived fields actually changed.
    Platform engineers and admins only; no DAG transition.
    """
    logger.info("refresh_exports called.")
    try:
        state_dict, _, config = authorize_and_rehydrate(None)
    except Exception as e:
        return f"ERROR: {e}"

    variables = state_dict["variables"]
    bucket_name = get_bucket_name(config["ledger_uri"])
    bucket = state_mgr.gcs_client.bucket(bucket_name)

    # load_inventory returns (None, None) only for a genuinely absent blob;
    # any other failure is "unreadable HERE, not absent" — the fourth
    # degraded-input leg. Deriving from an empty inventory would recompute
    # image_map/node_shapes to nothing and re-parse chart files the
    # render-target knowledge would have excluded.
    inventory_failed = False
    try:
        inventory, _ = state_mgr.load_inventory(bucket)
    except Exception as e:
        logger.error(f"refresh_exports could not read the inventory: {e}")
        inventory = None
        inventory_failed = True
    inventory = inventory or {}

    # A source whose derivation inputs are unavailable in THIS environment is
    # skipped, not recomputed: refresh may run weeks later on another machine,
    # and a degraded recompute must never overwrite hook-published data with
    # nulls (refresh_document keeps a skipped source's fields, notes and
    # generation untouched).
    per_source = {}
    skipped = []

    scope = variables.get("discovery_scope") or {}
    root_dir = scope.get("root_dir")
    try:
        bucket.blob(exports_lib.MANIFEST_BLOB).reload()
        manifest_readable = True
    except Exception:
        manifest_readable = False
    seed_index_skip = None
    if inventory_failed:
        # The inventory carries the render-target (chart-root) knowledge the
        # seed derivation needs; deriving without it would change entries.
        seed_index_skip = "the inventory blob is unreadable here"
    elif not manifest_readable:
        seed_index_skip = "the discovery manifest is not readable here"
    elif root_dir and not os.path.isdir(root_dir):
        seed_index_skip = "the source checkout is not on this machine"
    if seed_index_skip is None:
        per_source["discovery"] = exports_lib.derive_discovery_fields(
            bucket, variables, inventory, discovery_scope_lib.filter_files)
    else:
        # Only the seed index needs those inputs. The repo coordinates
        # derive from the onboarding variables alone, and exports.target_repo
        # is the one channel a developer session can learn the ship target
        # through — the remedy path configure_repositories -> refresh_exports
        # must publish it even on a machine without the checkout, or the
        # ship stays structurally unreachable behind a green refresh.
        fields = {"source_repo": exports_lib.derive_source_repo(variables)}
        fields["target_repo"], repo_notes = exports_lib.derive_target_repo(
            variables)
        current_doc, _ = exports_lib.load_exports(bucket)
        repo_notes.extend(
            n for n in (current_doc or {}).get("derivation_notes") or []
            if str(n).startswith("discovery: component_seed_index"))
        per_source["discovery"] = (fields, repo_notes)
        skipped.append(f"discovery seed index ({seed_index_skip}; stored "
                       "index kept, repo coordinates republished from the "
                       "onboarding variables)")

    plan = variables.get("translation_plan") or {}
    done_ids = [u.get("unit_id") for u in plan.get("units", [])
                if u.get("status") == "done"]
    done_units, unit_warnings = [], []
    for unit_id in done_ids:
        try:
            done_units.append(json.loads(
                bucket.blob(f"{UNIT_BLOB_PREFIX}/{unit_id}.json").download_as_text()))
        except Exception as e:
            unit_warnings.append(f"unit blob '{unit_id}' unreadable ({e})")
    if unit_warnings:
        skipped.append("translation (" + "; ".join(unit_warnings) + ")")
    else:
        per_source["translation"] = exports_lib.derive_translation_fields(
            done_units, ksa_contract.parse_ksa_annotations, plan)

    clone_dir = variables.get("target_clone_path")
    if inventory_failed:
        # image_map and node_shapes derive from the inventory; an unreadable
        # blob must not recompute them to nothing.
        skipped.append("deployment (the inventory blob is unreadable here)")
    elif clone_dir and not os.path.isdir(clone_dir):
        skipped.append("deployment (the target clone is not on this machine)")
    else:
        planned_refs, cluster_scan = deployment_export_inputs(variables, inventory)
        per_source["deployment"] = exports_lib.derive_deployment_fields(
            variables, inventory, config.get("gcp_project"),
            planned_refs, cluster_scan)

    # The data slice joins the inventory's data_dependencies with the
    # deployment outcome store. Unlike every other source it needs no local
    # checkout and no clone, so it recomputes wherever the two ledger objects
    # are readable — which matters because this tool is the named remedy when
    # a developer's ship gate reports that the slice was never published.
    if inventory_failed:
        skipped.append("data (the inventory blob is unreadable here)")
    else:
        try:
            from servers.phases.deployment import datamigration as dm_lib
            from servers.phases.deployment.deployment_datamigration_2.tools \
                import load_migrations
            migrations, _ = load_migrations(bucket)
        except Exception as e:
            migrations = None
            logger.warning(f"refresh_exports could not read the outcome store: {e}")
        if migrations is None:
            skipped.append("data (platform/deployment/data_migrations.json is "
                           "not readable here; repair it rather than "
                           "republishing every service as outstanding)")
        else:
            data_fields, data_notes = exports_lib.derive_data_gate(
                inventory, migrations, dm_lib.key_of, dm_lib.status_of)
            if data_fields["data_gate"]["scanned"]:
                per_source["data"] = (data_fields, data_notes)
            else:
                # Same rule as publish_data_exports: a vanished section is a
                # degraded input, not evidence that the estate has none, and
                # laying "never scanned" over a good slice refuses every
                # component in the estate.
                skipped.append("data (the inventory carries no "
                               "data_dependencies section; the stored slice "
                               "is kept rather than republished as 'never "
                               "scanned')")

    try:
        doc, changed = exports_lib.refresh_document(bucket, per_source)
    except exceptions.PreconditionFailed:
        return ("ERROR: exports.json is being written concurrently; "
                "re-run refresh_exports.")
    except Exception as e:
        return f"ERROR: Failed to publish exports.json: {e}"

    summary = (
        f"SUCCESS: exports.json recomputed and published to "
        f"gs://{bucket_name}/{exports_lib.EXPORTS_BLOB}.\n"
        f"- Sources changed: {', '.join(changed) or 'none (document already current)'}\n"
        f"- Generations: {json.dumps(doc['generations'])}\n"
        f"- Derivation notes: {len(doc['derivation_notes'])}\n"
    )
    if skipped:
        summary += ("- Sources skipped (inputs unavailable here; stored values "
                    "kept): " + "; ".join(skipped) + "\n")
    return summary

@mcp.tool()
async def initialize_ledger(
    ctx: Context,
    workspace_name: str,
    gcp_project: str,
    ledger_bucket: str,
    roles: Dict[str, List[str]],
) -> str:
    """Initialize a new workspace ledger.
    Args:
        workspace_name: A unique identifier for this migration workspace
        gcp_project: The GCP Project ID hosting target GKE fleets
        ledger_bucket: The storage URI or path (e.g. gs://my-bucket)
        roles: Access control mapping containing admins, platform_engineers, and developers
    """
    global current_state, bootstrap_dag
    logger.debug(f"initialize_ledger called. currentState={current_state}")
    
    if current_state != "STATE_CONFIGURATION_GATHERING":
        raise ValueError(f"invalid state for initialize_ledger: {current_state}")
        
    if not workspace_name or not gcp_project or not ledger_bucket:
        raise ValueError("missing required arguments for initialize_ledger")
        
    if not roles or "admins" not in roles or not roles["admins"]:
        raise ValueError("Admins role (admins) is required and must contain at least one member email.")
    if "platform_engineers" not in roles or not roles["platform_engineers"]:
        raise ValueError("Platform Engineers role (platform_engineers) is required and must contain at least one member email.")
        
    state_def = bootstrap_dag["states"][current_state]
    previous_state = current_state
    current_state = state_def["transitions"]["on_tool_call_received"]
    logger.debug(f"Transitioned {previous_state} -> {current_state}")

    elicitation_state = bootstrap_dag["states"][current_state]
    if elicitation_state["type"] != "HITL_ELICITATION":
        raise ValueError(f"expected HITL_ELICITATION, got {elicitation_state['type']}")

    approved = False
    logger.debug("Requesting elicitation from user via custom elicitation")
    try:
        from mcp import types
        from mcp.shared.message import ServerMessageMetadata
        
        session = ctx.request_context.session
        related_request_id = ctx.request_id
        
        progress_token = None
        if ctx.request_context.meta:
            progress_token = ctx.request_context.meta.progressToken
            
        logger.debug(f"Custom elicit: progress_token={progress_token}")
        meta = types.RequestParams.Meta(progressToken=progress_token) if progress_token else None
        json_schema = ApprovalSchema.model_json_schema()
        
        params = types.ElicitRequestFormParams(
            message=(
                f"Approve the creation of the GCS ledger bucket {ledger_bucket} with uniform "
                f"bucket-level access, the managed folders platform/ and workloads/, and IAM "
                f"grants on it: storage.admin to the admins, objectAdmin on platform/ to the "
                f"platform engineers, read access to the workspace registry for everyone listed, "
                f"and — on the single exports.json object at the bucket root (the "
                f"platform-to-developer channel, which will expose ns/sa-to-GSA pairs and the "
                f"image map across team lines) — read for everyone listed and publish rights "
                f"for the platform engineers."
            ),
            requestedSchema=json_schema,
            _meta=meta
        )
        
        request = types.ElicitRequest(params=params)
        server_request = types.ServerRequest(request)
        
        res = await session.send_request(
            server_request,
            types.ElicitResult,
            metadata=ServerMessageMetadata(related_request_id=related_request_id),
        )
        
        if res.action == "accept":
            content = res.content
            if content is not None:
                validated_data = ApprovalSchema.model_validate(content)
                approved = validated_data.approved
            else:
                approved = True
        else:
            approved = False
            
    except Exception as e:
        logger.exception("Custom Elicitation request failed with exception")
        raise RuntimeError(f"failed to request elicitation: {e}")

    if approved:
        logger.debug("Elicitation approved")
        current_state = elicitation_state["transitions"]["on_approve"]
        try:
            execute_internal_mutations(workspace_name, gcp_project, ledger_bucket, roles)
        except Exception as e:
            logger.debug(f"Internal mutations failed: {e}")
            current_state = "STATE_ABORTED"
            raise
        # The graph now parks on STATE_WORKSPACE_ADMIN with no session cache
        # (bootstrap_migration removed it), so the admin tools learn which
        # workspace this session just created from here.
        bootstrap_phase.set_bootstrapped(ledger_bucket, workspace_name, gcp_project)
    else:
        logger.debug("Elicitation rejected")
        current_state = elicitation_state["transitions"]["on_reject"]
        raise ValueError("user rejected ledger creation")

    return "Ledger initialized and workspace provisioned"

def execute_internal_mutations(workspace_name: str, gcp_project: str, ledger_bucket: str, roles: dict):
    global current_state, bootstrap_dag
    logger.debug("execute_internal_mutations started")
    
    if not state_mgr.gcs_client:
        state_mgr.gcs_client = storage.Client(project=gcp_project)
        
    gcs_state = bootstrap_dag["states"][current_state]
    try:
        provision_gcs_bucket(gcp_project, ledger_bucket, workspace_name)
        current_state = gcs_state["transitions"]["on_success"]
    except Exception as e:
        current_state = gcs_state["transitions"]["on_failure"]
        raise

    iam_state = bootstrap_dag["states"][current_state]
    try:
        provision_ledger_iam(ledger_bucket, roles)
        current_state = iam_state["transitions"]["on_success"]
    except Exception as e:
        current_state = iam_state["transitions"]["on_failure"]
        raise

    ws_state = bootstrap_dag["states"][current_state]
    try:
        write_workspace_registry(ledger_bucket, workspace_name, gcp_project, roles)
        current_state = ws_state["transitions"]["on_success"]
    except Exception as e:
        current_state = ws_state["transitions"]["on_failure"]
        raise

    platform_state = bootstrap_dag["states"][current_state]
    try:
        write_platform_state_machine(ledger_bucket, workspace_name)
        current_state = platform_state["transitions"]["on_success"]
    except Exception as e:
        current_state = platform_state["transitions"]["on_failure"]
        raise

def provision_gcs_bucket(project_id: str, bucket_uri: str, workspace: str):
    logger.debug(f"provision_gcs_bucket bucket={bucket_uri} project={project_id} workspace={workspace}")
    if not state_mgr.gcs_client:
        raise ValueError("GCS client not initialized")
    bucket_name = get_bucket_name(bucket_uri)
    bucket = state_mgr.gcs_client.bucket(bucket_name)
    # Managed folders require uniform bucket-level access, so the ledger cannot
    # be given its prefix boundaries without it. Public access prevention is set
    # here rather than left to org policy: a ledger holds cluster inventory.
    bucket.iam_configuration.uniform_bucket_level_access_enabled = True
    bucket.iam_configuration.public_access_prevention = "enforced"
    try:
        bucket.create(project=project_id)
        return
    except Exception as e:
        if "You already own this bucket" not in str(e) and "409" not in str(e):
            raise RuntimeError(f"failed to create bucket: {e}")

    # Re-running bootstrap against an existing bucket. It may predate this
    # code, in which case it has no uniform access and cannot hold managed
    # folders — turn it on rather than fail the run.
    bucket.reload()
    if not bucket.iam_configuration.uniform_bucket_level_access_enabled:
        logger.warning(f"Enabling uniform bucket-level access on the existing bucket gs://{bucket_name}; "
                       "object ACLs on it will stop being honoured.")
        bucket.iam_configuration.uniform_bucket_level_access_enabled = True
        bucket.patch()


def provision_ledger_iam(bucket_uri: str, roles: dict):
    logger.debug(f"provision_ledger_iam bucket={bucket_uri}")
    ledger_iam.provision_ledger_iam(get_bucket_name(bucket_uri), roles)

def write_workspace_registry(bucket_uri: str, workspace: str, gcp_project: str, roles: dict):
    logger.debug(f"write_workspace_registry bucket={bucket_uri}")
    if not state_mgr.gcs_client:
        raise ValueError("GCS client not initialized")
    bucket_name = get_bucket_name(bucket_uri)
    registry = {
        "workspace_name": workspace,
        "gcp_project": gcp_project,
        "ledger_bucket": bucket_uri,
        "roles": roles
    }
    data = json.dumps(registry, indent=2)
    bucket = state_mgr.gcs_client.bucket(bucket_name)
    blob = bucket.blob("workspace_registry.yaml")
    try:
        blob.upload_from_string(data, if_generation_match=0)
    except Exception as e:
        if "412" not in str(e) and "Precondition Failed" not in str(e):
            raise RuntimeError(f"failed to write workspace registry: {e}")

def add_registry_member(bucket_uri: str, email: str, role_key: str) -> bool:
    """Adds one email to one role list in the workspace registry.

    write_workspace_registry cannot do this: it writes with if_generation_match=0
    and swallows the 412, so it only ever creates. This is the read-modify-write
    counterpart (server/ledger_admin.py), generation-matched against what it
    read so a concurrent join cannot be clobbered.

    Returns True if the registry was written, False if the member was already
    there. Callers must re-run provision_ledger_iam either way — see the note in
    action_register_ledger_member.
    """
    logger.debug(f"add_registry_member bucket={bucket_uri} email={email} role={role_key}")
    if not state_mgr.gcs_client:
        raise ValueError("GCS client not initialized")
    bucket = state_mgr.gcs_client.bucket(get_bucket_name(bucket_uri))
    return ledger_admin.add_member(bucket, email, role_key)


def action_register_ledger_member(variables: dict, config: dict) -> tuple[str, str]:
    """Registers a blocker owner in the ledger and applies the stashed assignment.

    Runs at STATE_REGISTER_MEMBER, after STATE_CONFIRM_NEW_MEMBER has asked which
    team the person is on. The assignment was parked in `pending_owner` by
    assign_blocker_owner rather than applied optimistically, so a declined or
    failed registration leaves the blocker plainly unowned instead of owned by
    someone who cannot open the ledger.
    """
    pending = variables.get("pending_owner") or {}
    email = pending.get("email")
    if not email:
        return "on_failure", "No pending blocker owner to register."

    answer = (variables.get("elicitation_responses") or {}).get("STATE_CONFIRM_NEW_MEMBER") or {}
    team = answer.get("team")
    role_key = {"platform": "platform_engineers", "application": "developers"}.get(team)
    if not role_key:
        return "on_failure", f"Cannot register {email}: no team was chosen."

    ledger_uri = config.get("ledger_uri")
    try:
        added = add_registry_member(ledger_uri, email, role_key)
    except Exception as e:
        logger.exception(f"Failed to add {email} to the workspace registry")
        return "on_failure", f"Could not register {email}: {e}"

    # Mandatory, not an optimisation. The registry grants a conditional
    # objectViewer on workspace_registry.yaml to the union of the three role
    # lists, and authorize_and_rehydrate reads that file as the caller. A member
    # added without this is registered and locked out with a 403. The call is
    # idempotent and merge-only, so re-running it costs nothing.
    try:
        bucket = state_mgr.gcs_client.bucket(get_bucket_name(ledger_uri))
        registry = yaml.safe_load(bucket.blob("workspace_registry.yaml").download_as_text()) or {}
        provision_ledger_iam(ledger_uri, registry.get("roles", {}))
    except Exception as e:
        logger.exception(f"Registered {email} but failed to grant ledger access")
        return "on_failure", (
            f"{email} was added to the registry but the IAM grant failed, so they cannot read "
            f"the ledger yet: {e}"
        )

    blocker_id = pending.get("blocker_id")
    target_close_date = pending.get("target_close_date")
    applied = False
    for blocker in variables.get("blockers") or []:
        if blocker.get("id") == blocker_id:
            blocker["owner"] = email
            blocker["target_close_date"] = target_close_date
            applied = True
            break

    variables.pop("pending_owner", None)

    if not applied:
        return "on_failure", (
            f"{email} was registered on the {team} team, but blocker '{blocker_id}' "
            "no longer exists, so no assignment was made."
        )

    where = "registered" if added else "already registered"
    return "on_success", (
        f"{email} {where} on the {team} team and assigned to {blocker_id} "
        f"with target close date {target_close_date}."
    )


def write_platform_state_machine(bucket_uri: str, workspace: str):
    """Puts the bundled platform graph in the ledger and seeds its state.

    The graph write is create-or-upgrade: a ledger already holding a graph of
    a different version gets the bundled one (generation-guarded), which is
    what makes join_ledger --reconfigure and upgrade_ledger_dags working
    upgrade paths. The state write is create-only.
    """
    logger.debug(f"write_platform_state_machine bucket={bucket_uri}")
    if not state_mgr.gcs_client:
        raise ValueError("GCS client not initialized")
    dag, dag_data = ledger_admin.bundled_graph("platform_dag.json")
    bucket = state_mgr.gcs_client.bucket(get_bucket_name(bucket_uri))
    ledger_admin.install_platform_graph(bucket, dag_data)
    ledger_admin.seed_platform_state(bucket, dag)

if __name__ == "__main__":
    mcp.run(transport="stdio")
