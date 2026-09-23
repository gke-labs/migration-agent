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

"""The ledger operations an admin performs after bootstrap.

Bootstrap writes the registry, the platform graph and the seeded platform
state once. Everything here is the read-modify-write counterpart over the
same objects: change who is in a team, put a newer bundled graph into the
ledger, rewind a graph to its start, or delete every migration artifact and
seed the ledger again. Each function takes the bucket handle and does one
thing under the same generation preconditions the rest of the server uses,
so a concurrent join or tool call is refused rather than clobbered.

Lives under servers/dag/server/ because two callers need it and neither may
import the other: main.py (bootstrap and the blocker-owner registration) and
the bootstrap phase package (the admin tools). Nothing here elicits or
transitions a graph — the tools decide when to ask and what to report.
"""

import json
import logging
import os
from datetime import datetime, timezone

import yaml
from google.api_core import exceptions

from server import exports as exports_lib
from server import workload_join as workload_join_lib

logger = logging.getLogger("migration-dag")

REGISTRY_OBJECT = "workspace_registry.yaml"
PLATFORM_DAG_OBJECT = "platform_dag.json"
PLATFORM_STATE_OBJECT = "platform/onboarding/state.json"
WORKLOADS_PREFIX = "workloads/"

# The team vocabulary the admin tools accept, mapped to the registry role
# key. Same words as NewMemberSchema in dispatch.py: an admin naming a team
# and a blocker owner picking one should not learn two vocabularies.
TEAM_ROLES = {"platform": "platform_engineers", "application": "developers"}
MANAGED_ROLES = tuple(TEAM_ROLES.values())

# Objects a migration reset keeps. The registry is the workspace itself —
# who may join and as what — and the IAM grants that reference it stay
# with the bucket; deleting it would turn a reset into a teardown.
KEPT_ON_RESET = frozenset({REGISTRY_OBJECT})

_DAG_DIR = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), ".."))


class LedgerAdminError(RuntimeError):
    """An administrative ledger operation could not be completed."""


# --- registry -----------------------------------------------------------

def load_registry(bucket) -> tuple[dict, int]:
    """(registry, generation). Raises LedgerAdminError when there is none."""
    blob = bucket.blob(REGISTRY_OBJECT)
    try:
        blob.reload()
        registry = yaml.safe_load(blob.download_as_text()) or {}
    except exceptions.NotFound:
        raise LedgerAdminError("workspace registry not found in the ledger bucket")
    return registry, blob.generation


def save_registry(bucket, registry: dict, generation: int) -> None:
    """Writes the registry back under the generation it was read at."""
    try:
        bucket.blob(REGISTRY_OBJECT).upload_from_string(
            json.dumps(registry, indent=2),
            content_type="application/json",
            if_generation_match=generation,
        )
    except exceptions.PreconditionFailed:
        raise LedgerAdminError(
            "the workspace registry changed while it was being updated; nothing was written")


def member_role(registry: dict, email: str):
    """The role key an email is registered under, or None.

    Admins win over any team role. An email may be dual-listed (testers
    routinely use one address for admin and platform engineer), and the
    registry stores roles in whatever key order the agent emitted; a naive
    first-match could report such an email as a team member and let a
    remove/move revoke touch the admin's grants. Checking admins first makes
    the admin-exclusion guard fire for a dual-listed admin.
    """
    roles = registry.get("roles") or {}
    if email in (roles.get("admins") or []):
        return "admins"
    for role_key, members in roles.items():
        if email in (members or []):
            return role_key
    return None


def add_member(bucket, email: str, role_key: str) -> bool:
    """Adds one email to one role list. True if written, False if already there."""
    registry, generation = load_registry(bucket)
    roles = registry.setdefault("roles", {})
    members = roles.setdefault(role_key, []) or []
    if email in members:
        return False
    members.append(email)
    roles[role_key] = members
    save_registry(bucket, registry, generation)
    return True


def move_member(bucket, email: str, role_key: str) -> tuple[str, dict]:
    """Puts an email in exactly one of the managed team roles.

    Returns (outcome, roles_after) where outcome is "added", "moved" or
    "unchanged". Refuses an admin: the admin list is fixed at bootstrap
    because it is what grants storage.admin on the bucket, and an admin
    demoted by mistake could not undo it.
    """
    if role_key not in MANAGED_ROLES:
        raise ValueError(f"unknown team role {role_key!r}; expected one of {list(MANAGED_ROLES)}")
    registry, generation = load_registry(bucket)
    roles = registry.setdefault("roles", {})
    current = member_role(registry, email)
    if current == "admins":
        raise LedgerAdminError(
            f"{email} is an admin; the admin list is fixed at bootstrap and is not "
            "changed from here")
    if current == role_key:
        return "unchanged", roles
    if current:
        roles[current] = [m for m in roles[current] if m != email]
    roles[role_key] = list(roles.get(role_key) or []) + [email]
    save_registry(bucket, registry, generation)
    return ("moved" if current else "added"), roles


def remove_member(bucket, email: str) -> tuple[str, dict]:
    """Removes an email from whichever managed team it is in.

    Returns (role_key_removed_from, roles_after). Refuses admins for the
    reason move_member gives, and an unknown email by name.
    """
    registry, generation = load_registry(bucket)
    roles = registry.setdefault("roles", {})
    current = member_role(registry, email)
    if current is None:
        raise LedgerAdminError(f"{email} is not registered in this workspace")
    if current == "admins":
        raise LedgerAdminError(
            f"{email} is an admin; the admin list is fixed at bootstrap and is not "
            "changed from here")
    roles[current] = [m for m in roles[current] if m != email]
    save_registry(bucket, registry, generation)
    return current, roles


# --- graphs -------------------------------------------------------------

def bundled_graph(filename: str) -> tuple[dict, str]:
    """(parsed graph, raw text) of a graph shipped beside main.py."""
    path = os.path.join(_DAG_DIR, filename)
    if not os.path.exists(path):
        raise FileNotFoundError(f"statically bundled {filename} not found at {path}")
    with open(path, "r", encoding="utf-8") as f:
        raw = f.read()
    return json.loads(raw), raw


def install_platform_graph(bucket, dag_raw: str) -> tuple[bool, str, str]:
    """Puts the bundled platform graph in the ledger.

    Create-only when absent. When the ledger already holds one, it is
    replaced (generation-guarded) only if its version differs from the
    bundle — this is what makes an upgrade a working path rather than a
    silent keep. Returns (replaced, ledger_version_before, bundled_version).
    """
    bundled_version = str(json.loads(dag_raw).get("version"))
    blob = bucket.blob(PLATFORM_DAG_OBJECT)
    try:
        blob.upload_from_string(dag_raw, if_generation_match=0)
        return False, None, bundled_version
    except Exception as e:
        if not _is_precondition_failure(e):
            raise LedgerAdminError(f"failed to write platform dag: {e}")
    try:
        blob.reload()
        existing_version = str(json.loads(blob.download_as_text()).get("version"))
        if existing_version == bundled_version:
            return False, existing_version, bundled_version
        blob.upload_from_string(dag_raw, if_generation_match=blob.generation)
        logger.info("Replaced ledger platform DAG v%s with bundled v%s.",
                    existing_version, bundled_version)
        return True, existing_version, bundled_version
    except Exception as upgrade_error:
        raise LedgerAdminError(
            f"ledger platform DAG exists but could not be upgraded: {upgrade_error}")


def seed_platform_state(bucket, dag: dict) -> bool:
    """Creates platform/onboarding/state.json at the graph's start. True if created."""
    initial_state = {"current_state": dag["start_state"], "history": [], "variables": {}}
    blob = bucket.blob(PLATFORM_STATE_OBJECT)
    try:
        blob.upload_from_string(json.dumps(initial_state, indent=2),
                                content_type="application/json", if_generation_match=0)
        logger.info("Initialized platform onboarding state in GCS.")
        return True
    except Exception as e:
        if _is_precondition_failure(e):
            return False
        raise LedgerAdminError(f"failed to write initial platform state: {e}")


def _is_precondition_failure(e: Exception) -> bool:
    return isinstance(e, exceptions.PreconditionFailed) or "412" in str(e) \
        or "Precondition Failed" in str(e)


def read_platform_state(bucket) -> tuple[dict, int]:
    blob = bucket.blob(PLATFORM_STATE_OBJECT)
    try:
        blob.reload()
        return json.loads(blob.download_as_text()), blob.generation
    except exceptions.NotFound:
        raise LedgerAdminError("platform onboarding state not found in the ledger")


def read_platform_graph_version(bucket):
    try:
        return str(json.loads(bucket.blob(PLATFORM_DAG_OBJECT).download_as_text()).get("version"))
    except exceptions.NotFound:
        return None


def reset_platform_state(bucket, dag: dict, actor: str) -> str:
    """Rewinds the platform graph to its start and clears the blackboard.

    History is kept and extended: a rewind is an event in the migration's
    record, not a reason to lose it. Returns the state it was rewound from.
    """
    state_dict, generation = read_platform_state(bucket)
    previous = state_dict.get("current_state")
    state_dict["current_state"] = dag["start_state"]
    state_dict["variables"] = {}
    state_dict.setdefault("history", []).append(
        f"Reset to {dag['start_state']} from {previous} by {actor} at {_now()}")
    try:
        bucket.blob(PLATFORM_STATE_OBJECT).upload_from_string(
            json.dumps(state_dict, indent=2), content_type="application/json",
            if_generation_match=generation)
    except exceptions.PreconditionFailed:
        raise LedgerAdminError(
            "the platform state changed while it was being reset; nothing was written")
    return previous


# --- components ---------------------------------------------------------

def list_components(bucket) -> list:
    """Component ids that have a state.json under workloads/, sorted."""
    components = []
    for blob in bucket.list_blobs(prefix=WORKLOADS_PREFIX):
        parts = blob.name[len(WORKLOADS_PREFIX):].split("/")
        if len(parts) == 2 and parts[1] == "state.json" and parts[0]:
            components.append(parts[0])
    return sorted(components)


def read_component(bucket, component: str) -> tuple[dict, int, dict]:
    """(state_dict, generation, ledger dag copy). Missing copy -> version 0.0."""
    blob_state = bucket.blob(f"{WORKLOADS_PREFIX}{component}/state.json")
    try:
        blob_state.reload()
        state_dict = json.loads(blob_state.download_as_text())
    except exceptions.NotFound:
        raise LedgerAdminError(f"component '{component}' is not initialized in this ledger")
    try:
        copy_dag = json.loads(bucket.blob(f"{WORKLOADS_PREFIX}{component}/dag.json").download_as_text())
    except exceptions.NotFound:
        copy_dag = {"version": "0.0"}
    return state_dict, blob_state.generation, copy_dag


def read_plan_guard(bucket, component: str):
    """The has_unparkable_units guard fact for a component, or None when the
    plan is absent or unreadable (the mapping then keeps the state and says
    so). Unreadable exports is not unknowable: no exports means no attach
    point, so nothing can unpark and the guard is False."""
    try:
        raw = bucket.blob(f"{WORKLOADS_PREFIX}{component}/plan.json").download_as_text()
    except Exception:
        return None
    try:
        exports_doc, _ = exports_lib.load_exports(bucket)
    except Exception:
        exports_doc = None
    try:
        return workload_join_lib.has_unparkable_units(json.loads(raw), exports_doc)
    except Exception:
        return None


def upgrade_component(bucket, component: str, bundled_dag: dict, bundled_raw: str) -> tuple[bool, list]:
    """Brings one component's graph copy up to the bundle, mapping its state.

    Mirrors _claim_component exactly. When a version hop is due the mapped
    state.json commits first, then dag.json is overwritten — a failure
    between the two leaves the copy old and the next run repeats the whole
    upgrade. When no hop is due the bundled version's guarded entries are
    still re-evaluated (apply_standing_guards): a standing re-entry rule is
    not a one-time migration hop, so a component already at the bundled
    version must get the same re-entry the next join would give it, or an
    admin upgrade would strand a parked unit a join would have freed.
    Returns (changed, notes).
    """
    state_dict, generation, copy_dag = read_component(bucket, component)
    guard_facts = None
    if workload_join_lib.needs_guard_facts(bundled_dag, copy_dag, state_dict):
        guard_facts = {"has_unparkable_units": read_plan_guard(bucket, component)}
    state_dict, upgraded, notes = workload_join_lib.upgrade_component_state(
        bundled_dag, copy_dag, state_dict, guard_facts)
    if not upgraded:
        # No version hop: re-evaluate the standing guarded entries, then stop
        # if nothing moved. Only state.json can change here — the dag.json
        # copy is already at the bundled version.
        state_dict, reentered, reentry_notes = workload_join_lib.apply_standing_guards(
            bundled_dag, state_dict, guard_facts)
        notes.extend(reentry_notes)
        if not reentered:
            return False, notes
        _write_component_state(bucket, component, state_dict, generation)
        return True, notes
    _write_component_state(bucket, component, state_dict, generation)
    bucket.blob(f"{WORKLOADS_PREFIX}{component}/dag.json").upload_from_string(
        bundled_raw, content_type="application/json")
    notes.append(f"developer DAG upgraded v{copy_dag.get('version')} -> v{bundled_dag.get('version')}")
    return True, notes


def _write_component_state(bucket, component: str, state_dict: dict, generation: int) -> None:
    try:
        bucket.blob(f"{WORKLOADS_PREFIX}{component}/state.json").upload_from_string(
            json.dumps(state_dict, indent=2), content_type="application/json",
            if_generation_match=generation)
    except exceptions.PreconditionFailed:
        raise LedgerAdminError(
            f"component '{component}' changed while it was being upgraded; nothing was written")


def reset_component_state(bucket, component: str, dag: dict, actor: str) -> str:
    """Rewinds a component's graph to its start, keeping the claim.

    `component` and `claim` survive because authorize_and_rehydrate_workload
    requires both: dropping them would lock the claimant out until a re-join
    re-recorded what the reset had no reason to forget.
    """
    state_dict, generation, _ = read_component(bucket, component)
    previous = state_dict.get("current_state")
    variables = state_dict.get("variables") or {}
    kept = {k: variables[k] for k in ("component", "claim") if k in variables}
    state_dict["current_state"] = dag["start_state"]
    state_dict["variables"] = kept
    state_dict.setdefault("history", []).append(
        f"Reset to {dag['start_state']} from {previous} by {actor} at {_now()}")
    try:
        bucket.blob(f"{WORKLOADS_PREFIX}{component}/state.json").upload_from_string(
            json.dumps(state_dict, indent=2), content_type="application/json",
            if_generation_match=generation)
    except exceptions.PreconditionFailed:
        raise LedgerAdminError(
            f"component '{component}' changed while it was being reset; nothing was written")
    return previous


# --- the whole migration ------------------------------------------------

def migration_objects(bucket) -> list:
    """Every object a migration reset would delete, sorted."""
    return sorted(b.name for b in bucket.list_blobs() if b.name not in KEPT_ON_RESET)


def wipe_migration(bucket) -> list:
    """Deletes every object except the ones KEPT_ON_RESET. Returns what went."""
    deleted = []
    for name in migration_objects(bucket):
        try:
            bucket.blob(name).delete()
            deleted.append(name)
        except exceptions.NotFound:
            pass  # something else removed it first; the outcome is the same
    return deleted


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()
