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

"""MCP tools for the workspace administration step (STATE_WORKSPACE_ADMIN).

Bootstrap used to end the moment the ledger was seeded, which left the admin
with no way to fix a mistyped email or a wrong team, and no way to start
over short of a new bucket. This step is where those corrections live. It
has no exit: the graph parks here, the agent hands the turn back, and every
later request maps to one of the tools below.

Two ways in. In the session that ran bootstrap there is no ledger session
cache (bootstrap_migration deletes it), so main.py records the freshly
provisioned workspace here via set_bootstrapped and the tools act on that.
Any later admin session reaches the same tools through join_ledger, where
the session cache plus the registry decide who may call them.

See instructions.md in this folder for the agent-facing playbook.
"""

import logging
from typing import Optional

from mcp.server.fastmcp import Context
from pydantic import BaseModel, Field

import servers.dag.state_management as state_mgr
from servers.dag.state_management import get_bucket_name, load_dag
from servers.dag import dispatch
from servers.dag.server import ledger_admin, ledger_iam
from servers.dag.server import workload_join as workload_join_lib
from servers.dag.server.ledger_admin import LedgerAdminError

logger = logging.getLogger("migration-dag")

STATE = "STATE_WORKSPACE_ADMIN"
PLATFORM_TARGET = "platform"


class ResetMigrationSchema(BaseModel):
    approved: bool = Field(
        description="Delete every migration artifact in the ledger and seed it again? "
                    "The bucket, the workspace registry and its IAM grants are kept; all "
                    "discovery, assessment, landing-zone, translation, deployment and "
                    "component progress is lost."
    )


class ResetDagSchema(BaseModel):
    approved: bool = Field(
        description="Rewind this graph to its start state? Its recorded progress is "
                    "cleared; the artifacts already in the ledger are kept."
    )


class UpgradeDagsSchema(BaseModel):
    approved: bool = Field(
        description="Replace the ledger's graphs with the versions this server ships and "
                    "map each recorded state forward? Progress and artifacts are kept; a "
                    "state the new graph no longer has must be rewound with reset_dag_state."
    )


RESET_MIGRATION_PROMPT = (
    "Reset the migration in workspace '{workspace_name}' (ledger {ledger_uri})? "
    "This deletes {object_count} object(s) — the platform onboarding state and "
    "every discovery, assessment, landing-zone, translation, deployment and "
    "component artifact ({component_count} component(s)) — and seeds a fresh "
    "platform graph. The bucket, the workspace registry and the IAM grants stay "
    "exactly as they are."
)

RESET_DAG_PROMPT = (
    "Rewind the {what} graph in workspace '{workspace_name}' from {current_state} "
    "to its start state {start_state}? Recorded progress is cleared{kept}; the "
    "artifacts in the ledger are kept."
)

UPGRADE_DAGS_PROMPT = (
    "Upgrade the graphs in workspace '{workspace_name}' to the versions this server "
    "ships? Platform graph {platform_change}; developer graph -> v{developer_version} "
    "across {component_count} component(s). Recorded progress and artifacts are kept "
    "and each state is mapped forward.{orphan_warning}"
)


# The workspace bootstrap provisioned in THIS server process, or None. Set by
# main.initialize_ledger once every mutation has landed and cleared by
# bootstrap_migration; the tools use it only while no ledger session cache
# exists, which is exactly the post-bootstrap session.
_bootstrapped: Optional[dict] = None


def set_bootstrapped(ledger_uri: str, workspace_name: str, gcp_project: str) -> None:
    global _bootstrapped
    _bootstrapped = {"ledger_uri": ledger_uri, "workspace_name": workspace_name,
                     "gcp_project": gcp_project}


def clear_bootstrapped() -> None:
    global _bootstrapped
    _bootstrapped = None


def resolve_context() -> dict:
    """Who is administering which workspace.

    A joined session wins: authorize_admin checks the registry and refuses
    anyone not registered as an admin (or who joined de-escalated). Without
    a session the workspace bootstrap just provisioned is used — the caller
    is the person who created the bucket and holds storage.admin on it, so
    GCS itself is the check. Neither -> nothing to administer.
    """
    if state_mgr.active_config_path():
        config, email, bucket = state_mgr.authorize_admin(None)
        return {
            "ledger_uri": config["ledger_uri"],
            "workspace_name": config.get("workspace_name"),
            "gcp_project": config.get("gcp_project"),
            "actor": email,
            "bucket": bucket,
        }
    if _bootstrapped:
        if not state_mgr.gcs_client:
            raise ValueError("GCS client not initialized")
        try:
            actor = state_mgr.get_authenticated_user_email()
        except Exception:
            actor = "bootstrap admin"
        return {
            **_bootstrapped,
            "actor": actor,
            "bucket": state_mgr.gcs_client.bucket(get_bucket_name(_bootstrapped["ledger_uri"])),
        }
    raise PermissionError(
        "No workspace to administer in this session: bootstrap one, or join_ledger "
        "as an admin first.")


def team_role(team: str) -> str:
    key = ledger_admin.TEAM_ROLES.get((team or "").strip().lower())
    if not key:
        raise ValueError(
            f"team must be one of {sorted(ledger_admin.TEAM_ROLES)} "
            f"(platform engineers / application developers), got {team!r}")
    return key


def team_of(role_key: str) -> str:
    return next((t for t, r in ledger_admin.TEAM_ROLES.items() if r == role_key), role_key)


def clean_email(email: str) -> str:
    email = (email or "").strip()
    if "@" not in email or any(c.isspace() for c in email):
        raise ValueError(f"{email!r} does not look like an email address")
    return email


def format_workspace(context: dict, registry: dict, platform: dict, components: list) -> str:
    """The describe_workspace report. Pure."""
    roles = registry.get("roles") or {}
    lines = [
        f"Workspace: {context.get('workspace_name')}",
        f"Project: {context.get('gcp_project')}",
        f"Ledger: {context.get('ledger_uri')}",
        "",
        "Roles:",
    ]
    for role_key in ("admins", "platform_engineers", "developers"):
        members = roles.get(role_key) or []
        lines.append(f"  {role_key}: {', '.join(members) if members else '(none)'}")
    lines.append("")
    ledger_v = platform.get("ledger_version")
    bundled_v = platform.get("bundled_version")
    behind = "" if ledger_v == bundled_v else f" — bundled is v{bundled_v}"
    ledger_v_display = f"v{ledger_v}" if ledger_v else "absent"
    lines.append(f"Platform graph: {ledger_v_display}{behind}; state {platform.get('state') or 'absent'}")
    lines.append(f"Components (developer graph bundled v{platform.get('developer_bundled_version')}):")
    if not components:
        lines.append("  (none claimed yet)")
    for c in components:
        claimant = c.get("claimant") or "unclaimed"
        lines.append(f"  {c['component']}: v{c.get('version')}, {c.get('state')}, {claimant}")
    return "\n".join(lines)


def _component_rows(bucket) -> list:
    rows = []
    for component in ledger_admin.list_components(bucket):
        try:
            state_dict, _, copy_dag = ledger_admin.read_component(bucket, component)
        except LedgerAdminError:
            continue
        variables = state_dict.get("variables") or {}
        rows.append({
            "component": component,
            "version": copy_dag.get("version"),
            "state": state_dict.get("current_state"),
            "claimant": (variables.get("claim") or {}).get("claimant"),
        })
    return rows


def _known_components(bucket) -> str:
    """Comma-joined component ids for an error hint; never raises."""
    try:
        return ", ".join(ledger_admin.list_components(bucket)) or "none"
    except Exception:
        return "unavailable"


def _component_grants_note(bucket, email: str, bucket_name: str) -> str:
    """Per-component managed-folder grants the server cannot revoke, if any.

    revoke_ledger_member clears the bucket policy and the platform/ folder,
    but a developer's per-component write lives on a workloads/<component>/
    managed folder an admin bound out of band — the server holds no
    setIamPolicy there. When a member who still claims components leaves or
    changes team, name those folders and print the revoke command, so the
    caller is not told "access revoked" while a folder grant lingers.
    """
    try:
        claimed = [r["component"] for r in _component_rows(bucket)
                   if r.get("claimant") == email]
    except Exception:
        return ""
    if not claimed:
        return ""
    # A service account or an already-qualified address is not user:; reuse the
    # same principal mapping provision/revoke use so the printed command works.
    principal = ledger_iam._principals([email])[0]
    lines = [f"\nManual step — {len(claimed)} component folder grant(s) the server "
             f"cannot revoke ({', '.join(claimed)}):"]
    for component in claimed:
        lines.append(
            f"  gcloud storage managed-folders remove-iam-policy-binding "
            f"gs://{bucket_name}/workloads/{component}/ "
            f"--member={principal} --role=roles/storage.objectAdmin")
    return "\n".join(lines)


async def _confirm(ctx, schema_cls, template: str, fields: dict, context: dict):
    """(approved, error). Puts the question through the same elicitation
    primitive the HITL states use, so the harness renders it as a form."""
    state_def = {"prompt_template": template}
    state_dict = {"variables": fields}
    config = {"gcp_project": context.get("gcp_project"),
              "workspace_name": context.get("workspace_name")}
    try:
        approved, _ = await dispatch.run_elicitation(
            ctx, STATE, state_def, schema_cls, state_dict, config)
    except Exception as e:
        logger.exception(f"Elicitation failed at {STATE}")
        return False, f"ERROR: Elicitation request failed: {e}"
    return approved, None


async def describe_workspace(ctx: Context = None) -> str:
    """Shows the workspace: roles, platform graph version and state, and every
    claimed component with its graph version, state and claimant."""
    logger.info("describe_workspace called.")
    try:
        context = resolve_context()
        bucket = context["bucket"]
        registry, _ = ledger_admin.load_registry(bucket)
        try:
            platform_state = ledger_admin.read_platform_state(bucket)[0].get("current_state")
        except LedgerAdminError:
            platform_state = None
        platform = {
            "ledger_version": ledger_admin.read_platform_graph_version(bucket),
            "bundled_version": str(ledger_admin.bundled_graph("platform_dag.json")[0].get("version")),
            "developer_bundled_version": str(ledger_admin.bundled_graph("developer_dag.json")[0].get("version")),
            "state": platform_state,
        }
        components = _component_rows(bucket)
    except Exception as e:
        return f"ERROR: {e}"
    return format_workspace(context, registry, platform, components)


async def add_workspace_member(email: str, team: str, ctx: Context = None) -> str:
    """Adds a person to a team, or moves them to it from the other one.

    On a fresh add, IAM is granted before the registry is written: a
    principal GCS refuses (a typo, a domain that does not exist) never
    reaches the registry, so it cannot make every later grant fail. On a
    move, the old team's grants are revoked first, then the new team's are
    granted, then the registry is rewritten; a failure of the new grant
    leaves the member without access and says so plainly, rather than
    claiming they "were not registered". Admins are not managed here.

    A move does not touch the per-component workloads/<component>/ managed
    folders (the server holds no setIamPolicy there); when the member still
    claims components, the reply names them and prints the revoke command.

    Args:
        email: The exact address the admin gave.
        team: "platform" (platform engineer: write access to platform/) or
            "application" (developer).
    """
    logger.info(f"add_workspace_member called: {email} -> {team}")
    try:
        context = resolve_context()
        role_key = team_role(team)
        email = clean_email(email)
    except Exception as e:
        return f"ERROR: {e}"
    bucket = context["bucket"]
    bucket_name = get_bucket_name(context["ledger_uri"])

    try:
        registry, _ = ledger_admin.load_registry(bucket)
    except Exception as e:
        return f"ERROR: {e}"
    current = ledger_admin.member_role(registry, email)
    if current == "admins":
        return (f"ERROR: {email} is an admin; the admin list is fixed at bootstrap and is "
                "not changed from here.")
    if current == role_key:
        return f"{email} is already registered as a {team_of(role_key)} team member; nothing changed."

    roles_after = {k: [m for m in (v or []) if m != email]
                   for k, v in (registry.get("roles") or {}).items()}
    roles_after[role_key] = roles_after.get(role_key, []) + [email]

    if current:
        # A move: drop the old team's grants so a platform engineer turned
        # developer does not keep write access to platform/.
        try:
            ledger_iam.revoke_ledger_member(bucket_name, email)
        except Exception as e:
            return (f"ERROR: could not revoke {email}'s {team_of(current)} team grants: {e}. "
                    "The registry was not changed.")
    try:
        ledger_iam.provision_ledger_iam(bucket_name, roles_after)
    except Exception as e:
        if current:
            # The team-level grants are gone; any per-component folder grants
            # are not (revoke never touches them), so name them rather than
            # claim the member has no access at all.
            note = _component_grants_note(bucket, email, bucket_name)
            return (f"ERROR: {email}'s {team_of(current)} team grants were revoked but the "
                    f"{team_of(role_key)} team grant failed: {e}. Their team-level ledger "
                    "access was withdrawn; re-run add_workspace_member to finish the move. "
                    f"The registry still lists them under the {team_of(current)} team." + note)
        return (f"ERROR: the IAM grant for {email} failed, so they were not registered: {e}. "
                "Check the address is a real Google identity and try again.")

    try:
        outcome, roles = ledger_admin.move_member(bucket, email, role_key)
    except Exception as e:
        return (f"ERROR: {email} was granted access but the registry write failed: {e}. "
                "Re-run add_workspace_member; the grant is idempotent.")

    verb = {"added": "added to", "moved": f"moved from the {team_of(current)} team to",
            "unchanged": "already on"}[outcome]
    note = _component_grants_note(bucket, email, bucket_name) if outcome == "moved" else ""
    return (f"{email} {verb} the {team_of(role_key)} team and granted ledger access.\n"
            f"Roles now: " + _roles_line(roles) + note)


async def remove_workspace_member(email: str, ctx: Context = None) -> str:
    """Removes a platform engineer or developer and revokes their ledger grants.

    The registry entry goes first, then the IAM bindings on the bucket and
    platform/. An address that is not registered still has its stale
    bindings revoked, so a half-finished earlier removal can be completed by
    calling this again. Per-component workloads/<component>/ managed-folder
    grants are not revoked here (the server holds no setIamPolicy there); the
    reply names any the member still holds and prints the revoke command.
    Admins are not managed here.

    Args:
        email: The exact address the admin gave.
    """
    logger.info(f"remove_workspace_member called: {email}")
    try:
        context = resolve_context()
        email = clean_email(email)
    except Exception as e:
        return f"ERROR: {e}"
    bucket = context["bucket"]
    bucket_name = get_bucket_name(context["ledger_uri"])

    try:
        registry, _ = ledger_admin.load_registry(bucket)
    except Exception as e:
        return f"ERROR: {e}"
    current = ledger_admin.member_role(registry, email)
    if current == "admins":
        return (f"ERROR: {email} is an admin; the admin list is fixed at bootstrap and is "
                "not changed from here.")

    roles = registry.get("roles") or {}
    if current:
        try:
            _, roles = ledger_admin.remove_member(bucket, email)
        except Exception as e:
            return f"ERROR: {e}"

    try:
        revoked = ledger_iam.revoke_ledger_member(bucket_name, email)
    except Exception as e:
        if current:
            return (f"{email} was removed from the {team_of(current)} team in the registry, "
                    f"but revoking their IAM grants failed: {e}. They can no longer act "
                    "through the server; call remove_workspace_member again to retry the revoke.")
        return f"ERROR: {email} is not registered, and revoking stale grants failed: {e}"

    note = _component_grants_note(bucket, email, bucket_name)
    if not current:
        if not revoked and not note:
            return f"ERROR: {email} is not registered in this workspace and holds no ledger grants."
        return (f"{email} was not in the registry; {revoked} stale IAM binding(s) revoked.\n"
                "Roles now: " + _roles_line(roles) + note)
    return (f"{email} removed from the {team_of(current)} team; {revoked} IAM binding(s) revoked.\n"
            "Roles now: " + _roles_line(roles) + note)


async def reset_migration(ctx: Context = None) -> str:
    """Starts the migration over: deletes every migration artifact in the
    ledger and seeds a fresh platform graph and state.

    Asks for approval first. The bucket, the workspace registry and its IAM
    grants are kept — this is a reset, not a teardown — so every registered
    member can join the fresh workspace without another bootstrap.
    """
    logger.info("reset_migration called.")
    try:
        context = resolve_context()
        bucket = context["bucket"]
        objects = ledger_admin.migration_objects(bucket)
        components = ledger_admin.list_components(bucket)
    except Exception as e:
        return f"ERROR: {e}"

    approved, error = await _confirm(ctx, ResetMigrationSchema, RESET_MIGRATION_PROMPT, {
        "ledger_uri": context["ledger_uri"],
        "object_count": len(objects),
        "component_count": len(components),
    }, context)
    if error:
        return error
    if not approved:
        return "Reset declined; nothing was changed."

    try:
        deleted = ledger_admin.wipe_migration(bucket)
        dag, raw = ledger_admin.bundled_graph("platform_dag.json")
        ledger_admin.install_platform_graph(bucket, raw)
        ledger_admin.seed_platform_state(bucket, dag)
    except Exception as e:
        return (f"ERROR: the reset stopped part-way: {e}. Call reset_migration again; "
                "deleting is idempotent and the seed only writes what is missing.")

    logger.info(f"Migration reset by {context['actor']}: {len(deleted)} object(s) deleted")
    return (f"Migration reset. {len(deleted)} object(s) deleted, including "
            f"{len(components)} component(s); platform graph v{dag.get('version')} seeded at "
            f"{dag['start_state']}. The registry and IAM grants are unchanged — members can "
            "join_ledger as before.")


async def upgrade_ledger_dags(ctx: Context = None) -> str:
    """Brings the ledger's graphs up to the versions this server ships.

    Replaces platform_dag.json when the bundled version differs, and
    upgrades every component's dag.json copy through the same state mapping
    the developer join applies. Reports a platform state the new graph no
    longer has rather than moving it: that is a reset_dag_state decision.

    Asks for approval first. The preview it confirms is read before anything
    is written, so an upgrade that would strand the platform team on a
    removed state is visible in the prompt, not discovered afterwards.
    """
    logger.info("upgrade_ledger_dags called.")
    try:
        context = resolve_context()
        bucket = context["bucket"]
        dag, raw = ledger_admin.bundled_graph("platform_dag.json")
        dev_dag = ledger_admin.bundled_graph("developer_dag.json")[0]
        ledger_platform_v = ledger_admin.read_platform_graph_version(bucket)
        bundled_platform_v = str(dag.get("version"))
        try:
            platform_current = ledger_admin.read_platform_state(bucket)[0].get("current_state")
        except LedgerAdminError:
            platform_current = None
        orphaned = platform_current is not None and platform_current not in dag.get("states", {})
        component_count = len(ledger_admin.list_components(bucket))
    except Exception as e:
        return f"ERROR: {e}"

    if ledger_platform_v is None:
        platform_change = f"installed at v{bundled_platform_v} (the ledger had none)"
    elif ledger_platform_v == bundled_platform_v:
        platform_change = f"already v{bundled_platform_v}"
    else:
        platform_change = f"v{ledger_platform_v} -> v{bundled_platform_v}"
    if ledger_platform_v == bundled_platform_v and component_count == 0 and not orphaned:
        return (f"Nothing to upgrade: the platform graph is already v{bundled_platform_v} "
                "and no components are claimed.")

    orphan_warning = ""
    if orphaned:
        orphan_warning = (f" WARNING: the platform state {platform_current} does not exist in "
                          f"v{bundled_platform_v}; the platform team will be stranded until "
                          "reset_dag_state(\"platform\") rewinds it.")
    approved, error = await _confirm(ctx, UpgradeDagsSchema, UPGRADE_DAGS_PROMPT, {
        "platform_change": platform_change,
        "developer_version": dev_dag.get("version"),
        "component_count": component_count,
        "orphan_warning": orphan_warning,
    }, context)
    if error:
        return error
    if not approved:
        return "Upgrade declined; nothing was changed."

    try:
        replaced, before, after = ledger_admin.install_platform_graph(bucket, raw)
    except Exception as e:
        return f"ERROR: {e}"

    lines = []
    if replaced:
        lines.append(f"Platform graph: v{before} -> v{after}.")
    elif before is None:
        lines.append(f"Platform graph: installed v{after} (the ledger had none).")
    else:
        lines.append(f"Platform graph: already v{after}.")
    try:
        state_dict, _ = ledger_admin.read_platform_state(bucket)
        current = state_dict.get("current_state")
        if current not in dag.get("states", {}):
            lines.append(
                f"  WARNING: the platform state {current} does not exist in v{after}; "
                "the platform team cannot continue until reset_dag_state(\"platform\") rewinds it.")
    except LedgerAdminError:
        if ledger_admin.seed_platform_state(bucket, dag):
            lines.append(f"  Platform state was missing; seeded at {dag['start_state']}.")

    try:
        dev_dag, dev_raw = ledger_admin.bundled_graph("developer_dag.json")
        components = ledger_admin.list_components(bucket)
    except Exception as e:
        lines.append(f"Components: not upgraded: {e}")
        return "\n".join(lines)
    if not components:
        lines.append("Components: none claimed yet.")
    for component in components:
        try:
            upgraded, notes = ledger_admin.upgrade_component(bucket, component, dev_dag, dev_raw)
        except Exception as e:
            lines.append(f"  {component}: not upgraded: {e}")
            continue
        detail = "; ".join(notes) if notes else f"already v{dev_dag.get('version')}"
        lines.append(f"  {component}: {'upgraded' if upgraded else 'unchanged'} — {detail}")
    return "\n".join(lines)


async def reset_dag_state(target: str, ctx: Context = None) -> str:
    """Rewinds one graph to its start state, after asking for approval.

    Args:
        target: "platform" for the platform team's onboarding graph, or a
            component id for that component's developer graph. A component
            keeps its claim, so the claimant can continue without re-joining.
    """
    logger.info(f"reset_dag_state called: {target}")
    target = (target or "").strip()
    try:
        context = resolve_context()
    except Exception as e:
        return f"ERROR: {e}"
    bucket = context["bucket"]

    if target == PLATFORM_TARGET:
        try:
            dag = load_dag(bucket, ledger_admin.PLATFORM_DAG_OBJECT)
            state_dict, _ = ledger_admin.read_platform_state(bucket)
        except Exception as e:
            return f"ERROR: {e}"
        what, kept = "platform onboarding", ""
        kept_keys = frozenset()  # reset_platform_state clears every variable
        reset = lambda: ledger_admin.reset_platform_state(bucket, dag, context["actor"])
    else:
        invalid = workload_join_lib.validate_component_id(target)
        if invalid:
            return (f"ERROR: target must be \"{PLATFORM_TARGET}\" or a component id — {invalid}. "
                    f"Known components: {_known_components(bucket)}")
        try:
            state_dict, _, copy_dag = ledger_admin.read_component(bucket, target)
        except Exception as e:
            return f"ERROR: {e}. Known components: {_known_components(bucket)}"
        dag = copy_dag if copy_dag.get("start_state") else ledger_admin.bundled_graph("developer_dag.json")[0]
        what, kept = f"component '{target}'", " (the claim is kept)"
        # reset_component_state keeps exactly these; extras are the work reset drops.
        kept_keys = frozenset({"component", "claim"})
        reset = lambda: ledger_admin.reset_component_state(bucket, target, dag, context["actor"])

    current = state_dict.get("current_state")
    extra_vars = set(state_dict.get("variables") or {}) - kept_keys
    if current == dag["start_state"] and not extra_vars:
        return f"The {what} graph is already at its start state {current}; nothing to reset."

    approved, error = await _confirm(ctx, ResetDagSchema, RESET_DAG_PROMPT, {
        "what": what, "current_state": current, "start_state": dag["start_state"], "kept": kept,
    }, context)
    if error:
        return error
    if not approved:
        return "Reset declined; nothing was changed."

    try:
        previous = reset()
    except Exception as e:
        return f"ERROR: {e}"
    return f"The {what} graph was rewound from {previous} to {dag['start_state']}{kept}."


def _roles_line(roles: dict) -> str:
    return "; ".join(
        f"{key}: {', '.join(roles.get(key) or []) or '(none)'}"
        for key in ("admins", "platform_engineers", "developers"))


def register(mcp):
    """Register this step's tools on the FastMCP server."""
    mcp.tool()(describe_workspace)
    mcp.tool()(add_workspace_member)
    mcp.tool()(remove_workspace_member)
    mcp.tool()(reset_migration)
    mcp.tool()(upgrade_ledger_dags)
    mcp.tool()(reset_dag_state)
