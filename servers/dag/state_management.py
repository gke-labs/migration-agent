import asyncio
import functools
import hashlib
import inspect
import json
import logging
import os
import urllib.request
from typing import Optional

from google.cloud import storage
from google.api_core import exceptions
import google.auth
from google.auth.transport.requests import Request
import yaml
from mcp.server.fastmcp import Context

from server.dag_validation import validate_dag
from server.workload_join import admin_binding_step

logger = logging.getLogger("migration-dag")

gcs_client = None
# Legacy machine-global session cache. Read fallback only, consumed on first
# use: the first session that reads it adopts it into its own scoped path and
# deletes this file (see _adopt_legacy_config) — otherwise it would act as a
# default session for every new working directory forever.
LEDGER_CONFIG_PATH = os.path.expanduser("~/.ledger_config.yaml")
# Per-cwd session caches, one file per working directory (P0c: two servers in
# different directories no longer clobber each other's session).
LEDGER_CONFIG_DIR = os.path.expanduser("~/.ledger_config.d")
ROLE_HIERARCHY = {"admins": 3, "platform": 2, "developers": 1}

INVENTORY_BLOB_PATH = "platform/discovery/inventory.json"
_INVENTORY_SCHEMA_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), "server", "schema", "inventory.json"
)
# Top-level inventory sections writable by LLM enrichment (write_discovery_inventory).
#
# data_dependencies is deliberately absent — and so are cluster_dns,
# address_space and the three scan-notes sections: they are harvested
# deterministically by the scan pipeline (discovery_init_1/datastores.py,
# discovery_init_1/clusterdns.py, discovery_init_1/addressspace.py) and
# carried across extraction re-runs by SCAN_OWNED_KEYS. Resource types, module sources and ARNs
# are exact strings with nothing to infer, and the workload data gate blocks a
# developer's pipeline on this section — which mixed scanner/model provenance
# could not support, since after the fact there would
# be no telling which wrote what. A worker that spots a data service the scanner cannot reach records it
# in `findings`, the section that exists for facts not fitting a structured
# field, rather than overwriting scanned facts here.
INVENTORY_ANALYSIS_KEYS = frozenset(
    {
        "clusters",
        "triggers",
        "storage",
        "observability",
        "escalations",
        "notes",
    }
)

def get_bucket_name(uri: str) -> str:
    return uri[5:] if uri.startswith("gs://") else uri

def get_authenticated_user_email() -> str:
    """Attempts to retrieve the active user OIDC email from Application Default Credentials."""
    try:
        credentials, project = google.auth.default()
        sa_email = getattr(credentials, "service_account_email", None)
        if sa_email and sa_email != "default":
            return sa_email

        if not credentials.valid:
            credentials.refresh(Request())

        # Metadata-server credentials (GCE VM, GKE pod, Cloud Run, CI) report
        # the alias "default" as their service_account_email until the first
        # refresh; only afterwards do they expose the real address. Never
        # return the alias — it would register the user "default" in the
        # ledger — fall through to tokeninfo and the env fallback instead.
        sa_email = getattr(credentials, "service_account_email", None)
        if sa_email and sa_email != "default":
            return sa_email

        url = f"https://oauth2.googleapis.com/tokeninfo?access_token={credentials.token}"
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req) as response:
            data = json.loads(response.read().decode())
            email = data.get("email")
            if email:
                return email
    except Exception as e:
        logger.error(f"Failed to resolve authenticated email: {e}")

    email_env = os.environ.get("GKE_MIGRATION_USER_EMAIL")
    if email_env:
        logger.debug(f"Using GKE_MIGRATION_USER_EMAIL fallback: {email_env}")
        return email_env

    raise PermissionError("Could not resolve authenticated user email from credentials.")

def scoped_config_path(cwd: Optional[str] = None) -> str:
    """Session-cache path scoped to a working directory.

    ~/.ledger_config.d/<sha256(cwd)[:16]>.yaml. Two sessions in the SAME cwd
    still share one cache (documented, acceptable). The DAG server always
    scopes by its own process cwd; a helper process serving another session's
    scope (the review frontend runs from the repo root) passes that session's
    cwd explicitly — deliberately NOT via an env var honored here, so an
    exported variable cannot silently collapse every server onto one scope.
    """
    if cwd is None:
        cwd = os.getcwd()
    digest = hashlib.sha256(cwd.encode("utf-8")).hexdigest()[:16]
    return os.path.join(LEDGER_CONFIG_DIR, f"{digest}.yaml")

def active_config_path(cwd: Optional[str] = None) -> Optional[str]:
    """The path an existing session would be read from, or None.

    The scoped file wins; the legacy machine-global file is a read-only
    fallback so pre-scoping sessions keep working (one-shot: reading it
    through read_local_config adopts it into the scoped path).
    """
    scoped = scoped_config_path(cwd)
    if os.path.exists(scoped):
        return scoped
    if os.path.exists(LEDGER_CONFIG_PATH):
        return LEDGER_CONFIG_PATH
    return None

def write_local_config(ledger_uri: str, resolved_role: str, workspace_name: str, gcp_project: str, **kwargs):
    config = {
        "ledger_uri": ledger_uri,
        "resolved_role": resolved_role,
        "workspace_name": workspace_name,
        "gcp_project": gcp_project,
        **kwargs
    }
    path = scoped_config_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        yaml.safe_dump(config, f)
    logger.debug(f"Cached session config in {path}")

def _adopt_legacy_config(config: dict, cwd: Optional[str]) -> None:
    """One-shot migration of the legacy machine-global cache (P0c).

    The legacy file records no cwd, so it cannot be tied to the session that
    wrote it: left in place it would serve as a default session for EVERY
    directory that never joined, indefinitely — the wrong-workspace attach
    P0c exists to kill. The first session that actually reads it adopts it
    into its own scoped path and deletes the global file, so pre-scoping
    sessions keep working and the fallback window closes after one read.
    Best-effort: any failure leaves the legacy file for the next read.
    """
    try:
        path = scoped_config_path(cwd)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            yaml.safe_dump(config, f)
        os.remove(LEDGER_CONFIG_PATH)
        logger.info(
            f"Adopted the legacy session cache {LEDGER_CONFIG_PATH} into {path} "
            f"(workspace '{config.get('workspace_name')}') and removed the legacy file."
        )
    except FileNotFoundError:
        pass  # another process of this session adopted it concurrently
    except Exception as e:
        logger.warning(f"Could not adopt the legacy session cache: {e}")

def read_local_config(cwd: Optional[str] = None) -> dict:
    path = active_config_path(cwd)
    if path is None:
        raise ValueError("No active workspace session found. Please run join_ledger first.")
    with open(path, "r") as f:
        config = yaml.safe_load(f)
    if not isinstance(config, dict) or not config:
        # An interrupted write leaves an empty or truncated cache file;
        # yaml.safe_load then yields None (or a non-mapping), which would
        # surface as an AttributeError at some .get() call site instead of an
        # actionable instruction. Malformed cache -> a clean re-join request.
        raise ValueError(
            f"Malformed session cache at {path} (expected a non-empty "
            "mapping). Re-run join_ledger from this working directory.")
    if path == LEDGER_CONFIG_PATH:
        _adopt_legacy_config(config, cwd)
    return config

def load_dag(bucket: storage.Bucket, dag_filename: str) -> dict:
    blob = bucket.blob(dag_filename)
    try:
        data = blob.download_as_text()
        dag = json.loads(data)
        # The ledger copy is writable by anyone with bucket access, so it is
        # validated on read rather than trusted because the bundled copy passed
        # at start-up.
        validate_dag(dag, f"{dag_filename} (ledger)")
        return dag
    except exceptions.NotFound:
        raise ValueError(f"DAG definition ({dag_filename}) not found in the GCS ledger bucket. Make sure bootstrapping was run.")
    except Exception as e:
        logger.error(f"Failed to download DAG {dag_filename} from GCS: {e}")
        raise

def load_inventory_schema() -> dict:
    """The inventory contract, as a dict.

    Public because a tool that lets a human set a schema-constrained field has
    to check the value BEFORE persisting it: an out-of-enum disposition
    recorded as a durable override would fail every later scan's save, and the
    scan is not where a review's typo should surface. Reading the schema
    rather than restating its enums is what keeps the two from drifting.
    """
    with open(_INVENTORY_SCHEMA_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def validate_inventory(inventory: dict) -> None:
    """Validates an inventory document against server/schema/inventory.json.

    Raises ValueError on the first schema violation. Machine sections are strict;
    LLM-authored sections (clusters, triggers, escalations, notes) are permissive
    by design, so this mostly guards the contract the replication step consumes.
    """
    import jsonschema

    validator = jsonschema.Draft202012Validator(load_inventory_schema())
    for err in sorted(validator.iter_errors(inventory), key=lambda e: list(e.absolute_path)):
        where = "/".join(str(p) for p in err.absolute_path) or "<root>"
        raise ValueError(f"Inventory failed schema validation at {where}: {err.message}")

def load_inventory(bucket: storage.Bucket) -> tuple[Optional[dict], Optional[int]]:
    """Reads platform/discovery/inventory.json from the ledger.

    Returns (inventory, generation), or (None, None) if no inventory has been
    written yet. The generation is passed back to save_inventory for the
    optimistic-concurrency precondition.
    """
    blob = bucket.blob(INVENTORY_BLOB_PATH)
    try:
        blob.reload()
        data = blob.download_as_text()
        return json.loads(data), blob.generation
    except exceptions.NotFound:
        return None, None

def save_inventory(bucket: storage.Bucket, inventory: dict, generation: Optional[int]) -> None:
    """Writes the inventory to the ledger under an optimistic-concurrency precondition.

    Pass the generation returned by load_inventory, or None when creating the
    blob (translated to if_generation_match=0, create-only). Validates against
    the inventory schema first. PreconditionFailed propagates to the caller —
    same convention as the state.json writes.
    """
    validate_inventory(inventory)
    blob = bucket.blob(INVENTORY_BLOB_PATH)
    data = json.dumps(inventory, indent=2)
    blob.upload_from_string(
        data,
        content_type="application/json",
        if_generation_match=generation if generation is not None else 0,
    )

def check_role_compatibility(requested: str, registered: str):
    if requested not in ROLE_HIERARCHY:
        raise ValueError(f"Unknown requested role: {requested}")
    if registered not in ROLE_HIERARCHY:
        raise ValueError(f"Unknown registered role: {registered}")
    if ROLE_HIERARCHY[requested] > ROLE_HIERARCHY[registered]:
        raise PermissionError(f"Access Denied: Cannot assume role '{requested}' with registered role '{registered}'.")

def _resolve_session(ctx: Context) -> tuple[dict, str, "storage.Bucket", str, str]:
    """Shared front half of the rehydrate paths (platform and workload).

    Reads the session config, resolves the caller's identity from ADC,
    re-reads the registry, applies the P0c workspace_name mismatch guard,
    normalizes and validates the cached/registered roles, and rejects
    privilege escalation. Returns (config, user_email, bucket, cached_role,
    registered_role); config also carries the resolved e-mail as
    ``user_email``. Prefix authorization is the caller's job: the platform
    path restricts to admins/platform, the workload path accepts developers.
    """
    config = read_local_config()
    ledger_uri = config["ledger_uri"]
    cached_role = config["resolved_role"]

    # 1. Resolve registered role dynamically from GCS registry
    user_email = get_authenticated_user_email()
    # Carried on the in-memory session only (never written to the cache
    # file) so a mutation that acts in the user's name — the PR commits —
    # can take it from the config the drain loop hands it, instead of
    # resolving ADC a second time.
    config["user_email"] = user_email
    bucket_name = get_bucket_name(ledger_uri)

    global gcs_client
    if not gcs_client:
        gcs_client = storage.Client(project=config["gcp_project"])

    bucket = gcs_client.bucket(bucket_name)
    blob_registry = bucket.blob("workspace_registry.yaml")

    try:
        registry_data = blob_registry.download_as_text()
    except exceptions.NotFound:
        raise PermissionError("Workspace registry not found in the ledger bucket.")
    except Exception as e:
        raise PermissionError(f"Failed to read workspace registry from GCS: {e}")

    registry = yaml.safe_load(registry_data)

    # P0c guard: a session cache naming one workspace while the registry it
    # points at names another means the cache is stale or was written by a
    # different session — refuse rather than act on the wrong workspace.
    registry_ws = registry.get("workspace_name")
    cached_ws = config.get("workspace_name")
    if registry_ws is not None and cached_ws != registry_ws:
        raise ValueError(
            f"Session cache mismatch: cached workspace_name '{cached_ws}' does not "
            f"match workspace '{registry_ws}' in the registry at '{ledger_uri}'. "
            "Re-run join_ledger from this working directory."
        )

    # Resolve registered role
    registered_role = None
    roles = registry.get("roles", {})
    for role_name, emails in roles.items():
        if user_email in emails:
            registered_role = role_name
            break

    if not registered_role:
         raise PermissionError(f"User email '{user_email}' is not registered in this workspace.")

    # Handle role de-escalation mapping
    if registered_role == "platform_engineers":
        registered_role = "platform"
    if cached_role == "platform_engineers":
        cached_role = "platform"

    # 2. Check role de-escalation validation
    if cached_role not in ROLE_HIERARCHY:
        raise PermissionError(f"Invalid cached role: {cached_role}")
    if registered_role not in ROLE_HIERARCHY:
        raise PermissionError(f"Invalid registered role: {registered_role}")

    if ROLE_HIERARCHY[cached_role] > ROLE_HIERARCHY[registered_role]:
        raise PermissionError("Privilege escalation detected: local cached role exceeds GCS registered privileges.")

    return config, user_email, bucket, cached_role, registered_role


def authorize_admin(ctx: Context) -> tuple[dict, str, "storage.Bucket"]:
    """The admin-only front half: session, identity, registry, no graph state.

    Workspace administration (members, resets, graph upgrades) reads no
    onboarding state — a reset may be running precisely because that state
    is gone — so this stops after the registry check. Both the registered
    role AND the cached role must be admins: an admin who joined
    de-escalated as a platform engineer chose to act as one.
    """
    config, user_email, bucket, cached_role, registered_role = _resolve_session(ctx)
    if registered_role != "admins" or cached_role != "admins":
        raise PermissionError(
            "Access Denied: workspace administration requires an admin session "
            "(registered as an admin and joined without de-escalating).")
    return config, user_email, bucket


def authorize_and_rehydrate(ctx: Context) -> tuple[dict, int, dict]:
    """Authenticates the user, checks authorization, and retrieves the current state and generation."""
    config, _, bucket, cached_role, _ = _resolve_session(ctx)

    # Resolve path prefix
    if cached_role not in ["admins", "platform"]:
        raise PermissionError("Access Denied: Only platform engineers or administrators can access this onboarding prefix.")

    state_blob_path = "platform/onboarding/state.json"
    blob_state = bucket.blob(state_blob_path)

    try:
        blob_state.reload()
        state_data = blob_state.download_as_text()
        generation = blob_state.generation
    except exceptions.NotFound:
        raise ValueError("Onboarding state file not found in GCS. Please run join_ledger first.")

    state_dict = json.loads(state_data)
    return state_dict, generation, config


def authorize_and_rehydrate_workload(
    ctx: Context, component: Optional[str] = None, enforce_claimant: bool = True
) -> tuple[dict, int, dict]:
    """Rehydrates a component's workload state — the developer-path twin.

    Accepts developers and above (the escalation check ran in
    _resolve_session; no prefix restriction beyond it). `component` defaults
    to the session cache's. This is the ONE place claimant enforcement lives: every state-mutating workload tool calls this with
    the default enforce_claimant=True; the read-only browse tool passes False.
    """
    config, user_email, bucket, _, _ = _resolve_session(ctx)
    component = component or config.get("component")
    if not component:
        raise ValueError(
            "No component is recorded for this session. Re-run join_ledger "
            "with component=<id> from this working directory."
        )

    blob_state = bucket.blob(f"workloads/{component}/state.json")
    try:
        blob_state.reload()
        state_data = blob_state.download_as_text()
        generation = blob_state.generation
    except exceptions.NotFound:
        raise ValueError(
            f"Component '{component}' is not initialized in this ledger — "
            f"run join_ledger with component='{component}' first."
        )
    except exceptions.Forbidden:
        raise PermissionError(
            f"GCS denied access to workloads/{component}/ — the component's "
            "managed-folder binding has not been granted yet.\n"
            + admin_binding_step(
                get_bucket_name(config["ledger_uri"]), component, user_email)
        )

    state_dict = json.loads(state_data)
    variables = state_dict.get("variables") or {}

    # The authorized object and the object the tools mutate must be the SAME
    # component: tools derive their write paths from what this function hands
    # back, and a state.json whose variables.component names a different id
    # (hand-edited, or written by a prior claimant) would let the claimant
    # check guard component A while the write lands on component B.
    recorded_component = variables.get("component")
    if recorded_component is not None and recorded_component != component:
        raise ValueError(
            f"workloads/{component}/state.json records component "
            f"'{recorded_component}' — the ledger object is inconsistent, "
            "refusing to act on it. Re-run join_ledger for the component you "
            "intend to work on."
        )

    if enforce_claimant:
        claimant = (variables.get("claim") or {}).get("claimant")
        if not claimant:
            # An absent claimant is a hard stop, not a pass: state.json is
            # writable ledger data (same trust posture load_dag applies to
            # the graph copy), and "nobody recorded" must not mean "anybody
            # may mutate".
            raise PermissionError(
                f"Component '{component}' records no claimant — the claim is "
                "missing or was edited away. Re-run join_ledger(..., "
                f"component='{component}') to (re)record the claim before "
                "mutating this component."
            )
        if claimant != user_email:
            raise PermissionError(
                f"Component '{component}' is claimed by {claimant}; only the "
                "claimant may act on it. Take it over deliberately with "
                f"join_ledger(..., component='{component}', "
                "reclaim_component=True)."
            )
    # Hand the authorized component back on the config so every tool writes
    # exactly where this call authorized (session caches predating the
    # component key, or an explicit `component` argument, are normalized too).
    config["component"] = component
    return state_dict, generation, config


def workload_write_denied(config: dict, component: str) -> str:
    """The D11 joined-but-blocked instruction for a 403 on a workload WRITE.

    The read path converts Forbidden inside authorize_and_rehydrate_workload,
    but a write can still 403 on its own: the admin bound objectViewer
    instead of objectAdmin, the grant was revoked or is still propagating
    after a takeover. Every workload upload converts that into this
    instruction instead of leaking a stack trace (DESIGN 4.3).
    """
    try:
        email = get_authenticated_user_email()
    except PermissionError:
        email = "<your-email>"
    return (
        f"ERROR: GCS denied the write to workloads/{component}/ — the "
        "component's managed-folder binding is missing, read-only, or still "
        "propagating.\n"
        + admin_binding_step(
            get_bucket_name(config["ledger_uri"]), component, email)
    )


def dag_transition(expected_state: str, dag_filename: str = "platform_dag.json"):
    def decorator(func):
        is_async = inspect.iscoroutinefunction(func)

        def _pre_transition(ctx, func_name):
            state_dict, generation, config = authorize_and_rehydrate(ctx)
            current_state = state_dict["current_state"]
            if current_state != expected_state:
                raise ValueError(f"Agent is in state '{current_state}', but {func_name} requires '{expected_state}'.")

            bucket_name = get_bucket_name(config["ledger_uri"])
            bucket = gcs_client.bucket(bucket_name)
            dag = load_dag(bucket, dag_filename)

            state_def = dag["states"].get(current_state)
            if not state_def:
                raise ValueError(f"Unknown state in DAG {dag_filename}: {current_state}")
            if "on_tool_call_received" not in state_def.get("transitions", {}):
                raise ValueError(f"No 'on_tool_call_received' transition defined for state '{current_state}'.")

            return state_dict, generation, config, state_def

        def _post_transition(state_dict, generation, config, state_def, func_name):
            next_state = state_def["transitions"]["on_tool_call_received"]
            state_dict["history"].append(f"Transitioned {state_dict['current_state']} -> {next_state} via {func_name}")
            state_dict["current_state"] = next_state

            data = json.dumps(state_dict, indent=2)
            bucket_name = get_bucket_name(config["ledger_uri"])
            bucket = gcs_client.bucket(bucket_name)
            blob_state = bucket.blob("platform/onboarding/state.json")
            blob_state.upload_from_string(data, content_type="application/json", if_generation_match=generation)

        if is_async:
            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                ctx = next((arg for arg in args if isinstance(arg, Context)), kwargs.get("ctx"))

                try:
                    state_dict, generation, config, state_def = await asyncio.to_thread(_pre_transition, ctx, func.__name__)
                except Exception as e:
                    return f"ERROR: {e}"

                result = await func(*args, **kwargs)

                if isinstance(result, str) and result.startswith("ERROR"):
                    return result

                try:
                    await asyncio.to_thread(_post_transition, state_dict, generation, config, state_def, func.__name__)
                except exceptions.PreconditionFailed:
                    return "ERROR: Concurrent update conflict. Your changes were not saved."
                except Exception as e:
                    return f"ERROR: Failed to update state: {e}"

                return result
            return async_wrapper
        else:
            @functools.wraps(func)
            def sync_wrapper(*args, **kwargs):
                ctx = next((arg for arg in args if isinstance(arg, Context)), kwargs.get("ctx"))

                try:
                    state_dict, generation, config, state_def = _pre_transition(ctx, func.__name__)
                except Exception as e:
                    return f"ERROR: {e}"

                result = func(*args, **kwargs)

                if isinstance(result, str) and result.startswith("ERROR"):
                    return result

                try:
                    _post_transition(state_dict, generation, config, state_def, func.__name__)
                except exceptions.PreconditionFailed:
                    return "ERROR: Concurrent update conflict. Your changes were not saved."
                except Exception as e:
                    return f"ERROR: Failed to update state: {e}"

                return result
            return sync_wrapper
    return decorator
