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

"""Read-only review frontend for the migration ledger.

Serves a single-page DAG view of the migration: every state in
platform_dag.json, the state the workspace is currently in, the artifacts each
step produced (manifest, scope, fragments, inventory, readiness report), and
the role the operator is acting under. Strictly read-only: approvals and
scope changes flow through the MCP tools in the agent conversation — this UI
never writes to the ledger and cannot advance the DAG.

Reads the same live GCS ledger the MCP server uses, resolved via the session
cache (cwd-scoped ~/.ledger_config.d/<hash>.yaml, legacy ~/.ledger_config.yaml
as read fallback) and Application Default Credentials. The launcher spawns
this process from the repo root and pins it to the spawning session via
GKMA_SESSION_CWD; that variable is interpreted HERE (passed explicitly into
the session-cache lookup) — the DAG server itself always scopes by its own
working directory and ignores it.

Run from the repo root:
    PYTHONPATH=. python -m servers.frontend.server [--port 8642]
"""

import argparse
import json
import logging
import os
import sys
import time

import yaml
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.trustedhost import TrustedHostMiddleware
from starlette.responses import FileResponse, JSONResponse
from starlette.routing import Mount, Route
from starlette.staticfiles import StaticFiles

from servers.frontend import ledger_view

# The ledger backend (servers.dag.state_management) imports its server/
# package as a top-level `server`, so servers/dag itself must be on sys.path
# — `PYTHONPATH=. python -m servers.frontend.server` alone would not put it
# there, and neither does the launcher's spawned environment.
_DAG_DIR = os.path.realpath(os.path.join(
    os.path.dirname(os.path.realpath(__file__)), "..", "dag"))
if _DAG_DIR not in sys.path:
    sys.path.append(_DAG_DIR)

logger = logging.getLogger("migration-frontend")

STATIC_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "static")
STATE_BLOB = "platform/onboarding/state.json"
REGISTRY_BLOB = "workspace_registry.yaml"
DAG_BLOB = "platform_dag.json"
PROGRESS_BLOB = "platform/discovery/extraction-progress.json"
FRAGMENT_PREFIX = "platform/discovery/fragments/"
TRANSLATION_PROGRESS_BLOB = "platform/translation/translation-progress.json"
TRANSLATION_UNIT_PREFIX = "platform/translation/units/"
BLOB_SIZE_LIMIT = 2_000_000
HISTORY_TAIL = 100
# Within one extraction run a fragment is effectively write-once (resume
# skips chunks that already have one), so caching by path keeps the
# live-progress poll from re-reading every fragment each tick. The cache is
# dropped whenever the progress blob changes — a new run (rediscovery,
# amended scope, retry after a transient read failure) may rewrite the same
# content-addressed paths with different worker output.
FRAGMENT_CACHE_LIMIT = 4096
_PROGRESS_CACHE_KEY = "\x00progress"  # never collides with a blob path

_MERGER = None


def _discovery_merger():
    """Loads the pure fragment merger straight from its file.

    Importing it as servers.phases.discovery.* would execute the phase
    package __init__, which drags in mcp/git dependencies the frontend has
    no other need for. merger.py itself imports nothing.
    """
    global _MERGER
    if _MERGER is None:
        import importlib.util

        path = os.path.realpath(os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "..", "phases",
            "discovery", "discovery_extract_3", "merger.py"))
        spec = importlib.util.spec_from_file_location(
            "gkma_discovery_merger", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        _MERGER = module
    return _MERGER


# The DAG and registry change rarely; cache them briefly so the UI's poll
# loop doesn't hammer GCS. State is always read fresh.
CACHE_TTL_SECONDS = 30.0


class GcsLedger:
    """Reads the live ledger with the same config and credentials as the MCP server.

    Construction never touches the network or the local session config —
    everything resolves lazily per request, so the server starts (and keeps
    serving a structured error) before join_ledger has run, and picks up a
    re-join to a different workspace without a restart.
    """

    mode = "gcs"

    def __init__(self):
        self.config = {}
        self.client = None
        self.bucket = None
        self.bucket_name = None
        # Bumped whenever a config change swaps the bucket, so make_app can
        # drop caches that would otherwise bleed one workspace into another.
        self.generation = 0

    def _ensure(self):
        # Imported lazily so this module stays importable (and make_app testable
        # against a fake ledger) without eagerly resolving state_management.
        from servers.dag import state_management as state_mgr

        from google.cloud import storage

        self._state_mgr = state_mgr
        # GKMA_SESSION_CWD pins this (repo-root-spawned) process to the scope
        # of the DAG server that launched it; unset on a hand-run instance,
        # which then scopes by its own cwd like any session would.
        session_cwd = os.environ.get("GKMA_SESSION_CWD") or None
        config = state_mgr.read_local_config(cwd=session_cwd)  # raises the join_ledger hint if absent
        if self.client is None or config != self.config:
            self.config = config
            self.bucket_name = state_mgr.get_bucket_name(config["ledger_uri"])
            self.client = storage.Client(project=config.get("gcp_project"))
            self.bucket = self.client.bucket(self.bucket_name)
            self.generation += 1

    def read_text(self, path: str):
        from google.api_core import exceptions
        self._ensure()
        try:
            return self.bucket.blob(path).download_as_text()
        except exceptions.NotFound:
            return None

    def exists(self, path: str) -> bool:
        """Metadata-only presence probe — no object download."""
        self._ensure()
        return self.bucket.blob(path).exists()

    def read_text_and_generation(self, path: str):
        from google.api_core import exceptions
        self._ensure()
        blob = self.bucket.blob(path)
        try:
            blob.reload()
            return blob.download_as_text(), blob.generation
        except exceptions.NotFound:
            return None, None

    def list_paths(self, prefix: str) -> list:
        self._ensure()
        return sorted(b.name for b in self.client.list_blobs(self.bucket_name, prefix=prefix))

    def identity(self) -> dict:
        try:
            self._ensure()
        except Exception as e:
            return {"mode": self.mode, "user_email": None, "acting_role": "unknown",
                    "ledger_uri": None, "identity_error": str(e)}
        acting = self.config.get("resolved_role") or "unknown"
        if acting == "platform_engineers":
            acting = "platform"
        email, error = None, None
        try:
            email = self._state_mgr.get_authenticated_user_email()
        except Exception as e:
            error = str(e)
        return {
            "mode": self.mode,
            "user_email": email,
            "acting_role": acting,
            "ledger_uri": self.config.get("ledger_uri"),
            "identity_error": error,
        }


def _json_or_none(text):
    if text is None:
        return None
    try:
        return json.loads(text)
    except (json.JSONDecodeError, TypeError):
        return None


def make_app(ledger, allowed_hosts=None) -> Starlette:
    cache = {}
    fragment_cache = {}
    # Same write-once discipline as fragment_cache: a translated unit's blob is
    # immutable within a run, so once we've confirmed a unit finished this run
    # we cache its terminal status (done/error) and stop re-downloading it every
    # poll. Dropped whenever the progress blob's run token changes.
    translation_cache = {}
    seen_generation = [getattr(ledger, "generation", 0)]

    def drop_stale_caches():
        """A re-join swaps the bucket under the same process; anything cached
        from the previous workspace (DAG, registry, fragments) must go with it."""
        ensure = getattr(ledger, "_ensure", None)
        if ensure is not None:
            try:
                ensure()  # cheap: re-reads the small local session file
            except Exception:
                pass  # no usable session yet — the read paths report that
        generation = getattr(ledger, "generation", 0)
        if generation != seen_generation[0]:
            seen_generation[0] = generation
            cache.clear()
            fragment_cache.clear()
            translation_cache.clear()

    def cached_read(path: str):
        drop_stale_caches()
        now = time.monotonic()
        hit = cache.get(path)
        if hit and now - hit[0] < CACHE_TTL_SECONDS:
            return hit[1]
        text = ledger.read_text(path)
        cache[path] = (now, text)
        return text

    def load_context():
        """Reads dag/state/registry; returns (context, error_message)."""
        dag_text = cached_read(DAG_BLOB)
        if dag_text is None:
            return None, f"DAG definition ({DAG_BLOB}) not found in the ledger. Run bootstrapping first."
        dag = _json_or_none(dag_text)
        if not dag:
            return None, f"DAG definition ({DAG_BLOB}) in the ledger is not valid JSON."
        state_text, state_generation = ledger.read_text_and_generation(STATE_BLOB)
        if state_text is None:
            return None, "Onboarding state not found in the ledger. Run join_ledger first."
        state = _json_or_none(state_text)
        if not state:
            return None, f"Onboarding state ({STATE_BLOB}) in the ledger is not valid JSON."
        registry, registry_ok = {}, False
        registry_text = cached_read(REGISTRY_BLOB)
        if registry_text:
            try:
                parsed = yaml.safe_load(registry_text)
            except yaml.YAMLError:
                parsed = None
            if isinstance(parsed, dict):
                registry, registry_ok = parsed, True
        return {"dag": dag, "state": state, "state_generation": state_generation,
                "registry": registry, "registry_ok": registry_ok}, None

    def overview(request):
        identity = ledger.identity()
        context, error = load_context()
        if error:
            return JSONResponse({"error": error, "identity": identity})

        state = context["state"]
        variables = state.get("variables") or {}
        config = getattr(ledger, "config", {}) or {}
        roles = ledger_view.resolve_roles(
            {**config, "resolved_role": identity["acting_role"]},
            context["registry"],
            identity.get("user_email"),
        )
        return JSONResponse({
            "identity": identity,
            "roles": roles,
            "members": ledger_view.registry_members(context["registry"]),
            "registry_ok": context["registry_ok"],
            "dag": ledger_view.build_dag_view(context["dag"]),
            "dag_name": context["dag"].get("name"),
            "dag_version": context["dag"].get("version"),
            "current_state": state.get("current_state"),
            "state_generation": context["state_generation"],
            "visited": ledger_view.parse_visited(state.get("history")),
            "history": (state.get("history") or [])[-HISTORY_TAIL:],
            "variable_keys": {k: len(json.dumps(v, default=str)) for k, v in variables.items()},
        })

    def state_detail(request):
        name = request.path_params["name"]
        context, error = load_context()
        if error:
            return JSONResponse({"error": error}, status_code=503)
        if name not in context["dag"].get("states", {}):
            return JSONResponse({"error": f"Unknown state: {name}"}, status_code=404)

        node = next(n for n in ledger_view.build_dag_view(context["dag"])["nodes"] if n["name"] == name)
        variables = context["state"].get("variables") or {}

        artifacts = []
        for artifact in ledger_view.artifacts_for_state(name):
            entry = {"id": artifact["id"], "label": artifact["label"], "render": artifact["render"]}
            caveat = (artifact.get("caveats") or {}).get(name)
            if caveat:
                entry["caveat"] = caveat
            source = artifact["source"]
            if "blob" in source:
                entry["blob"] = source["blob"]
                # Metadata-only: availability must not download multi-MB
                # artifacts just to derive a boolean on every 4s poll.
                entry["available"] = ledger.exists(source["blob"])
            elif "prefix" in source:
                items = ledger.list_paths(source["prefix"])
                entry["items"] = items
                entry["available"] = bool(items)
            elif "variable" in source:
                value = variables.get(source["variable"])
                entry["available"] = value is not None
                if entry["available"]:
                    serialized = json.dumps(value, default=str)
                    if len(serialized) <= ledger_view.INLINE_VARIABLE_LIMIT:
                        entry["content"] = value
                    else:
                        entry["note"] = f"value too large to inline ({len(serialized)} bytes)"
            elif "variables" in source:
                composite = {k: variables.get(k) for k in source["variables"] if k in variables}
                entry["available"] = bool(composite)
                entry["content"] = composite
            elif "endpoint" in source:
                entry["endpoint"] = source["endpoint"]
                entry["available"] = True  # the endpoint reports its own emptiness
            artifacts.append(entry)

        transitions = context["dag"]["states"][name].get("transitions") or {}
        return JSONResponse({"node": node, "transitions": transitions, "artifacts": artifacts})

    def extraction_progress(request):
        """Live view of a running extraction: fragments merged so far.

        Aggregates the same per-chunk fragments the final merge uses, so the
        reviewer watches the inventory grow resource by resource while the
        workers are still running. Read-only and best-effort — absent
        progress just means no extraction has started for this scope.
        """
        drop_stale_caches()  # fragment cache must not survive a re-join
        progress_text = ledger.read_text(PROGRESS_BLOB)
        progress = _json_or_none(progress_text)
        if not progress:
            return JSONResponse({"available": False})
        if fragment_cache.get(_PROGRESS_CACHE_KEY) != progress_text:
            fragment_cache.clear()
            fragment_cache[_PROGRESS_CACHE_KEY] = progress_text

        fragments = []
        done_ids = []
        for chunk_id in progress.get("chunk_ids") or []:
            path = f"{FRAGMENT_PREFIX}{chunk_id}.json"
            text = fragment_cache.get(path)
            if text is None:
                text = ledger.read_text(path)
                if text is not None and len(fragment_cache) < FRAGMENT_CACHE_LIMIT:
                    fragment_cache[path] = text
            fragment = _json_or_none(text)
            if fragment is None:
                continue
            done_ids.append(chunk_id)
            fragments.append(fragment)

        merger = _discovery_merger()
        return JSONResponse({
            "available": True,
            "total": progress.get("total_chunks"),
            "done": len(done_ids),
            "reused": progress.get("reused"),
            "inventory": merger.merge_fragments(fragments) if fragments else None,
        })

    def translation_progress(request):
        """Live view of a running translation: units produced so far.

        Mirrors extraction_progress. run_translation writes a progress blob
        (run token, total, reused, the ids being translated this run) before the
        fan-out and persists each unit blob the moment its worker finishes, so
        the reviewer watches the bar fill unit by unit instead of nothing until
        the whole multi-minute batch completes. A unit counts as finished only
        when its blob carries the current run token — a revised unit's stale
        blob from a prior run has an older token and stays 'in progress' until
        this run re-produces it. A unit whose blob is an error (status 'error'
        or a null result) is reported separately so the UI can flag it instead
        of drawing it as a green success. Read-only and best-effort.
        """
        drop_stale_caches()  # unit-status cache must not survive a re-join
        progress = _json_or_none(ledger.read_text(TRANSLATION_PROGRESS_BLOB))
        if not progress:
            return JSONResponse({"available": False})

        run = progress.get("run")
        # A finished unit blob is write-once within a run, so cache its terminal
        # status by unit id and drop the whole cache when the run token changes
        # (a revision/retry re-produces units under a new token). Only positive
        # (produced-this-run) results are cached — a unit still carrying a stale
        # blob, or none at all, is re-read every poll until this run produces it.
        if translation_cache.get(_PROGRESS_CACHE_KEY) != run:
            translation_cache.clear()
            translation_cache[_PROGRESS_CACHE_KEY] = run

        pending_ids = progress.get("pending_ids") or []
        done_ids = []
        error_ids = []
        for unit_id in pending_ids:
            status = translation_cache.get(unit_id)
            if status is None:
                blob = _json_or_none(ledger.read_text(f"{TRANSLATION_UNIT_PREFIX}{unit_id}.json"))
                if blob is not None and blob.get("run") == run:
                    unit = blob.get("unit") or {}
                    status = ("error" if unit.get("status") == "error"
                              or blob.get("result") is None else "done")
                    if len(translation_cache) < FRAGMENT_CACHE_LIMIT:
                        translation_cache[unit_id] = status
            if status == "done":
                done_ids.append(unit_id)
            elif status == "error":
                error_ids.append(unit_id)

        reused = progress.get("reused") or 0
        return JSONResponse({
            "available": True,
            "total": progress.get("total_units"),
            "done": reused + len(done_ids),
            "failed": len(error_ids),
            "reused": reused,
            "done_ids": sorted(done_ids),
            "error_ids": sorted(error_ids),
            "pending_ids": sorted(pending_ids),
        })

    def blob(request):
        path = request.query_params.get("path", "")
        if not ledger_view.is_blob_allowed(path):
            return JSONResponse({"error": f"Blob not servable: {path}"}, status_code=403)
        text = ledger.read_text(path)
        if text is None:
            return JSONResponse({"error": f"Blob not found: {path}"}, status_code=404)
        truncated = len(text) > BLOB_SIZE_LIMIT
        return JSONResponse({
            "path": path,
            "content": text[:BLOB_SIZE_LIMIT],
            "truncated": truncated,
        })

    def index(request):
        return FileResponse(os.path.join(STATIC_DIR, "index.html"))

    def ledger_error(request, exc):
        """Backend failures (expired ADC token, IAM change, GCS outage, missing
        session config) surface as the structured JSON the UI renders, instead
        of a bare 500 the poll loop would misreport as 'server unreachable'."""
        logger.exception("Ledger access failed")
        try:
            identity = ledger.identity()
        except Exception:
            identity = None
        return JSONResponse(
            {"error": f"Ledger access failed: {type(exc).__name__}: {exc}", "identity": identity},
            status_code=503,
        )

    # Host-header validation blocks DNS-rebinding: a hostile page resolving
    # its own domain to 127.0.0.1 would otherwise read the ledger through the
    # victim's browser (the server itself has no auth).
    middleware = [Middleware(TrustedHostMiddleware, allowed_hosts=allowed_hosts)] if allowed_hosts else []

    return Starlette(
        routes=[
            Route("/", index),
            Route("/api/overview", overview),
            Route("/api/state/{name}", state_detail),
            Route("/api/progress/extraction", extraction_progress),
            Route("/api/progress/translation", translation_progress),
            Route("/api/blob", blob),
            Mount("/static", StaticFiles(directory=STATIC_DIR), name="static"),
        ],
        middleware=middleware,
        exception_handlers={Exception: ledger_error},
    )


def main():
    parser = argparse.ArgumentParser(description="Migration ledger review frontend (read-only)")
    parser.add_argument("--host", default="127.0.0.1",
                        help="bind address (default 127.0.0.1; the UI has no auth of its own)")
    parser.add_argument("--port", type=int, default=8642)
    args = parser.parse_args()

    ledger = GcsLedger()
    try:
        ledger._ensure()
        print(f"Serving live ledger {ledger.config.get('ledger_uri')} "
              f"as role '{ledger.config.get('resolved_role')}'")
    except Exception as e:
        print(f"No usable workspace session yet ({e}) — "
              "the UI will show this error until join_ledger is run.")

    allowed_hosts = sorted({args.host, "127.0.0.1", "localhost", "::1"})
    import uvicorn
    uvicorn.run(make_app(ledger, allowed_hosts=allowed_hosts),
                host=args.host, port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
