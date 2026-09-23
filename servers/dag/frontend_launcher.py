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

"""Best-effort auto-start of the read-only review frontend.

When a workspace session becomes active (join_ledger, or the first
get_next_stage after a server restart), the DAG server makes sure the review
UI from servers/frontend is running and returns its URL. The announcement
instructs the agent to open that URL in its integrated browser (so the human
does not have to alt-tab to Chrome) and to also surface the plain link.

The frontend resolves the session cache lazily per request, so one
instance follows re-joins to other workspaces without a restart. The cache is
scoped per working directory (state_management.scoped_config_path), and the
frontend is spawned with cwd=repo root — so the spawn passes this server's
cwd via GKMA_SESSION_CWD, pinning the child to the spawning session's scope.
A running instance keeps the scope of whichever session spawned it, so the
launcher reuses one only when the identity it serves (/api/overview's
ledger_uri) matches THIS session's cached ledger — handing the reviewer a
healthy instance pinned to another session's workspace would silently render
the wrong ledger. On mismatch the port walk continues and a per-session
instance is spawned on the next free port. Instances are detached on
purpose: the UI outlives the MCP server so the reviewer's tab keeps working
across server restarts.

Nothing here may ever fail a tool call: every problem degrades to "no URL"
with a log line. Set GKE_AGENTIC_MIGRATION_FRONTEND=0 to disable, GKE_AGENTIC_MIGRATION_FRONTEND_PORT to
move the port (default 8642; the next 9 ports are tried when taken by
something else), and GKE_AGENTIC_MIGRATION_FRONTEND_OPEN=0 to stop the reviewer's browser
from being opened automatically when the UI first comes up.

Inside a unit-test process both switches default to OFF instead of on: when
GKE_AGENTIC_MIGRATION_FRONTEND / _OPEN are unset and `unittest` or `pytest` has
been imported (the DAG server itself imports neither), ensure_review_frontend
returns None without probing a port and no browser is opened. A test that
reaches join_ledger without thinking about the launcher therefore cannot
spawn a server or pop a browser; a test that wants the real behaviour sets
the variable explicitly, and an explicit value always wins. Real users are
unaffected: the default stays on outside a test runner. Because that default
rests on module introspection, the flip is never silent: the first time an
unset switch reads as off for this reason the launcher logs one INFO line
naming the variable to set, and frontend_launcher_test.py checks in a fresh
interpreter that importing the server pulls in neither runner.
"""

import json
import logging
import os
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

logger = logging.getLogger("migration-dag")

DEFAULT_PORT = 8642
PORT_ATTEMPTS = 10
STARTUP_WAIT_SECONDS = 8.0
PROBE_TIMEOUT_SECONDS = 3.0  # /api/overview does auth+ledger I/O in gcs mode
ENSURE_BUDGET_SECONDS = 20.0  # hard wall-clock cap for one ensure call
FAILURE_CACHE_SECONDS = 300.0  # don't re-walk ports on every poll after failing
_POLL_INTERVAL_SECONDS = 0.25

_cached_url = None
_announced_url = None
_opened_url = None  # browser popped once per process (again only if the URL changes)
_failed_until = 0.0  # time.monotonic() before which ensure won't retry
_last_proc = None  # last Popen this process spawned (tests terminate exactly it)
_runner_default_logged = False  # the "off under a test runner" INFO line goes out once


def _repo_root() -> str:
    return os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _under_test_runner() -> bool:
    """True inside a unit-test process: unittest or pytest has been imported.

    The DAG server never imports either, so this is exactly "a test suite is
    running", whichever runner or module started it.
    """
    return "unittest" in sys.modules or "pytest" in sys.modules


def _switched_off(name: str) -> bool:
    """Whether a GKE_AGENTIC_MIGRATION_FRONTEND* switch is off.

    An explicit value always wins. Unset means on for real users and off
    inside a test runner (see the module docstring).
    """
    global _runner_default_logged
    raw = os.environ.get(name)
    if raw is None:
        if not _under_test_runner():
            return False
        # The flip to off rests on which modules happen to be imported, so it
        # must never be silent: say so once per process and name the override.
        if not _runner_default_logged:
            _runner_default_logged = True
            logger.info(
                "%s is unset and a test runner (unittest or pytest) is imported in"
                " this process; treating the review frontend switch as off."
                " Set %s=1 to override.",
                name, name,
            )
        return True
    return raw.strip().lower() in ("0", "false", "no", "off")


def _disabled() -> bool:
    return _switched_off("GKE_AGENTIC_MIGRATION_FRONTEND")


def _base_port() -> int:
    try:
        return int(os.environ.get("GKE_AGENTIC_MIGRATION_FRONTEND_PORT", str(DEFAULT_PORT)))
    except ValueError:
        logger.warning("Invalid GKE_AGENTIC_MIGRATION_FRONTEND_PORT; using %d.", DEFAULT_PORT)
        return DEFAULT_PORT


def _probe_identity(port: int):
    """The identity dict our review UI serves on the port, or None.

    None means "not our app". A dict (possibly empty) means the /api/overview
    contract answered — this asks "is OUR app listening here", not "is the
    ledger healthy": the UI serves its structured error as a 503 with the
    same identity envelope (no session yet, expired credentials, GCS outage),
    and treating that as "not ours" would kill and respawn a perfectly good
    instance — the UI itself is the surface that shows the reviewer what is
    wrong.
    """
    try:
        try:
            with urllib.request.urlopen(
                f"http://127.0.0.1:{port}/api/overview", timeout=PROBE_TIMEOUT_SECONDS
            ) as resp:
                if resp.status != 200:
                    return None
                body = resp.read()
        except urllib.error.HTTPError as e:
            if e.code != 503:
                return None
            body = e.read()  # our structured ledger-error response
        data = json.loads(body.decode("utf-8"))
        if not (isinstance(data, dict) and "identity" in data):
            return None
        identity = data.get("identity")
        return identity if isinstance(identity, dict) else {}
    except Exception:
        return None


def _is_review_frontend(port: int) -> bool:
    """True if our review UI answers on the port (its /api/overview contract)."""
    return _probe_identity(port) is not None


def _session_ledger_uri():
    """THIS session's cached ledger_uri, or None when there is no session.

    Reads the cache file directly (no read_local_config) so a health probe
    never triggers the one-shot legacy-cache adoption as a side effect.
    """
    try:
        import state_management
        path = state_management.active_config_path()
        if path is None:
            return None
        import yaml
        with open(path, "r") as f:
            return (yaml.safe_load(f) or {}).get("ledger_uri")
    except Exception:
        return None


def _scope_matches(identity, session_uri) -> bool:
    """Whether a running instance's served identity is THIS session's scope.

    No local session means nothing to compare against (reuse is safe: the
    instance can only render what our announcement claims once we have one).
    A served identity without a ledger_uri is another session's scope or a
    broken instance — do not present it as this session's review surface.
    """
    if session_uri is None:
        return True
    return identity.get("ledger_uri") == session_uri


def _port_in_use(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.5)
        return s.connect_ex(("127.0.0.1", port)) == 0


def _spawn(port: int, deadline: float) -> bool:
    global _last_proc
    root = _repo_root()
    env = dict(os.environ)
    # servers/dag as well: the ledger backend imports its server/ package
    # top-level, so the repo root alone is not enough (server.py also guards
    # this itself, but the spawned env should stand on its own).
    env["PYTHONPATH"] = (root + os.pathsep + os.path.join(root, "servers", "dag")
                         + os.pathsep + env.get("PYTHONPATH", ""))
    # The session cache is scoped per cwd, and the child runs from the repo
    # root — hand it this server's scope so it reads the same session.
    # Unconditionally: an inherited value (e.g. exported in the user's shell
    # for a hand-run frontend) must not repoint the child at another scope.
    env["GKMA_SESSION_CWD"] = os.getcwd()
    log_path = os.path.join(
        tempfile.gettempdir(),
        f"gke-agentic-migration-frontend-{port}-{os.getuid()}.log"
    )
    try:
        with open(log_path, "ab") as log_file:
            proc = subprocess.Popen(
                [sys.executable, "-m", "servers.frontend.server", "--port", str(port)],
                cwd=root,
                env=env,
                stdin=subprocess.DEVNULL,
                stdout=log_file,
                stderr=log_file,
                start_new_session=True,  # outlive the MCP server on purpose
            )
    except Exception as e:
        logger.warning("Could not spawn the review frontend: %r", e)
        return False

    _last_proc = proc
    wait_until = min(time.monotonic() + STARTUP_WAIT_SECONDS, deadline)
    while time.monotonic() < wait_until:
        if _is_review_frontend(port):
            return True
        if proc.poll() is not None:
            logger.warning(
                "Review frontend exited with code %s during startup; see %s.",
                proc.returncode, log_path,
            )
            return False
        time.sleep(_POLL_INTERVAL_SECONDS)
    logger.warning("Review frontend did not answer within %.0fs; see %s.",
                   STARTUP_WAIT_SECONDS, log_path)
    # Only an instance that passed the health probe may outlive us — an
    # unresponsive one would otherwise squat its port as a detached orphan
    # and push every later walk one port further, spawning more of them.
    try:
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            proc.kill()
    except Exception:
        pass
    return False


def ensure_review_frontend():
    """Returns the review UI URL, starting or reusing the server as needed.

    Returns None (never raises) when disabled or unavailable.
    """
    global _cached_url, _failed_until
    try:
        if _disabled():
            return None
        session_uri = _session_ledger_uri()
        if _cached_url is not None:
            port = int(_cached_url.rsplit(":", 1)[1])
            identity = _probe_identity(port)
            if identity is not None and _scope_matches(identity, session_uri):
                return _cached_url
            _cached_url = None
        if time.monotonic() < _failed_until:
            return None  # failed recently; don't re-walk the ports every poll
        deadline = time.monotonic() + ENSURE_BUDGET_SECONDS
        base = _base_port()
        for port in range(base, base + PORT_ATTEMPTS):
            if time.monotonic() >= deadline:
                break
            identity = _probe_identity(port)
            if identity is not None:
                if _scope_matches(identity, session_uri):
                    _cached_url = f"http://127.0.0.1:{port}"
                    logger.info("Reusing review frontend at %s.", _cached_url)
                    return _cached_url
                # A healthy instance, but pinned to another session's scope —
                # reusing it would show the reviewer the wrong workspace.
                logger.info(
                    "Review frontend on port %d serves %s, not this session's %s;"
                    " continuing the port walk.",
                    port, identity.get("ledger_uri"), session_uri,
                )
                continue
            if _port_in_use(port):
                continue  # something else owns this port
            if _spawn(port, deadline):
                _cached_url = f"http://127.0.0.1:{port}"
                logger.info("Started review frontend at %s.", _cached_url)
                return _cached_url
            # A spawn that came up broken would come up broken on the next
            # port too — stop the walk instead of fanning out more attempts.
            break
        _failed_until = time.monotonic() + FAILURE_CACHE_SECONDS
        logger.warning(
            "Review frontend unavailable on ports %d..%d; will not retry for %.0fs.",
            base, base + PORT_ATTEMPTS - 1, FAILURE_CACHE_SECONDS,
        )
        return None
    except Exception as e:
        _failed_until = time.monotonic() + FAILURE_CACHE_SECONDS
        logger.warning("Review frontend unavailable: %r", e)
        return None


def _maybe_open_browser(url: str) -> None:
    """Best-effort: pop the reviewer's system browser at the UI, once.

    The announcement also tells the agent to open the URL in its integrated
    browser, but that depends on the harness honoring the instruction —
    opening the browser here makes the live view appear without anyone doing
    anything. GKE_AGENTIC_MIGRATION_FRONTEND_OPEN=0 turns it off (headless runs,
    CI), and it is off by default inside a test runner.
    """
    global _opened_url
    if url == _opened_url or _switched_off("GKE_AGENTIC_MIGRATION_FRONTEND_OPEN"):
        return
    _opened_url = url
    try:
        import webbrowser
        webbrowser.open(url)
    except Exception as e:
        logger.debug("Could not open a browser for the review UI: %r", e)


def announcement(force: bool = False) -> str:
    """A line for tool output pointing the human at the review UI, or ''.

    join_ledger passes force=True so every join re-checks and names the URL
    (also surfacing a changed port). Other callers get it once per server
    process — and, having announced, skip the health probe entirely so DAG
    polls never pay for it.
    """
    global _announced_url
    if _announced_url is not None and not force:
        return ""
    url = ensure_review_frontend()
    if not url:
        return ""
    _announced_url = url
    _maybe_open_browser(url)
    return (
        f"\nReview UI (read-only, for the human reviewer): {url}"
        f"\nOpen this URL in your integrated/embedded browser so the reviewer can"
        f" watch here without switching to Chrome, and also show the plain link to"
        f" the human. Do not fetch or poll the URL as a data source — it is a live"
        f" view of the ledger, not an API for you."
    )
