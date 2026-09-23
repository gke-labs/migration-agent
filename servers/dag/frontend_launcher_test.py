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

"""Tests for the review-frontend auto-launcher.

Run from servers/dag:
    python -m unittest frontend_launcher_test
"""

import json
import os
import socket
import subprocess
import sys
import unittest
import urllib.request
from unittest import mock

os.environ["GKE_AGENTIC_MIGRATION_FRONTEND_OPEN"] = "0"  # tests must never pop a browser
import frontend_launcher


class EnsureReviewFrontendTest(unittest.TestCase):
    def setUp(self):
        frontend_launcher._cached_url = None
        frontend_launcher._announced_url = None
        frontend_launcher._failed_until = 0.0
        env = mock.patch.dict(
            os.environ, {"GKE_AGENTIC_MIGRATION_FRONTEND": "1", "GKE_AGENTIC_MIGRATION_FRONTEND_PORT": "8642"}
        )
        env.start()
        self.addCleanup(env.stop)
        # Deterministic session scope: never read the developer's real cache.
        uri = mock.patch.object(
            frontend_launcher, "_session_ledger_uri",
            return_value="gs://this-session",
        )
        uri.start()
        self.addCleanup(uri.stop)

    def test_disabled_by_env(self):
        with mock.patch.dict(os.environ, {"GKE_AGENTIC_MIGRATION_FRONTEND": "0"}):
            with mock.patch.object(frontend_launcher.subprocess, "Popen") as popen:
                self.assertIsNone(frontend_launcher.ensure_review_frontend())
                self.assertEqual(frontend_launcher.announcement(force=True), "")
                popen.assert_not_called()

    def test_off_by_default_inside_a_test_runner(self):
        # Every test process has `unittest` imported; with the switch unset the
        # launcher must neither probe a port nor spawn, so a suite that reaches
        # join_ledger without setting the variable stays hermetic.
        with mock.patch.dict(os.environ):
            os.environ.pop("GKE_AGENTIC_MIGRATION_FRONTEND", None)
            with mock.patch.object(
                frontend_launcher, "_probe_identity"
            ) as probe, mock.patch.object(
                frontend_launcher.subprocess, "Popen"
            ) as popen:
                self.assertIsNone(frontend_launcher.ensure_review_frontend())
                self.assertEqual(frontend_launcher.announcement(force=True), "")
                probe.assert_not_called()
                popen.assert_not_called()

    def test_unset_switch_means_on_outside_a_test_runner(self):
        # The product default is unchanged: with no runner in the process the
        # unset switch reads as on (and an explicit "0" still turns it off).
        with mock.patch.object(frontend_launcher, "_under_test_runner", return_value=False):
            with mock.patch.dict(os.environ):
                os.environ.pop("GKE_AGENTIC_MIGRATION_FRONTEND", None)
                self.assertFalse(frontend_launcher._disabled())
            with mock.patch.dict(os.environ, {"GKE_AGENTIC_MIGRATION_FRONTEND": "0"}):
                self.assertTrue(frontend_launcher._disabled())

    def test_runner_default_is_logged_once_per_process(self):
        # The off-under-a-runner default rests on module introspection, so it
        # must leave a trace: exactly one INFO line per process naming the
        # variable that overrides it, however many times a switch is read.
        with mock.patch.object(frontend_launcher, "_runner_default_logged", False):
            with mock.patch.dict(os.environ):
                os.environ.pop("GKE_AGENTIC_MIGRATION_FRONTEND", None)
                os.environ.pop("GKE_AGENTIC_MIGRATION_FRONTEND_OPEN", None)
                with self.assertLogs("migration-dag", level="INFO") as logs:
                    self.assertTrue(frontend_launcher._disabled())
                    self.assertTrue(frontend_launcher._disabled())
                    self.assertTrue(
                        frontend_launcher._switched_off("GKE_AGENTIC_MIGRATION_FRONTEND_OPEN")
                    )
        lines = [r for r in logs.records if "test runner" in r.getMessage()]
        self.assertEqual(len(lines), 1, logs.output)
        self.assertEqual(lines[0].levelname, "INFO")
        self.assertIn("GKE_AGENTIC_MIGRATION_FRONTEND=1", lines[0].getMessage())

    def test_browser_not_opened_by_default_inside_a_test_runner(self):
        frontend_launcher._opened_url = None
        self.addCleanup(setattr, frontend_launcher, "_opened_url", None)
        with mock.patch("webbrowser.open") as open_browser:
            with mock.patch.dict(os.environ):
                os.environ.pop("GKE_AGENTIC_MIGRATION_FRONTEND_OPEN", None)
                frontend_launcher._maybe_open_browser("http://127.0.0.1:8642")
            open_browser.assert_not_called()
            # An explicit opt-in still opens it, once per URL.
            with mock.patch.dict(os.environ, {"GKE_AGENTIC_MIGRATION_FRONTEND_OPEN": "1"}):
                frontend_launcher._maybe_open_browser("http://127.0.0.1:8642")
                frontend_launcher._maybe_open_browser("http://127.0.0.1:8642")
            open_browser.assert_called_once_with("http://127.0.0.1:8642")

    def test_reuses_running_instance_serving_this_session(self):
        with mock.patch.object(
            frontend_launcher, "_probe_identity",
            return_value={"ledger_uri": "gs://this-session"},
        ), mock.patch.object(frontend_launcher.subprocess, "Popen") as popen:
            self.assertEqual(
                frontend_launcher.ensure_review_frontend(), "http://127.0.0.1:8642"
            )
            popen.assert_not_called()

    def test_never_reuses_an_instance_pinned_to_another_session(self):
        # A healthy frontend on the base port that serves ANOTHER session's
        # workspace must not be announced as this session's review surface —
        # the walk continues and a per-session instance is spawned.
        spawned = {}

        def fake_popen(cmd, **kwargs):
            spawned["port"] = int(cmd[-1])
            proc = mock.Mock()
            proc.poll.return_value = None
            return proc

        def fake_probe(port):
            if port == 8642:
                return {"ledger_uri": "gs://other-session"}
            if port == spawned.get("port"):
                return {"ledger_uri": "gs://this-session"}
            return None

        with mock.patch.object(
            frontend_launcher, "_probe_identity", side_effect=fake_probe
        ), mock.patch.object(
            frontend_launcher, "_port_in_use", return_value=False
        ), mock.patch.object(
            frontend_launcher.subprocess, "Popen", side_effect=fake_popen
        ):
            url = frontend_launcher.ensure_review_frontend()

        self.assertEqual(url, "http://127.0.0.1:8643")
        self.assertEqual(spawned["port"], 8643)

    def test_skips_foreign_port_and_spawns_on_next(self):
        spawned = {}

        def fake_popen(cmd, **kwargs):
            spawned["cmd"] = cmd
            spawned["cwd"] = kwargs["cwd"]
            proc = mock.Mock()
            proc.poll.return_value = None
            return proc

        def fake_probe(port):
            # Our UI answers on 8643 only once the process was spawned.
            if port == 8643 and "cmd" in spawned:
                return {"ledger_uri": "gs://this-session"}
            return None

        with mock.patch.object(
            frontend_launcher, "_probe_identity", side_effect=fake_probe
        ), mock.patch.object(
            frontend_launcher, "_port_in_use", side_effect=lambda p: p == 8642
        ), mock.patch.object(
            frontend_launcher.subprocess, "Popen", side_effect=fake_popen
        ):
            url = frontend_launcher.ensure_review_frontend()

        self.assertEqual(url, "http://127.0.0.1:8643")
        self.assertEqual(spawned["cmd"][-2:], ["--port", "8643"])
        self.assertEqual(spawned["cmd"][1:3], ["-m", "servers.frontend.server"])
        # Spawned from the repo root so the servers.* packages resolve.
        self.assertTrue(
            os.path.isdir(os.path.join(spawned["cwd"], "servers", "frontend"))
        )

    def test_spawn_that_dies_yields_none(self):
        proc = mock.Mock()
        proc.poll.return_value = 1
        proc.returncode = 1
        with mock.patch.object(
            frontend_launcher, "_probe_identity", return_value=None
        ), mock.patch.object(
            frontend_launcher, "_port_in_use", return_value=False
        ), mock.patch.object(
            frontend_launcher.subprocess, "Popen", return_value=proc
        ) as popen:
            self.assertIsNone(frontend_launcher.ensure_review_frontend())
            spawns = popen.call_count
            self.assertGreater(spawns, 0)
            # The failure is negatively cached: an immediate retry (a DAG
            # poll) must not fork another wave of processes.
            self.assertIsNone(frontend_launcher.ensure_review_frontend())
            self.assertEqual(popen.call_count, spawns)

    def test_announcement_repeats_only_when_forced(self):
        with mock.patch.object(
            frontend_launcher,
            "ensure_review_frontend",
            return_value="http://127.0.0.1:8642",
        ) as ensure:
            self.assertIn("http://127.0.0.1:8642", frontend_launcher.announcement())
            # Once announced, polls neither repeat the line nor pay for the
            # health probe.
            self.assertEqual(frontend_launcher.announcement(), "")
            self.assertEqual(ensure.call_count, 1)
            self.assertIn(
                "http://127.0.0.1:8642", frontend_launcher.announcement(force=True)
            )

    def test_forced_announcement_surfaces_a_changed_url(self):
        with mock.patch.object(
            frontend_launcher,
            "ensure_review_frontend",
            side_effect=["http://127.0.0.1:8642", "http://127.0.0.1:8643"],
        ):
            self.assertIn("8642", frontend_launcher.announcement())
            self.assertIn("8643", frontend_launcher.announcement(force=True))

    def test_announcement_instructs_opening_in_browser(self):
        with mock.patch.object(
            frontend_launcher,
            "ensure_review_frontend",
            return_value="http://127.0.0.1:8642",
        ):
            line = frontend_launcher.announcement()
        # The exact URL substring the DAG tools rely on stays intact...
        self.assertIn(
            "Review UI (read-only, for the human reviewer): http://127.0.0.1:8642",
            line,
        )
        # ...and the agent is now told to open it in its integrated browser.
        self.assertIn("integrated", line.lower())
        self.assertIn("browser", line.lower())


class ServerImportsNoTestRunnerTest(unittest.TestCase):
    """Guards the premise of the off-under-a-runner default.

    doctest, unittest.mock and the third-party mock package all import
    unittest, so one lazy import in the server or a dependency would switch
    the review UI off for every real user with nothing but an INFO line to
    show for it. This boots a fresh interpreter the way the MCP server is
    launched and checks that importing `main` pulls in neither runner; the
    day it does, this fails instead of users losing the UI.
    """

    def test_importing_main_imports_neither_unittest_nor_pytest(self):
        env = {
            k: v for k, v in os.environ.items()
            if not k.startswith("GKE_AGENTIC_MIGRATION_FRONTEND")
        }
        env["PYTHONPATH"] = os.pathsep.join([".", os.path.join("servers", "dag")])
        script = (
            "import json, sys\n"
            "import main\n"
            "print(json.dumps([m for m in ('unittest', 'pytest') if m in sys.modules]))\n"
        )
        result = subprocess.run(
            [sys.executable, "-c", script],
            cwd=frontend_launcher._repo_root(), env=env,
            capture_output=True, text=True, timeout=120,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        present = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertEqual(
            present, [],
            f"importing main pulled in {present}; the review frontend would now"
            " default to off for every user (see frontend_launcher._under_test_runner)",
        )


class RealSpawnTest(unittest.TestCase):
    """Actually boots the frontend once: proves the spawn command, cwd,
    PYTHONPATH, and the /api/overview health contract line up."""

    def test_spawn_serves_overview(self):
        frontend_launcher._cached_url = None
        frontend_launcher._failed_until = 0.0
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            port = s.getsockname()[1]

        with mock.patch.dict(
            os.environ,
            {"GKE_AGENTIC_MIGRATION_FRONTEND": "1", "GKE_AGENTIC_MIGRATION_FRONTEND_PORT": str(port)},
        ):
            url = frontend_launcher.ensure_review_frontend()
        try:
            self.assertEqual(url, f"http://127.0.0.1:{port}")
            # Without a workspace session the UI serves its structured error
            # as a 503 with the same identity envelope — still "ours".
            try:
                with urllib.request.urlopen(f"{url}/api/overview", timeout=5) as resp:
                    body = resp.read()
            except urllib.error.HTTPError as e:
                self.assertEqual(e.code, 503)
                body = e.read()
            data = json.loads(body.decode("utf-8"))
            self.assertIn("identity", data)
        finally:
            proc = frontend_launcher._last_proc
            if proc is not None:
                proc.terminate()
                proc.wait(timeout=10)
            frontend_launcher._cached_url = None


if __name__ == "__main__":
    unittest.main()
