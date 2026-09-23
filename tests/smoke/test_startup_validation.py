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

"""Smoke test: a malformed bundled DAG — or a broken machine-read knowledge
table — stops the server from starting.

The unit tests cover which inputs validate. This covers the consequence: that
validate_dag and the knowledge-table validators are wired into start-up as a
refusal to start rather than a warning.
A server that boots on a broken graph serves states with no procedure, which
surfaces several turns later as confused agent behaviour instead of as a crash.

Runs against a throwaway copy of servers/ and reference/ so the real tree is never mutated.
Makes no GCP calls.

Env: E2E_MCP_SERVER — path to the mcp-server launcher (used to locate the
     server's virtualenv, which must already exist; test_server_boot builds it).
"""

import json
import os
import shutil
import subprocess
import sys
import tempfile

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", ".."))
BOOT_TIMEOUT_S = 120

# scratch/ holds per-run target-repo clones and is large; the venv is rebuilt
# per location and would be meaningless here.
IGNORE = shutil.ignore_patterns(".venv", "scratch", "__pycache__", "*.pyc")


def server_python() -> str:
    launcher = os.environ.get("E2E_MCP_SERVER", os.path.join(REPO_ROOT, "servers", "dag", "mcp-server"))
    python = os.path.join(os.path.dirname(os.path.abspath(launcher)), ".venv", "bin", "python")
    if not os.path.isfile(python):
        print(f"[test_startup_validation] FAILED: server venv not found at {python}")
        sys.exit(1)
    return python


def boot(sandbox: str, python: str):
    """Starts the copied server with stdin closed and returns the finished process.

    Closing stdin means a server that gets as far as serving exits cleanly on
    EOF, so a non-zero status is attributable to start-up rather than to the
    transport never receiving a request.
    """
    return subprocess.run(
        [python, os.path.join(sandbox, "servers", "dag", "main.py")],
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        timeout=BOOT_TIMEOUT_S,
        cwd=os.path.join(sandbox, "servers", "dag"),
    )


def make_sandbox(tmp: str) -> str:
    sandbox = os.path.join(tmp, "repo")
    os.makedirs(sandbox)
    shutil.copytree(os.path.join(REPO_ROOT, "servers"), os.path.join(sandbox, "servers"), ignore=IGNORE)
    # The server also parses reference/api-translation.md at start-up (the
    # annotation disposition table), anchored at the repository root.
    shutil.copytree(os.path.join(REPO_ROOT, "reference"), os.path.join(sandbox, "reference"), ignore=IGNORE)
    return sandbox


def break_dag(sandbox: str, filename: str) -> None:
    """Points the start state's first transition at a state that does not exist."""
    path = os.path.join(sandbox, "servers", "dag", filename)
    with open(path, "r", encoding="utf-8") as f:
        dag = json.load(f)

    state = dag["states"][dag["start_state"]]
    outcome = sorted(state["transitions"])[0]
    state["transitions"][outcome] = "STATE_DOES_NOT_EXIST"

    with open(path, "w", encoding="utf-8") as f:
        json.dump(dag, f, indent=2)


def break_coverage_map(sandbox: str) -> None:
    """Rewrites one owner cell to a value outside the parser's vocabulary."""
    path = os.path.join(
        sandbox, "servers", "phases", "landingzone", "knowledge", "coverage-map.md")
    with open(path, "r", encoding="utf-8") as f:
        text = f.read()
    broken = text.replace("| terraform | landing-zone |", "| terraform | somebody-else |", 1)
    if broken == text:
        raise RuntimeError("coverage-map.md no longer holds the expected owner cell")
    with open(path, "w", encoding="utf-8") as f:
        f.write(broken)


def run() -> int:
    python = server_python()
    failures = []

    with tempfile.TemporaryDirectory(prefix="gke-migration-startup-") as tmp:
        # Control: the copy boots. Without this a broken-copy failure could just
        # mean the sandbox itself is unrunnable.
        control = boot(make_sandbox(tmp), python)
        if control.returncode != 0:
            failures.append(
                f"unmodified copy failed to start (rc={control.returncode}):\n{control.stderr[-2000:]}")
        else:
            print("[test_startup_validation] control: unmodified copy starts and exits cleanly")

    for filename in ("bootstrap_dag.json", "platform_dag.json"):
        with tempfile.TemporaryDirectory(prefix="gke-migration-startup-") as tmp:
            sandbox = make_sandbox(tmp)
            break_dag(sandbox, filename)
            proc = boot(sandbox, python)

            if proc.returncode == 0:
                failures.append(f"{filename}: server started despite a dangling transition")
                continue
            if "STATE_DOES_NOT_EXIST" not in proc.stderr:
                failures.append(
                    f"{filename}: exited {proc.returncode} but never named the bad state:\n"
                    f"{proc.stderr[-2000:]}")
                continue
            print(f"[test_startup_validation] {filename}: refused to start "
                  f"(rc={proc.returncode}, bad state named in stderr)")

    with tempfile.TemporaryDirectory(prefix="gke-migration-startup-") as tmp:
        sandbox = make_sandbox(tmp)
        break_coverage_map(sandbox)
        proc = boot(sandbox, python)

        if proc.returncode == 0:
            failures.append("coverage-map.md: server started despite an unknown owner")
        elif "somebody-else" not in proc.stderr:
            failures.append(
                f"coverage-map.md: exited {proc.returncode} but never named the bad "
                f"owner:\n{proc.stderr[-2000:]}")
        else:
            print(f"[test_startup_validation] coverage-map.md: refused to start "
                  f"(rc={proc.returncode}, bad owner named in stderr)")

    if failures:
        for f in failures:
            print(f"[test_startup_validation] FAILED: {f}")
        return 1

    print("[test_startup_validation] PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(run())
