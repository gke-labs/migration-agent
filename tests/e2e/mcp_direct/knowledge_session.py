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

"""E2E: phase knowledge is served once per server process, and again after a restart.

Knowledge-delivery bookkeeping is held in server process memory rather than in
the ledger. The reasoning is that a durable "already delivered" flag outlives
the context window that received the document: the agent cannot ask for
something it has no memory of being given, so the server would sit on material
the agent needs. Process and client context are lost together, which makes the
process the honest scope.

That is a claim about a process boundary, so only a test that crosses one can
check it. A unit test clearing the module-level set would pass just as happily
if the set were persisted to GCS.

Three phases:
  1. bootstrap + initialize_ledger + join_ledger, then park the ledger on a
     state that declares knowledge (STATE_ASSESSMENT). join_ledger is what
     caches the local session config; without it the server stays on the
     in-memory bootstrap graph, whose states declare no knowledge.
  2. a fresh server: get_next_stage twice. The document arrives once.
  3. another fresh server: get_next_stage. The document arrives again.

Env:
  E2E_MCP_SERVER          path to the mcp-server launcher
  E2E_WORKSPACE           workspace_name
  E2E_GCP_PROJECT         gcp_project
  E2E_LEDGER_BUCKET       ledger bucket URI (gs://...)
  E2E_ADMINS              semicolon-separated admin emails
  E2E_PLATFORM_ENGINEERS  semicolon-separated platform engineer emails
"""

import asyncio
import json
import os
import sys

from google.cloud import storage
from mcp import ClientSession, StdioServerParameters, types
from mcp.client.stdio import stdio_client

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import session_cache  # noqa: E402  (shared snapshot/restore of ~/.ledger_config*)

INIT_TIMEOUT_S = 300
TOOL_TIMEOUT_S = 180

# STATE_ASSESSMENT declares this document in platform_dag.json. (It is the
# knowledge-declaring state since the map-reduce discovery pipeline carries
# its rules in worker prompts rather than a knowledge doc.)
PARKED_STATE = "STATE_ASSESSMENT"
MARKER = "--- Phase knowledge: migration-assessment ---"
STATE_BLOB = "platform/onboarding/state.json"


async def approve(context, params):
    return types.ElicitResult(action="accept", content={"approved": True})


def text_of(result) -> str:
    return result.content[0].text if result.content else ""


async def in_session(server_cmd, body):
    """Runs body against a freshly spawned server, then lets the process exit."""
    # Pass the invoking environment through (the MCP SDK spawns the server
    # with a sanitized minimal env by default), so auth fallbacks like
    # GKE_MIGRATION_USER_EMAIL reach the server — as the phase-harness does.
    env = dict(os.environ)
    # Headless run: no one watches the review UI, so skip it entirely.
    env.setdefault("GKE_AGENTIC_MIGRATION_FRONTEND", "0")
    params = StdioServerParameters(command=server_cmd, env=env)
    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write, elicitation_callback=approve) as session:
            await asyncio.wait_for(session.initialize(), INIT_TIMEOUT_S)
            return await body(session)


async def call(session, tool, args=None):
    result = await asyncio.wait_for(session.call_tool(tool, args or {}), TOOL_TIMEOUT_S)
    if result.isError:
        raise RuntimeError(f"{tool} errored: {text_of(result)}")
    return text_of(result)


def park_ledger_on(project: str, bucket_uri: str, state_name: str) -> None:
    """Rewrites the ledger's current state, standing in for a real traversal.

    Reaching STATE_ASSESSMENT through the graph would mean driving repository
    configuration and the whole discovery pipeline first; none of that affects
    what this test is checking.
    """
    bucket_name = bucket_uri[5:] if bucket_uri.startswith("gs://") else bucket_uri
    blob = storage.Client(project=project).bucket(bucket_name).blob(STATE_BLOB)
    state = json.loads(blob.download_as_text())
    state["current_state"] = state_name
    blob.upload_from_string(json.dumps(state, indent=2), content_type="application/json")
    print(f"[knowledge_session] ledger parked on {state_name}")


async def run() -> int:
    server_cmd = os.environ["E2E_MCP_SERVER"]
    workspace = os.environ["E2E_WORKSPACE"]
    project = os.environ["E2E_GCP_PROJECT"]
    bucket_uri = os.environ["E2E_LEDGER_BUCKET"]
    admins = [e for e in os.environ["E2E_ADMINS"].split(";") if e]
    platform_engineers = [e for e in os.environ["E2E_PLATFORM_ENGINEERS"].split(";") if e]

    async def provision(session):
        await call(session, "bootstrap_migration")
        await call(session, "initialize_ledger", {
            "workspace_name": workspace,
            "gcp_project": project,
            "ledger_bucket": bucket_uri,
            "roles": {
                "admins": admins,
                "platform_engineers": platform_engineers,
                "developers": [],
            },
        })
        # Caches the session locally, which is what switches later servers off
        # the bootstrap graph and onto the ledger's platform graph.
        await call(session, "join_ledger", {"ledger_uri": bucket_uri})

    async def twice(session):
        return await call(session, "get_next_stage"), await call(session, "get_next_stage")

    # join_ledger writes the cwd-scoped session cache, and bootstrap deletes
    # both cache paths — files the developer may also have for real work.
    # Snapshot both and put back exactly what existed before the run.
    snap = session_cache.snapshot()
    try:
        await in_session(server_cmd, provision)
        park_ledger_on(project, bucket_uri, PARKED_STATE)

        first, second = await in_session(server_cmd, twice)
        after_restart = await in_session(server_cmd, lambda s: call(s, "get_next_stage"))
    finally:
        session_cache.restore(snap, "knowledge_session")

    failures = []
    if PARKED_STATE not in first:
        failures.append(f"expected the server to be on {PARKED_STATE}, got: {first[:200]!r}")
    if MARKER not in first:
        failures.append("first get_next_stage did not serve the declared knowledge document")
    if MARKER in second:
        failures.append("second get_next_stage re-sent the document within the same process")
    if "--- Step instructions ---" not in second:
        failures.append("second get_next_stage dropped the step instructions along with the "
                        "knowledge; only knowledge is deduped")
    if MARKER not in after_restart:
        failures.append("a restarted server withheld the document, so delivery is being "
                        "remembered across processes (ledger-scoped, not session-scoped)")

    if failures:
        for f in failures:
            print(f"[knowledge_session] FAILED: {f}")
        return 1

    print("[knowledge_session] PASSED: served once per process, re-sent after restart")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(run()))
