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

"""E2E (harness-independent): full bootstrap flow via the python MCP client.

Drives bootstrap_migration -> initialize_ledger -> get_next_stage against the
stdio MCP server, answering the ledger-creation elicitation with approval.
Deterministic (no LLM involved); this is the primary gate for "does the
bucket actually get created".

Env:
  E2E_MCP_SERVER          path to the mcp-server launcher
  E2E_WORKSPACE           workspace_name
  E2E_GCP_PROJECT         gcp_project
  E2E_LEDGER_BUCKET       ledger bucket URI (gs://...)
  E2E_ADMINS              semicolon-separated admin emails
  E2E_PLATFORM_ENGINEERS  semicolon-separated platform engineer emails
"""

import asyncio
import os
import sys

from mcp import ClientSession, StdioServerParameters, types
from mcp.client.stdio import stdio_client

INIT_TIMEOUT_S = 300
TOOL_TIMEOUT_S = 180

elicitations = []


async def elicitation_callback(context, params):
    elicitations.append(params)
    print(f"[bootstrap_flow] elicitation received: {params.message!r} -> approving")
    return types.ElicitResult(action="accept", content={"approved": True})


def text_of(result) -> str:
    return result.content[0].text if result.content else ""


async def run() -> int:
    server_cmd = os.environ["E2E_MCP_SERVER"]
    workspace = os.environ["E2E_WORKSPACE"]
    project = os.environ["E2E_GCP_PROJECT"]
    bucket_uri = os.environ["E2E_LEDGER_BUCKET"]
    admins = [e for e in os.environ["E2E_ADMINS"].split(";") if e]
    platform_engineers = [e for e in os.environ["E2E_PLATFORM_ENGINEERS"].split(";") if e]

    # Pass the invoking environment through (the MCP SDK spawns the server
    # with a sanitized minimal env by default), so auth fallbacks like
    # GKE_MIGRATION_USER_EMAIL reach the server — as the phase-harness does.
    env = dict(os.environ)
    # Headless run: no one watches the review UI, so skip it entirely.
    env.setdefault("GKE_AGENTIC_MIGRATION_FRONTEND", "0")
    params = StdioServerParameters(command=server_cmd, env=env)
    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write,
                                 elicitation_callback=elicitation_callback) as session:
            await asyncio.wait_for(session.initialize(), INIT_TIMEOUT_S)

            r1 = await asyncio.wait_for(
                session.call_tool("bootstrap_migration", {}), TOOL_TIMEOUT_S)
            print(f"[bootstrap_flow] bootstrap_migration: {text_of(r1)}")
            if r1.isError:
                print("[bootstrap_flow] FAILED: bootstrap_migration errored")
                return 1

            r2 = await asyncio.wait_for(
                session.call_tool("initialize_ledger", {
                    "workspace_name": workspace,
                    "gcp_project": project,
                    "ledger_bucket": bucket_uri,
                    "roles": {
                        "admins": admins,
                        "platform_engineers": platform_engineers,
                        "developers": [],
                    },
                }), TOOL_TIMEOUT_S)
            print(f"[bootstrap_flow] initialize_ledger: isError={r2.isError} -> {text_of(r2)}")
            if r2.isError:
                print("[bootstrap_flow] FAILED: initialize_ledger errored")
                return 1

            r3 = await asyncio.wait_for(
                session.call_tool("get_next_stage", {}), TOOL_TIMEOUT_S)
            final_state = text_of(r3)
            print(f"[bootstrap_flow] get_next_stage: {final_state}")

            # The graph parks on the admin's standing step; its read tool must
            # work through the live server on the freshly bootstrapped ledger
            # (the resolve_context post-bootstrap path, no session cache). The
            # mutating tools grant/revoke real IAM and are covered by unit
            # tests; this smoke proves the step is reachable and reads.
            r4 = await asyncio.wait_for(
                session.call_tool("describe_workspace", {}), TOOL_TIMEOUT_S)
            describe = text_of(r4)
            print(f"[bootstrap_flow] describe_workspace: isError={r4.isError} -> {describe}")
            describe_ok = (not r4.isError) and workspace in describe \
                and all(a in describe for a in admins)

    if not elicitations:
        print("[bootstrap_flow] FAILED: server never sent the approval elicitation")
        return 1
    # Success is not a terminal: the graph parks on the admin's standing step,
    # where the role lists can still be corrected (DESIGN.md §6.1).
    if "STATE_WORKSPACE_ADMIN" not in final_state:
        print(f"[bootstrap_flow] FAILED: expected STATE_WORKSPACE_ADMIN, got: {final_state}")
        return 1
    if not describe_ok:
        print("[bootstrap_flow] FAILED: describe_workspace did not report the "
              "bootstrapped workspace and its admins")
        return 1

    print(f"[bootstrap_flow] PASSED: {len(elicitations)} elicitation(s) approved, "
          f"DAG parked on STATE_WORKSPACE_ADMIN, describe_workspace reads the ledger")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(run()))
