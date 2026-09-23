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

"""E2E (Antigravity SDK): the stdio MCP server is importable and its tools are
callable through the Antigravity harness.

Scope is deliberately limited to what the SDK supports: register the MCP server,
then have the agent call two tools that do NOT trigger an MCP elicitation:
  - bootstrap_migration (resets in-memory state)
  - get_next_stage      (reports current state)
Success = both tool results come back and reflect the bootstrap start state.

The full initialize_ledger -> elicitation -> bucket flow is intentionally NOT
exercised here: the Antigravity SDK (as of google-antigravity 0.1.8) has no path
to answer an MCP elicitation, so that flow hangs. Bucket creation is covered by
the mcp-direct and cc-approve tiers instead.

Env:
  E2E_MCP_SERVER   path to the mcp-server launcher
  E2E_GCP_PROJECT  GCP project for the Vertex-backed agent
  AGY_LOCATION     Vertex location (default: global)
"""

import asyncio
import os
import sys

from google.antigravity import Agent, LocalAgentConfig, CapabilitiesConfig
from google.antigravity.types import McpStdioServer

PROMPT = """You are running an automated MCP connectivity test. Use ONLY the MCP tools
from the 'dag' server and perform exactly these two steps in order:
1. Call the tool `bootstrap_migration` (no arguments).
2. Call the tool `get_next_stage` (no arguments).
Do not call any other tools and do not read or write files. Then output a report
titled TOOL RESULTS with each tool name followed by its raw result text VERBATIM.
"""

RUN_TIMEOUT_S = 300


async def run() -> int:
    server_cmd = os.environ["E2E_MCP_SERVER"]
    project = os.environ["E2E_GCP_PROJECT"]
    location = os.environ.get("AGY_LOCATION", "global")

    config = LocalAgentConfig(
        mcp_servers=[
            McpStdioServer(
                name="dag",
                command=server_cmd,
                args=[],
                env={"HOME": os.environ["HOME"], "PATH": os.environ["PATH"]},
            )
        ],
        capabilities=CapabilitiesConfig(),
        workspaces=[os.environ.get("AGY_WORKSPACE", "/tmp/agy-e2e-ws")],
        vertex=True,
        project=project,
        location=location,
    )

    os.makedirs(config.workspaces[0], exist_ok=True)

    called = set()
    async with Agent(config) as agent:
        response = await asyncio.wait_for(agent.chat(PROMPT), RUN_TIMEOUT_S)
        text = await response.text()
        # response.tool_calls is an async generator in this SDK version.
        async for tc in response.tool_calls:
            name = getattr(tc, "name", None)
            if name:
                called.add(name)
    print(f"[agy_mcp_import] tools called: {sorted(called)}")
    print(f"[agy_mcp_import] agent text:\n{text}")

    # The MCP server was importable and its tools were invoked through the
    # Antigravity harness if bootstrap_migration ran and the reported state is
    # the bootstrap start state.
    if "bootstrap_migration" not in called:
        print("[agy_mcp_import] FAILED: bootstrap_migration was not invoked via the MCP server")
        return 1
    if "STATE_CONFIGURATION_GATHERING" not in text:
        print("[agy_mcp_import] FAILED: expected STATE_CONFIGURATION_GATHERING in tool output")
        return 1

    print("[agy_mcp_import] PASSED: MCP server imported and tools callable via Antigravity SDK")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(run()))
    except asyncio.TimeoutError:
        print("[agy_mcp_import] FAILED: agent run timed out")
        sys.exit(1)
