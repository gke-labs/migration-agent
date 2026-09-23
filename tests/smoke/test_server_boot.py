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

"""Smoke test: the MCP server boots over stdio and exposes the expected tools.

Connects with the official python MCP client, runs initialize + tools/list,
and asserts the bootstrapping tools are present. Makes no GCP calls.

Env: E2E_MCP_SERVER — path to the mcp-server launcher.
"""

import asyncio
import os
import sys

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

REQUIRED_TOOLS = {
    "bootstrap_migration",
    "initialize_ledger",
    "get_next_stage",
    "join_ledger",
}

# First boot builds servers/dag/.venv, which can take a while.
INIT_TIMEOUT_S = 300


async def run() -> int:
    server_cmd = os.environ["E2E_MCP_SERVER"]
    params = StdioServerParameters(command=server_cmd)
    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write) as session:
            init = await asyncio.wait_for(session.initialize(), INIT_TIMEOUT_S)
            print(f"[test_server_boot] initialized: server={init.serverInfo.name} "
                  f"protocol={init.protocolVersion}")
            tools = await asyncio.wait_for(session.list_tools(), 60)
            names = {t.name for t in tools.tools}
            print(f"[test_server_boot] {len(names)} tools: {', '.join(sorted(names))}")
            missing = REQUIRED_TOOLS - names
            if missing:
                print(f"[test_server_boot] FAILED: missing tools: {', '.join(sorted(missing))}")
                return 1
    print("[test_server_boot] PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(run()))
