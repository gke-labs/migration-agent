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

"""Bootstrap phase package.

The admin's phase: create the workspace, then keep it right. Configuration
gathering (STATE_CONFIGURATION_GATHERING) is still driven by the
`bootstrapping` skill and `initialize_ledger` in main.py; this package owns
what comes after provisioning — the workspace administration step, where
the admin corrects the role lists, resets the migration, upgrades the ledger
graphs or rewinds one of them.

Each bootstrap_<purpose>_N subfolder owns one step:
  - instructions.md  agent instructions returned by get_next_stage for the
                     DAG state that references this step
  - tools.py         the MCP tools the agent is expected to call at this step

The bootstrap DAG (servers/dag/bootstrap_dag.json) references steps by
folder path, e.g. "step": "servers/phases/bootstrap/bootstrap_admin_2".
"""

from .bootstrap_admin_2 import tools as bootstrap_admin_2_tools
from .bootstrap_admin_2.tools import set_bootstrapped, clear_bootstrapped  # noqa: F401


def register(mcp):
    """Register all bootstrap phase MCP tools on the given FastMCP server."""
    bootstrap_admin_2_tools.register(mcp)
