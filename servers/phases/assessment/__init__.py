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

"""Assessment phase package.

Sits between discovery and landing zone design. Discovery says what is there;
assessment says whether it can move, what stops it, and who owns each thing that
stops it. Landing zone design is unreachable until every blocker has an owner and
a target close date.

Each assessment_<purpose>_N subfolder owns one step:
  - instructions.md  agent instructions returned by get_next_stage for the
                     DAG state that references this step
  - tools.py         the MCP tools the agent is expected to call at this step

The platform DAG (servers/dag/platform_dag.json) references steps by folder
path, e.g. "step": "servers/phases/assessment/assessment_review_1".

The phase knowledge doc under knowledge/ is not just background reading: its
"Step 4 — Identify blockers" table is parsed at start-up and is the authority on
which blocker categories the server will accept. See
servers/dag/server/blocker_criteria.py.
"""

from .assessment_review_1 import tools as assessment_review_1_tools
from .assessment_blockers_2 import tools as assessment_blockers_2_tools


def register(mcp):
    """Register all assessment phase MCP tools on the given FastMCP server."""
    assessment_review_1_tools.register(mcp)
    assessment_blockers_2_tools.register(mcp)
