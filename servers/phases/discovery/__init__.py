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

"""Discovery phase package.

Each discovery_<purpose>_N subfolder (discovery_livescan_0, discovery_init_1,
discovery_scope_2, discovery_datascan_3, discovery_datareview_3,
discovery_extract_3) owns one step of the discovery phase (the human review of
what EXTRACTION produced is a different review and lives with the assessment
phase, assessment_review_1):
  - instructions.md  agent instructions returned by get_next_stage for the
                     DAG state that references this step
  - tools.py         the MCP tools the agent is expected to call at this step

The platform DAG (servers/dag/platform_dag.json) references steps by folder
path, e.g. "step": "servers/phases/discovery/discovery_init_1".

"""

from .discovery_livescan_0 import tools as discovery_livescan_0_tools
from .discovery_init_1 import tools as discovery_init_1_tools
from .discovery_scope_2 import tools as discovery_scope_2_tools
from .discovery_datascan_3 import tools as discovery_datascan_3_tools
from .discovery_datareview_3 import tools as discovery_datareview_3_tools
from .discovery_extract_3 import tools as discovery_extract_3_tools


def register(mcp):
    """Register all discovery phase MCP tools on the given FastMCP server."""
    discovery_livescan_0_tools.register(mcp)
    discovery_init_1_tools.register(mcp)
    discovery_scope_2_tools.register(mcp)
    discovery_datascan_3_tools.register(mcp)
    discovery_datareview_3_tools.register(mcp)
    discovery_extract_3_tools.register(mcp)
