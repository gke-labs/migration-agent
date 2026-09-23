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

"""Workload phase package (developer persona).

Each workload_<purpose>_N subfolder owns one step of the per-component
developer graph (servers/dag/developer_dag.json — copied to
workloads/<component>/dag.json at join):
  - instructions.md  agent instructions returned by get_next_stage for the
                     DAG state that references this step
  - tools.py         the MCP tools the agent calls at this step

workload_await_3 was the v0.1/v0.2 parking step (no tools); it remains on
disk because not-yet-upgraded ledger graph copies still reference its
instructions, but graph v0.3 no longer routes to it. workload_translate_3
holds the worker contract (translator.py), the deterministic transforms
(transforms.py, operative since v0.3) and the fan-out tool;
workload_review_4 the unit review tools; workload_validate_5 the
manifest-gated validation, the ship elicitation walk and the PR action.
See README.md for the step table and the claimant-enforcement rule.
"""

from .workload_scope_1 import tools as workload_scope_1_tools
from .workload_scopeconfirm_2 import tools as workload_scopeconfirm_2_tools
from .workload_plan_2 import tools as workload_plan_2_tools
from .workload_planreview_4 import tools as workload_planreview_4_tools
from .workload_translate_3 import tools as workload_translate_3_tools
from .workload_review_4 import tools as workload_review_4_tools
from .workload_validate_5 import tools as workload_validate_5_tools


def register(mcp):
    """Register all workload phase MCP tools on the given FastMCP server."""
    workload_scope_1_tools.register(mcp)
    workload_scopeconfirm_2_tools.register(mcp)
    workload_plan_2_tools.register(mcp)
    workload_planreview_4_tools.register(mcp)
    workload_translate_3_tools.register(mcp)
    workload_review_4_tools.register(mcp)
    workload_validate_5_tools.register(mcp)
