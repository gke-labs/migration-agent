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

"""Landing zone phase package.

Designs the GCP target foundation and hands it to a human as a Pull Request:
project hierarchy, Shared VPC, GKE clusters, org policies, baseline IAM,
observability and budgets, written as Terraform into the target GitOps repo.
Nothing here applies anything.

Each landingzone_<purpose>_N subfolder owns one step:
  - instructions.md  agent instructions returned by get_next_stage for the
                     DAG state that references this step
  - tools.py         register(mcp) for the tools this step's agent calls

The platform DAG (servers/dag/platform_dag.json) references steps by folder
path, e.g. "step": "servers/phases/landingzone/landingzone_design_2".

Two modules sit beside the steps because they are phase-wide rather than
step-owned: actions.py holds the server-side actions for the phase's
INTERNAL_TASK_SERVER_MUTATION states (main.run_internal_mutation dispatches
through its ACTIONS table), and workspace.py is the single definition of
where a landing zone's target clone lives — the path the allocating tool and
both actions all need, and the reason the tools could not move here without
the actions.

This phase also carries the translation states (run, review, validate, ship,
PR); their steps live under servers/phases/translation/ but they belong to
the landingzone phase in the DAG, so the whole target build reads as one band
in the review UI.

Phase knowledge docs live under knowledge/.
"""

from .landingzone_design_2 import tools as landingzone_design_2_tools
from .landingzone_translationplan_3 import tools as landingzone_translationplan_3_tools
from .landingzone_planreview_4 import tools as landingzone_planreview_4_tools


def register(mcp):
    """Register all landing zone phase MCP tools on the given FastMCP server."""
    landingzone_design_2_tools.register(mcp)
    landingzone_translationplan_3_tools.register(mcp)
    landingzone_planreview_4_tools.register(mcp)
