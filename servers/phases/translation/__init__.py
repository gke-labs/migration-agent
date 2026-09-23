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

"""Translation phase package.

Each translation_<purpose>_N subfolder (translation_translate_1,
translation_humanreview_2, translation_validate_3) owns one step of the
translation phase:
  - instructions.md  agent instructions returned by get_next_stage for the
                     DAG state that references this step
  - tools.py         the MCP tools the agent is expected to call at this step

The phase generates the code — Terraform and/or Kubernetes manifests:
agreeing WHAT to translate (the unit plan
and its sign-off) happens at the end of the landing zone phase
(servers/phases/landingzone/landingzone_translationplan_3 and _planreview_4),
which also authors the plan object these steps consume.

The platform DAG (servers/dag/platform_dag.json) references steps by folder
path, e.g. "step": "servers/phases/translation/translation_translate_1".
"""

from .translation_translate_1 import tools as translation_translate_1_tools
from .translation_humanreview_2 import tools as translation_humanreview_2_tools
from .translation_validate_3 import tools as translation_validate_3_tools


def register(mcp):
    """Register all translation phase MCP tools on the given FastMCP server."""
    translation_translate_1_tools.register(mcp)
    translation_humanreview_2_tools.register(mcp)
    translation_validate_3_tools.register(mcp)
