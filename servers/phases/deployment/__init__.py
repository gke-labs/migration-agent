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

"""Deployment phase package. See README.md."""

from .deployment_provision_1 import tools as provision_tools
from .deployment_datamigration_2 import tools as datamigration_tools


def register(mcp):
    """Register every deployment step's tools on the FastMCP server."""
    provision_tools.register(mcp)
    datamigration_tools.register(mcp)
