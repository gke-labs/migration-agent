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

"""The one place that knows where a landing zone's target clone lives.

Three call sites need this path — the tool that allocates it and the two
actions that read it — and they used to compute it independently from their
own __file__. That is what kept the tools in main.py: moving them without
moving the actions would have split the path across two directories and
silently pointed the validator at somewhere the generator never wrote.

The directory stays under servers/dag/ regardless of which package asks for
it. It is server-owned scratch that predates this phase, it is what
.gitignore and DESIGN.md §3.1 name, and clones from earlier runs are already
there.
"""

import os

# servers/phases/landingzone/workspace.py -> repository root.
_REPO_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "..", "..")
)

SCRATCH_DIR = os.path.join(_REPO_ROOT, "servers", "dag", "scratch")


def target_clone_path(branch_uuid: str) -> str:
    """Absolute path of the target-repo clone for one landing zone branch."""
    return os.path.join(SCRATCH_DIR, f"target-repo-{branch_uuid}")
