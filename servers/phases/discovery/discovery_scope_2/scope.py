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

"""Pure scope logic for discovery — moved to servers.phases.scope_algebra.

The algebra is shared with the workload phase now, so it lives in the
phase-neutral module; this file re-exports it so every existing import
(discovery tools, assessment review, the exports derivation glue) keeps
working unchanged. Behavior is identical.
"""

from servers.phases.scope_algebra import (
    apply_scope_update,
    filter_files,
    index_included_paths,
    is_excluded,
    matches,
    normalize_pattern,
    summarize_scope,
)

__all__ = [
    "apply_scope_update",
    "filter_files",
    "index_included_paths",
    "is_excluded",
    "matches",
    "normalize_pattern",
    "summarize_scope",
]
