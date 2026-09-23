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

"""Hermetic process environment for unit tests that drive the DAG server.

Import this module at the top of any *_test.py that reaches join_ledger,
get_next_stage or bootstrap_migration through `main`. Importing it does two
things for the whole interpreter, so no test has to remember either:

1. Turns the review-frontend launcher and its browser pop-up off
   (GKE_AGENTIC_MIGRATION_FRONTEND=0, GKE_AGENTIC_MIGRATION_FRONTEND_OPEN=0).
   The launcher also refuses to start on its own inside a test runner, but
   an explicit switch keeps the suite hermetic even if that guard changes.

2. Creates one session-config directory for this process and exports the
   paths tests point `main` / `state_management` at, so the cache files the
   suite writes, and that bootstrap_migration (both paths) and the
   legacy-cache adoption in state_management.read_local_config (the legacy
   path) delete, never collide with another checkout running the same suite
   at the same time (a shared /tmp literal made concurrent runs delete each
   other's session and fail with "No active workspace session found"). The
   directory is removed when the interpreter exits. This module is also the
   intended fix for the eight tool tests that still point at fixed
   /tmp/test_*_config.* literals (bootstrap_admin_2 and the seven workload_*
   tools_test.py).
"""

import atexit
import os
import shutil
import tempfile

os.environ["GKE_AGENTIC_MIGRATION_FRONTEND"] = "0"
os.environ["GKE_AGENTIC_MIGRATION_FRONTEND_OPEN"] = "0"

_root = tempfile.mkdtemp(prefix="gkma-unit-session-")
atexit.register(shutil.rmtree, _root, ignore_errors=True)

# Per-process stand-ins for ~/.ledger_config.yaml and ~/.ledger_config.d.
LEDGER_CONFIG_PATH = os.path.join(_root, "ledger_config.yaml")
LEDGER_CONFIG_DIR = os.path.join(_root, "ledger_config.d")
