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

"""confirm_translation_plan: the plan findings reach the sign-off prompt.

`plan.findings` (decisions that disagree, facts that argue against a recorded
choice) are computed inside build_translation_plan and rendered here OUTSIDE
the best-effort coverage sentence: a rendered line lands in the elicitation
prompt, and a render failure is an ERROR return, never a dropped line. No test
covered this tool before; these pin exactly that seam, on the in-memory ledger.
"""

import json
import os
import shutil
import tempfile
import unittest
from unittest.mock import patch

import servers.dag.state_management as state_mgr
from servers.dag import fake_gcs
from servers.phases.landingzone.landingzone_planreview_4 import tools
from servers.phases.landingzone.landingzone_translationplan_3 import planner

_DAG_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "..", "dag", "platform_dag.json")
REGISTRY = {"workspace_name": "ws-plat", "roles": {"admins": [], "platform_engineers": ["eng@x.com"]}}

INVENTORY = {
    "nodegroups": [{"name": "system", "instance_types": ["m5.large"]}],
    "autoscaling": {"karpenter": True},
    "triggers": {"karpenter": True, "privileged_daemonsets": True, "gpu_tpu": False,
                 "vpc_peering": False},
}


class ConfirmTranslationPlanTest(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.addCleanup(self._restore, state_mgr.LEDGER_CONFIG_PATH,
                        state_mgr.LEDGER_CONFIG_DIR, state_mgr.gcs_client)
        config_dir = tempfile.mkdtemp(prefix="planreview-tools-")
        self.addCleanup(shutil.rmtree, config_dir, ignore_errors=True)
        state_mgr.LEDGER_CONFIG_PATH = os.path.join(config_dir, "ledger_config.yaml")
        state_mgr.LEDGER_CONFIG_DIR = os.path.join(config_dir, "ledger_config.d")
        self.fake = fake_gcs.FakeStorageClient()
        state_mgr.gcs_client = self.fake
        self.bucket = self.fake.bucket("plat-ledger")
        self.bucket.blob("workspace_registry.yaml").upload_from_string(json.dumps(REGISTRY))
        with open(os.path.normpath(_DAG_PATH)) as f:
            self.bucket.blob("platform_dag.json").upload_from_string(f.read())
        state_mgr.write_local_config("gs://plat-ledger", "platform", "ws-plat", "proj")
        email = patch.object(state_mgr, "get_authenticated_user_email", return_value="eng@x.com")
        email.start()
        self.addCleanup(email.stop)

    @staticmethod
    def _restore(config_path, config_dir, client):
        (state_mgr.LEDGER_CONFIG_PATH, state_mgr.LEDGER_CONFIG_DIR,
         state_mgr.gcs_client) = config_path, config_dir, client

    def _seed(self, decisions):
        plan = planner.build_translation_plan(INVENTORY, decisions)
        state = {"current_state": "STATE_LZ_TRANSLATION_PLAN_REVIEW", "history": [],
                 "variables": {"translation_plan": plan, "lz_decisions": decisions,
                               "discovery_inventory": INVENTORY}}
        self.bucket.blob("platform/onboarding/state.json").upload_from_string(json.dumps(state))
        return plan

    async def _confirm(self):
        prompts = []

        async def fake_elicitation(ctx, state_name, elicit_def, schema_cls, state_dict, config):
            prompts.append(elicit_def["prompt_template"])
            return True, {}

        with patch.object(tools, "run_elicitation", side_effect=fake_elicitation):
            result = await tools.confirm_translation_plan(ctx=None)
        return result, prompts

    async def test_a_finding_line_reaches_the_sign_off_prompt(self):
        plan = self._seed({"karpenter": "GKE_AUTOPILOT", "privileged_daemonsets": "GKE_STANDARD"})
        self.assertEqual([f["kind"] for f in plan["findings"]], ["decisions-disagree"])
        result, prompts = await self._confirm()
        self.assertNotIn("ERROR", result)
        self.assertEqual(len(prompts), 1)
        self.assertIn("CONFLICT [decisions-disagree] cluster_mode", prompts[0])
        self.assertIn("karpenter=GKE_AUTOPILOT", prompts[0])

    async def test_no_findings_no_findings_sentence(self):
        plan = self._seed({"karpenter": "GKE_STANDARD_NAP", "privileged_daemonsets": "GKE_STANDARD"})
        self.assertEqual(plan["findings"], [])
        _result, prompts = await self._confirm()
        self.assertNotIn("Findings:", prompts[0])

    async def test_a_render_failure_is_an_error_not_a_dropped_line(self):
        self._seed({"karpenter": "GKE_STANDARD_NAP", "privileged_daemonsets": "GKE_STANDARD"})
        with patch.object(planner, "render_findings", side_effect=RuntimeError("boom")):
            result, prompts = await self._confirm()
        self.assertTrue(result.startswith("ERROR: Could not render the plan findings"), result)
        self.assertEqual(prompts, [])


if __name__ == "__main__":
    unittest.main()
