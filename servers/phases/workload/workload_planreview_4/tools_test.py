"""Tool-layer tests for the workload plan review gate.

The elicitation is stubbed (the harness idiom). Covers: skip/unskip with
plan.json kept consistent, approve -> parking tail, reject -> PLAN,
all-skipped refusal (with the placeholder remedy), claimant enforcement,
wrong state.
"""

import json
import os
import shutil
import unittest
from unittest.mock import AsyncMock, patch

import servers.dag.state_management as state_mgr
from servers.dag import fake_gcs
from servers.phases.workload.workload_plan_2 import planner
from servers.phases.workload.workload_planreview_4 import tools

COMPONENT = "orders-component"
REGISTRY = {
    "workspace_name": "ws-dev", "gcp_project": "proj",
    "roles": {"developers": ["dev-a@x.com", "dev-b@x.com"]},
}

_DAG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         "..", "..", "..", "dag", "developer_dag.json")


def make_plan():
    """A real planner shape: manifests planned, routing parked, two
    placeholders — built by hand so the test is loader-independent."""
    units = [
        planner._unit("wkld-manifests", "planned",
                      {"documents": [{"path": "k8s/app.yaml", "doc_index": 0,
                                      "kind": "Deployment"}]}, ["brief"]),
        planner._unit("wkld-identity", "skipped", {"documents": []},
                      ["skip"], placeholder=True),
        planner._unit("wkld-storage", "skipped", {"documents": []},
                      ["skip"], placeholder=True),
        planner._unit("wkld-routing", "parked",
                      {"documents": [{"path": "k8s/ing.yaml", "doc_index": 0,
                                      "kind": "Ingress"}]}, ["parked brief"]),
    ]
    return {"component": COMPONENT, "units": units, "sources": {},
            "carriers": [], "notes": [],
            "exports_stamp": {"generated_at": None, "generations": None}}


class PlanReviewTest(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self._saved = (state_mgr.LEDGER_CONFIG_PATH, state_mgr.LEDGER_CONFIG_DIR,
                       state_mgr.gcs_client)
        state_mgr.LEDGER_CONFIG_PATH = "/tmp/test_wkldreview_config.yaml"
        state_mgr.LEDGER_CONFIG_DIR = "/tmp/test_wkldreview_config.d"
        shutil.rmtree(state_mgr.LEDGER_CONFIG_DIR, ignore_errors=True)
        self.fake = fake_gcs.FakeStorageClient()
        state_mgr.gcs_client = self.fake
        self.bucket = self.fake.bucket("dev-ledger")
        self.bucket.blob("workspace_registry.yaml").upload_from_string(
            json.dumps(REGISTRY))
        with open(os.path.normpath(_DAG_PATH)) as f:
            self.bucket.blob(f"workloads/{COMPONENT}/dag.json") \
                .upload_from_string(f.read())
        self.write_state()
        self.bucket.blob(f"workloads/{COMPONENT}/plan.json") \
            .upload_from_string(json.dumps(make_plan()))
        state_mgr.write_local_config("gs://dev-ledger", "developers", "ws-dev",
                                     "proj", component=COMPONENT)
        self._email = patch.object(state_mgr, "get_authenticated_user_email",
                                   return_value="dev-a@x.com")
        self._email.start()

    def tearDown(self):
        self._email.stop()
        shutil.rmtree(state_mgr.LEDGER_CONFIG_DIR, ignore_errors=True)
        (state_mgr.LEDGER_CONFIG_PATH, state_mgr.LEDGER_CONFIG_DIR,
         state_mgr.gcs_client) = self._saved

    def write_state(self, current="STATE_WKLD_PLAN_REVIEW", plan=...):
        if plan is ...:
            plan = make_plan()
        variables = {"component": COMPONENT,
                     "claim": {"claimant": "dev-a@x.com", "claimed_at": "T0"}}
        if plan is not None:
            variables["workload_plan"] = plan
        state = {"current_state": current, "history": [],
                 "variables": variables}
        self.bucket.blob(f"workloads/{COMPONENT}/state.json") \
            .upload_from_string(json.dumps(state))

    def read_state(self):
        return json.loads(self.bucket.blob(
            f"workloads/{COMPONENT}/state.json").download_as_text())

    def elicit(self, approved):
        return patch.object(tools, "run_elicitation",
                            AsyncMock(return_value=(approved, None)))

    async def test_skip_and_unskip_update_variables_and_plan_blob(self):
        out = await tools.update_workload_plan(skip=["wkld-manifests"])
        self.assertIn("wkld-manifests -> skipped", out)
        state = self.read_state()
        unit = state["variables"]["workload_plan"]["units"][0]
        self.assertEqual(unit["status"], "skipped")
        blob_plan = json.loads(self.bucket.blob(
            f"workloads/{COMPONENT}/plan.json").download_as_text())
        self.assertEqual(blob_plan["units"][0]["status"], "skipped")
        out = await tools.update_workload_plan(unskip=["wkld-manifests"])
        self.assertIn("wkld-manifests -> planned", out)

    async def test_unskipping_a_placeholder_is_refused_at_the_tool_layer(self):
        out = await tools.update_workload_plan(unskip=["wkld-identity"])
        self.assertIn("wkld-identity NOT restored", out)
        self.assertIn("Amend the component scope and re-plan", out)
        unit = self.read_state()["variables"]["workload_plan"]["units"][1]
        self.assertEqual(unit["status"], "skipped")
        self.assertTrue(unit["placeholder"])

    async def test_unskipping_a_parked_unit_restores_parked_not_planned(self):
        await tools.update_workload_plan(skip=["wkld-routing"])
        out = await tools.update_workload_plan(unskip=["wkld-routing"])
        self.assertIn("wkld-routing -> parked", out)
        unit = self.read_state()["variables"]["workload_plan"]["units"][3]
        self.assertEqual(unit["status"], "parked")

    async def test_unknown_unit_id_is_reported_not_fatal(self):
        out = await tools.update_workload_plan(skip=["nope"])
        self.assertIn("'nope' not found in plan (ignored)", out)

    async def test_approve_advances_into_the_execution_slice(self):
        # v0.2 parked approved plans at the graph tail; since v0.3 the
        # approval enters the pipeline at STATE_WKLD_TRANSLATE.
        with self.elicit(True):
            out = await tools.confirm_workload_plan(None)
        self.assertIn("SUCCESS", out)
        state = self.read_state()
        self.assertEqual(state["current_state"], "STATE_WKLD_TRANSLATE")
        self.assertTrue(any("plan approved" in h for h in state["history"]))

    async def test_reject_returns_to_plan(self):
        with self.elicit(False):
            out = await tools.confirm_workload_plan(None)
        self.assertIn("not approved", out)
        state = self.read_state()
        self.assertEqual(state["current_state"], "STATE_WKLD_PLAN")
        self.assertTrue(any("declined" in h for h in state["history"]))

    async def test_prompt_summary_includes_parked_and_placeholder_units(self):
        captured = {}

        async def spy(ctx, state_name, state_def, schema, state_dict, config):
            captured["prompt"] = state_def["prompt_template"]
            return True, None

        with patch.object(tools, "run_elicitation", side_effect=spy):
            await tools.confirm_workload_plan(None)
        self.assertIn("wkld-routing: parked", captured["prompt"])
        self.assertIn("wkld-identity: skipped (placeholder)",
                      captured["prompt"])

    async def test_all_skipped_is_refused_with_the_placeholder_remedy(self):
        plan = make_plan()
        for unit in plan["units"]:
            unit["status"] = "skipped"
        self.write_state(plan=plan)
        with self.elicit(True):
            out = await tools.confirm_workload_plan(None)
        self.assertIn("ERROR", out)
        self.assertIn("every unit is skipped", out)
        self.assertIn("amend the component scope", out)

    async def test_missing_plan_is_refused(self):
        self.write_state(plan=None)
        out = await tools.update_workload_plan(skip=["wkld-manifests"])
        self.assertIn("ERROR: No workload plan", out)
        with self.elicit(True):
            out = await tools.confirm_workload_plan(None)
        self.assertIn("ERROR: No workload plan", out)

    async def test_wrong_state_is_refused(self):
        self.write_state(current="STATE_WKLD_PLAN")
        out = await tools.update_workload_plan(skip=["wkld-manifests"])
        self.assertIn("ERROR: Invalid state", out)
        with self.elicit(True):
            out = await tools.confirm_workload_plan(None)
        self.assertIn("ERROR: Invalid state", out)

    async def test_claimant_is_enforced_on_both_tools(self):
        with patch.object(state_mgr, "get_authenticated_user_email",
                          return_value="dev-b@x.com"):
            out = await tools.update_workload_plan(skip=["wkld-manifests"])
            self.assertIn("claimed by dev-a@x.com", out)
            with self.elicit(True):
                out = await tools.confirm_workload_plan(None)
            self.assertIn("claimed by dev-a@x.com", out)


if __name__ == "__main__":
    unittest.main()
