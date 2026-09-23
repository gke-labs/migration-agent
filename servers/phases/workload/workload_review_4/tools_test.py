"""Tool-layer tests for the workload unit review (STATE_WKLD_REVIEW).

FakeGCS. Covers: revise routes back to TRANSLATE with feedback (and only
for revisable units — parked/skipped refuse), skip records the reason
without a transition, approve requires done-ness (parked never blocks),
retranslate marks every active unit revise, and claimant enforcement.
"""

import json
import os
import shutil
import unittest
from unittest.mock import patch

import servers.dag.state_management as state_mgr
from servers.dag import fake_gcs
from servers.phases.workload.workload_review_4 import tools

COMPONENT = "orders-component"
REGISTRY = {
    "workspace_name": "ws-dev", "gcp_project": "proj",
    "roles": {"developers": ["dev-a@x.com", "dev-b@x.com"]},
}
_DAG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         "..", "..", "..", "dag", "developer_dag.json")


def unit(unit_id, status):
    return {"unit_id": unit_id, "family": unit_id, "title": unit_id,
            "status": status, "planned_status": status, "placeholder": False,
            "inputs": {"documents": []}, "notes": [], "feedback": None,
            "error": None}


class ReviewToolsTest(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self._saved = (state_mgr.LEDGER_CONFIG_PATH, state_mgr.LEDGER_CONFIG_DIR,
                       state_mgr.gcs_client)
        state_mgr.LEDGER_CONFIG_PATH = "/tmp/test_wkldrev_config.yaml"
        state_mgr.LEDGER_CONFIG_DIR = "/tmp/test_wkldrev_config.d"
        shutil.rmtree(state_mgr.LEDGER_CONFIG_DIR, ignore_errors=True)
        self.fake = fake_gcs.FakeStorageClient()
        state_mgr.gcs_client = self.fake
        self.bucket = self.fake.bucket("dev-ledger")
        self.bucket.blob("workspace_registry.yaml").upload_from_string(
            json.dumps(REGISTRY))
        with open(os.path.normpath(_DAG_PATH)) as f:
            self.bucket.blob(f"workloads/{COMPONENT}/dag.json") \
                .upload_from_string(f.read())
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

    def write_state(self, units, current="STATE_WKLD_REVIEW"):
        plan = {"component": COMPONENT, "units": units,
                "sources": {"charts": [], "kustomize": []}, "carriers": [],
                "notes": [], "exports_stamp": {"generated_at": "T0",
                                               "generations": {}}}
        state = {"current_state": current, "history": [],
                 "variables": {"component": COMPONENT,
                               "claim": {"claimant": "dev-a@x.com",
                                         "claimed_at": "T0"},
                               "workload_plan": plan}}
        self.bucket.blob(f"workloads/{COMPONENT}/state.json") \
            .upload_from_string(json.dumps(state))

    def read_state(self):
        return json.loads(self.bucket.blob(
            f"workloads/{COMPONENT}/state.json").download_as_text())

    async def test_revise_routes_back_to_translate_with_feedback(self):
        self.write_state([unit("wkld-manifests", "done"),
                          unit("wkld-identity", "done")])
        out = await tools.request_workload_unit_revision(
            ["wkld-identity"], "wrong annotation")
        self.assertIn("SUCCESS", out)
        state = self.read_state()
        self.assertEqual(state["current_state"], "STATE_WKLD_TRANSLATE")
        statuses = {u["unit_id"]: u for u
                    in state["variables"]["workload_plan"]["units"]}
        self.assertEqual(statuses["wkld-identity"]["status"], "revise")
        self.assertEqual(statuses["wkld-identity"]["feedback"],
                         "wrong annotation")
        self.assertEqual(statuses["wkld-manifests"]["status"], "done")

    async def test_parked_and_skipped_units_cannot_be_revised(self):
        self.write_state([unit("wkld-routing", "parked"),
                          unit("wkld-storage", "skipped")])
        for unit_id in ("wkld-routing", "wkld-storage"):
            out = await tools.request_workload_unit_revision([unit_id], "f")
            self.assertIn("ERROR", out)
            self.assertIn("cannot be revised", out)

    async def test_skip_records_reason_without_transition(self):
        self.write_state([unit("wkld-manifests", "done"),
                          unit("wkld-identity", "error")])
        out = await tools.skip_workload_units(["wkld-identity"], "not migrating")
        self.assertIn("SUCCESS", out)
        state = self.read_state()
        self.assertEqual(state["current_state"], "STATE_WKLD_REVIEW")
        skipped = state["variables"]["workload_plan"]["units"][1]
        self.assertEqual(skipped["status"], "skipped")
        self.assertIn("Skipped in review: not migrating", skipped["notes"])

    async def test_approve_requires_every_active_unit_done(self):
        self.write_state([unit("wkld-manifests", "done"),
                          unit("wkld-identity", "error")])
        out = await tools.approve_workload_translation("approve")
        self.assertIn("ERROR", out)
        self.assertIn("wkld-identity", out)
        self.assertEqual(self.read_state()["current_state"],
                         "STATE_WKLD_REVIEW")

    async def test_parked_units_never_block_approval(self):
        self.write_state([unit("wkld-manifests", "done"),
                          unit("wkld-routing", "parked")])
        out = await tools.approve_workload_translation("approve")
        self.assertIn("SUCCESS", out)
        self.assertEqual(self.read_state()["current_state"],
                         "STATE_WKLD_VALIDATE")

    async def test_approve_with_zero_done_units_is_refused(self):
        self.write_state([unit("wkld-manifests", "skipped"),
                          unit("wkld-routing", "parked")])
        out = await tools.approve_workload_translation("approve")
        self.assertIn("ERROR", out)
        self.assertIn("no unit is done", out)

    async def test_retranslate_marks_active_units_revise(self):
        self.write_state([unit("wkld-manifests", "done"),
                          unit("wkld-routing", "parked"),
                          unit("wkld-storage", "skipped")])
        out = await tools.approve_workload_translation(
            "retranslate", feedback="redo all")
        self.assertIn("retranslation", out)
        state = self.read_state()
        self.assertEqual(state["current_state"], "STATE_WKLD_TRANSLATE")
        statuses = {u["unit_id"]: u["status"] for u
                    in state["variables"]["workload_plan"]["units"]}
        self.assertEqual(statuses["wkld-manifests"], "revise")
        self.assertEqual(statuses["wkld-routing"], "parked")
        self.assertEqual(statuses["wkld-storage"], "skipped")

    async def test_replan_returns_to_the_plan_state_without_touching_units(self):
        """The only way out of a staleness finding: retranslating re-stamps
        every blob with the plan's FROZEN exports_stamp, so the cross-check
        keeps failing until plan_workload_translation rebuilds the stamp."""
        self.write_state([unit("wkld-manifests", "done"),
                          unit("wkld-routing", "parked")])
        out = await tools.approve_workload_translation("replan")
        self.assertIn("plan_workload_translation", out)
        state = self.read_state()
        self.assertEqual(state["current_state"], "STATE_WKLD_PLAN")
        statuses = {u["unit_id"]: u["status"] for u
                    in state["variables"]["workload_plan"]["units"]}
        self.assertEqual(statuses, {"wkld-manifests": "done",
                                    "wkld-routing": "parked"})

    async def test_an_unknown_action_names_all_three(self):
        self.write_state([unit("wkld-manifests", "done")])
        out = await tools.approve_workload_translation("reticulate")
        self.assertIn("ERROR", out)
        for action in ("approve", "retranslate", "replan"):
            self.assertIn(action, out)

    async def test_claimant_is_enforced_on_every_mutating_tool(self):
        self.write_state([unit("wkld-manifests", "done")])
        with patch.object(state_mgr, "get_authenticated_user_email",
                          return_value="dev-b@x.com"):
            for call in (
                    tools.request_workload_unit_revision(["wkld-manifests"], "f"),
                    tools.skip_workload_units(["wkld-manifests"], "r"),
                    tools.approve_workload_translation("approve")):
                out = await call
                self.assertIn("claimed by dev-a@x.com", out)

    async def test_get_results_reads_the_unit_blob(self):
        self.write_state([unit("wkld-manifests", "done")])
        self.bucket.blob(
            f"workloads/{COMPONENT}/units/wkld-manifests.json") \
            .upload_from_string(json.dumps({
                "unit": {"unit_id": "wkld-manifests", "status": "done"},
                "result": {"files": [{"path": "a.yaml", "content": "x"}],
                           "tradeoffs": "t", "assumptions": ["check quota"],
                           "open_questions": ["who owns DNS?"]},
                "run": 1, "exports_generations": {}}))
        out = await tools.get_workload_results("wkld-manifests")
        self.assertIn("a.yaml", out)
        self.assertIn("check quota", out)
        self.assertIn("who owns DNS?", out)
        summary = await tools.get_workload_results()
        self.assertIn("wkld-manifests", summary)


if __name__ == "__main__":
    unittest.main()
