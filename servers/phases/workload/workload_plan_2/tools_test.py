"""Tool-layer tests for plan_workload_translation (STATE_WKLD_PLAN).

FakeGCS + a temp source clone. Covers: claimant enforcement, wrong state,
scope-absent refusal, missing source_root, degraded-null exports,
placeholder-only refusal (no transition, nothing persisted), the persist
paths (plan.json + variables under precondition) and the transition.
"""

import json
import os
import shutil
import tempfile
import unittest
from unittest.mock import patch

from google.api_core import exceptions

import servers.dag.state_management as state_mgr
from servers.dag import fake_gcs
from servers.phases.workload.workload_plan_2 import planner, tools

COMPONENT = "orders-component"
REGISTRY = {
    "workspace_name": "ws-dev", "gcp_project": "proj",
    "roles": {"developers": ["dev-a@x.com", "dev-b@x.com"]},
}
SCOPE_DOC = {"component": COMPONENT, "included": ["k8s/"], "excluded": [],
             "resolved_paths": ["k8s/app.yaml"], "degraded": False}
EXPORTS_DOC = {
    "gateway": None,
    "cluster": {"name": "gke-1", "type": "autopilot", "location": "us"},
    "node_shapes": [], "storage_class_menu": ["gp3-encrypted"],
    "gsa_bindings": None, "artifact_registry": None, "staging_bucket": None,
    "generated_at": "2026-08-14T00:00:00+00:00",
    "generations": {"discovery": 3, "translation": 1, "deployment": 0},
}

_DAG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         "..", "..", "..", "dag", "developer_dag.json")

APP_YAML = ("apiVersion: apps/v1\nkind: Deployment\n"
            "metadata:\n  name: web\n  namespace: shop\n")


class PlanToolTest(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self._saved = (state_mgr.LEDGER_CONFIG_PATH, state_mgr.LEDGER_CONFIG_DIR,
                       state_mgr.gcs_client)
        state_mgr.LEDGER_CONFIG_PATH = "/tmp/test_wkldplan_config.yaml"
        state_mgr.LEDGER_CONFIG_DIR = "/tmp/test_wkldplan_config.d"
        shutil.rmtree(state_mgr.LEDGER_CONFIG_DIR, ignore_errors=True)
        self.fake = fake_gcs.FakeStorageClient()
        state_mgr.gcs_client = self.fake
        self.bucket = self.fake.bucket("dev-ledger")
        self.bucket.blob("workspace_registry.yaml").upload_from_string(
            json.dumps(REGISTRY))
        with open(os.path.normpath(_DAG_PATH)) as f:
            self.bucket.blob(f"workloads/{COMPONENT}/dag.json") \
                .upload_from_string(f.read())
        self.bucket.blob(f"workloads/{COMPONENT}/scope.json") \
            .upload_from_string(json.dumps(SCOPE_DOC))
        self.bucket.blob("exports.json").upload_from_string(
            json.dumps(EXPORTS_DOC))
        self.write_state()
        state_mgr.write_local_config("gs://dev-ledger", "developers", "ws-dev",
                                     "proj", component=COMPONENT)
        self._email = patch.object(state_mgr, "get_authenticated_user_email",
                                   return_value="dev-a@x.com")
        self._email.start()
        self.root = tempfile.mkdtemp(prefix="wkld_plan_tool_")
        self.addCleanup(shutil.rmtree, self.root, True)
        os.makedirs(os.path.join(self.root, "k8s"), exist_ok=True)
        with open(os.path.join(self.root, "k8s", "app.yaml"), "w") as f:
            f.write(APP_YAML)

    def tearDown(self):
        self._email.stop()
        shutil.rmtree(state_mgr.LEDGER_CONFIG_DIR, ignore_errors=True)
        (state_mgr.LEDGER_CONFIG_PATH, state_mgr.LEDGER_CONFIG_DIR,
         state_mgr.gcs_client) = self._saved

    def write_state(self, current="STATE_WKLD_PLAN"):
        state = {
            "current_state": current,
            "history": [],
            "variables": {"component": COMPONENT,
                          "claim": {"claimant": "dev-a@x.com",
                                    "claimed_at": "T0"}},
        }
        self.bucket.blob(f"workloads/{COMPONENT}/state.json") \
            .upload_from_string(json.dumps(state))

    def read_state(self):
        return json.loads(
            self.bucket.blob(f"workloads/{COMPONENT}/state.json").download_as_text())

    async def test_success_persists_plan_and_transitions(self):
        out = await tools.plan_workload_translation(source_root=self.root)
        self.assertIn("SUCCESS", out)
        plan = json.loads(self.bucket.blob(
            f"workloads/{COMPONENT}/plan.json").download_as_text())
        self.assertEqual(plan["component"], COMPONENT)
        self.assertEqual(len(plan["units"]), 4)
        self.assertEqual(plan["exports_stamp"]["generations"]["discovery"], 3)
        state = self.read_state()
        self.assertEqual(state["current_state"], "STATE_WKLD_PLAN_REVIEW")
        self.assertEqual(state["variables"]["workload_plan"]["component"],
                         COMPONENT)
        self.assertEqual(state["variables"]["workload_source_root"], self.root)
        self.assertTrue(any("Workload plan built" in h
                            for h in state["history"]))

    async def test_reuses_recorded_source_root_on_replan(self):
        await tools.plan_workload_translation(source_root=self.root)
        self.write_state()  # back to PLAN, but keep variables via re-read
        state = self.read_state()
        state["variables"]["workload_source_root"] = self.root
        self.bucket.blob(f"workloads/{COMPONENT}/state.json") \
            .upload_from_string(json.dumps(state))
        out = await tools.plan_workload_translation()
        self.assertIn("SUCCESS", out)

    async def test_degraded_exports_stamps_nulls(self):
        self.bucket.blob("exports.json").delete()
        out = await tools.plan_workload_translation(source_root=self.root)
        self.assertIn("SUCCESS", out)
        plan = json.loads(self.bucket.blob(
            f"workloads/{COMPONENT}/plan.json").download_as_text())
        self.assertEqual(plan["exports_stamp"],
                         {"generated_at": None, "generations": None,
                          "non_gateway_digest": None})

    async def test_missing_scope_is_refused_toward_the_scope_step(self):
        self.bucket.blob(f"workloads/{COMPONENT}/scope.json").delete()
        out = await tools.plan_workload_translation(source_root=self.root)
        self.assertIn("ERROR", out)
        self.assertIn("scope.json is absent", out)
        self.assertEqual(self.read_state()["current_state"], "STATE_WKLD_PLAN")

    async def test_missing_source_root_is_refused(self):
        out = await tools.plan_workload_translation()
        self.assertIn("ERROR", out)
        self.assertIn("source_root", out)

    async def test_placeholder_only_plan_is_refused_without_transition(self):
        os.remove(os.path.join(self.root, "k8s", "app.yaml"))
        out = await tools.plan_workload_translation(source_root=self.root)
        self.assertIn("ERROR", out)
        self.assertIn("placeholder", out)
        self.assertIn("amend the scope", out)
        self.assertEqual(self.read_state()["current_state"], "STATE_WKLD_PLAN")
        self.assertFalse(
            self.bucket.blob(f"workloads/{COMPONENT}/plan.json").exists())

    async def test_a_plan_whose_only_source_failed_to_render_is_refused(self):
        """The placeholder flags cannot see this one: wkld-manifests is
        `planned` (its open question is real work) with zero documents."""
        os.remove(os.path.join(self.root, "k8s", "app.yaml"))
        with open(os.path.join(self.root, "k8s", "Chart.yaml"), "w") as f:
            f.write("name: c\nversion: 1.0\n")
        with patch.object(planner, "helm_template",
                          return_value=(None, "helm exploded")):
            out = await tools.plan_workload_translation(source_root=self.root)
        self.assertIn("ERROR", out)
        self.assertIn("failed to render", out)
        self.assertIn("helm exploded", out)
        self.assertEqual(self.read_state()["current_state"], "STATE_WKLD_PLAN")
        self.assertFalse(
            self.bucket.blob(f"workloads/{COMPONENT}/plan.json").exists())

    async def test_an_unexpected_planner_failure_becomes_an_error_string(self):
        with patch.object(planner, "build_workload_plan",
                          side_effect=OSError("disk went away")):
            out = await tools.plan_workload_translation(source_root=self.root)
        self.assertIn("ERROR", out)
        self.assertIn("OSError: disk went away", out)
        self.assertEqual(self.read_state()["current_state"], "STATE_WKLD_PLAN")

    async def test_wrong_state_is_refused(self):
        self.write_state(current="STATE_WKLD_SCOPE")
        out = await tools.plan_workload_translation(source_root=self.root)
        self.assertIn("ERROR: Invalid state", out)

    async def test_claimant_is_enforced(self):
        with patch.object(state_mgr, "get_authenticated_user_email",
                          return_value="dev-b@x.com"):
            out = await tools.plan_workload_translation(source_root=self.root)
        self.assertIn("ERROR", out)
        self.assertIn("claimed by dev-a@x.com", out)

    async def test_forbidden_plan_write_reports_the_admin_binding_step(self):
        real_blob = self.bucket.blob

        def denying_blob(name):
            blob = real_blob(name)
            if name.endswith("plan.json"):
                def deny(*args, **kwargs):
                    raise exceptions.Forbidden("403")
                blob.upload_from_string = deny
            return blob

        with patch.object(self.bucket, "blob", side_effect=denying_blob):
            out = await tools.plan_workload_translation(source_root=self.root)
        self.assertIn("denied the write", out)
        self.assertEqual(self.read_state()["current_state"], "STATE_WKLD_PLAN")


if __name__ == "__main__":
    unittest.main()
