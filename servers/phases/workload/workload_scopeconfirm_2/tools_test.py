"""Tool-layer tests for confirm_workload_scope (the scope sign-off gate).

The elicitation is stubbed (the harness idiom used by the platform gate
tests): approve persists scope.json with the exports stamps and parks the
component; decline transitions back to SCOPE and persists nothing.
"""

import json
import os
import shutil
import unittest
from unittest.mock import AsyncMock, patch

from google.api_core import exceptions

import servers.dag.state_management as state_mgr
from servers.dag import fake_gcs
from servers.phases.workload.workload_scopeconfirm_2 import tools

COMPONENT = "orders-component"
REGISTRY = {
    "workspace_name": "ws-dev", "gcp_project": "proj",
    "roles": {"developers": ["dev-a@x.com", "dev-b@x.com"]},
}
RESOLUTION = {
    "resolved_paths": ["charts/orders/Chart.yaml", "charts/orders/values.yaml"],
    "degraded": False,
    "exports_generated_at": "2026-08-14T00:00:00+00:00",
    "exports_generations": {"discovery": 3, "translation": 1, "deployment": 0},
}
DRAFT = {"root_dir": "", "included": ["charts/orders"], "excluded": []}

_DAG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         "..", "..", "..", "dag", "developer_dag.json")


class ConfirmScopeTest(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self._saved = (state_mgr.LEDGER_CONFIG_PATH, state_mgr.LEDGER_CONFIG_DIR,
                       state_mgr.gcs_client)
        state_mgr.LEDGER_CONFIG_PATH = "/tmp/test_wkldconfirm_config.yaml"
        state_mgr.LEDGER_CONFIG_DIR = "/tmp/test_wkldconfirm_config.d"
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

    def write_state(self, current="STATE_WKLD_SCOPE_CONFIRM"):
        state = {
            "current_state": current,
            "history": [],
            "variables": {"component": COMPONENT,
                          "claim": {"claimant": "dev-a@x.com", "claimed_at": "T0"},
                          "workload_scope": dict(DRAFT),
                          "workload_scope_resolution": dict(RESOLUTION)},
        }
        self.bucket.blob(f"workloads/{COMPONENT}/state.json") \
            .upload_from_string(json.dumps(state))

    def read_state(self):
        return json.loads(
            self.bucket.blob(f"workloads/{COMPONENT}/state.json").download_as_text())

    def elicit(self, approved):
        return patch.object(tools, "run_elicitation",
                            AsyncMock(return_value=(approved, None)))

    async def test_approve_persists_scope_json_with_stamps_and_advances(self):
        with self.elicit(True):
            out = await tools.confirm_workload_scope(None)
        self.assertIn("SUCCESS", out)
        scope = json.loads(
            self.bucket.blob(f"workloads/{COMPONENT}/scope.json").download_as_text())
        self.assertEqual(scope["component"], COMPONENT)
        self.assertEqual(scope["included"], ["charts/orders"])
        self.assertEqual(scope["resolved_paths"], RESOLUTION["resolved_paths"])
        self.assertFalse(scope["degraded"])
        self.assertEqual(scope["exports_generated_at"],
                         "2026-08-14T00:00:00+00:00")
        self.assertEqual(scope["exports_generations"]["discovery"], 3)
        self.assertEqual(scope["confirmed_by"], "dev-a@x.com")
        self.assertTrue(scope["confirmed_at"])
        state = self.read_state()
        # v0.2: scope approval feeds the planning pipeline, not the parking
        # tail — the destination comes from the graph copy, not the tool.
        self.assertEqual(state["current_state"], "STATE_WKLD_PLAN")

    # --- the early half of the data gate --------------------

    def write_exports(self, services, scanned=True):
        """exports with a data gate and a seed index that places 'orders' in
        the very files the draft resolution resolved to."""
        self.bucket.blob("exports.json").upload_from_string(json.dumps({
            "generated_at": "2026-08-14T00:00:00+00:00",
            "generations": {"discovery": 3, "translation": 1,
                            "deployment": 0, "data": 2},
            "component_seed_index": {
                "charts/orders/Chart.yaml": {
                    "kinds": ["helm-chart"], "namespaces": [],
                    "team_labels": [], "names": []},
                "charts/orders/values.yaml": {
                    "kinds": ["Deployment"], "namespaces": ["orders"],
                    "team_labels": [], "names": ["Deployment/orders"]}},
            "data_gate": {"schema_version": 1, "scanned": scanned,
                          "services": list(services)},
        }))

    def outstanding(self, consumers):
        return {"service": "rds", "identifier": "orders-db",
                "address": "aws_db_instance.orders", "directory": "tf",
                "disposition": "migrate", "status": None,
                "consumers": consumers}

    async def test_the_advisory_rides_the_elicitation_and_never_blocks(self):
        self.write_exports([self.outstanding(
            [{"workload": "orders", "kind": "workload", "namespace": "orders",
              "source_path": None, "detection": "human_review"}])])
        elicit = AsyncMock(return_value=(True, None))
        with patch.object(tools, "run_elicitation", elicit):
            out = await tools.confirm_workload_scope(None)

        notice = elicit.await_args.kwargs["notice"]
        self.assertIn("rds orders-db", notice)
        self.assertIn("does not block scoping", notice,
                      "warn early: the answer is allowed to be 'proceed'")
        self.assertIn("SUCCESS", out)
        self.assertIn("rds orders-db", out)
        state = self.read_state()
        self.assertEqual(state["current_state"], "STATE_WKLD_PLAN",
                         "the advisory must not stop the component")
        self.assertTrue(any("open data-dependency advisory" in h
                            for h in state["history"]),
                        "confirming over an open advisory is worth recording")

    async def test_a_clear_component_gets_no_notice(self):
        self.write_exports([])
        elicit = AsyncMock(return_value=(True, None))
        with patch.object(tools, "run_elicitation", elicit):
            out = await tools.confirm_workload_scope(None)
        self.assertIsNone(elicit.await_args.kwargs["notice"])
        self.assertNotIn("Data dependencies", out)

    async def test_an_unreadable_exports_degrades_to_no_notice(self):
        """The warn half must never stop a sign-off. The ship gate is where
        an unreadable exports.json becomes a refusal."""
        original = fake_gcs.FakeBlob.download_as_text

        def deny(blob_self, *args, **kwargs):
            if blob_self.name == "exports.json":
                raise exceptions.Forbidden("no exports grant")
            return original(blob_self, *args, **kwargs)

        self.write_exports([self.outstanding([])])
        # A capturing mock, not self.elicit: the point of this test is that
        # NO notice was produced, and the helper cannot see that.
        elicit = AsyncMock(return_value=(True, None))
        with patch.object(fake_gcs.FakeBlob, "download_as_text", deny), \
                patch.object(tools, "run_elicitation", elicit):
            out = await tools.confirm_workload_scope(None)
        self.assertIn("SUCCESS", out)
        self.assertIsNone(elicit.await_args.kwargs["notice"])
        self.assertNotIn("Data dependencies", out)

    async def test_decline_returns_to_scope_and_persists_nothing(self):
        with self.elicit(False):
            out = await tools.confirm_workload_scope(None)
        self.assertIn("not approved", out)
        self.assertFalse(
            self.bucket.blob(f"workloads/{COMPONENT}/scope.json").exists())
        state = self.read_state()
        self.assertEqual(state["current_state"], "STATE_WKLD_SCOPE")
        self.assertTrue(any("declined" in h for h in state["history"]))

    async def test_wrong_state_is_refused(self):
        self.write_state(current="STATE_WKLD_SCOPE")
        with self.elicit(True):
            out = await tools.confirm_workload_scope(None)
        self.assertIn("ERROR: Invalid state", out)

    async def test_claimant_is_enforced(self):
        with patch.object(state_mgr, "get_authenticated_user_email",
                          return_value="dev-b@x.com"), self.elicit(True):
            out = await tools.confirm_workload_scope(None)
        self.assertIn("ERROR", out)
        self.assertIn("claimed by dev-a@x.com", out)

    async def test_forbidden_scope_write_reports_the_admin_binding_step(self):
        # scope.json is the one write with its own generation read: a 403
        # there must become the D11 instruction, not a stack trace.
        real_blob = self.bucket.blob

        def denying_blob(name):
            blob = real_blob(name)
            if name.endswith("scope.json"):
                def deny(*args, **kwargs):
                    raise exceptions.Forbidden("403")
                blob.upload_from_string = deny
            return blob

        with patch.object(self.bucket, "blob", side_effect=denying_blob), \
             self.elicit(True):
            out = await tools.confirm_workload_scope(None)
        self.assertIn("ERROR", out)
        self.assertIn("denied the write", out)
        self.assertIn("gcloud storage managed-folders add-iam-policy-binding", out)
        # The transition was not recorded: the component is still confirmable.
        self.assertEqual(self.read_state()["current_state"],
                         "STATE_WKLD_SCOPE_CONFIRM")

    async def test_approve_overwrites_an_existing_scope_json(self):
        self.bucket.blob(f"workloads/{COMPONENT}/scope.json") \
            .upload_from_string(json.dumps({"stale": True}))
        with self.elicit(True):
            out = await tools.confirm_workload_scope(None)
        self.assertIn("SUCCESS", out)
        scope = json.loads(
            self.bucket.blob(f"workloads/{COMPONENT}/scope.json").download_as_text())
        self.assertNotIn("stale", scope)
        self.assertEqual(scope["component"], COMPONENT)


if __name__ == "__main__":
    unittest.main()
