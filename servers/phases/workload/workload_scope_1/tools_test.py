"""Tool-layer tests for the workload scope step (browse / update / submit).

Runs the real tools over FakeGCS through authorize_and_rehydrate_workload:
registry, developer session cache, component state and exports are all
seeded; no real GCS, credentials or home files are touched.
"""

import json
import os
import shutil
import unittest
from unittest.mock import patch

import servers.dag.state_management as state_mgr
from google.api_core import exceptions

from servers.dag import fake_gcs
from servers.dag.server import exports as exports_lib
from servers.phases.workload.workload_scope_1 import tools

COMPONENT = "orders-component"
REGISTRY = {
    "workspace_name": "ws-dev", "gcp_project": "proj",
    "roles": {"developers": ["dev-a@x.com", "other@x.com"]},
}
SEED_INDEX = {
    "charts/orders/Chart.yaml": {"kinds": ["helm-chart"], "namespaces": [],
                                 "team_labels": []},
    "charts/orders/values.yaml": {"kinds": ["helm-chart"], "namespaces": [],
                                  "team_labels": []},
    "apps/cart/dep.yaml": {"kinds": ["Deployment"], "namespaces": ["acme-shop"],
                           "team_labels": ["team-cart"]},
    "infra/sc.yaml": {"kinds": ["StorageClass"], "namespaces": [],
                      "team_labels": []},
}

_DAG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         "..", "..", "..", "dag", "developer_dag.json")


class _WorkloadToolsBase(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self._saved = (state_mgr.LEDGER_CONFIG_PATH, state_mgr.LEDGER_CONFIG_DIR,
                       state_mgr.gcs_client)
        state_mgr.LEDGER_CONFIG_PATH = "/tmp/test_wkld_config.yaml"
        state_mgr.LEDGER_CONFIG_DIR = "/tmp/test_wkld_config.d"
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

    def write_state(self, current="STATE_WKLD_SCOPE", variables=None):
        state = {
            "current_state": current,
            "history": [],
            "variables": {"component": COMPONENT,
                          "claim": {"claimant": "dev-a@x.com",
                                    "claimed_at": "T0"},
                          **(variables or {})},
        }
        self.bucket.blob(f"workloads/{COMPONENT}/state.json") \
            .upload_from_string(json.dumps(state))

    def publish_exports(self, index=SEED_INDEX):
        doc = exports_lib.empty_exports()
        doc["component_seed_index"] = index
        doc["generated_at"] = "2026-08-14T00:00:00+00:00"
        doc["generations"] = {"discovery": 3, "translation": 1, "deployment": 0}
        self.bucket.blob("exports.json").upload_from_string(json.dumps(doc))

    def read_state(self):
        return json.loads(
            self.bucket.blob(f"workloads/{COMPONENT}/state.json").download_as_text())


class BrowseTest(_WorkloadToolsBase):

    async def test_terrain_first_then_labeled_guess_then_entries(self):
        self.publish_exports()
        out = await tools.browse_component_seed()
        self.assertIn("Terrain (4 file(s)", out)
        self.assertIn("GUESS", out)
        self.assertIn("charts/orders/Chart.yaml", out)
        # Order: terrain before the guess, guess before the entries.
        self.assertLess(out.index("Terrain"), out.index("GUESS"))
        self.assertLess(out.index("GUESS"), out.index("Entries:"))
        self.assertIn("StorageClass", out)
        self.assertIn("platform-owned", out)

    async def test_filtered_browse_skips_the_guess(self):
        self.publish_exports()
        out = await tools.browse_component_seed(path_glob="apps/")
        self.assertNotIn("GUESS", out)
        self.assertIn("Entries: 1 matched of 4", out)
        self.assertIn("apps/cart/dep.yaml", out)

    async def test_browse_is_allowed_for_a_non_claimant(self):
        self.publish_exports()
        with patch.object(state_mgr, "get_authenticated_user_email",
                          return_value="other@x.com"):
            out = await tools.browse_component_seed()
        self.assertNotIn("ERROR", out)
        self.assertIn("Terrain", out)

    async def test_degraded_when_exports_absent(self):
        out = await tools.browse_component_seed()
        self.assertIn("DEGRADED MODE", out)
        self.assertIn("has not been published", out)
        self.assertIn("not an error", out)
        self.assertIn("UNVERIFIED", out)
        self.assertIn("platform/*", out)

    async def test_degraded_when_seed_index_null(self):
        self.publish_exports(index=None)
        out = await tools.browse_component_seed()
        self.assertIn("DEGRADED MODE", out)
        self.assertIn("component_seed_index is null or empty", out)

    async def test_forbidden_exports_read_names_the_iam_remedy(self):
        """A 403 on exports.json is an IAM defect (the conditional
        developer read grant is missing), never conflated with "not
        published yet" and never a raw traceback: the degraded reason
        names provision_ledger_iam."""
        self.publish_exports()
        real = fake_gcs.FakeBlob.download_as_text

        def deny_exports(blob_self, *args, **kwargs):
            if blob_self.name == "exports.json":
                raise exceptions.Forbidden("no exports grant")
            return real(blob_self, *args, **kwargs)

        with patch.object(fake_gcs.FakeBlob, "download_as_text",
                          deny_exports):
            out = await tools.browse_component_seed()
        self.assertIn("DEGRADED MODE", out)
        self.assertIn("DENIED", out)
        self.assertIn("provision_ledger_iam", out)
        self.assertNotIn("has not been published", out)

    async def test_invalid_state_is_refused(self):
        self.publish_exports()
        self.write_state(current="STATE_WKLD_AWAIT_PIPELINE")
        out = await tools.browse_component_seed()
        self.assertIn("ERROR: Invalid state", out)


class UpdateScopeTest(_WorkloadToolsBase):

    async def test_update_persists_the_draft_and_reports_the_count(self):
        self.publish_exports()
        out = await tools.update_workload_scope(include=["charts/orders/"])
        self.assertIn("Scope updated", out)
        self.assertIn("In-scope against the seed: 2 of 4", out)
        state = self.read_state()
        self.assertEqual(state["variables"]["workload_scope"]["included"],
                         ["charts/orders"])
        self.assertEqual(state["current_state"], "STATE_WKLD_SCOPE")  # no transition

    async def test_update_enforces_the_claimant(self):
        self.publish_exports()
        with patch.object(state_mgr, "get_authenticated_user_email",
                          return_value="other@x.com"):
            out = await tools.update_workload_scope(include=["charts/orders/"])
        self.assertIn("ERROR", out)
        self.assertIn("dev-a@x.com", out)

    async def test_update_in_degraded_mode_says_unverifiable(self):
        out = await tools.update_workload_scope(include=["charts/orders/"])
        self.assertIn("unverifiable", out)

    async def test_exclude_then_reinclude_restores_the_selection(self):
        # Component scopes select from nothing: re-including a previously
        # excluded pattern must land it back in `included`, not just
        # un-exclude it (which selects nothing under the inverted algebra).
        self.publish_exports()
        await tools.update_workload_scope(
            include=["charts/orders/Chart.yaml", "charts/orders/values.yaml"])
        await tools.update_workload_scope(exclude=["charts/orders/values.yaml"])
        out = await tools.update_workload_scope(
            include=["charts/orders/values.yaml"])
        self.assertIn("In-scope against the seed: 2 of 4", out)
        draft = self.read_state()["variables"]["workload_scope"]
        self.assertIn("charts/orders/values.yaml", draft["included"])
        self.assertEqual(draft["excluded"], [])

    async def test_forbidden_write_reports_the_admin_binding_step(self):
        # A write can 403 on its own (viewer-only binding, revoked grant):
        # the tool must return the D11 instruction, not a stack trace.
        from google.api_core import exceptions as gexc
        self.publish_exports()
        real_blob = self.bucket.blob

        def denying_blob(name):
            blob = real_blob(name)
            if name.endswith("state.json"):
                def deny(*args, **kwargs):
                    raise gexc.Forbidden("403")
                blob.upload_from_string = deny
            return blob

        with patch.object(self.bucket, "blob", side_effect=denying_blob):
            out = await tools.update_workload_scope(include=["charts/orders/"])
        self.assertIn("ERROR", out)
        self.assertIn("denied the write", out)
        self.assertIn("gcloud storage managed-folders add-iam-policy-binding", out)
        self.assertIn(f"workloads/{COMPONENT}/", out)


class SubmitScopeTest(_WorkloadToolsBase):

    async def test_submit_resolves_stamps_and_advances(self):
        self.publish_exports()
        await tools.update_workload_scope(include=["charts/orders/"])
        out = await tools.submit_workload_scope()
        self.assertIn("SUCCESS", out)
        self.assertIn("Resolved 2 of 4", out)
        state = self.read_state()
        self.assertEqual(state["current_state"], "STATE_WKLD_SCOPE_CONFIRM")
        resolution = state["variables"]["workload_scope_resolution"]
        self.assertEqual(resolution["resolved_paths"],
                         ["charts/orders/Chart.yaml", "charts/orders/values.yaml"])
        self.assertFalse(resolution["degraded"])
        self.assertEqual(resolution["exports_generated_at"],
                         "2026-08-14T00:00:00+00:00")
        self.assertEqual(resolution["exports_generations"]["discovery"], 3)

    async def test_empty_resolution_is_refused_not_advanced(self):
        self.publish_exports()
        await tools.update_workload_scope(include=["no-such-dir/"])
        out = await tools.submit_workload_scope()
        self.assertIn("ERROR", out)
        self.assertIn("0 of the 4", out)
        self.assertIn("no candidates exist", out)  # D13's honesty rule
        self.assertEqual(self.read_state()["current_state"], "STATE_WKLD_SCOPE")

    async def test_degraded_submit_requires_an_include_and_stays_null(self):
        out = await tools.submit_workload_scope()
        self.assertIn("ERROR: Degraded mode", out)
        await tools.update_workload_scope(include=["charts/orders/"])
        out = await tools.submit_workload_scope()
        self.assertIn("SUCCESS", out)
        self.assertIn("unverified", out)
        resolution = self.read_state()["variables"]["workload_scope_resolution"]
        self.assertIsNone(resolution["resolved_paths"])
        self.assertTrue(resolution["degraded"])
        self.assertIsNone(resolution["exports_generated_at"])

    async def test_submit_enforces_the_claimant(self):
        self.publish_exports()
        with patch.object(state_mgr, "get_authenticated_user_email",
                          return_value="other@x.com"):
            out = await tools.submit_workload_scope()
        self.assertIn("ERROR", out)
        self.assertIn("dev-a@x.com", out)


if __name__ == "__main__":
    unittest.main()
