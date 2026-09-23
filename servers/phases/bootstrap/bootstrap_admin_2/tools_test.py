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

"""Tool-layer tests for the workspace administration step.

FakeGCS holds the ledger; IAM is patched at the ledger_iam function level
(nothing reaches storage/v1); the elicitation raised by the two resets and by
the DAG upgrade is patched to approve or decline. Both ways into the step are covered: the post-bootstrap
session (set_bootstrapped, no session cache) and a joined admin session
(session cache + registry), including the refusals for a platform engineer
and for an admin who joined de-escalated.
"""

import json
import os
import shutil
import unittest
from unittest.mock import patch

import servers.dag.state_management as state_mgr
from servers.dag import dispatch, fake_gcs
from servers.dag.server import ledger_admin, ledger_iam
from servers.phases.bootstrap.bootstrap_admin_2 import tools

_DAG_DIR = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                         "..", "..", "..", "dag"))
REGISTRY = {
    "workspace_name": "ws", "gcp_project": "proj", "ledger_bucket": "gs://b",
    "roles": {"admins": ["ada@x.com"], "platform_engineers": ["pat@x.com"],
              "developers": ["dev@x.com"]},
}


async def approve(ctx, state_name, state_def, schema_cls, state_dict, config, notice=None):
    approve.prompts.append(dispatch.render_prompt(state_name, state_def, state_dict, config))
    return True, {"approved": True}


async def decline(ctx, state_name, state_def, schema_cls, state_dict, config, notice=None):
    return False, None


class AdminToolsTest(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self._saved = (state_mgr.LEDGER_CONFIG_PATH, state_mgr.LEDGER_CONFIG_DIR, state_mgr.gcs_client)
        state_mgr.LEDGER_CONFIG_PATH = "/tmp/test_bootstrap_admin_config.yaml"
        state_mgr.LEDGER_CONFIG_DIR = "/tmp/test_bootstrap_admin_config.d"
        shutil.rmtree(state_mgr.LEDGER_CONFIG_DIR, ignore_errors=True)
        self.fake = fake_gcs.FakeStorageClient()
        state_mgr.gcs_client = self.fake
        self.bucket = self.fake.bucket("b")
        self.bucket.blob("workspace_registry.yaml").upload_from_string(json.dumps(REGISTRY))
        with open(os.path.join(_DAG_DIR, "platform_dag.json")) as f:
            self.platform_raw = f.read()
        self.platform_dag = json.loads(self.platform_raw)
        with open(os.path.join(_DAG_DIR, "developer_dag.json")) as f:
            self.developer_raw = f.read()
        self.developer_dag = json.loads(self.developer_raw)
        self.bucket.blob("platform_dag.json").upload_from_string(self.platform_raw)
        self.bucket.blob("platform/onboarding/state.json").upload_from_string(json.dumps({
            "current_state": "STATE_DISCOVERY", "history": ["h"],
            "variables": {"source_repo_url": "sso://src"}}))
        approve.prompts = []
        # The post-bootstrap way in: no session cache, the workspace recorded.
        tools.set_bootstrapped("gs://b", "ws", "proj")
        for name in ("provision_ledger_iam", "revoke_ledger_member"):
            p = patch.object(ledger_iam, name, return_value=0)
            setattr(self, name, p.start())
            self.addCleanup(p.stop)
        p = patch.object(state_mgr, "get_authenticated_user_email", return_value="ada@x.com")
        p.start()
        self.addCleanup(p.stop)

    def tearDown(self):
        tools.clear_bootstrapped()
        shutil.rmtree(state_mgr.LEDGER_CONFIG_DIR, ignore_errors=True)
        (state_mgr.LEDGER_CONFIG_PATH, state_mgr.LEDGER_CONFIG_DIR, state_mgr.gcs_client) = self._saved

    def registry_roles(self):
        return json.loads(self.bucket.blob("workspace_registry.yaml").download_as_text())["roles"]

    def seed_component(self, component, version=None, state="STATE_WKLD_PLAN"):
        dag = dict(self.developer_dag)
        if version:
            dag["version"] = version
        self.bucket.blob(f"workloads/{component}/dag.json").upload_from_string(json.dumps(dag))
        self.bucket.blob(f"workloads/{component}/state.json").upload_from_string(json.dumps({
            "current_state": state, "history": [],
            "variables": {"component": component,
                          "claim": {"claimant": "dev@x.com", "claimed_at": "T"},
                          "workload_scope": {"include": ["k8s/"]}}}))

    # -- who may call -----------------------------------------------------

    async def test_without_a_bootstrap_or_a_session_there_is_nothing_to_administer(self):
        tools.clear_bootstrapped()
        out = await tools.describe_workspace()
        self.assertTrue(out.startswith("ERROR:"))
        self.assertIn("bootstrap one, or join_ledger", out)

    async def test_a_joined_admin_session_wins_over_the_bootstrap_record(self):
        tools.clear_bootstrapped()
        state_mgr.write_local_config("gs://b", "admins", "ws", "proj")
        out = await tools.describe_workspace()
        self.assertIn("Workspace: ws", out)
        self.assertIn("admins: ada@x.com", out)

    async def test_a_platform_engineer_session_is_refused(self):
        tools.clear_bootstrapped()
        state_mgr.write_local_config("gs://b", "platform", "ws", "proj")
        with patch.object(state_mgr, "get_authenticated_user_email", return_value="pat@x.com"):
            out = await tools.add_workspace_member("new@x.com", "application")
        self.assertIn("ERROR: Access Denied", out)
        self.assertNotIn("new@x.com", self.registry_roles()["developers"])

    async def test_an_admin_who_joined_de_escalated_is_refused(self):
        tools.clear_bootstrapped()
        state_mgr.write_local_config("gs://b", "platform", "ws", "proj")
        out = await tools.remove_workspace_member("dev@x.com")
        self.assertIn("ERROR: Access Denied", out)
        self.assertIn("dev@x.com", self.registry_roles()["developers"])

    # -- describe -----------------------------------------------------------

    async def test_describe_reports_roles_graph_versions_and_components(self):
        self.seed_component("orders", version="0.3")
        out = await tools.describe_workspace()
        self.assertIn("platform_engineers: pat@x.com", out)
        self.assertIn(f"Platform graph: v{self.platform_dag['version']}; state STATE_DISCOVERY", out)
        self.assertIn("orders: v0.3, STATE_WKLD_PLAN, dev@x.com", out)

    async def test_describe_flags_a_ledger_graph_behind_the_bundle(self):
        old = dict(self.platform_dag)
        old["version"] = "2.7"
        self.bucket.blob("platform_dag.json").upload_from_string(json.dumps(old))
        out = await tools.describe_workspace()
        self.assertIn(f"v2.7 — bundled is v{self.platform_dag['version']}", out)

    # -- members ------------------------------------------------------------

    async def test_add_grants_iam_before_writing_the_registry(self):
        order = []
        self.provision_ledger_iam.side_effect = lambda *a, **k: order.append(
            ("iam", self.registry_roles()["developers"]))
        out = await tools.add_workspace_member("new@x.com", "application")
        self.assertIn("new@x.com added to the application team", out)
        self.assertEqual(self.registry_roles()["developers"], ["dev@x.com", "new@x.com"])
        # At grant time the registry did not yet hold the new member, and the
        # grant was asked for the roles as they would be after the write.
        self.assertEqual(order, [("iam", ["dev@x.com"])])
        granted_roles = self.provision_ledger_iam.call_args[0][1]
        self.assertIn("new@x.com", granted_roles["developers"])

    async def test_a_failed_grant_never_reaches_the_registry(self):
        # The friction-log poisoning case: an address IAM refuses must not be
        # recorded, or every later grant for everyone fails on it.
        self.provision_ledger_iam.side_effect = ledger_iam.LedgerIamError("invalid principal")
        out = await tools.add_workspace_member("priya@acme.example", "application")
        self.assertTrue(out.startswith("ERROR:"))
        self.assertIn("not registered", out)
        self.assertNotIn("priya@acme.example", self.registry_roles()["developers"])

    async def test_add_moves_a_member_between_teams_and_revokes_the_old_grants(self):
        out = await tools.add_workspace_member("pat@x.com", "application")
        self.assertIn("moved from the platform team to the application team", out)
        roles = self.registry_roles()
        self.assertEqual(roles["platform_engineers"], [])
        self.assertIn("pat@x.com", roles["developers"])
        self.revoke_ledger_member.assert_called_once_with("b", "pat@x.com")
        self.assertNotIn("pat@x.com", self.provision_ledger_iam.call_args[0][1]["platform_engineers"])

    async def test_a_move_whose_new_grant_fails_says_access_was_withdrawn(self):
        # The old team's grants are revoked before the new team's are granted;
        # if the new grant fails the reply must say the team-level access was
        # withdrawn rather than claim they "were not registered".
        self.provision_ledger_iam.side_effect = ledger_iam.LedgerIamError("network")
        out = await tools.add_workspace_member("pat@x.com", "application")
        self.assertTrue(out.startswith("ERROR:"))
        self.assertIn("were revoked", out)
        self.assertIn("team-level ledger access was withdrawn", out)
        self.assertIn("still lists them under the platform team", out)
        self.revoke_ledger_member.assert_called_once_with("b", "pat@x.com")
        self.assertIn("pat@x.com", self.registry_roles()["platform_engineers"])

    async def test_a_failed_move_still_names_the_component_folders_it_left(self):
        # revoke never touches per-component folders, so a move that fails on the
        # new grant must not claim total loss of access — it names the folders.
        self.seed_component("orders")  # claimed by dev@x.com
        self.provision_ledger_iam.side_effect = ledger_iam.LedgerIamError("network")
        out = await tools.add_workspace_member("dev@x.com", "platform")
        self.assertTrue(out.startswith("ERROR:"))
        self.assertIn("team-level ledger access was withdrawn", out)
        self.assertIn("component folder grant(s) the server cannot revoke (orders)", out)
        self.assertIn("workloads/orders/", out)

    async def test_move_names_the_component_folders_the_member_still_holds(self):
        self.seed_component("orders")  # claimed by dev@x.com
        out = await tools.add_workspace_member("dev@x.com", "platform")
        self.assertIn("moved from the application team to the platform team", out)
        self.assertIn("component folder grant(s) the server cannot revoke (orders)", out)
        self.assertIn("workloads/orders/", out)

    async def test_add_refuses_an_admin_and_an_unknown_team(self):
        out = await tools.add_workspace_member("ada@x.com", "application")
        self.assertIn("ERROR: ada@x.com is an admin", out)
        out = await tools.add_workspace_member("new@x.com", "ops")
        self.assertIn("ERROR: team must be one of", out)
        self.provision_ledger_iam.assert_not_called()

    async def test_add_takes_the_email_as_given(self):
        out = await tools.add_workspace_member("not an email", "application")
        self.assertIn("ERROR:", out)
        self.assertIn("does not look like an email", out)

    async def test_remove_writes_the_registry_then_revokes(self):
        self.revoke_ledger_member.return_value = 3
        out = await tools.remove_workspace_member("dev@x.com")
        self.assertIn("dev@x.com removed from the application team; 3 IAM binding(s) revoked", out)
        self.assertEqual(self.registry_roles()["developers"], [])
        self.revoke_ledger_member.assert_called_once_with("b", "dev@x.com")

    async def test_remove_of_a_poisoned_entry_succeeds_with_nothing_to_revoke(self):
        # Registered but never bound (the grant failed): the entry goes, and
        # a zero-binding revoke is the expected outcome, not an error.
        ledger_admin.add_member(self.bucket, "priya@acme.example", "developers")
        out = await tools.remove_workspace_member("priya@acme.example")
        self.assertFalse(out.startswith("ERROR:"), out)
        self.assertNotIn("priya@acme.example", self.registry_roles()["developers"])

    async def test_remove_reports_a_registry_write_that_left_iam_behind(self):
        self.revoke_ledger_member.side_effect = ledger_iam.LedgerIamError("setIamPolicy denied")
        out = await tools.remove_workspace_member("dev@x.com")
        self.assertIn("removed from the application team in the registry, but revoking", out)
        self.assertEqual(self.registry_roles()["developers"], [])

    async def test_remove_of_an_unregistered_email_still_clears_stale_bindings(self):
        self.revoke_ledger_member.return_value = 2
        out = await tools.remove_workspace_member("gone@x.com")
        self.assertIn("was not in the registry; 2 stale IAM binding(s) revoked", out)
        self.revoke_ledger_member.return_value = 0
        out = await tools.remove_workspace_member("gone@x.com")
        self.assertIn("ERROR: gone@x.com is not registered", out)

    async def test_remove_names_the_component_folders_it_cannot_revoke(self):
        # revoke_ledger_member clears the bucket + platform/ bindings, but the
        # per-component managed folder is bound out of band; name it and print
        # the revoke command instead of implying a full revoke.
        self.seed_component("orders")  # claimed by dev@x.com
        self.revoke_ledger_member.return_value = 2
        out = await tools.remove_workspace_member("dev@x.com")
        self.assertIn("removed from the application team", out)
        self.assertIn("component folder grant(s) the server cannot revoke (orders)", out)
        self.assertIn("gcloud storage managed-folders remove-iam-policy-binding", out)
        self.assertIn("workloads/orders/", out)

    async def test_a_dual_listed_admin_is_refused_and_never_revoked(self):
        # One email under both a team and admins, stored team-first: the admin
        # guard must still fire, or the revoke would strip roles/storage.admin.
        self.bucket.blob("workspace_registry.yaml").upload_from_string(json.dumps({
            "workspace_name": "ws", "gcp_project": "proj", "ledger_bucket": "gs://b",
            "roles": {"platform_engineers": ["ada@x.com"], "developers": [],
                      "admins": ["ada@x.com"]}}))
        out = await tools.remove_workspace_member("ada@x.com")
        self.assertIn("ERROR: ada@x.com is an admin", out)
        out = await tools.add_workspace_member("ada@x.com", "application")
        self.assertIn("ERROR: ada@x.com is an admin", out)
        self.revoke_ledger_member.assert_not_called()

    async def test_remove_refuses_an_admin(self):
        out = await tools.remove_workspace_member("ada@x.com")
        self.assertIn("ERROR: ada@x.com is an admin", out)
        self.assertEqual(self.registry_roles()["admins"], ["ada@x.com"])

    # -- reset_migration ----------------------------------------------------

    @patch.object(dispatch, "run_elicitation", approve)
    async def test_reset_migration_wipes_and_reseeds_keeping_the_registry(self):
        self.seed_component("orders")
        self.bucket.blob("exports.json").upload_from_string("{}")
        self.bucket.blob("platform/discovery/inventory.json").upload_from_string("{}")

        out = await tools.reset_migration()

        self.assertIn("Migration reset. 6 object(s) deleted, including 1 component(s)", out)
        self.assertIn("deletes 6 object(s)", approve.prompts[0])
        self.assertEqual(self.registry_roles(), REGISTRY["roles"])
        state, _ = ledger_admin.read_platform_state(self.bucket)
        self.assertEqual(state["current_state"], self.platform_dag["start_state"])
        self.assertEqual(state["variables"], {})
        self.assertEqual(ledger_admin.list_components(self.bucket), [])
        self.assertEqual(ledger_admin.read_platform_graph_version(self.bucket),
                         str(self.platform_dag["version"]))

    @patch.object(dispatch, "run_elicitation", decline)
    async def test_reset_migration_declined_changes_nothing(self):
        self.seed_component("orders")
        out = await tools.reset_migration()
        self.assertEqual(out, "Reset declined; nothing was changed.")
        self.assertEqual(ledger_admin.list_components(self.bucket), ["orders"])
        self.assertEqual(ledger_admin.read_platform_state(self.bucket)[0]["current_state"],
                         "STATE_DISCOVERY")

    # -- upgrade_ledger_dags ------------------------------------------------

    @patch.object(dispatch, "run_elicitation", approve)
    async def test_upgrade_replaces_an_older_platform_graph_and_component_copies(self):
        old = dict(self.platform_dag)
        old["version"] = "2.7"
        self.bucket.blob("platform_dag.json").upload_from_string(json.dumps(old))
        self.seed_component("orders", version="0.3", state="STATE_WKLD_PLAN")
        self.seed_component("billing")

        out = await tools.upgrade_ledger_dags()

        self.assertIn(f"Platform graph: v2.7 -> v{self.platform_dag['version']}.", out)
        self.assertIn("orders: upgraded", out)
        self.assertIn(f"billing: unchanged — already v{self.developer_dag['version']}", out)
        # The preview the admin confirmed was read BEFORE anything was written.
        self.assertIn("v2.7 -> v", approve.prompts[0])
        self.assertEqual(ledger_admin.read_platform_graph_version(self.bucket),
                         str(self.platform_dag["version"]))
        _, _, copy_dag = ledger_admin.read_component(self.bucket, "orders")
        self.assertEqual(copy_dag["version"], self.developer_dag["version"])

    @patch.object(dispatch, "run_elicitation", approve)
    async def test_upgrade_warns_when_the_platform_state_is_not_in_the_new_graph(self):
        old = dict(self.platform_dag)
        old["version"] = "2.7"
        self.bucket.blob("platform_dag.json").upload_from_string(json.dumps(old))
        self.bucket.blob("platform/onboarding/state.json").upload_from_string(json.dumps({
            "current_state": "STATE_GONE", "history": [], "variables": {}}))
        out = await tools.upgrade_ledger_dags()
        self.assertIn("WARNING: the platform state STATE_GONE does not exist", out)
        self.assertIn('reset_dag_state("platform")', out)
        # The stranding is surfaced in the confirmation, before the overwrite.
        self.assertIn("STATE_GONE does not exist", approve.prompts[0])

    @patch.object(dispatch, "run_elicitation", approve)
    async def test_upgrade_at_the_bundled_version_is_a_no_op(self):
        out = await tools.upgrade_ledger_dags()
        self.assertIn(f"Nothing to upgrade: the platform graph is already v{self.platform_dag['version']}",
                      out)
        self.assertIn("no components are claimed", out)
        self.assertEqual(approve.prompts, [])  # nothing to confirm — no elicitation raised

    @patch.object(dispatch, "run_elicitation", decline)
    async def test_upgrade_declined_writes_nothing(self):
        old = dict(self.platform_dag)
        old["version"] = "2.7"
        self.bucket.blob("platform_dag.json").upload_from_string(json.dumps(old))
        out = await tools.upgrade_ledger_dags()
        self.assertEqual(out, "Upgrade declined; nothing was changed.")
        self.assertEqual(ledger_admin.read_platform_graph_version(self.bucket), "2.7")

    # -- reset_dag_state ----------------------------------------------------

    @patch.object(dispatch, "run_elicitation", approve)
    async def test_reset_platform_rewinds_to_the_ledger_graphs_start(self):
        out = await tools.reset_dag_state("platform")
        start = self.platform_dag["start_state"]
        self.assertEqual(out, f"The platform onboarding graph was rewound from STATE_DISCOVERY to {start}.")
        self.assertIn(f"from STATE_DISCOVERY to its start state {start}", approve.prompts[0])
        state, _ = ledger_admin.read_platform_state(self.bucket)
        self.assertEqual(state["current_state"], start)
        self.assertEqual(state["variables"], {})
        self.assertIn("by ada@x.com", state["history"][-1])

    @patch.object(dispatch, "run_elicitation", approve)
    async def test_reset_component_keeps_the_claim(self):
        self.seed_component("orders", state="STATE_WKLD_TRANSLATE")
        out = await tools.reset_dag_state("orders")
        self.assertIn("component 'orders' graph was rewound from STATE_WKLD_TRANSLATE", out)
        self.assertIn("(the claim is kept)", out)
        state, _, _ = ledger_admin.read_component(self.bucket, "orders")
        self.assertEqual(state["current_state"], self.developer_dag["start_state"])
        self.assertEqual(state["variables"]["claim"]["claimant"], "dev@x.com")
        self.assertNotIn("workload_scope", state["variables"])

    @patch.object(dispatch, "run_elicitation", decline)
    async def test_reset_declined_changes_nothing(self):
        out = await tools.reset_dag_state("platform")
        self.assertEqual(out, "Reset declined; nothing was changed.")
        self.assertEqual(ledger_admin.read_platform_state(self.bucket)[0]["current_state"],
                         "STATE_DISCOVERY")

    async def test_reset_names_the_known_components_on_a_bad_target(self):
        self.seed_component("orders")
        out = await tools.reset_dag_state("ghost")
        self.assertIn("ERROR: component 'ghost' is not initialized", out)
        self.assertIn("Known components: orders", out)
        out = await tools.reset_dag_state("Not A Slug")
        self.assertIn('target must be "platform" or a component id', out)

    async def test_reset_of_a_pristine_graph_asks_nothing(self):
        self.bucket.blob("platform/onboarding/state.json").upload_from_string(json.dumps({
            "current_state": self.platform_dag["start_state"], "history": [], "variables": {}}))
        with patch.object(dispatch, "run_elicitation", approve):
            out = await tools.reset_dag_state("platform")
        self.assertIn("already at its start state", out)
        self.assertEqual(approve.prompts, [])

    async def test_reset_of_a_freshly_joined_component_asks_nothing(self):
        # A just-joined component sits at the start state carrying only its
        # component id and claim — the kept keys, not work to unwind.
        start = self.developer_dag["start_state"]
        self.bucket.blob("workloads/orders/dag.json").upload_from_string(json.dumps(self.developer_dag))
        self.bucket.blob("workloads/orders/state.json").upload_from_string(json.dumps({
            "current_state": start, "history": [],
            "variables": {"component": "orders",
                          "claim": {"claimant": "dev@x.com", "claimed_at": "T"}}}))
        with patch.object(dispatch, "run_elicitation", approve):
            out = await tools.reset_dag_state("orders")
        self.assertIn("already at its start state", out)
        self.assertEqual(approve.prompts, [])


if __name__ == "__main__":
    unittest.main()
