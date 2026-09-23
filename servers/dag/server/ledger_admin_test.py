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

"""ledger_admin against the in-memory GCS fake: real generations, real
preconditions, so the concurrency refusals are exercised rather than mocked."""

import json
import unittest

from google.api_core import exceptions

import fake_gcs
from server import ledger_admin
from server.ledger_admin import LedgerAdminError

REGISTRY = {
    "workspace_name": "ws", "gcp_project": "proj", "ledger_bucket": "gs://b",
    "roles": {"admins": ["ada@x.com"], "platform_engineers": ["pat@x.com"],
              "developers": ["dev@x.com"]},
}


def graph(version, start="STATE_A", extra=()):
    states = {start: {"type": "AGENT_TASK", "transitions": {"on_tool_call_received": "STATE_END"}},
              "STATE_END": {"type": "TERMINAL", "status": "SUCCESS"}}
    for name in extra:
        states[name] = {"type": "AGENT_TASK", "transitions": {"on_tool_call_received": "STATE_END"}}
    return {"name": "g", "version": version, "start_state": start, "states": states}


class LedgerAdminTest(unittest.TestCase):

    def setUp(self):
        self.client = fake_gcs.FakeStorageClient()
        self.bucket = self.client.bucket("b")
        self.bucket.blob("workspace_registry.yaml").upload_from_string(json.dumps(REGISTRY))

    def registry(self):
        return json.loads(self.bucket.blob("workspace_registry.yaml").download_as_text())

    # -- registry ---------------------------------------------------------

    def test_move_member_adds_a_new_email_to_the_team(self):
        outcome, roles = ledger_admin.move_member(self.bucket, "new@x.com", "developers")
        self.assertEqual(outcome, "added")
        self.assertEqual(self.registry()["roles"]["developers"], ["dev@x.com", "new@x.com"])

    def test_move_member_moves_between_the_two_teams_in_one_write(self):
        outcome, _ = ledger_admin.move_member(self.bucket, "pat@x.com", "developers")
        self.assertEqual(outcome, "moved")
        roles = self.registry()["roles"]
        self.assertEqual(roles["platform_engineers"], [])
        self.assertEqual(roles["developers"], ["dev@x.com", "pat@x.com"])

    def test_move_member_is_a_no_op_when_already_on_the_team(self):
        before = self.bucket.blob("workspace_registry.yaml")
        before.reload()
        outcome, _ = ledger_admin.move_member(self.bucket, "dev@x.com", "developers")
        after = self.bucket.blob("workspace_registry.yaml")
        after.reload()
        self.assertEqual(outcome, "unchanged")
        self.assertEqual(before.generation, after.generation, "nothing should be written")

    def test_member_role_reports_admins_first_for_a_dual_listed_email(self):
        # Roles stored team-first, admin last: a naive first-match would call
        # the email a platform engineer and let a revoke touch its admin grant.
        registry = {"roles": {"platform_engineers": ["ada@x.com"], "developers": [],
                              "admins": ["ada@x.com"]}}
        self.assertEqual(ledger_admin.member_role(registry, "ada@x.com"), "admins")

    def test_a_dual_listed_admin_cannot_be_moved_or_removed(self):
        self.bucket.blob("workspace_registry.yaml").upload_from_string(json.dumps({
            "roles": {"platform_engineers": ["ada@x.com"], "developers": [],
                      "admins": ["ada@x.com"]}}))
        with self.assertRaises(LedgerAdminError):
            ledger_admin.move_member(self.bucket, "ada@x.com", "developers")
        with self.assertRaises(LedgerAdminError):
            ledger_admin.remove_member(self.bucket, "ada@x.com")

    def test_admins_cannot_be_moved_or_removed(self):
        with self.assertRaises(LedgerAdminError):
            ledger_admin.move_member(self.bucket, "ada@x.com", "developers")
        with self.assertRaises(LedgerAdminError):
            ledger_admin.remove_member(self.bucket, "ada@x.com")
        self.assertEqual(self.registry()["roles"]["admins"], ["ada@x.com"])

    def test_only_the_two_team_roles_are_writable(self):
        with self.assertRaises(ValueError):
            ledger_admin.move_member(self.bucket, "x@x.com", "admins")

    def test_remove_member_names_the_team_it_left(self):
        role, roles = ledger_admin.remove_member(self.bucket, "pat@x.com")
        self.assertEqual(role, "platform_engineers")
        self.assertEqual(self.registry()["roles"]["platform_engineers"], [])

    def test_remove_member_refuses_an_unknown_email(self):
        with self.assertRaises(LedgerAdminError) as caught:
            ledger_admin.remove_member(self.bucket, "ghost@x.com")
        self.assertIn("not registered", str(caught.exception))

    def test_a_concurrent_registry_write_is_refused_not_clobbered(self):
        registry, generation = ledger_admin.load_registry(self.bucket)
        # Someone else writes between our read and our write.
        registry["roles"]["developers"].append("racer@x.com")
        ledger_admin.save_registry(self.bucket, registry, generation)
        with self.assertRaises(LedgerAdminError):
            ledger_admin.save_registry(self.bucket, {"roles": {}}, generation)
        self.assertIn("racer@x.com", self.registry()["roles"]["developers"])

    # -- platform graph ---------------------------------------------------

    def test_install_creates_then_keeps_a_same_version_graph(self):
        replaced, before, after = ledger_admin.install_platform_graph(
            self.bucket, json.dumps(graph("2.8")))
        self.assertEqual((replaced, before, after), (False, None, "2.8"))
        replaced, before, after = ledger_admin.install_platform_graph(
            self.bucket, json.dumps(graph("2.8")))
        self.assertEqual((replaced, before, after), (False, "2.8", "2.8"))

    def test_install_replaces_a_different_version(self):
        ledger_admin.install_platform_graph(self.bucket, json.dumps(graph("2.7")))
        replaced, before, after = ledger_admin.install_platform_graph(
            self.bucket, json.dumps(graph("2.8")))
        self.assertEqual((replaced, before, after), (True, "2.7", "2.8"))
        self.assertEqual(ledger_admin.read_platform_graph_version(self.bucket), "2.8")

    def test_seed_is_create_only(self):
        self.assertTrue(ledger_admin.seed_platform_state(self.bucket, graph("1")))
        state, _ = ledger_admin.read_platform_state(self.bucket)
        state["current_state"] = "STATE_END"
        self.bucket.blob(ledger_admin.PLATFORM_STATE_OBJECT).upload_from_string(json.dumps(state))
        self.assertFalse(ledger_admin.seed_platform_state(self.bucket, graph("1")))
        self.assertEqual(ledger_admin.read_platform_state(self.bucket)[0]["current_state"], "STATE_END")

    def test_reset_platform_rewinds_clears_variables_and_keeps_history(self):
        dag = graph("1")
        ledger_admin.seed_platform_state(self.bucket, dag)
        self.bucket.blob(ledger_admin.PLATFORM_STATE_OBJECT).upload_from_string(json.dumps({
            "current_state": "STATE_END", "history": ["did things"],
            "variables": {"source_repo_url": "sso://x", "blockers": [1]}}))
        previous = ledger_admin.reset_platform_state(self.bucket, dag, "ada@x.com")
        state, _ = ledger_admin.read_platform_state(self.bucket)
        self.assertEqual(previous, "STATE_END")
        self.assertEqual(state["current_state"], "STATE_A")
        self.assertEqual(state["variables"], {})
        self.assertEqual(state["history"][0], "did things")
        self.assertIn("Reset to STATE_A from STATE_END by ada@x.com", state["history"][1])

    # -- components -------------------------------------------------------

    def seed_component(self, component, version="0.3", state="STATE_A", variables=None):
        self.bucket.blob(f"workloads/{component}/dag.json").upload_from_string(
            json.dumps(graph(version)))
        self.bucket.blob(f"workloads/{component}/state.json").upload_from_string(json.dumps({
            "current_state": state, "history": [],
            "variables": variables if variables is not None else {
                "component": component,
                "claim": {"claimant": "dev@x.com", "claimed_at": "T"},
                "workload_scope": {"include": ["k8s/"]}}}))

    def test_list_components_reads_only_state_objects(self):
        self.seed_component("orders")
        self.seed_component("billing")
        self.bucket.blob("workloads/stray/plan.json").upload_from_string("{}")
        self.bucket.blob("workloads/state.json").upload_from_string("{}")
        self.assertEqual(ledger_admin.list_components(self.bucket), ["billing", "orders"])

    def test_reset_component_keeps_the_claim_and_drops_the_work(self):
        self.seed_component("orders", state="STATE_END")
        previous = ledger_admin.reset_component_state(self.bucket, "orders", graph("0.3"), "ada@x.com")
        state, _, _ = ledger_admin.read_component(self.bucket, "orders")
        self.assertEqual(previous, "STATE_END")
        self.assertEqual(state["current_state"], "STATE_A")
        self.assertEqual(set(state["variables"]), {"component", "claim"})
        self.assertEqual(state["variables"]["claim"]["claimant"], "dev@x.com")

    def test_reset_component_refuses_an_unknown_component(self):
        with self.assertRaises(LedgerAdminError):
            ledger_admin.reset_component_state(self.bucket, "ghost", graph("0.3"), "ada@x.com")

    def test_upgrade_component_maps_the_state_and_rewrites_the_copy(self):
        # The real v0.2 mapping: AWAIT_PIPELINE -> PLAN.
        self.bucket.blob("workloads/orders/dag.json").upload_from_string(
            json.dumps(graph("0.1", start="STATE_WKLD_AWAIT_PIPELINE")))
        self.bucket.blob("workloads/orders/state.json").upload_from_string(json.dumps({
            "current_state": "STATE_WKLD_AWAIT_PIPELINE", "history": [], "variables": {}}))
        bundled = graph("0.2", start="STATE_WKLD_PLAN")
        upgraded, notes = ledger_admin.upgrade_component(
            self.bucket, "orders", bundled, json.dumps(bundled))
        self.assertTrue(upgraded)
        state, _, copy_dag = ledger_admin.read_component(self.bucket, "orders")
        self.assertEqual(state["current_state"], "STATE_WKLD_PLAN")
        self.assertEqual(copy_dag["version"], "0.2")
        self.assertTrue(any("v0.1 -> v0.2" in n for n in notes))

    def test_upgrade_component_is_a_no_op_at_the_bundled_version(self):
        self.seed_component("orders", version="0.3")
        bundled = graph("0.3")
        upgraded, _ = ledger_admin.upgrade_component(self.bucket, "orders", bundled, json.dumps(bundled))
        self.assertFalse(upgraded)

    def test_upgrade_component_re_enters_a_standing_guard_without_a_version_hop(self):
        # The real v0.4 standing rule: DONE -> TRANSLATE when a parked unit
        # has a published attach point. The copy is already v0.4, so there is
        # no version hop; this only fires because upgrade_component re-evaluates
        # the guard exactly as a developer join does.
        bundled = graph("0.4", start="STATE_WKLD_PLAN",
                        extra=("STATE_WKLD_DONE", "STATE_WKLD_TRANSLATE"))
        self.bucket.blob("workloads/orders/dag.json").upload_from_string(json.dumps(bundled))
        self.bucket.blob("workloads/orders/state.json").upload_from_string(json.dumps({
            "current_state": "STATE_WKLD_DONE", "history": [], "variables": {}}))
        self.bucket.blob("workloads/orders/plan.json").upload_from_string(
            json.dumps({"units": [{"status": "parked"}]}))
        self.bucket.blob("exports.json").upload_from_string(
            json.dumps({"gateway": {"name": "gw", "namespace": "ns"}}))

        changed, notes = ledger_admin.upgrade_component(
            self.bucket, "orders", bundled, json.dumps(bundled))

        self.assertTrue(changed)
        state, _, copy_dag = ledger_admin.read_component(self.bucket, "orders")
        self.assertEqual(state["current_state"], "STATE_WKLD_TRANSLATE")
        self.assertEqual(copy_dag["version"], "0.4")  # no hop: the copy is untouched
        self.assertTrue(any("re-entered" in n for n in notes))

    def test_upgrade_component_at_the_bundled_version_leaves_a_dormant_guard_alone(self):
        # Same v0.4 DONE state, but no plan -> the guard is unknowable, so the
        # standing rule keeps the state and reports nothing changed.
        bundled = graph("0.4", start="STATE_WKLD_PLAN",
                        extra=("STATE_WKLD_DONE", "STATE_WKLD_TRANSLATE"))
        self.bucket.blob("workloads/orders/dag.json").upload_from_string(json.dumps(bundled))
        self.bucket.blob("workloads/orders/state.json").upload_from_string(json.dumps({
            "current_state": "STATE_WKLD_DONE", "history": [], "variables": {}}))

        changed, _ = ledger_admin.upgrade_component(
            self.bucket, "orders", bundled, json.dumps(bundled))

        self.assertFalse(changed)
        state, _, _ = ledger_admin.read_component(self.bucket, "orders")
        self.assertEqual(state["current_state"], "STATE_WKLD_DONE")

    # -- the whole migration ---------------------------------------------

    def test_wipe_deletes_everything_but_the_registry(self):
        ledger_admin.install_platform_graph(self.bucket, json.dumps(graph("2.8")))
        ledger_admin.seed_platform_state(self.bucket, graph("2.8"))
        self.seed_component("orders")
        self.bucket.blob("exports.json").upload_from_string("{}")
        self.bucket.blob("platform/discovery/inventory.json").upload_from_string("{}")

        deleted = ledger_admin.wipe_migration(self.bucket)

        self.assertNotIn("workspace_registry.yaml", deleted)
        self.assertEqual(sorted(b.name for b in self.bucket.list_blobs()), ["workspace_registry.yaml"])
        self.assertEqual(len(deleted), 6)
        self.assertEqual(self.registry(), REGISTRY)


if __name__ == "__main__":
    unittest.main()
