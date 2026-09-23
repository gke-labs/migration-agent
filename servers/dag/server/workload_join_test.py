"""Unit tests for the pure developer join/claim core (server/workload_join.py).

Pure logic only: slug validation, version comparison, the upgrade mapping,
the claim decision table, and the reported admin binding step. No GCS.
"""

import unittest
from unittest import mock

from server import workload_join as wj


DAG_V01 = {
    "version": "0.1",
    "start_state": "STATE_WKLD_SCOPE",
    "states": {
        "STATE_WKLD_SCOPE": {},
        "STATE_WKLD_SCOPE_CONFIRM": {},
        "STATE_WKLD_AWAIT_PIPELINE": {},
    },
}


class SlugTest(unittest.TestCase):

    def test_accepts_valid_slugs(self):
        for slug in ("orders-component", "ab", "a1", "x" + "y" * 61,
                     "cart-2-component"):
            with self.subTest(slug=slug):
                self.assertIsNone(wj.validate_component_id(slug))

    def test_rejects_invalid_slugs(self):
        cases = (
            "a",                 # too short
            "x" + "y" * 63,      # too long (64)
            "1orders",           # starts with a digit
            "-orders",           # starts with a hyphen
            "orders-",           # ends with a hyphen
            "Orders",            # uppercase
            "orders.component",  # dot
            "orders/component",  # slash
            "orders_component",  # underscore
            "",
            None,
            42,
        )
        for slug in cases:
            with self.subTest(slug=slug):
                error = wj.validate_component_id(slug)
                self.assertIsNotNone(error)
                self.assertIn("RFC-1123", error)


class VersionTest(unittest.TestCase):

    def test_parses_major_minor(self):
        self.assertEqual(wj.parse_version("0.1"), (0, 1))
        self.assertEqual(wj.parse_version("0.10"), (0, 10))
        self.assertLess(wj.parse_version("0.2"), wj.parse_version("0.10"))

    def test_unparseable_degrades_to_zero(self):
        for text in ("garbage", "", None, "1.x"):
            with self.subTest(text=text):
                self.assertEqual(wj.parse_version(text), (0, 0))


class UpgradeTest(unittest.TestCase):

    def state(self, current="STATE_WKLD_AWAIT_PIPELINE"):
        return {"current_state": current, "history": [], "variables": {}}

    def test_same_version_is_unchanged(self):
        state = self.state()
        new, upgraded, notes = wj.upgrade_component_state(
            DAG_V01, {"version": "0.1"}, state)
        self.assertIs(new, state)
        self.assertFalse(upgraded)
        self.assertEqual(notes, [])

    def test_newer_copy_is_unchanged(self):
        _, upgraded, _ = wj.upgrade_component_state(
            DAG_V01, {"version": "0.2"}, self.state())
        self.assertFalse(upgraded)

    def test_older_copy_maps_unnamed_states_to_themselves(self):
        state = self.state("STATE_WKLD_AWAIT_PIPELINE")
        new, upgraded, _ = wj.upgrade_component_state(
            DAG_V01, {"version": "0.0"}, state)
        self.assertTrue(upgraded)
        self.assertEqual(new["current_state"], "STATE_WKLD_AWAIT_PIPELINE")
        self.assertIn("v0.0 -> v0.1", new["history"][-1])
        # Pure: the input dict is untouched.
        self.assertEqual(state["history"], [])

    def test_unparseable_copy_version_upgrades_with_a_note(self):
        new, upgraded, notes = wj.upgrade_component_state(
            DAG_V01, {"version": "garbage"}, self.state())
        self.assertTrue(upgraded)
        self.assertTrue(any("unparseable" in n for n in notes))

    def test_nontrivial_mapping_moves_the_state(self):
        # A synthetic graph pins the mechanism itself; the real v0.2 entry
        # (AWAIT_PIPELINE -> PLAN) is exercised end-to-end in main_test's
        # DeveloperJoinTest upgrade cases. patch.dict overrides the real row.
        bundled = {
            "version": "0.2",
            "start_state": "STATE_WKLD_SCOPE",
            "states": {"STATE_WKLD_SCOPE": {}, "STATE_WKLD_PLAN": {}},
        }
        with mock.patch.dict(wj.STATE_MAPPINGS, {
                "0.2": {"STATE_WKLD_AWAIT_PIPELINE": "STATE_WKLD_PLAN"}}):
            new, upgraded, _ = wj.upgrade_component_state(
                bundled, {"version": "0.1"}, self.state("STATE_WKLD_AWAIT_PIPELINE"))
        self.assertTrue(upgraded)
        self.assertEqual(new["current_state"], "STATE_WKLD_PLAN")
        self.assertIn("STATE_WKLD_AWAIT_PIPELINE -> STATE_WKLD_PLAN", new["history"][-1])

    def test_mappings_compose_across_every_skipped_version(self):
        """A component parked since v0.1 rejoining a v0.3 graph must be
        walked through the v0.2 hop too — reading STATE_MAPPINGS[bundled]
        alone would leave it on a state v0.3 no longer has."""
        bundled = {"version": "0.3", "start_state": "A",
                   "states": {"A": {}, "B": {}, "C": {}}}
        with mock.patch.dict(wj.STATE_MAPPINGS,
                             {"0.2": {"A": "B"}, "0.3": {"B": "C"}}):
            new, upgraded, notes = wj.upgrade_component_state(
                bundled, {"version": "0.1"}, self.state("A"))
        self.assertTrue(upgraded)
        self.assertEqual(new["current_state"], "C")
        self.assertIn("A -> C", new["history"][-1])
        self.assertTrue(any("v0.2 -> v0.3" in n for n in notes))

    def test_real_v03_mapping_moves_parked_components_to_translate(self):
        """The declared v0.2 -> v0.3 mapping (M3): a component parked at
        STATE_WKLD_AWAIT_PIPELINE with an approved plan enters the execution
        slice at STATE_WKLD_TRANSLATE; every other state maps to itself."""
        import json
        import os
        path = os.path.join(os.path.dirname(os.path.dirname(
            os.path.abspath(__file__))), "developer_dag.json")
        with open(path) as f:
            bundled = json.load(f)
        new, upgraded, _ = wj.upgrade_component_state(
            bundled, {"version": "0.2"},
            self.state("STATE_WKLD_AWAIT_PIPELINE"))
        self.assertTrue(upgraded)
        self.assertEqual(new["current_state"], "STATE_WKLD_TRANSLATE")
        untouched, _, _ = wj.upgrade_component_state(
            bundled, {"version": "0.2"}, self.state("STATE_WKLD_PLAN"))
        self.assertEqual(untouched["current_state"], "STATE_WKLD_PLAN")

    def test_a_hop_the_component_is_already_past_is_not_replayed(self):
        bundled = {"version": "0.3", "start_state": "A",
                   "states": {"A": {}, "B": {}, "C": {}}}
        with mock.patch.dict(wj.STATE_MAPPINGS,
                             {"0.2": {"A": "B"}, "0.3": {"B": "C"}}):
            new, _, notes = wj.upgrade_component_state(
                bundled, {"version": "0.2"}, self.state("A"))
        self.assertEqual(new["current_state"], "A")  # only the v0.3 hop ran
        self.assertEqual(notes, [])

    def test_mapped_to_missing_state_is_a_hard_error(self):
        bundled = {"version": "0.2", "start_state": "S", "states": {"STATE_OTHER": {}}}
        with mock.patch.dict(wj.STATE_MAPPINGS, {"0.2": {}}):
            with self.assertRaisesRegex(ValueError, "not in the bundled graph"):
                wj.upgrade_component_state(
                    bundled, {"version": "0.1"}, self.state("STATE_WKLD_SCOPE"))

    def test_missing_mapping_entry_is_a_hard_error(self):
        bundled = {"version": "9.9", "start_state": "S", "states": {"S": {}}}
        with self.assertRaisesRegex(ValueError, "STATE_MAPPINGS"):
            wj.upgrade_component_state(bundled, {"version": "0.1"}, self.state("S"))


class ClassifyStateTest(unittest.TestCase):

    @staticmethod
    def state(name, variables=None):
        return {"current_state": name, "variables": variables or {}}

    def test_classification(self):
        self.assertEqual(
            wj.classify_state(DAG_V01, self.state("STATE_WKLD_SCOPE")), "initial")
        self.assertEqual(
            wj.classify_state(DAG_V01, self.state("STATE_WKLD_AWAIT_PIPELINE")),
            "parked")
        self.assertEqual(
            wj.classify_state(DAG_V01, self.state("STATE_WKLD_SCOPE_CONFIRM")),
            "midflight")

    def test_start_state_with_a_scope_draft_is_midflight(self):
        # The whole scope draft is iterated INSIDE the start state: once work
        # exists, a second joiner must get the refusal, not last-writer-wins.
        drafted = self.state("STATE_WKLD_SCOPE", {
            "workload_scope": {"root_dir": "", "included": ["charts/orders"],
                               "excluded": []}})
        self.assertEqual(wj.classify_state(DAG_V01, drafted), "midflight")

    def test_a_terminal_state_is_done_read_from_the_graph(self):
        """Read from type == TERMINAL, not a hardcoded name: a shipped
        component was classified "midflight" and the next developer got
        "mid-flight and claimed by <other>" over finished work."""
        dag = {"version": "0.3", "start_state": "STATE_WKLD_SCOPE",
               "states": {"STATE_WKLD_SCOPE": {},
                          "STATE_WKLD_DONE": {"type": "TERMINAL"}}}
        self.assertEqual(
            wj.classify_state(dag, self.state("STATE_WKLD_DONE")), "done")

    def test_start_state_with_empty_draft_object_stays_initial(self):
        # A falsy/absent draft is "no work": nothing is lost by a handover.
        for variables in ({}, {"workload_scope": None}, {"workload_scope": {}}):
            with self.subTest(variables=variables):
                self.assertEqual(
                    wj.classify_state(
                        DAG_V01, self.state("STATE_WKLD_SCOPE", variables)),
                    "initial")


class DecideClaimTest(unittest.TestCase):
    """The full decision table of spec v2 decision 11."""

    def test_fresh_claim_allows(self):
        self.assertEqual(
            wj.decide_claim(None, "dev-a@x.com", "initial", False)[0], "allow")

    def test_self_rejoin_allows_any_state(self):
        for state_class in ("initial", "parked", "midflight"):
            with self.subTest(state_class=state_class):
                decision, message = wj.decide_claim(
                    "dev-a@x.com", "dev-a@x.com", state_class, False)
                self.assertEqual(decision, "allow")
                self.assertEqual(message, "resumed")

    def test_initial_race_is_last_writer_wins(self):
        decision, message = wj.decide_claim(
            "dev-a@x.com", "dev-b@x.com", "initial", False)
        self.assertEqual(decision, "allow")
        self.assertIn("dev-a@x.com", message)  # the change is recorded

    def test_parked_component_hands_over(self):
        decision, message = wj.decide_claim(
            "dev-a@x.com", "dev-b@x.com", "parked", False)
        self.assertEqual(decision, "allow")
        self.assertIn("dev-a@x.com", message)

    def test_done_component_hands_over_without_a_reclaim_flag(self):
        """A shipped component is not mid-flight: the next developer joins
        to read the PR and follow up, and must not be told to reclaim."""
        decision, message = wj.decide_claim(
            "dev-a@x.com", "dev-b@x.com", "done", False)
        self.assertEqual(decision, "allow")
        self.assertIn("dev-a@x.com", message)
        self.assertNotIn("reclaim_component", message)

    def test_midflight_refusal_names_claimant_and_reclaim_path(self):
        decision, message = wj.decide_claim(
            "dev-a@x.com", "dev-b@x.com", "midflight", False)
        self.assertEqual(decision, "refuse")
        self.assertIn("dev-a@x.com", message)
        self.assertIn("reclaim_component=True", message)

    def test_reclaim_takes_over_midflight(self):
        decision, message = wj.decide_claim(
            "dev-a@x.com", "dev-b@x.com", "midflight", True)
        self.assertEqual(decision, "takeover")
        self.assertIn("Claim takeover: dev-a@x.com -> dev-b@x.com", message)


class InitialStateTest(unittest.TestCase):

    def test_seeded_state_records_the_claim(self):
        state = wj.initial_state_dict(
            DAG_V01, "orders-component", "dev-a@x.com", now_iso="T0")
        self.assertEqual(state["current_state"], "STATE_WKLD_SCOPE")
        self.assertEqual(state["variables"]["component"], "orders-component")
        self.assertEqual(state["variables"]["claim"],
                         {"claimant": "dev-a@x.com", "claimed_at": "T0"})
        self.assertIn("claimed by dev-a@x.com", state["history"][0])


class BundledVersionMappingTest(unittest.TestCase):

    def test_bundled_developer_dag_version_has_a_mapping_entry(self):
        # The declarative half of the init() start-up gate: a version bump
        # without its STATE_MAPPINGS entry must fail HERE (and at start-up),
        # not on the first re-join of an existing component post-release.
        import json
        import os
        path = os.path.join(os.path.dirname(os.path.dirname(
            os.path.abspath(__file__))), "developer_dag.json")
        with open(path) as f:
            bundled = json.load(f)
        self.assertIn(
            str(bundled["version"]), wj.STATE_MAPPINGS,
            "developer_dag.json's version must have a STATE_MAPPINGS entry "
            "(add the old-state -> new-state mapping for the bump)")


class AdminBindingStepTest(unittest.TestCase):

    def test_names_folder_member_and_registry_step(self):
        text = wj.admin_binding_step("bkt", "orders-component", "dev-a@x.com")
        self.assertIn("gs://bkt/workloads/orders-component/", text)
        self.assertIn("--member=user:dev-a@x.com", text)
        self.assertIn("roles/storage.objectAdmin", text)
        self.assertIn("register_ledger_member", text)
        self.assertIn("managed-folders create", text)


class GuardedMappingTest(unittest.TestCase):
    """The v0.4 guarded DONE -> TRANSLATE re-entry (spec v2 §2 M4)."""

    @staticmethod
    def bundled():
        import json
        import os
        path = os.path.join(os.path.dirname(os.path.dirname(
            os.path.abspath(__file__))), "developer_dag.json")
        with open(path) as f:
            return json.load(f)

    def state(self, current="STATE_WKLD_DONE"):
        return {"current_state": current, "history": [], "variables": {}}

    def upgrade(self, current, guard_facts):
        return wj.upgrade_component_state(
            self.bundled(), {"version": "0.3"}, self.state(current),
            guard_facts)

    def test_done_with_parked_units_reenters_translate(self):
        new, upgraded, notes = self.upgrade(
            "STATE_WKLD_DONE", {"has_unparkable_units": True})
        self.assertTrue(upgraded)
        self.assertEqual(new["current_state"], "STATE_WKLD_TRANSLATE")
        self.assertIn("STATE_WKLD_DONE -> STATE_WKLD_TRANSLATE",
                      new["history"][-1])
        self.assertEqual(notes, [])

    def test_done_without_parked_units_stays_done(self):
        new, upgraded, notes = self.upgrade(
            "STATE_WKLD_DONE", {"has_unparkable_units": False})
        self.assertTrue(upgraded)  # the version bump still applies
        self.assertEqual(new["current_state"], "STATE_WKLD_DONE")
        self.assertEqual(notes, [])

    def test_unknowable_guard_keeps_state_with_a_warning(self):
        for facts in (None, {}, {"has_unparkable_units": None}):
            with self.subTest(facts=facts):
                new, _, notes = self.upgrade("STATE_WKLD_DONE", facts)
                self.assertEqual(new["current_state"], "STATE_WKLD_DONE")
                self.assertTrue(any("could not be evaluated" in n
                                    for n in notes))

    def test_every_non_done_state_maps_to_itself(self):
        for state in ("STATE_WKLD_SCOPE", "STATE_WKLD_PLAN",
                      "STATE_WKLD_TRANSLATE", "STATE_WKLD_REVIEW",
                      "STATE_WKLD_VALIDATE", "STATE_WKLD_APPROVED",
                      "STATE_WKLD_SUBMIT_PR"):
            with self.subTest(state=state):
                new, _, _ = self.upgrade(state, {"has_unparkable_units": True})
                self.assertEqual(new["current_state"], state)

    def test_unknown_guard_token_is_a_refusal_not_a_silent_pass(self):
        bundled = self.bundled()
        bundled["states"]["STATE_WKLD_DONE"]  # sanity: the state exists
        with mock.patch.dict(
                wj.STATE_MAPPINGS,
                {"0.4": {"STATE_WKLD_DONE": {"to": "STATE_WKLD_TRANSLATE",
                                             "when": "has_typo_units"}}},
                clear=False):
            with self.assertRaises(ValueError) as ctx:
                wj.upgrade_component_state(
                    bundled, {"version": "0.3"}, self.state(),
                    {"has_typo_units": True})
        self.assertIn("has_typo_units", str(ctx.exception))

    def test_guard_needs_both_a_parked_unit_and_a_published_gateway(self):
        parked = {"units": [{"status": "done"}, {"status": "parked"}]}
        gw = {"gateway": {"name": "g", "namespace": "gateway-infra"}}
        self.assertIs(wj.has_unparkable_units(parked, gw), True)
        # Parked, but the platform Gateway has not published yet.
        for blind in (None, {}, {"gateway": None},
                      {"gateway": {"name": "g"}},
                      {"gateway": {"name": "g", "namespace": "  "}}):
            with self.subTest(exports=blind):
                self.assertIs(wj.has_unparkable_units(parked, blind), False)
        # Published, but nothing is parked.
        self.assertIs(wj.has_unparkable_units(
            {"units": [{"status": "done"}, {"status": "skipped"}]}, gw), False)
        for malformed in (None, "x", {}, {"units": "x"}, {"units": None}):
            with self.subTest(malformed=malformed):
                self.assertIsNone(wj.has_unparkable_units(malformed, gw))

    def test_needs_guard_facts_only_when_a_guarded_hop_is_consulted(self):
        bundled = self.bundled()
        done = self.state("STATE_WKLD_DONE")
        self.assertTrue(wj.needs_guard_facts(
            bundled, {"version": "0.3"}, done))
        # A guarded entry is STANDING: an already-0.4 component is re-checked
        # on every join, so the gateway can publish long after the upgrade.
        self.assertTrue(wj.needs_guard_facts(
            bundled, {"version": "0.4"}, done))
        # A state with no guarded entry needs no plan read.
        self.assertFalse(wj.needs_guard_facts(
            bundled, {"version": "0.3"}, self.state("STATE_WKLD_REVIEW")))
        self.assertFalse(wj.needs_guard_facts(
            bundled, {"version": "0.4"}, self.state("STATE_WKLD_REVIEW")))

    def test_standing_guard_reenters_a_component_already_at_0_4(self):
        bundled = self.bundled()
        new, moved, notes = wj.apply_standing_guards(
            bundled, self.state(), {"has_unparkable_units": True})
        self.assertTrue(moved)
        self.assertEqual(new["current_state"], "STATE_WKLD_TRANSLATE")
        self.assertIn("Re-entry: guarded mapping", new["history"][-1])
        self.assertTrue(any("re-entered STATE_WKLD_DONE -> "
                            "STATE_WKLD_TRANSLATE" in n for n in notes))

    def test_standing_guard_leaves_a_still_blind_component_alone(self):
        bundled = self.bundled()
        new, moved, notes = wj.apply_standing_guards(
            bundled, self.state(), {"has_unparkable_units": False})
        self.assertFalse(moved)
        self.assertEqual(new["current_state"], "STATE_WKLD_DONE")
        self.assertEqual(new["history"], [])
        self.assertEqual(notes, [])


if __name__ == "__main__":
    unittest.main()
