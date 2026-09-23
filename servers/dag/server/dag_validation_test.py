import unittest
import copy
import json
import os
import re

from server import dag_validation
from server.dag_validation import (
    DagValidationError,
    check_schema,
    check_structure,
    report_unconverted,
    validate_dag,
)

DAG_DIR = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), ".."))


def valid_dag():
    """A minimal graph that passes both layers against the real repository.

    Every case below starts from this and breaks exactly one thing, so a
    failure names the rule that caught it rather than a pile of unrelated
    errors. It points at the assessment phase because that phase is fully
    converted: the step folder, instructions and knowledge doc all exist.
    (It used to point at discovery, whose knowledge doc was deleted when the
    map-reduce pipeline replaced it.)
    """
    return {
        "name": "test-graph",
        "version": "1.0",
        "start_state": "STATE_START",
        "states": {
            "STATE_START": {
                "type": "AGENT_TASK",
                "phase": "assessment",
                "step": "servers/phases/assessment/assessment_review_1",
                "instructions": "servers/phases/assessment/assessment_review_1/instructions.md",
                "knowledge": ["migration-assessment.md"],
                "expected_tool_call": "submit_assessment",
                "transitions": {"on_tool_call_received": "STATE_END"},
            },
            "STATE_END": {"type": "TERMINAL", "status": "SUCCESS"},
        },
    }


class SchemaLayerTest(unittest.TestCase):
    """Document-shape rules enforced by dag_schema.json."""

    def assert_rejected(self, dag, *expected_fragments):
        errors = check_schema(dag, "test.json")
        self.assertTrue(errors, "expected the schema to reject this document")
        joined = " | ".join(errors)
        for fragment in expected_fragments:
            self.assertIn(fragment, joined)

    def test_baseline_is_accepted(self):
        # Guards the negative cases below: if the baseline itself were invalid
        # every one of them would pass for the wrong reason.
        self.assertEqual(check_schema(valid_dag(), "test.json"), [])

    def test_unknown_state_type_rejected(self):
        dag = valid_dag()
        dag["states"]["STATE_START"]["type"] = "AGENT_TAKS"
        self.assert_rejected(dag, "STATE_START")

    def test_missing_root_key_rejected(self):
        dag = valid_dag()
        del dag["start_state"]
        self.assert_rejected(dag, "start_state")

    def test_unknown_root_key_rejected(self):
        dag = valid_dag()
        dag["retries"] = 3
        self.assert_rejected(dag, "retries")

    def test_lowercase_state_id_rejected(self):
        dag = valid_dag()
        dag["states"]["state_start"] = dag["states"].pop("STATE_START")
        self.assert_rejected(dag, "state_start")

    def test_transition_key_must_use_on_prefix(self):
        dag = valid_dag()
        dag["states"]["STATE_START"]["transitions"] = {"tool_call_received": "STATE_END"}
        self.assert_rejected(dag, "STATE_START")

    def test_action_specific_transition_keys_accepted(self):
        # The open-ended on_* pattern exists because outcome names are
        # action-specific. An earlier schema enumerated them and would have
        # rejected the graph the server actually ships.
        dag = valid_dag()
        dag["states"]["STATE_START"]["transitions"] = {
            "on_pending": "STATE_START",
            "on_ssm_creation_required": "STATE_END",
            "on_rediscover": "STATE_START",
        }
        self.assertEqual(check_schema(dag, "test.json"), [])

    def test_empty_transitions_rejected(self):
        dag = valid_dag()
        dag["states"]["STATE_START"]["transitions"] = {}
        self.assert_rejected(dag, "STATE_START")

    def test_knowledge_with_path_separator_rejected(self):
        dag = valid_dag()
        dag["states"]["STATE_START"]["knowledge"] = ["../../../etc/passwd"]
        self.assert_rejected(dag, "knowledge")

    def test_knowledge_must_be_an_array(self):
        dag = valid_dag()
        dag["states"]["STATE_START"]["knowledge"] = "migration-assessment.md"
        self.assert_rejected(dag, "knowledge")

    def test_duplicate_knowledge_entries_rejected(self):
        dag = valid_dag()
        dag["states"]["STATE_START"]["knowledge"] = ["migration-assessment.md", "migration-assessment.md"]
        self.assert_rejected(dag, "knowledge")

    def test_knowledge_without_phase_rejected(self):
        # knowledge names are resolved within the phase directory, so one
        # without the other cannot be looked up at all.
        dag = valid_dag()
        state = dag["states"]["STATE_START"]
        del state["phase"]
        del state["step"]
        del state["instructions"]
        self.assert_rejected(dag, "phase")

    def test_instructions_outside_phases_tree_rejected(self):
        dag = valid_dag()
        dag["states"]["STATE_START"]["instructions"] = "skills/dag_executor/SKILL.md"
        self.assert_rejected(dag, "instructions")

    def test_hitl_requires_both_approve_and_reject(self):
        dag = valid_dag()
        dag["states"]["STATE_START"] = {
            "type": "HITL_ELICITATION",
            "prompt_template": "Approve?",
            "transitions": {"on_approve": "STATE_END"},
        }
        self.assert_rejected(dag, "on_reject")

    def test_internal_task_requires_both_success_and_failure(self):
        dag = valid_dag()
        dag["states"]["STATE_START"] = {
            "type": "INTERNAL_TASK_SERVER_MUTATION",
            "action": "provision_ledger",
            "transitions": {"on_success": "STATE_END"},
        }
        self.assert_rejected(dag, "on_failure")

    def test_terminal_requires_status(self):
        dag = valid_dag()
        del dag["states"]["STATE_END"]["status"]
        self.assert_rejected(dag, "status")

    def test_agent_task_rejects_unknown_field(self):
        dag = valid_dag()
        dag["states"]["STATE_START"]["action"] = "provision_ledger"
        self.assert_rejected(dag, "action")

    def test_agent_task_accepts_prompt_template(self):
        # A step whose tool raises the approval elicitation in-call keeps its
        # prompt on the state, like an HITL node does (submit_assessment).
        dag = valid_dag()
        dag["states"]["STATE_START"]["prompt_template"] = "Approve?"
        self.assertEqual(check_schema(dag, "test.json"), [])


class StructuralLayerTest(unittest.TestCase):
    """Reference and on-disk checks the schema cannot express."""

    def assert_rejected(self, dag, *expected_fragments):
        errors = check_structure(dag, "test.json")
        self.assertTrue(errors, "expected the structural checks to reject this document")
        joined = " | ".join(errors)
        for fragment in expected_fragments:
            self.assertIn(fragment, joined)

    def test_baseline_is_accepted(self):
        self.assertEqual(check_structure(valid_dag(), "test.json"), [])

    def test_dangling_transition_target_rejected(self):
        dag = valid_dag()
        dag["states"]["STATE_START"]["transitions"]["on_tool_call_received"] = "STATE_TYPO"
        self.assert_rejected(dag, "STATE_TYPO", "not a defined state")

    def test_undefined_start_state_rejected(self):
        dag = valid_dag()
        dag["start_state"] = "STATE_NOWHERE"
        self.assert_rejected(dag, "STATE_NOWHERE", "not a defined state")

    def test_phase_without_package_directory_rejected(self):
        # A state can name a phase and carry no content fields at all, so this
        # is checked in its own right rather than incidentally via a file path.
        dag = valid_dag()
        state = dag["states"]["STATE_START"]
        state["phase"] = "nosuchphase"
        del state["step"]
        del state["instructions"]
        del state["knowledge"]
        self.assert_rejected(dag, "nosuchphase", "no package directory")

    def test_missing_instructions_file_rejected(self):
        dag = valid_dag()
        state = dag["states"]["STATE_START"]
        state["step"] = "servers/phases/assessment/assessment_absent_9"
        state["instructions"] = "servers/phases/assessment/assessment_absent_9/instructions.md"
        self.assert_rejected(dag, "not found")

    def test_missing_knowledge_doc_rejected(self):
        dag = valid_dag()
        dag["states"]["STATE_START"]["knowledge"] = ["no-such-doc.md"]
        self.assert_rejected(dag, "no-such-doc.md", "not found")

    def test_instructions_must_sit_in_the_declared_step_folder(self):
        dag = valid_dag()
        dag["states"]["STATE_START"]["instructions"] = (
            "servers/phases/assessment/assessment_blockers_2/instructions.md"
        )
        self.assert_rejected(dag, "does not sit in step folder")

    def test_path_escaping_the_repository_rejected(self):
        # Unreachable through the schema, whose path patterns are anchored, but
        # check_structure is also called on graphs read back from the ledger.
        dag = valid_dag()
        dag["states"]["STATE_START"]["instructions"] = "../../../../etc/passwd"
        self.assert_rejected(dag, "escapes the repository")

    def test_hitl_state_without_an_escape_edge_is_rejected(self):
        """The dispatch exhaustion arm persists off a HITL state via
        on_exhausted/on_reject; a graph without that edge would park a
        workspace ON the elicitation — a state no tool accepts."""
        dag = valid_dag()
        dag["states"]["STATE_ASK"] = {
            "type": "HITL_ELICITATION",
            "prompt_template": "Approve?",
            "transitions": {"on_approve": "STATE_END",
                            "on_reject": "STATE_ASK"},
        }
        dag["states"]["STATE_START"]["transitions"][
            "on_tool_call_received"] = "STATE_ASK"
        self.assert_rejected(dag, "STATE_ASK", "no escape edge")

    def test_hitl_state_with_a_real_escape_edge_is_accepted(self):
        dag = valid_dag()
        dag["states"]["STATE_ASK"] = {
            "type": "HITL_ELICITATION",
            "prompt_template": "Approve?",
            "transitions": {"on_approve": "STATE_END",
                            "on_reject": "STATE_END"},
        }
        dag["states"]["STATE_START"]["transitions"][
            "on_tool_call_received"] = "STATE_ASK"
        self.assertEqual(check_structure(dag, "test.json"), [])

    def test_repo_root_is_overridable(self):
        # The default resolves relative to this file; an explicit root is what
        # lets CI validate a checkout somewhere else.
        errors = check_structure(valid_dag(), "test.json", repo_root="/nonexistent")
        self.assertTrue(errors)


class ValidateDagTest(unittest.TestCase):

    def load_bundled(self, filename):
        with open(os.path.join(DAG_DIR, filename), "r") as f:
            return json.load(f)

    def test_bundled_dags_are_valid(self):
        # The same call init() makes at start-up. If this fails the server will
        # refuse to boot, so it is worth failing here first with a clear name.
        for filename in ("bootstrap_dag.json", "platform_dag.json", "developer_dag.json"):
            with self.subTest(dag=filename):
                validate_dag(self.load_bundled(filename), filename)

    def test_developer_dag_shape(self):
        # The third graph's v0.3 contract (spec v2 decision 1): the execution
        # slice with the ladder's FIRST TERMINAL, both edges declared on every
        # gate, and the deliberate SUBMIT_PR divergence (on_failure returns to
        # the ship approval, NOT to validation as the platform graph does).
        dag = self.load_bundled("developer_dag.json")
        self.assertEqual(dag["version"], "0.4")
        self.assertEqual(dag["start_state"], "STATE_WKLD_SCOPE")
        states = dag["states"]
        self.assertNotIn("STATE_WKLD_AWAIT_PIPELINE", states)
        plan_review = states["STATE_WKLD_PLAN_REVIEW"]
        self.assertEqual(plan_review["transitions"]["on_tool_call_received"],
                         "STATE_WKLD_TRANSLATE")
        translate = states["STATE_WKLD_TRANSLATE"]
        self.assertEqual(translate["expected_tool_call"],
                         "run_workload_translation")
        self.assertEqual(translate["transitions"]["on_tool_call_received"],
                         "STATE_WKLD_REVIEW")
        review = states["STATE_WKLD_REVIEW"]
        self.assertEqual(review["transitions"]["on_revise_units"],
                         "STATE_WKLD_TRANSLATE")
        self.assertEqual(review["transitions"]["on_approve"],
                         "STATE_WKLD_VALIDATE")
        validate = states["STATE_WKLD_VALIDATE"]
        self.assertEqual(validate["transitions"]["on_success"],
                         "STATE_WKLD_APPROVED")
        self.assertEqual(validate["transitions"]["on_failure"],
                         "STATE_WKLD_REVIEW")
        approved = states["STATE_WKLD_APPROVED"]
        self.assertEqual(approved["type"], "HITL_ELICITATION")
        self.assertEqual(approved["transitions"]["on_reject"],
                         "STATE_WKLD_REVIEW")
        submit = states["STATE_WKLD_SUBMIT_PR"]
        self.assertEqual(submit["type"], "INTERNAL_TASK_SERVER_MUTATION")
        self.assertEqual(submit["action"], "submit_workload_pr")
        self.assertEqual(submit["transitions"]["on_failure"],
                         "STATE_WKLD_APPROVED")
        done = states["STATE_WKLD_DONE"]
        self.assertEqual((done["type"], done["status"]),
                         ("TERMINAL", "SUCCESS"))
        for name, state in states.items():
            if state["type"] == "AGENT_TASK":
                self.assertEqual(state.get("phase"), "workload", name)
                self.assertTrue(state.get("instructions"), name)

    def test_developer_dag_terminal_is_reachable(self):
        # Walk every declared edge from the start state: the first TERMINAL
        # of the ladder must actually be reachable, not decorative.
        dag = self.load_bundled("developer_dag.json")
        seen, queue = set(), [dag["start_state"]]
        while queue:
            state = queue.pop()
            if state in seen:
                continue
            seen.add(state)
            queue.extend(
                dag["states"][state].get("transitions", {}).values())
        self.assertIn("STATE_WKLD_DONE", seen)

    def test_raises_on_invalid_dag(self):
        dag = valid_dag()
        dag["states"]["STATE_START"]["transitions"]["on_tool_call_received"] = "STATE_TYPO"
        with self.assertRaises(DagValidationError) as ctx:
            validate_dag(dag, "test.json")
        self.assertIn("STATE_TYPO", str(ctx.exception))

    def test_reports_both_layers_together(self):
        # One pass surfaces schema and structural problems at once; fixing a
        # graph should not be a game of one error per run.
        dag = valid_dag()
        dag["states"]["STATE_START"]["type"] = "AGENT_TAKS"
        dag["start_state"] = "STATE_NOWHERE"
        errors = check_schema(dag, "test.json") + check_structure(dag, "test.json")
        self.assertGreaterEqual(len(errors), 2)

    def test_unconverted_states_warn_but_do_not_raise(self):
        # Phase conversion is incomplete. Until it finishes, an AGENT_TASK with
        # no instructions falls back to the dag_executor skill and must not stop
        # the server from starting.
        dag = valid_dag()
        state = dag["states"]["STATE_START"]
        del state["phase"]
        del state["step"]
        del state["instructions"]
        del state["knowledge"]

        validate_dag(dag, "test.json")

        with self.assertLogs(dag_validation.logger, level="WARNING") as logs:
            report_unconverted(dag, "test.json")
        self.assertIn("STATE_START", "\n".join(logs.output))

    def test_no_warning_when_every_agent_task_is_converted(self):
        with self.assertNoLogs(dag_validation.logger, level="WARNING"):
            report_unconverted(valid_dag(), "test.json")

    def test_validation_does_not_mutate_the_graph(self):
        dag = valid_dag()
        before = copy.deepcopy(dag)
        validate_dag(dag, "test.json")
        self.assertEqual(dag, before)


class InstructionsHeaderTest(unittest.TestCase):
    """DESIGN §9.2 states the header as a requirement, and nothing enforced it.

    These are RUNTIME files — `build_stage_payload` hands one to the agent —
    so a step document that never names the state the agent is standing in is
    the one document in the system that cannot orient it. A parking state is
    not an exception: `workload_await_3` carries
    `**Expected tool call:** none (report and stop)`.
    """

    def _instruction_files(self):
        root = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(
                os.path.abspath(__file__)))), "phases")
        for folder, _dirs, files in os.walk(root):
            if "instructions.md" in files:
                yield os.path.join(folder, "instructions.md")

    def test_every_step_document_names_its_state_and_tool(self):
        found = list(self._instruction_files())
        self.assertGreater(len(found), 15, "walk found no step documents")

        # BOTH halves. The tool half is the one that tells the agent what to
        # do, and a check for the state alone would let the next document ship
        # without it — this test asserting only what its own name says twice.
        missing = []
        for path in found:
            with open(path) as handle:
                text = handle.read()
            for half in ("**DAG state:**", "**Expected tool call:**"):
                if half not in text:
                    missing.append(f"{os.path.relpath(path)}: {half}")

        self.assertEqual(
            missing, [],
            "every instructions.md needs the DESIGN 9.2 header line")

    def test_no_step_document_names_an_adjacent_step(self):
        """§9.2: a runtime file must not tell the agent where the graph goes.

        The narrow, green half of that rule. `**Previous step:**` and
        `**Next step:**` are header fields naming a destination outright, and
        no document carries one — `bootstrap_admin_2` was the only one that
        ever did. The broad half (any foreign state name, in prose) is red on
        21 files today and is sequenced behind the sweep in DESIGN issue 37;
        this guard costs three lines and stops the deleted header returning
        while that waits.
        """
        offenders, checked = [], 0
        for path in self._instruction_files():
            checked += 1
            with open(path) as handle:
                text = handle.read()
            for field in ("**Previous step:**", "**Next step:**"):
                if field in text:
                    offenders.append(f"{os.path.relpath(path)}: {field}")

        # Both siblings in this class guard the walk, and this one needs it
        # more than either: they fail loudly when the root stops resolving,
        # while an all-clear assertion over zero files is indistinguishable
        # from a pass — including over a corpus where the header came back.
        self.assertGreater(checked, 15, "walk found no step documents")
        self.assertEqual(
            offenders, [],
            "a step document must not name an adjacent step (DESIGN 9.2); "
            "the agent is told its state one at a time, by the server")

    def test_a_counted_list_has_that_many_bullets(self):
        """Found twice now: round 5 in the deployment README ("three
        decisions" over five bullets) and round 50 in this step's own
        instructions, where a bullet was added and the count left. These are
        RUNTIME files, so the agent has to work out which item is not one of
        the four. Checks the counted intros that exist; adding a new phrasing
        does not silently opt out, because the phrase list is the assertion.
        """
        words = {"two": 2, "three": 3, "four": 4, "five": 5, "six": 6,
                 "seven": 7, "eight": 8, "nine": 9, "ten": 10}
        pattern = re.compile(
            r"\b(" + "|".join(words) + r") (?:things|kinds|decisions|answers"
            r"|questions|reasons|cases|options) ", re.IGNORECASE)
        checked, wrong = 0, []

        for path in self._instruction_files():
            with open(path) as handle:
                lines = handle.read().split("\n")
            for index, line in enumerate(lines):
                match = pattern.search(line)
                if not match:
                    continue
                # The intro sentence wraps, so walk past its continuation and
                # the blank line to the list itself, then count top-level
                # bullets until something that is not one of them.
                rest = lines[index + 1:]
                while rest and not rest[0].startswith("- "):
                    if rest[0].startswith(("#", "|")):
                        break
                    rest = rest[1:]
                bullets = 0
                for after in rest:
                    if after.startswith("- "):
                        bullets += 1
                    elif after and not after.startswith(" "):
                        break
                if not bullets:
                    continue        # prose, not an intro to a list
                checked += 1
                claimed = words[match.group(1).lower()]
                if claimed != bullets:
                    wrong.append(f"{os.path.relpath(path)}:{index + 1} says "
                                 f"{match.group(1)} over {bullets} bullets")

        self.assertGreater(checked, 0, "the phrase list matched nothing")
        self.assertEqual(wrong, [])


if __name__ == "__main__":
    unittest.main()
