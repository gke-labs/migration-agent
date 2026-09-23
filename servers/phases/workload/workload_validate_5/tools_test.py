"""Tool-layer tests for run_workload_validation (STATE_WKLD_VALIDATE).

FakeGCS + a stubbed dispatch loop environment (elicitation patched, the
mutation runner wired straight to the workload ACTIONS table, git mocked at
the client-function level — nothing pushes). The ledger layout is the
REALISTIC one: no workspace-root state.json (nothing writes one), no
target-repo coordinates seeded into component variables (nothing seeds
them) — the coordinates ride exports.target_repo, exactly the channel a
developer's IAM can read. Covers: a manifest finding routes to review, the
staleness finding (bumped exports generations), the ship-completeness
refusal, the scratch-clone BLOCKING finding, clone self-heal (origin
re-verified, re-clone on changed coordinates), the deterministic re-check
gate, ship decline -> REVIEW, the PR action with a mocked remote
(branch-name shape, committed path scope, DONE), the PR failure edge back
to the ship approval, and the exhaustion escape off the HITL state.
"""

import json
import os
import re
import shutil
import tempfile
import unittest
from unittest.mock import patch

from google.api_core import exceptions

import servers.dag.state_management as state_mgr
from servers.dag import dispatch, fake_gcs
from servers.dag.server import git_client
from servers.phases.landingzone import workspace
from servers.phases.workload.workload_validate_5 import actions, tools

COMPONENT = "orders-component"
REGISTRY = {
    "workspace_name": "ws-dev", "gcp_project": "proj",
    "roles": {"developers": ["dev-a@x.com", "dev-b@x.com"]},
}
GENERATIONS = {"discovery": 3, "translation": 1, "deployment": 0, "data": 1}
# An estate the data scan ran over and found nothing in. The default has to
# be a PUBLISHED empty gate rather than an absent one: an absent slice is the
# "platform has not published" refusal, which is a different test.
EMPTY_DATA_GATE = {"schema_version": 1, "scanned": True, "services": []}
EXPORTS_DOC = {
    "generated_at": "T0", "generations": dict(GENERATIONS),
    "data_gate": dict(EMPTY_DATA_GATE),
    "target_repo": {"url": "sso://target", "branch": "main", "path": None},
    "gsa_bindings": {"acme-shop/orders": "orders@proj.iam.gserviceaccount.com"},
    "artifact_registry": {"destinations": [], "image_map": {
        "111.dkr.ecr.us-east-1.amazonaws.com/acme/orders:v1": {
            "dest_ref": "us-docker.pkg.dev/proj/repo/orders:v1",
            "status": "replicated"}}},
}
GOOD_DOC = "apiVersion: v1\nkind: Service\nmetadata:\n  name: web\n"
_DAG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         "..", "..", "..", "dag", "developer_dag.json")


def unit(unit_id, status="done"):
    return {"unit_id": unit_id, "family": unit_id, "title": unit_id,
            "status": status, "planned_status": status, "placeholder": False,
            "inputs": {"documents": [
                {"path": "k8s/app.yaml", "doc_index": 0,
                 "rendered_from": None}]},
            "notes": [], "feedback": None, "error": None}


def blob_payload(u, content=GOOD_DOC, generations=None):
    return {"unit": {**u, "status": "done"},
            "result": {"files": [{"path": "svc.yaml", "content": content}],
                       "tradeoffs": "t", "assumptions": ["a1"],
                       "open_questions": ["q1"]},
            "run": 1,
            "exports_generations": dict(GENERATIONS)
            if generations is None else generations}


async def approve_elicitation(ctx, state_name, state_def, schema_cls,
                              state_dict, config, notice=None):
    return True, {"approved": True}


async def decline_elicitation(ctx, state_name, state_def, schema_cls,
                              state_dict, config, notice=None):
    return False, None


class ValidateToolTest(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self._saved = (state_mgr.LEDGER_CONFIG_PATH, state_mgr.LEDGER_CONFIG_DIR,
                       state_mgr.gcs_client)
        state_mgr.LEDGER_CONFIG_PATH = "/tmp/test_wkldval_config.yaml"
        state_mgr.LEDGER_CONFIG_DIR = "/tmp/test_wkldval_config.d"
        shutil.rmtree(state_mgr.LEDGER_CONFIG_DIR, ignore_errors=True)
        self.fake = fake_gcs.FakeStorageClient()
        state_mgr.gcs_client = self.fake
        self.bucket = self.fake.bucket("dev-ledger")
        self.bucket.blob("workspace_registry.yaml").upload_from_string(
            json.dumps(REGISTRY))
        with open(os.path.normpath(_DAG_PATH)) as f:
            self.bucket.blob(f"workloads/{COMPONENT}/dag.json") \
                .upload_from_string(f.read())
        self.bucket.blob("exports.json").upload_from_string(
            json.dumps(EXPORTS_DOC))
        state_mgr.write_local_config("gs://dev-ledger", "developers", "ws-dev",
                                     "proj", component=COMPONENT)
        self._email = patch.object(state_mgr, "get_authenticated_user_email",
                                   return_value="dev-a@x.com")
        self._email.start()
        # The recorded clone path must live under THIS machine's scratch
        # root (the host-validity predicate re-derives anything else), so
        # the tests scope the scratch root itself to a temp dir.
        self.scratch_root = tempfile.mkdtemp(prefix="wkld_val_scratch_")
        self.addCleanup(shutil.rmtree, self.scratch_root, True)
        scratch_patch = patch.object(workspace, "SCRATCH_DIR",
                                     self.scratch_root)
        scratch_patch.start()
        self.addCleanup(scratch_patch.stop)
        self.clone = os.path.join(self.scratch_root, "target-repo-test")
        os.makedirs(self.clone, exist_ok=True)
        # Nothing reaches a remote: the clone is a local `git init`, which
        # is enough to make the work-tree guard and stage_and_commit real.
        self.clones = []

        def fake_clone(url, branch, target_dir):
            os.makedirs(target_dir, exist_ok=True)
            git_client.git.Repo.init(target_dir)
            self.clones.append((url, branch, target_dir))

        for name, fake in (("clone_repository", fake_clone),
                           ("create_and_checkout_branch", lambda d, b: None)):
            patcher = patch.object(git_client, name, fake)
            patcher.start()
            self.addCleanup(patcher.stop)
        self._saved_runner = dispatch._mutation_runner
        dispatch.set_mutation_runner(
            lambda action, variables, config:
            actions.ACTIONS[action](variables, config))

    def tearDown(self):
        dispatch.set_mutation_runner(self._saved_runner)
        self._email.stop()
        shutil.rmtree(state_mgr.LEDGER_CONFIG_DIR, ignore_errors=True)
        (state_mgr.LEDGER_CONFIG_PATH, state_mgr.LEDGER_CONFIG_DIR,
         state_mgr.gcs_client) = self._saved

    def write_state(self, units, current="STATE_WKLD_VALIDATE"):
        plan = {"component": COMPONENT, "units": units,
                "sources": {"charts": [], "kustomize": []}, "carriers": [],
                "notes": [], "exports_stamp": {"generated_at": "T0",
                                               "generations": dict(GENERATIONS)}}
        # NO target_repo_url/target_branch here: the product never writes
        # them into component state (workload_join seeds component + claim
        # only), so seeding them would test a ledger the pipeline cannot
        # produce. The coordinates ride exports.target_repo above.
        state = {"current_state": current, "history": [],
                 "variables": {"component": COMPONENT,
                               "claim": {"claimant": "dev-a@x.com",
                                         "claimed_at": "T0"},
                               "workload_plan": plan,
                               "workload_clone_path": self.clone,
                               "workload_branch_uuid": "feed",
                               "workload_branch_name":
                                   f"migration/workload-{COMPONENT}-feed"}}
        self.bucket.blob(f"workloads/{COMPONENT}/state.json") \
            .upload_from_string(json.dumps(state))

    def write_blob(self, u, **kwargs):
        self.bucket.blob(
            f"workloads/{COMPONENT}/units/{u['unit_id']}.json") \
            .upload_from_string(json.dumps(blob_payload(u, **kwargs)))

    def read_state(self):
        return json.loads(self.bucket.blob(
            f"workloads/{COMPONENT}/state.json").download_as_text())

    async def test_manifest_finding_routes_back_to_review(self):
        u = unit("wkld-manifests")
        self.write_state([u])
        self.write_blob(u, content="kind: Service\nmetadata:\n  name: x\n")
        out = await tools.run_workload_validation()
        self.assertIn("FAILED", out)
        self.assertIn("apiVersion", out)
        self.assertEqual(self.read_state()["current_state"],
                         "STATE_WKLD_REVIEW")
        report = json.loads(self.bucket.blob(
            f"workloads/{COMPONENT}/validation-report.json")
            .download_as_text())
        self.assertFalse(report["all_valid"])
        self.assertEqual(len(report["manifests"]["invalid"]), 1)

    async def test_stale_blob_generations_are_a_named_finding(self):
        u = unit("wkld-manifests")
        self.write_state([u])
        self.write_blob(u, generations=dict(GENERATIONS, discovery=2))
        out = await tools.run_workload_validation()
        self.assertIn("FAILED", out)
        self.assertIn("'discovery': 2", out)
        self.assertIn("'discovery': 3", out)
        self.assertEqual(self.read_state()["current_state"],
                         "STATE_WKLD_REVIEW")

    async def test_a_done_unit_whose_blob_carries_no_files_is_a_finding(self):
        """The plan's status is not proof of output. An emptied blob would
        materialize zero files and ship a PR silently missing the unit."""
        u = unit("wkld-manifests")
        self.write_state([u])
        payload = blob_payload(u)
        payload["result"]["files"] = []
        self.bucket.blob(
            f"workloads/{COMPONENT}/units/wkld-manifests.json") \
            .upload_from_string(json.dumps(payload))
        out = await tools.run_workload_validation()
        self.assertIn("FAILED", out)
        self.assertIn("carries no files", out)
        self.assertEqual(self.read_state()["current_state"],
                         "STATE_WKLD_REVIEW")
        report = json.loads(self.bucket.blob(
            f"workloads/{COMPONENT}/validation-report.json")
            .download_as_text())
        self.assertEqual(report["ledger"][0]["unit_id"], "wkld-manifests")

    async def test_a_done_unit_whose_blob_is_not_done_is_a_finding(self):
        u = unit("wkld-manifests")
        self.write_state([u])
        payload = blob_payload(u)
        payload["unit"]["status"] = "revise"
        self.bucket.blob(
            f"workloads/{COMPONENT}/units/wkld-manifests.json") \
            .upload_from_string(json.dumps(payload))
        out = await tools.run_workload_validation()
        self.assertIn("FAILED", out)
        self.assertIn("'revise'", out)

    async def test_a_scratch_clone_is_a_blocking_finding_not_a_pass(self):
        """No resolvable target repo -> the gates still run, but validation
        FAILS with the clone finding: the ship elicitation must never rise
        (and burn its one retry) over a PR that cannot be opened. The
        remedy names the platform-side pair, not a tool the developer
        cannot call."""
        u = unit("wkld-manifests")
        self.write_state([u])
        self.write_blob(u)
        exports = {k: v for k, v in EXPORTS_DOC.items() if k != "target_repo"}
        self.bucket.blob("exports.json").upload_from_string(
            json.dumps(exports))
        asked = []

        async def counting_elicitation(ctx, state_name, state_def, schema_cls,
                                       state_dict, config, notice=None):
            asked.append(state_name)
            return True, {"approved": True}

        with patch.object(dispatch, "run_elicitation", counting_elicitation):
            out = await tools.run_workload_validation()
        self.assertEqual(self.clones, [])
        self.assertEqual(asked, [])  # the ship approval never rises
        self.assertIn("FAILED", out)
        self.assertIn("SCRATCH", out)
        self.assertIn("refresh_exports", out)
        self.assertIn("configure_repositories", out)
        self.assertEqual(self.read_state()["current_state"],
                         "STATE_WKLD_REVIEW")
        report = json.loads(self.bucket.blob(
            f"workloads/{COMPONENT}/validation-report.json")
            .download_as_text())
        self.assertFalse(report["all_valid"])
        self.assertTrue(report["clone"]["scratch"])
        self.assertTrue(report["clone"]["findings"])

    async def test_exhausted_ship_retries_persist_off_the_hitl_state(self):
        """SUBMIT_PR's on_failure points back at the approval that produced
        it, so an approve-every-time human would loop forever. One retry,
        then the loop follows the elicitation's own escape edge (on_reject
        -> REVIEW) before stopping: persisted ON the HITL state, no
        workload tool would accept the workspace ever again (the
        approve-twice wedge — every recovery needed a hand-edited
        ledger)."""
        u = unit("wkld-manifests")
        self.write_state([u])
        self.write_blob(u)
        asked = []

        async def counting_elicitation(ctx, state_name, state_def, schema_cls,
                                       state_dict, config, notice=None):
            asked.append(state_name)
            return True, {"approved": True}

        with patch.object(dispatch, "run_elicitation", counting_elicitation), \
                patch.object(git_client, "stage_and_commit",
                             return_value=None), \
                patch.object(git_client, "rebase_and_push",
                             side_effect=RuntimeError("remote said no")):
            out = await tools.run_workload_validation()
        self.assertEqual(asked, ["STATE_WKLD_APPROVED"] * 2)
        self.assertIn("not asking again", out)
        self.assertIn("remote said no", out)
        persisted = self.read_state()["current_state"]
        self.assertEqual(persisted, "STATE_WKLD_REVIEW")
        # The graph invariant behind the escape: the persisted state is one
        # whose tools are callable, never the elicitation itself.
        with open(os.path.normpath(_DAG_PATH)) as f:
            graph = json.load(f)
        self.assertEqual(graph["states"][persisted]["type"], "AGENT_TASK")

    async def test_ship_completeness_refusal(self):
        done_u, error_u = unit("wkld-manifests"), unit("wkld-identity",
                                                       status="error")
        self.write_state([done_u, error_u])
        self.write_blob(done_u)
        out = await tools.run_workload_validation()
        self.assertIn("FAILED", out)
        self.assertIn("wkld-identity", out)
        self.assertEqual(self.read_state()["current_state"],
                         "STATE_WKLD_REVIEW")

    # --- the data gate --------------------------------------

    def write_scope(self, paths):
        """The confirmed scope the data gate attributes consumers against."""
        self.bucket.blob(f"workloads/{COMPONENT}/scope.json") \
            .upload_from_string(json.dumps(
                {"component": COMPONENT, "resolved_paths": list(paths),
                 "degraded": False}))

    def write_gate(self, services, scanned=True, seed=None):
        doc = dict(EXPORTS_DOC)
        # The join key for a consumer with no chart path: an IRSA service
        # account is a NAME, and the seed index is where the developer side
        # learns which of its files declares one.
        doc["component_seed_index"] = seed if seed is not None else {
            "k8s/app.yaml": {"kinds": ["Deployment", "ServiceAccount"],
                             "namespaces": ["web"], "team_labels": [],
                             "names": ["Deployment/orders",
                                       "ServiceAccount/orders"]}}
        doc["data_gate"] = {"schema_version": 1, "scanned": scanned,
                            "services": list(services)}
        self.bucket.blob("exports.json").upload_from_string(json.dumps(doc))

    def outstanding_rds(self, consumers, status=None):
        return {"service": "rds", "identifier": "orders-db",
                "address": "aws_db_instance.orders", "directory": "tf",
                "disposition": "migrate", "status": status,
                "consumers": consumers}

    async def test_an_outstanding_data_service_holds_the_ship_at_validate(self):
        """Validation PASSES and the component still does not ship. The
        elicitation must not rise: nothing is wrong with the manifests, and
        neither exit is a developer's to take."""
        u = unit("wkld-manifests")
        self.write_state([u])
        self.write_blob(u)
        self.write_scope(["k8s/app.yaml"])
        self.write_gate([self.outstanding_rds(
            [{"workload": "orders", "kind": "service_account",
              "namespace": None, "source_path": None, "detection": "irsa"}])])
        asked = []

        async def counting_elicitation(ctx, state_name, state_def, schema_cls,
                                       state_dict, config, notice=None):
            asked.append(state_name)
            return True, {}

        with patch.object(dispatch, "run_elicitation", counting_elicitation):
            out = await tools.run_workload_validation()

        self.assertIn("Validation passed", out)
        self.assertIn("HELD", out)
        self.assertIn("rds orders-db", out)
        self.assertIn("mark_data_service_migrated", out)
        self.assertEqual(asked, [], "the ship elicitation must not rise")
        state = self.read_state()
        self.assertEqual(state["current_state"], "STATE_WKLD_VALIDATE",
                         "a park stays put; it does not fail back to review")
        self.assertTrue(any("held by the data gate" in h
                            for h in state["history"]))
        self.assertFalse(any("Transitioned" in h for h in state["history"]),
                         "nothing transitioned, so nothing says it did")

    async def test_a_held_component_keeps_its_report_and_comparisons(self):
        u = unit("wkld-manifests")
        self.write_state([u])
        self.write_blob(u)
        self.write_scope(["k8s/app.yaml"])
        self.write_gate([self.outstanding_rds(
            [{"workload": "orders", "kind": "service_account",
              "namespace": None, "source_path": None, "detection": "irsa"}])])

        out = await tools.run_workload_validation()

        self.assertIn("HELD", out)
        report = json.loads(self.bucket.blob(
            f"workloads/{COMPONENT}/validation-report.json").download_as_text())
        self.assertTrue(report["all_valid"], (
            "the data gate is a park, not a validation finding — a manifest "
            "that is correct must not be recorded as invalid"))
        self.assertEqual(len(report["data"]["blocking"]), 1)
        comparisons = json.loads(self.bucket.blob(
            f"workloads/{COMPONENT}/comparisons.json").download_as_text())
        self.assertEqual(comparisons[0]["unit_id"], "wkld-manifests")

    async def test_the_hold_clears_when_the_service_is_reported_migrated(self):
        u = unit("wkld-manifests")
        self.write_state([u])
        self.write_blob(u)
        self.write_scope(["k8s/app.yaml"])
        consumers = [{"workload": "orders", "kind": "service_account",
                      "namespace": None, "source_path": None,
                      "detection": "irsa"}]
        self.write_gate([self.outstanding_rds(consumers)])
        with patch.object(dispatch, "run_elicitation", decline_elicitation):
            held = await tools.run_workload_validation()
        self.assertIn("HELD", held)

        self.write_gate([self.outstanding_rds(consumers, status="migrated")])
        with patch.object(dispatch, "run_elicitation", decline_elicitation):
            out = await tools.run_workload_validation()

        self.assertNotIn("HELD", out)
        self.assertIn("Ship declined", out, "the elicitation rose this time")

    async def test_an_unattributed_service_is_reported_but_holds_nobody(self):
        u = unit("wkld-manifests")
        self.write_state([u])
        self.write_blob(u)
        self.write_scope(["k8s/app.yaml"])
        self.write_gate([self.outstanding_rds([])])
        seen = []

        async def declining(ctx, state_name, state_def, schema_cls,
                            state_dict, config, notice=None):
            seen.append(notice)
            return False, None

        with patch.object(dispatch, "run_elicitation", declining):
            out = await tools.run_workload_validation()

        self.assertNotIn("HELD", out)
        # Reported where it can still change the answer, and NOT repeated in
        # the response afterwards — the same paragraph twice, the second time
        # about a decision already taken, is what the duplicate would be.
        self.assertIn("no attributed consumer", seen[0],
                      "it holds nobody, and the ship gate says so out loud")
        self.assertNotIn("no attributed consumer", out)
        report = json.loads(self.bucket.blob(
            f"workloads/{COMPONENT}/validation-report.json").download_as_text())
        self.assertEqual(len(report["data"]["unattributed"]), 1,
                         "the durable copy is the report blob")

    async def test_the_residue_reaches_the_ship_elicitation_not_the_response(self):
        """What the gate could not hold on has to be in front of the
        developer while they decide. Returned with the response it arrives
        after the pull request it should have informed is already open."""
        u = unit("wkld-manifests")
        self.write_state([u])
        self.write_blob(u)
        self.write_scope(["k8s/app.yaml"])
        self.write_gate([self.outstanding_rds([])])   # no consumer: holds nobody
        seen = []

        async def capturing_elicitation(ctx, state_name, state_def, schema_cls,
                                        state_dict, config, notice=None):
            seen.append((state_name, notice))
            return False, None

        with patch.object(dispatch, "run_elicitation", capturing_elicitation):
            await tools.run_workload_validation()

        self.assertEqual(len(seen), 1)
        state_name, notice = seen[0]
        self.assertEqual(state_name, "STATE_WKLD_APPROVED")
        self.assertIsNotNone(notice, "the ship elicitation rose with no warning")
        self.assertIn("Before you approve", notice)
        self.assertIn("no attributed consumer", notice)
        self.assertIn("rds orders-db", notice)

    async def test_a_clear_component_gets_no_ship_notice(self):
        # The positive control: the notice must be absent when there is
        # nothing to say, not merely present when there is.
        u = unit("wkld-manifests")
        self.write_state([u])
        self.write_blob(u)
        self.write_scope(["k8s/app.yaml"])
        self.write_gate([])
        seen = []

        async def capturing_elicitation(ctx, state_name, state_def, schema_cls,
                                        state_dict, config, notice=None):
            seen.append(notice)
            return False, None

        with patch.object(dispatch, "run_elicitation", capturing_elicitation):
            await tools.run_workload_validation()

        self.assertEqual(seen, [None])

    async def test_polling_a_held_component_stops_growing_the_history(self):
        """The step's own instructions say to re-run this tool to re-check,
        and a data migration takes weeks. An unconditional append writes the
        same sentences into a document every authenticated call reads, once
        per poll, for as long as the wait lasts."""
        u = unit("wkld-manifests")
        self.write_state([u])
        self.write_blob(u)
        self.write_scope(["k8s/app.yaml"])
        consumers = [{"workload": "orders", "kind": "service_account",
                      "namespace": None, "source_path": None,
                      "detection": "irsa"}]
        self.write_gate([self.outstanding_rds(consumers)])

        await tools.run_workload_validation()   # clones
        await tools.run_workload_validation()   # reuses the clone: a NEW note
        settled = len(self.read_state()["history"])
        for _ in range(5):
            await tools.run_workload_validation()

        history = self.read_state()["history"]
        self.assertEqual(len(history), settled,
                         "five further polls said nothing new and must leave "
                         f"nothing behind; history grew to {len(history)}")
        self.assertTrue(any("held by the data gate" in h for h in history),
                        "the hold itself must still be on the record")

    async def test_a_degraded_scope_is_re_resolved_and_still_holds(self):
        """A scope confirmed before the seed index published persists
        `resolved_paths: null` for ever — the graph has no edge back to
        STATE_WKLD_SCOPE. Read at face value that disabled the gate for the
        whole component, silently and permanently."""
        u = unit("wkld-manifests")
        self.write_state([u])
        self.write_blob(u)
        self.bucket.blob(f"workloads/{COMPONENT}/scope.json") \
            .upload_from_string(json.dumps(
                {"component": COMPONENT, "resolved_paths": None,
                 "degraded": True, "included": ["k8s/**"], "excluded": []}))
        self.write_gate([self.outstanding_rds(
            [{"workload": "orders", "kind": "service_account",
              "namespace": None, "source_path": None, "detection": "irsa"}])])

        out = await tools.run_workload_validation()

        self.assertIn("HELD", out)
        self.assertIn("rds orders-db", out)
        self.assertEqual(self.read_state()["current_state"],
                         "STATE_WKLD_VALIDATE")

    async def test_a_re_resolved_scope_is_marked_so_downstream(self):
        """The wiring half of the rule datagate_test pins: the tool has to
        TELL the verdict that these paths came from globs nobody verified,
        or the suppression never fires."""
        u = unit("wkld-manifests")
        self.write_state([u])
        self.write_blob(u)
        self.bucket.blob(f"workloads/{COMPONENT}/scope.json") \
            .upload_from_string(json.dumps(
                {"component": COMPONENT, "resolved_paths": None,
                 "degraded": True, "included": ["k8s/**"], "excluded": []}))
        # 'billing' is nowhere in the re-resolved scope, and every file in it
        # carries a name — so without the marking this would be `elsewhere`.
        self.write_gate([{"service": "rds", "identifier": "billing-db",
                          "address": "aws_db_instance.billing",
                          "directory": "tf", "disposition": "migrate",
                          "status": None,
                          "consumers": [{"workload": "billing",
                                         "kind": "service_account",
                                         "namespace": None, "source_path": None,
                                         "detection": "irsa"}]}])

        with patch.object(dispatch, "run_elicitation", decline_elicitation):
            await tools.run_workload_validation()

        report = json.loads(self.bucket.blob(
            f"workloads/{COMPONENT}/validation-report.json").download_as_text())
        self.assertTrue(report["data"]["scope_reresolved"])
        self.assertEqual(report["data"]["elsewhere"], [])
        self.assertEqual(len(report["data"]["untestable"]), 1)

    async def test_an_unreadable_scope_object_holds_rather_than_ships(self):
        """A transient read failure on scope.json used to clear the gate: it
        attributed nothing, the component shipped, and the developer was told
        their scope 'never resolved to a file list' — false, and naming no
        remedy. The sibling exports.json read on this path fails closed."""
        u = unit("wkld-manifests")
        self.write_state([u])
        self.write_blob(u)
        self.write_scope(["k8s/app.yaml"])
        self.write_gate([self.outstanding_rds(
            [{"workload": "orders", "kind": "service_account",
              "namespace": None, "source_path": None, "detection": "irsa"}])])
        original = fake_gcs.FakeBlob.download_as_text

        def flaky(blob_self, *args, **kwargs):
            if blob_self.name.endswith("/scope.json"):
                raise exceptions.ServiceUnavailable("backend error")
            return original(blob_self, *args, **kwargs)

        with patch.object(fake_gcs.FakeBlob, "download_as_text", flaky):
            out = await tools.run_workload_validation()

        self.assertIn("HELD", out)
        self.assertIn("could not be read on this run", out)
        self.assertIn("re-run run_workload_validation", out)
        self.assertNotIn("never resolved to a file list", out,
                         "the scope resolved fine; saying otherwise is false")
        self.assertEqual(self.read_state()["current_state"],
                         "STATE_WKLD_VALIDATE")

    async def test_a_non_object_scope_body_is_classified_not_crashed(self):
        """`null` parses fine and then `doc.get` raised AttributeError out of
        the tool — unhandled, after the whole run and the report write,
        naming no object. Same class of input as a truncated body, which was
        already classified."""
        u = unit("wkld-manifests")
        self.write_state([u])
        self.write_blob(u)
        self.bucket.blob(f"workloads/{COMPONENT}/scope.json") \
            .upload_from_string("null")
        self.write_gate([self.outstanding_rds(
            [{"workload": "orders", "kind": "service_account",
              "namespace": None, "source_path": None, "detection": "irsa"}])])

        out = await tools.run_workload_validation()

        self.assertNotIn("AttributeError", out)
        self.assertIn("HELD", out)
        self.assertIn("could not be read on this run", out)

    async def test_a_re_resolved_scope_explains_itself_to_the_developer(self):
        """The wiring for the reason, not just the bucket: a re-resolved
        scope has nothing blind, so a message keyed on `blind` rendered
        'hold nothing as a result — .' into the ship elicitation."""
        u = unit("wkld-manifests")
        self.write_state([u])
        self.write_blob(u)
        self.bucket.blob(f"workloads/{COMPONENT}/scope.json") \
            .upload_from_string(json.dumps(
                {"component": COMPONENT, "resolved_paths": None,
                 "degraded": True, "included": ["k8s/**"], "excluded": []}))
        self.write_gate([{"service": "rds", "identifier": "billing-db",
                          "address": "aws_db_instance.billing",
                          "directory": "tf", "disposition": "migrate",
                          "status": None,
                          "consumers": [{"workload": "billing",
                                         "kind": "service_account",
                                         "namespace": None, "source_path": None,
                                         "detection": "irsa"}]}])
        seen = []

        async def declining(ctx, state_name, state_def, schema_cls,
                            state_dict, config, notice=None):
            seen.append(notice)
            return False, None

        with patch.object(dispatch, "run_elicitation", declining):
            await tools.run_workload_validation()

        self.assertIn("re-resolved", seen[0])
        self.assertNotIn("— .", seen[0])

    async def test_an_absent_scope_object_is_not_treated_as_a_failed_read(self):
        # ABSENT is a definite answer a re-run cannot change, so holding on
        # it would wedge the component with no remedy to name.
        u = unit("wkld-manifests")
        self.write_state([u])
        self.write_blob(u)
        self.write_gate([self.outstanding_rds(
            [{"workload": "orders", "kind": "service_account",
              "namespace": None, "source_path": None, "detection": "irsa"}])])

        with patch.object(dispatch, "run_elicitation", decline_elicitation):
            out = await tools.run_workload_validation()

        self.assertNotIn("could not be read on this run", out)
        self.assertIn("Ship declined", out)

    async def test_a_degraded_scope_whose_globs_match_nothing_reports_itself(self):
        # The positive control: re-resolution is not a blanket pass, and the
        # case it cannot answer says so rather than going quiet.
        u = unit("wkld-manifests")
        self.write_state([u])
        self.write_blob(u)
        self.bucket.blob(f"workloads/{COMPONENT}/scope.json") \
            .upload_from_string(json.dumps(
                {"component": COMPONENT, "resolved_paths": None,
                 "degraded": True, "included": ["nothing/**"], "excluded": []}))
        self.write_gate([self.outstanding_rds(
            [{"workload": "orders", "kind": "service_account",
              "namespace": None, "source_path": None, "detection": "irsa"}])])
        seen = []

        async def declining(ctx, state_name, state_def, schema_cls,
                            state_dict, config, notice=None):
            seen.append(notice)
            return False, None

        with patch.object(dispatch, "run_elicitation", declining):
            out = await tools.run_workload_validation()

        self.assertNotIn("HELD", out)
        self.assertIn("never resolved to a file list", seen[0])

    async def test_an_unpublished_data_gate_holds_the_ship(self):
        u = unit("wkld-manifests")
        self.write_state([u])
        self.write_blob(u)
        doc = {k: v for k, v in EXPORTS_DOC.items() if k != "data_gate"}
        self.bucket.blob("exports.json").upload_from_string(json.dumps(doc))

        out = await tools.run_workload_validation()

        self.assertIn("HELD", out)
        self.assertIn("refresh_exports", out)
        self.assertIn("not a finding against your manifests", out)
        self.assertEqual(self.read_state()["current_state"],
                         "STATE_WKLD_VALIDATE")

    async def test_the_data_slice_moving_does_not_make_a_unit_stale(self):
        """A database landing is not brief input. Left in the staleness
        comparison it would demand a replan of every in-flight component on
        every operator report."""
        u = unit("wkld-manifests")
        self.write_state([u])
        self.write_blob(u, generations=dict(GENERATIONS, data=1))
        self.write_scope(["k8s/app.yaml"])
        self.write_gate([])
        doc = dict(EXPORTS_DOC)
        doc["generations"] = dict(GENERATIONS, data=9)
        doc["data_gate"] = {"schema_version": 1, "scanned": True, "services": []}
        self.bucket.blob("exports.json").upload_from_string(json.dumps(doc))

        with patch.object(dispatch, "run_elicitation", decline_elicitation):
            out = await tools.run_workload_validation()

        self.assertIn("Validation passed", out)
        self.assertIn("0 stale", out)
        self.assertNotIn("replan", out)

    async def test_a_stamp_taken_before_the_data_source_existed_still_matches(self):
        u = unit("wkld-manifests")
        self.write_state([u])
        self.write_blob(u, generations={"discovery": 3, "translation": 1,
                                        "deployment": 0})
        self.write_scope(["k8s/app.yaml"])
        self.write_gate([])
        with patch.object(dispatch, "run_elicitation", decline_elicitation):
            out = await tools.run_workload_validation()
        self.assertIn("Validation passed", out)
        self.assertIn("0 stale", out)
        self.assertNotIn("replan", out,
                         "filtering BOTH sides means a pre-data stamp still "
                         "compares equal")

    async def test_a_mapping_disagreement_is_reported_and_still_holds(self):
        u = unit("wkld-manifests")
        self.write_state([u])
        self.write_blob(u)
        self.write_scope(["k8s/app.yaml"])
        # The seed index puts 'orders' in namespace 'web'; discovery recorded
        # it in 'payments'. The gate holds on the name and says both.
        doc = dict(EXPORTS_DOC)
        doc["component_seed_index"] = {
            "k8s/app.yaml": {"kinds": ["Deployment"], "namespaces": ["web"],
                             "team_labels": [], "names": ["Deployment/orders"]}}
        doc["data_gate"] = {"schema_version": 1, "scanned": True, "services": [
            self.outstanding_rds([{"workload": "orders", "kind": "workload",
                                   "namespace": "payments", "source_path": None,
                                   "detection": "human_review"}])]}
        self.bucket.blob("exports.json").upload_from_string(json.dumps(doc))

        out = await tools.run_workload_validation()

        self.assertIn("HELD", out)
        self.assertIn("Mapping disagreement", out)
        self.assertIn("namespace 'payments'", out)

    async def test_parked_units_never_block_and_are_listed(self):
        done_u, parked_u = unit("wkld-manifests"), unit("wkld-routing",
                                                        status="parked")
        self.write_state([done_u, parked_u])
        self.write_blob(done_u)
        with patch.object(dispatch, "run_elicitation", decline_elicitation):
            out = await tools.run_workload_validation()
        self.assertIn("Validation passed", out)
        self.assertIn("wkld-routing", out)
        self.assertEqual(self.read_state()["current_state"],
                         "STATE_WKLD_REVIEW")  # declined ship parks at review

    async def test_ship_decline_returns_to_review_with_report_persisted(self):
        u = unit("wkld-manifests")
        self.write_state([u])
        self.write_blob(u)
        with patch.object(dispatch, "run_elicitation", decline_elicitation):
            out = await tools.run_workload_validation()
        self.assertIn("Ship declined", out)
        state = self.read_state()
        self.assertEqual(state["current_state"], "STATE_WKLD_REVIEW")
        report = json.loads(self.bucket.blob(
            f"workloads/{COMPONENT}/validation-report.json")
            .download_as_text())
        self.assertTrue(report["all_valid"])
        comparisons = json.loads(self.bucket.blob(
            f"workloads/{COMPONENT}/comparisons.json").download_as_text())
        self.assertEqual(comparisons[0]["unit_id"], "wkld-manifests")

    async def test_approved_ship_opens_the_pr_with_scoped_commit(self):
        u = unit("wkld-manifests")
        self.write_state([u])
        self.write_blob(u)
        # No branch recorded: validate allocates the uuid branch itself.
        state = self.read_state()
        for key in ("workload_clone_path", "workload_branch_name",
                    "workload_branch_uuid"):
            state["variables"].pop(key, None)
        self.bucket.blob(f"workloads/{COMPONENT}/state.json") \
            .upload_from_string(json.dumps(state))
        commits, pushes = [], []
        with patch.object(dispatch, "run_elicitation", approve_elicitation), \
                patch.object(git_client, "stage_and_commit",
                             side_effect=lambda d, m, paths=None, user_email=None:
                             commits.append((d, m, paths, user_email))), \
                patch.object(git_client, "rebase_and_push",
                             side_effect=lambda d, b, user_email=None:
                             pushes.append((d, b, user_email))), \
                patch.object(actions.ssm_client, "SSMClient",
                             side_effect=RuntimeError("no ssm in tests")):
            out = await tools.run_workload_validation()
        self.assertIn("pull request was opened", out.lower())
        state = self.read_state()
        self.assertEqual(state["current_state"], "STATE_WKLD_DONE")
        branch = state["variables"]["workload_branch_name"]
        self.assertRegex(branch, rf"^migration/workload-{COMPONENT}-"
                                 r"[0-9a-f-]{36}$")
        self.assertEqual(len(commits), 1)
        self.assertEqual(commits[0][2], [f"workloads/{COMPONENT}"])
        self.assertEqual(pushes[0][1], branch)
        # The developer's resolved identity, carried on the session config
        # by _resolve_session, authors the commit and the rebase.
        self.assertEqual(commits[0][3], "dev-a@x.com")
        self.assertEqual(pushes[0][2], "dev-a@x.com")
        body = state["variables"]["workload_pr_body"]
        self.assertIn("q1", body)  # open questions ride the PR body
        shutil.rmtree(state["variables"]["workload_clone_path"],
                      ignore_errors=True)

    def test_ship_without_a_session_identity_fails_before_touching_git(self):
        # Mirrors the landing-zone guard: no resolved e-mail on the session
        # config means no commit under anyone's name, and no push.
        git_client.git.Repo.init(self.clone)
        variables = {"component": COMPONENT, "workload_branch_name": "b-1",
                     "workload_clone_path": self.clone,
                     "target_repo_url": "https://github.com/acme/infra",
                     "target_branch": "main"}
        with patch.object(git_client, "stage_and_commit") as commit, \
                patch.object(git_client, "rebase_and_push") as push:
            key, msg = actions.action_submit_workload_pr(variables, {})
        self.assertEqual(key, "on_failure")
        self.assertIn("caller identity", msg)
        commit.assert_not_called()
        push.assert_not_called()

    async def test_pr_failure_re_raises_the_ship_approval(self):
        """The deliberate divergence (§2.4): SUBMIT_PR's on_failure returns
        to STATE_WKLD_APPROVED, so the loop asks the human again — a second
        decline then parks at review with the push failure visible."""
        u = unit("wkld-manifests")
        self.write_state([u])
        self.write_blob(u)
        answers = [(True, {"approved": True}), (False, None)]
        asked = []

        async def flaky_elicitation(ctx, state_name, state_def, schema_cls,
                                    state_dict, config, notice=None):
            asked.append((state_name, notice))
            return answers.pop(0)

        with patch.object(dispatch, "run_elicitation", flaky_elicitation), \
                patch.object(git_client, "stage_and_commit",
                             return_value=None), \
                patch.object(git_client, "rebase_and_push",
                             side_effect=RuntimeError("remote said no")):
            out = await tools.run_workload_validation()
        self.assertEqual([name for name, _ in asked],
                         ["STATE_WKLD_APPROVED", "STATE_WKLD_APPROVED"])
        # The re-ask is not the identical static prompt: it names the failure.
        self.assertIsNone(asked[0][1])
        self.assertIn("remote said no", asked[1][1])
        self.assertIn("Git push failed", out)
        self.assertIn("remote said no", out)
        self.assertEqual(self.read_state()["current_state"],
                         "STATE_WKLD_REVIEW")

    async def test_clone_comes_from_exports_coordinates(self):
        """The realistic ledger: no root state.json, no coordinates in the
        component state — exports.target_repo alone carries the clone
        coordinates to a developer session (the only object its IAM can
        read them from)."""
        u = unit("wkld-manifests")
        self.write_state([u])
        self.write_blob(u)
        with patch.object(dispatch, "run_elicitation", decline_elicitation):
            out = await tools.run_workload_validation()
        self.assertIn("Validation passed", out)
        self.assertEqual(self.clones[0][:2], ("sso://target", "main"))
        state = self.read_state()
        self.assertEqual(state["variables"]["target_repo_url"], "sso://target")
        self.assertFalse(
            state["variables"].get("workload_clone_is_scratch", False))

    async def test_stale_scratch_leftover_self_heals_once_exports_publish(self):
        """A scratch directory left by a run before the coordinates were
        published (plus the stale scratch flag it persisted) is re-cloned
        and cleared on the next run — the retry changes the failing
        precondition instead of reporting it to a human."""
        u = unit("wkld-manifests")
        self.write_state([u])
        self.write_blob(u)
        state = self.read_state()
        state["variables"]["workload_clone_is_scratch"] = True
        self.bucket.blob(f"workloads/{COMPONENT}/state.json") \
            .upload_from_string(json.dumps(state))
        with patch.object(dispatch, "run_elicitation", decline_elicitation):
            out = await tools.run_workload_validation()
        self.assertIn("Validation passed", out)
        self.assertEqual(len(self.clones), 1)
        report = json.loads(self.bucket.blob(
            f"workloads/{COMPONENT}/validation-report.json")
            .download_as_text())
        self.assertFalse(report["clone"]["scratch"])
        self.assertFalse(
            self.read_state()["variables"].get("workload_clone_is_scratch",
                                               False))

    async def test_existing_clone_with_changed_origin_is_re_cloned(self):
        """Reuse is verified, never assumed: a work tree whose origin no
        longer matches the current coordinates is re-cloned from them."""
        u = unit("wkld-manifests")
        self.write_state([u])
        self.write_blob(u)
        repo = git_client.git.Repo.init(self.clone)
        repo.create_remote("origin", "sso://old-target")
        with patch.object(dispatch, "run_elicitation", decline_elicitation):
            out = await tools.run_workload_validation()
        self.assertIn("Validation passed", out)
        self.assertEqual(self.clones[0][:2], ("sso://target", "main"))

    async def test_matching_clone_is_reused_without_recloning(self):
        u = unit("wkld-manifests")
        self.write_state([u])
        self.write_blob(u)
        repo = git_client.git.Repo.init(self.clone)
        repo.create_remote("origin", "sso://target")
        with patch.object(dispatch, "run_elicitation", decline_elicitation):
            out = await tools.run_workload_validation()
        self.assertIn("Validation passed", out)
        self.assertEqual(self.clones, [])

    async def test_exports_coordinates_override_a_stale_variables_cache(self):
        """The variables tier is only the cache a previous resolution wrote
        back: exports.target_repo is AUTHORITATIVE over it. After the
        platform fixes a wrong URL (configure_repositories +
        refresh_exports), the next validation run must re-clone from the
        published coordinates — not reuse the stale clone with 'origin
        verified' and ship the PR to the old remote forever."""
        u = unit("wkld-manifests")
        self.write_state([u])
        self.write_blob(u)
        state = self.read_state()
        state["variables"]["target_repo_url"] = "sso://stale-repo"
        state["variables"]["target_branch"] = "old-base"
        self.bucket.blob(f"workloads/{COMPONENT}/state.json") \
            .upload_from_string(json.dumps(state))
        repo = git_client.git.Repo.init(self.clone)
        repo.create_remote("origin", "sso://stale-repo")
        with patch.object(dispatch, "run_elicitation", decline_elicitation):
            out = await tools.run_workload_validation()
        self.assertIn("Validation passed", out)
        self.assertEqual(self.clones[0][:2], ("sso://target", "main"))
        vs = self.read_state()["variables"]
        self.assertEqual(vs["target_repo_url"], "sso://target")
        self.assertEqual(vs["target_branch"], "main")

    async def test_forbidden_onboarding_read_names_the_developer_grant(self):
        """403 vs 404 on platform/onboarding/state.json are different facts
        and are never conflated. The 403 is the one a REAL developer
        session hits (§4.5: developers hold no platform/ grant, by design)
        — the message must say the failure is designed, and the scratch
        clone must still be a BLOCKING finding."""
        u = unit("wkld-manifests")
        self.write_state([u])
        self.write_blob(u)
        exports = {k: v for k, v in EXPORTS_DOC.items() if k != "target_repo"}
        self.bucket.blob("exports.json").upload_from_string(
            json.dumps(exports))
        real = fake_gcs.FakeBlob.download_as_text

        def deny_platform(blob_self, *args, **kwargs):
            if blob_self.name == "platform/onboarding/state.json":
                raise exceptions.Forbidden("developers hold no platform/ grant")
            return real(blob_self, *args, **kwargs)

        with patch.object(fake_gcs.FakeBlob, "download_as_text",
                          deny_platform):
            out = await tools.run_workload_validation()
        self.assertIn("FAILED", out)
        self.assertIn("SCRATCH", out)
        self.assertIn("a developer grant — by design", out)
        report = json.loads(self.bucket.blob(
            f"workloads/{COMPONENT}/validation-report.json")
            .download_as_text())
        self.assertTrue(report["clone"]["scratch"])
        self.assertTrue(report["clone"]["findings"])

    async def test_forbidden_exports_read_is_an_actionable_error(self):
        """A 403 on exports.json ITSELF — the one channel a developer may
        read — is an IAM defect (missing conditional read grant on a
        ledger bootstrapped before the grants existed, or a registry
        edited out of band). The tool answers in-band naming
        provision_ledger_iam, never a raw traceback into the MCP loop."""
        u = unit("wkld-manifests")
        self.write_state([u])
        self.write_blob(u)
        real = fake_gcs.FakeBlob.download_as_text

        def deny_exports(blob_self, *args, **kwargs):
            if blob_self.name == "exports.json":
                raise exceptions.Forbidden("no exports grant")
            return real(blob_self, *args, **kwargs)

        with patch.object(fake_gcs.FakeBlob, "download_as_text",
                          deny_exports):
            out = await tools.run_workload_validation()
        self.assertIn("ERROR", out)
        self.assertIn("provision_ledger_iam", out)
        self.assertIn("403", out)

    async def test_missing_onboarding_state_is_named_as_absent_not_denied(self):
        """The 404 half: no platform/onboarding/state.json exists at all
        (a ledger where configure_repositories never ran). Still the
        blocking scratch finding, with the absence named."""
        u = unit("wkld-manifests")
        self.write_state([u])
        self.write_blob(u)
        exports = {k: v for k, v in EXPORTS_DOC.items() if k != "target_repo"}
        self.bucket.blob("exports.json").upload_from_string(
            json.dumps(exports))
        out = await tools.run_workload_validation()
        self.assertIn("FAILED", out)
        self.assertIn("SCRATCH", out)
        self.assertIn("no platform/onboarding/state.json exists", out)
        report = json.loads(self.bucket.blob(
            f"workloads/{COMPONENT}/validation-report.json")
            .download_as_text())
        self.assertTrue(report["clone"]["scratch"])
        self.assertTrue(report["clone"]["findings"])

    async def test_foreign_clone_path_is_re_derived_not_used(self):
        """A clone path recorded by another machine (or hand-edited) is
        outside this machine's scratch root: it is re-derived from the
        branch uuid, never cloned into or cleared verbatim."""
        u = unit("wkld-manifests")
        self.write_state([u])
        self.write_blob(u)
        foreign = tempfile.mkdtemp(prefix="foreign_clone_")
        self.addCleanup(shutil.rmtree, foreign, True)
        state = self.read_state()
        state["variables"]["workload_clone_path"] = foreign
        self.bucket.blob(f"workloads/{COMPONENT}/state.json") \
            .upload_from_string(json.dumps(state))
        with patch.object(dispatch, "run_elicitation", decline_elicitation):
            out = await tools.run_workload_validation()
        self.assertIn("Validation passed", out)
        healed = self.read_state()["variables"]["workload_clone_path"]
        self.assertNotEqual(healed, foreign)
        self.assertTrue(healed.startswith(self.scratch_root + os.sep))
        self.assertEqual(self.clones[0][2], healed)

    async def test_deterministic_recheck_blocks_a_missed_rewrite(self):
        """A plain file shipping a value the deterministic pass would still
        rewrite (here: an IRSA role-arn whose gsa_bindings email is
        published) is a BLOCKING finding — 'validated' must never label
        output the server knows is wrong."""
        u = unit("wkld-manifests")
        self.write_state([u])
        sa = ("apiVersion: v1\nkind: ServiceAccount\nmetadata:\n"
              "  name: orders\n  namespace: acme-shop\n  annotations:\n"
              "    eks.amazonaws.com/role-arn: arn:aws:iam::1:role/orders\n")
        self.write_blob(u, content=sa)
        out = await tools.run_workload_validation()
        self.assertIn("FAILED", out)
        self.assertIn("deterministic re-check", out)
        self.assertIn("iam.gke.io/gcp-service-account", out)
        report = json.loads(self.bucket.blob(
            f"workloads/{COMPONENT}/validation-report.json")
            .download_as_text())
        self.assertTrue(report["transforms"]["blocking"])
        self.assertEqual(self.read_state()["current_state"],
                         "STATE_WKLD_REVIEW")

    async def test_honest_gaps_stay_non_blocking_in_the_recheck(self):
        """An unmapped image is an open question, not a change: honest gaps
        must not deadlock review (they were already routed to the unit's
        open questions at translate time)."""
        u = unit("wkld-manifests")
        self.write_state([u])
        dep = ("apiVersion: apps/v1\nkind: Deployment\nmetadata:\n"
               "  name: web\nspec:\n  template:\n    spec:\n"
               "      containers:\n      - name: app\n"
               "        image: 111.dkr.ecr.us-east-1.amazonaws.com/x/y:z\n")
        self.write_blob(u, content=dep)
        with patch.object(dispatch, "run_elicitation", decline_elicitation):
            out = await tools.run_workload_validation()
        self.assertIn("Validation passed", out)
        report = json.loads(self.bucket.blob(
            f"workloads/{COMPONENT}/validation-report.json")
            .download_as_text())
        self.assertEqual(report["transforms"]["blocking"], [])
        self.assertGreater(report["transforms"]["advisory"], 0)

    def routing_exports(self):
        doc = dict(EXPORTS_DOC, gateway={"name": "shared-entrypoint",
                                         "namespace": "platform-gateway"})
        self.bucket.blob("exports.json").upload_from_string(json.dumps(doc))

    @staticmethod
    def httproute(parent_refs):
        refs = json.dumps(parent_refs)
        return ("apiVersion: gateway.networking.k8s.io/v1\n"
                "kind: HTTPRoute\nmetadata:\n  name: shop\n"
                "  namespace: acme-shop\nspec:\n"
                f"  parentRefs: {refs}\n"
                "  rules:\n  - backendRefs:\n    - name: web\n      port: 80\n")

    async def test_routing_contract_deviation_is_a_blocking_finding(self):
        """The consumer half of the exports.gateway contract: a shipped
        HTTPRoute whose parentRefs is not the verbatim published attach
        point (or carries a sectionName) blocks — the brief mandates the
        copy, this gate catches the drift."""
        self.routing_exports()
        u = unit("wkld-routing")
        self.write_state([u])
        self.write_blob(u, content=self.httproute(
            [{"name": "invented-gw", "namespace": "platform-gateway"}]))
        out = await tools.run_workload_validation()
        self.assertIn("FAILED", out)
        self.assertIn("verbatim-copy contract", out)
        self.assertEqual(self.read_state()["current_state"],
                         "STATE_WKLD_REVIEW")
        report = json.loads(self.bucket.blob(
            f"workloads/{COMPONENT}/validation-report.json")
            .download_as_text())
        self.assertEqual(report["routing"][0]["route"], "shop")

    async def test_routing_section_name_is_refused(self):
        self.routing_exports()
        u = unit("wkld-routing")
        self.write_state([u])
        self.write_blob(u, content=self.httproute(
            [{"name": "shared-entrypoint", "namespace": "platform-gateway",
              "sectionName": "http"}]))
        out = await tools.run_workload_validation()
        self.assertIn("FAILED", out)
        self.assertIn("sectionName", out)

    async def test_routing_verbatim_parent_refs_pass(self):
        self.routing_exports()
        u = unit("wkld-routing")
        self.write_state([u])
        self.write_blob(u, content=self.httproute(
            [{"name": "shared-entrypoint", "namespace": "platform-gateway"}]))
        with patch.object(dispatch, "run_elicitation", decline_elicitation):
            out = await tools.run_workload_validation()
        self.assertIn("Validation passed", out)

    async def test_routing_route_without_published_gateway_blocks(self):
        """A done routing blob surviving a gateway retraction must not ship
        an unattachable route silently."""
        u = unit("wkld-routing")
        self.write_state([u])
        self.write_blob(u, content=self.httproute(
            [{"name": "shared-entrypoint", "namespace": "platform-gateway"}]))
        out = await tools.run_workload_validation()
        self.assertIn("FAILED", out)
        self.assertIn("no complete attach point", out)

    async def test_claimant_is_enforced(self):
        u = unit("wkld-manifests")
        self.write_state([u])
        self.write_blob(u)
        with patch.object(state_mgr, "get_authenticated_user_email",
                          return_value="dev-b@x.com"):
            out = await tools.run_workload_validation()
        self.assertIn("claimed by dev-a@x.com", out)

    async def test_wrong_state_is_refused(self):
        u = unit("wkld-manifests")
        self.write_state([u], current="STATE_WKLD_REVIEW")
        out = await tools.run_workload_validation()
        self.assertIn("ERROR: Invalid state", out)


class TransformGateUnitTest(unittest.TestCase):
    """_transform_gate over rendered chart text — the chart-carrier arm: a
    rendered document still carrying a mapped-replicated source ref proves
    the carrier transcription missed a rewrite, and blocks."""

    ECR = "111.dkr.ecr.us-east-1.amazonaws.com/acme/orders:v1"
    DEST = "us-docker.pkg.dev/proj/repo/orders:v1"

    def rendered(self, image):
        text = ("apiVersion: apps/v1\nkind: Deployment\nmetadata:\n"
                "  name: orders\nspec:\n  template:\n    spec:\n"
                "      containers:\n      - name: app\n"
                f"        image: {image}\n")
        return [({"dir": "workloads/c/charts/orders", "kind": "helm"}, text)]

    def test_rendered_chart_with_mapped_ecr_ref_is_blocking(self):
        gate = tools._transform_gate(
            "/nonexistent", self.rendered(self.ECR), [], EXPORTS_DOC)
        self.assertEqual(len(gate["blocking"]), 1)
        finding = gate["blocking"][0]
        self.assertEqual(finding["source"], "workloads/c/charts/orders")
        self.assertIn(self.DEST, finding["error"])

    def test_rendered_chart_with_transcribed_ref_is_clean(self):
        gate = tools._transform_gate(
            "/nonexistent", self.rendered(self.DEST), [], EXPORTS_DOC)
        self.assertEqual(gate["blocking"], [])
        self.assertEqual(gate["checked"], 1)



class RenderReleaseNameTest(unittest.TestCase):
    """validate's re-render of a materialized root chart passes the SOURCE
    path's release name, so `.Release.Name`-derived names match the plan's."""

    def test_helm_rerender_uses_the_source_release(self):
        seen = {}

        def fake_render(root, chart_dir, release=None):
            seen["args"] = (chart_dir, release)
            return GOOD_DOC, None

        with patch.object(tools.translator, "render_chart", fake_render):
            findings, rendered = tools._render_findings(
                "/clone", [{"kind": "helm", "dir": "workloads/c/chart", "source": "."}])
        self.assertEqual(seen["args"], ("workloads/c/chart", "wkld-render"))
        self.assertEqual(findings, [])
        self.assertEqual(len(rendered), 1)
        self.assertEqual(tools._rendered_documents(rendered)[0][0], "workloads/c/chart")
        self.assertEqual(tools._rendered_documents(rendered)[0][1]["kind"], "Service")


class PodDnsGateTest(ValidateToolTest):
    """The pod DNS contract wired into the validate step: a finding blocks
    and routes to review with the report key; a blob without facts is
    reported as not conservation-checked and passes; a clean translation
    with the removed address named passes."""

    FACTS = [{"label": "mailer", "kind": "Job", "namespace": "acme-shop", "name": "mailer",
              "node_path": "spec.template.spec", "dns_policy": "None", "host_network": False,
              "nameservers": ["172.20.0.10", "10.20.0.53"], "searches": [],
              "options": [], "host_aliases": []}]

    @staticmethod
    def job(policy, nameservers):
        cfg = ("  dnsConfig:\n    nameservers:\n"
               + "".join(f"    - {n}\n" for n in nameservers)) if nameservers else ""
        return ("apiVersion: batch/v1\nkind: Job\nmetadata:\n  name: mailer\n"
                "  namespace: acme-shop\nspec:\n  template:\n    spec:\n"
                f"      dnsPolicy: {policy}\n"
                + cfg.replace("  dnsConfig", "      dnsConfig").replace("    nameservers", "        nameservers").replace("    - ", "        - ")
                + "      containers:\n      - name: m\n        image: x\n")

    def facted_unit(self):
        u = unit("wkld-manifests")
        u["inputs"]["pod_dns_facts"] = list(self.FACTS)
        return u

    async def test_none_kept_over_the_cluster_dns_address_blocks(self):
        u = self.facted_unit()
        self.write_state([u])
        self.write_blob(u, content=self.job("None", ["10.20.0.53"]))
        out = await tools.run_workload_validation()
        self.assertIn("FAILED", out)
        self.assertIn("resolves no cluster name", out)
        self.assertIn("pod DNS contract finding(s)", out)
        self.assertEqual(self.read_state()["current_state"], "STATE_WKLD_REVIEW")
        report = json.loads(self.bucket.blob(
            f"workloads/{COMPONENT}/validation-report.json").download_as_text())
        self.assertEqual(report["pod_dns"]["findings"][0]["subject"], "Job/mailer")
        self.assertEqual(report["pod_dns"]["checked"], 1)

    async def test_clusterfirst_with_the_rest_named_passes(self):
        u = self.facted_unit()
        self.write_state([u])
        payload = blob_payload(u, content=self.job("ClusterFirst", []))
        payload["result"]["tradeoffs"] = "172.20.0.10 dropped: the node resolves cluster names on GKE"
        payload["result"]["open_questions"] = ["10.20.0.53 belongs in the cluster-dns unit"]
        self.bucket.blob(f"workloads/{COMPONENT}/units/{u['unit_id']}.json") \
            .upload_from_string(json.dumps(payload))
        with patch.object(dispatch, "run_elicitation", decline_elicitation):
            out = await tools.run_workload_validation()
        self.assertIn("Validation passed", out)
        self.assertIn("0 pod DNS contract finding(s) over 1 pod spec(s)", out)

    async def test_a_blob_without_facts_is_not_conservation_checked(self):
        u = unit("wkld-manifests")  # no pod_dns_facts key: a pre-field plan
        self.write_state([u])
        self.write_blob(u, content=self.job("None", ["10.20.0.53"]))
        with patch.object(dispatch, "run_elicitation", decline_elicitation):
            out = await tools.run_workload_validation()
        self.assertIn("Validation passed", out)
        self.assertIn("1 unit(s) without pod_dns_facts, not conservation-checked", out)
        self.assertNotIn("facts incomplete", out)


if __name__ == "__main__":
    unittest.main()
