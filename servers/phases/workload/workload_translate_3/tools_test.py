"""Tool-layer tests for run_workload_translation (STATE_WKLD_TRANSLATE).

FakeGCS at the blob level. Covers: the four-conjunct reuse predicate truth
table (each conjunct violated singly + the all-true reuse), lease refusal,
CAS single winner, superseded-token discard, incremental persistence with
run + exports-generations stamps, parked-unit exclusion, claimant
rejection, and the deterministic-transforms integration (rewrite applied,
flags routed to the envelope, double-run determinism).
"""

import asyncio
import json
import os
import shutil
import tempfile
import time
import unittest
from unittest.mock import patch

from google.api_core import exceptions

import servers.dag.state_management as state_mgr
from servers.dag import fake_gcs
from servers.phases.workload.workload_plan_2 import planner
from servers.phases.workload.workload_translate_3 import tools, translator

COMPONENT = "orders-component"
REGISTRY = {
    "workspace_name": "ws-dev", "gcp_project": "proj",
    "roles": {"developers": ["dev-a@x.com", "dev-b@x.com"]},
}
GENERATIONS = {"discovery": 3, "translation": 1, "deployment": 0}
EXPORTS_DOC = {
    "gateway": None, "cluster": None, "node_shapes": [],
    "storage_class_menu": ["standard-rwo"],
    "gsa_bindings": {"shop/web-sa": "web@proj.iam.gserviceaccount.com"},
    "artifact_registry": {"image_map": {
        "111.dkr.ecr.us-east-1.amazonaws.com/web:1": {
            "dest_ref": "us-docker.pkg.dev/proj/repo/web:1",
            "status": "replicated"}}},
    "staging_bucket": None,
    "generated_at": "2026-08-14T00:00:00+00:00",
    "generations": GENERATIONS,
}

APP_DOC = ("apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: web\n"
           "  namespace: shop\nspec:\n  template:\n    spec:\n      "
           "containers:\n      - name: web\n        image: "
           "111.dkr.ecr.us-east-1.amazonaws.com/web:1\n")

_DAG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         "..", "..", "..", "dag", "developer_dag.json")


def make_unit(unit_id="wkld-manifests", status="planned", rendered=None):
    return {"unit_id": unit_id, "family": unit_id, "title": unit_id,
            "status": status, "planned_status": status, "placeholder": False,
            "inputs": {"documents": [
                {"path": "k8s/app.yaml", "doc_index": 0, "kind": "Deployment",
                 "namespace": "shop", "name": "web",
                 "classification": "portable", "rendered_from": rendered}]},
            "notes": [], "feedback": None, "error": None}


def make_plan(units):
    return {"component": COMPONENT, "units": units,
            "sources": {"charts": [], "kustomize": []}, "carriers": [],
            "notes": [],
            "exports_stamp": {
                "generated_at": "T0",
                "generations": dict(GENERATIONS),
                "non_gateway_digest": planner.non_gateway_digest(
                    EXPORTS_DOC)}}


class ReusePredicateTest(unittest.TestCase):
    """The truth table: each conjunct violated singly + the all-true row."""

    def payload(self, **overrides):
        base = {"unit": {"unit_id": "wkld-manifests", "status": "done"},
                "result": {"files": [{"path": "a.yaml", "content": "x"}],
                           "tradeoffs": "t"},
                "run": 1, "exports_generations": dict(GENERATIONS)}
        base.update(overrides)
        return base

    def test_all_four_conjuncts_true_reuses(self):
        ok, reason = tools.blob_reusable(
            self.payload(), make_unit(), 2, dict(GENERATIONS))
        self.assertTrue(ok, reason)

    def test_a_missing_or_failed_blob_refuses(self):
        ok, reason = tools.blob_reusable(None, make_unit(), 2, GENERATIONS)
        self.assertFalse(ok)
        ok, reason = tools.blob_reusable(
            self.payload(result=None,
                         unit={"unit_id": "u", "status": "error"}),
            make_unit(), 2, GENERATIONS)
        self.assertFalse(ok)
        self.assertIn("no persisted successful result", reason)

    def test_b_revise_refuses(self):
        ok, reason = tools.blob_reusable(
            self.payload(), make_unit(status="revise"), 2, GENERATIONS)
        self.assertFalse(ok)
        self.assertIn("revise", reason)

    def test_c_run_token_above_counter_refuses(self):
        ok, reason = tools.blob_reusable(
            self.payload(run=5), make_unit(), 2, GENERATIONS)
        self.assertFalse(ok)
        self.assertIn("exceeds", reason)
        # equality passes; an unstamped blob fails closed
        ok, _ = tools.blob_reusable(self.payload(run=2), make_unit(), 2,
                                    GENERATIONS)
        self.assertTrue(ok)
        unstamped = self.payload()
        del unstamped["run"]
        ok, reason = tools.blob_reusable(unstamped, make_unit(), 2, GENERATIONS)
        self.assertFalse(ok)
        self.assertIn("no run token", reason)

    def test_d_exports_generation_mismatch_refuses(self):
        stale = dict(GENERATIONS, discovery=99)
        ok, reason = tools.blob_reusable(
            self.payload(exports_generations=stale), make_unit(), 2,
            dict(GENERATIONS))
        self.assertFalse(ok)
        self.assertIn("generations", reason)
        unstamped = self.payload()
        del unstamped["exports_generations"]
        ok, reason = tools.blob_reusable(unstamped, make_unit(), 2, GENERATIONS)
        self.assertFalse(ok)


class _FanOutBase(unittest.IsolatedAsyncioTestCase):
    """Shared FakeGCS harness for run_workload_translation tests."""

    def setUp(self):
        self._saved = (state_mgr.LEDGER_CONFIG_PATH, state_mgr.LEDGER_CONFIG_DIR,
                       state_mgr.gcs_client)
        state_mgr.LEDGER_CONFIG_PATH = "/tmp/test_wkldtr_config.yaml"
        state_mgr.LEDGER_CONFIG_DIR = "/tmp/test_wkldtr_config.d"
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
        self._auth = patch.object(translator, "check_llm_auth",
                                  return_value="")
        self._auth.start()
        self.root = tempfile.mkdtemp(prefix="wkld_tr_tool_")
        self.addCleanup(shutil.rmtree, self.root, True)
        os.makedirs(os.path.join(self.root, "k8s"))
        with open(os.path.join(self.root, "k8s", "app.yaml"), "w") as f:
            f.write(APP_DOC)

    def tearDown(self):
        self._email.stop()
        self._auth.stop()
        shutil.rmtree(state_mgr.LEDGER_CONFIG_DIR, ignore_errors=True)
        (state_mgr.LEDGER_CONFIG_PATH, state_mgr.LEDGER_CONFIG_DIR,
         state_mgr.gcs_client) = self._saved

    def write_state(self, plan, current="STATE_WKLD_TRANSLATE",
                    variables=None):
        state = {
            "current_state": current,
            "history": [],
            "variables": {"component": COMPONENT,
                          "claim": {"claimant": "dev-a@x.com",
                                    "claimed_at": "T0"},
                          "workload_plan": plan,
                          "workload_source_root": self.root,
                          **(variables or {})},
        }
        self.bucket.blob(f"workloads/{COMPONENT}/state.json") \
            .upload_from_string(json.dumps(state))

    def read_state(self):
        return json.loads(self.bucket.blob(
            f"workloads/{COMPONENT}/state.json").download_as_text())

    def worker_returning(self, result):
        async def fake_worker(prompt, model):
            return json.dumps(result)
        return fake_worker

    GOOD_RESULT = {
        "files": [{"path": "deployment.yaml", "content": APP_DOC}],
        "tradeoffs": "kept the deployment near-unchanged",
        "assumptions": [], "open_questions": [],
    }


class FanOutTest(_FanOutBase):
    """run_workload_translation against FakeGCS with the LLM stubbed."""

    async def test_fanout_persists_stamped_blobs_and_advances(self):
        self.write_state(make_plan([make_unit()]))
        with patch.object(translator, "_run_worker",
                          self.worker_returning(self.GOOD_RESULT)):
            out = await tools.run_workload_translation()
        self.assertIn("SUCCESS", out)
        state = self.read_state()
        self.assertEqual(state["current_state"], "STATE_WKLD_REVIEW")
        self.assertEqual(state["variables"]["workload_run"], 1)
        self.assertNotIn("workload_run_active", state["variables"])
        blob = json.loads(self.bucket.blob(
            f"workloads/{COMPONENT}/units/wkld-manifests.json")
            .download_as_text())
        self.assertEqual(blob["run"], 1)
        self.assertEqual(blob["exports_generations"], GENERATIONS)
        self.assertEqual(blob["unit"]["status"], "done")
        # The deterministic pass rewrote the mapped image in the FILE.
        content = blob["result"]["files"][0]["content"]
        self.assertIn("us-docker.pkg.dev/proj/repo/web:1", content)
        self.assertNotIn("dkr.ecr", content)

    async def test_transforms_flags_route_to_envelope_and_are_deterministic(self):
        unmapped = APP_DOC.replace(
            "111.dkr.ecr.us-east-1.amazonaws.com/web:1", "ghost:latest")
        result = {**self.GOOD_RESULT,
                  "files": [{"path": "deployment.yaml", "content": unmapped}]}
        contents = []
        for _ in range(2):  # determinism: same input -> same persisted output
            self.write_state(make_plan([make_unit()]))
            blob = self.bucket.blob(
                f"workloads/{COMPONENT}/units/wkld-manifests.json")
            if blob.exists():
                blob.delete()
            with patch.object(translator, "_run_worker",
                              self.worker_returning(result)):
                out = await tools.run_workload_translation()
            self.assertIn("SUCCESS", out)
            blob = json.loads(self.bucket.blob(
                f"workloads/{COMPONENT}/units/wkld-manifests.json")
                .download_as_text())
            contents.append(json.dumps(blob["result"], sort_keys=True))
            questions = " ".join(blob["result"]["open_questions"])
            self.assertIn("no image_map entry", questions)
            self.assertIn("[deterministic pass]", questions)
        self.assertEqual(contents[0], contents[1])

    async def test_parked_units_are_excluded_from_the_fanout(self):
        plan = make_plan([make_unit(),
                          make_unit("wkld-routing", status="parked")])
        self.write_state(plan)
        seen = []

        async def spy_worker(prompt, model):
            seen.append(prompt)
            return json.dumps(self.GOOD_RESULT)

        with patch.object(translator, "_run_worker", spy_worker):
            out = await tools.run_workload_translation()
        self.assertIn("SUCCESS", out)
        self.assertIn("Parked", out)
        self.assertIn("wkld-routing", out)
        self.assertEqual(len(seen), 1)  # only the manifests unit ran
        self.assertFalse(self.bucket.blob(
            f"workloads/{COMPONENT}/units/wkld-routing.json").exists())

    async def test_lease_refuses_a_second_fanout(self):
        import time as time_mod
        self.write_state(make_plan([make_unit()]), variables={
            "workload_run": 1,
            "workload_run_active": {"run": 1,
                                    "claimed_at": time_mod.time()}})
        out = await tools.run_workload_translation()
        self.assertIn("ERROR", out)
        self.assertIn("in flight", out)

    async def test_an_expired_lease_does_not_block_the_next_run(self):
        """The crashed-run recovery the reuse predicate exists to enable:
        a process killed mid-fan-out leaves workload_run_active behind, and
        after LEASE_SECONDS the next run must take it, not refuse forever."""
        import time as time_mod
        self.write_state(make_plan([make_unit()]), variables={
            "workload_run": 1,
            "workload_run_active": {
                "run": 1,
                "claimed_at": time_mod.time() - tools.LEASE_SECONDS - 1}})
        with patch.object(translator, "_run_worker",
                          self.worker_returning(self.GOOD_RESULT)):
            out = await tools.run_workload_translation()
        self.assertNotIn("in flight", out)
        self.assertEqual(self.read_state()["current_state"],
                         "STATE_WKLD_REVIEW")

    async def test_a_lease_claimed_in_the_future_is_refused_not_ignored(self):
        """A negative age means the claimant's clock is ahead of this one
        (an NTP step, a laptop and a VM). Treating that as 'not in flight'
        disabled the lease exactly when two machines were involved."""
        import time as time_mod
        self.write_state(make_plan([make_unit()]), variables={
            "workload_run": 1,
            "workload_run_active": {"run": 1,
                                    "claimed_at": time_mod.time() + 600}})
        out = await tools.run_workload_translation()
        self.assertIn("ERROR", out)
        self.assertIn("in flight", out)
        self.assertIn("FUTURE", out)

    async def test_the_lease_is_released_when_the_run_cannot_continue(self):
        """A refusal must not leave the component leased for an hour."""
        self.write_state(make_plan([make_unit()]))

        async def usurping_worker(prompt, model):
            state = self.read_state()
            state["variables"]["workload_run"] = 2
            self.bucket.blob(f"workloads/{COMPONENT}/state.json") \
                .upload_from_string(json.dumps(state))
            return json.dumps(self.GOOD_RESULT)

        with patch.object(translator, "_run_worker", usurping_worker):
            out = await tools.run_workload_translation()
        self.assertIn("superseded", out)
        self.assertIsNone(
            self.read_state()["variables"].get("workload_run_active"))

    async def test_a_unit_skipped_during_the_fanout_is_not_resurrected(self):
        """skip_workload_units is callable from STATE_WKLD_TRANSLATE on
        purpose. The apply loop writes into the RE-READ plan, so a unit
        skipped while the workers ran must keep its new status."""
        self.write_state(make_plan([make_unit()]))

        async def skipping_worker(prompt, model):
            state = self.read_state()
            state["variables"]["workload_plan"]["units"][0]["status"] = \
                "skipped"
            self.bucket.blob(f"workloads/{COMPONENT}/state.json") \
                .upload_from_string(json.dumps(state))
            return json.dumps(self.GOOD_RESULT)

        with patch.object(translator, "_run_worker", skipping_worker):
            out = await tools.run_workload_translation()
        self.assertIn("discarded", out)
        self.assertIn("wkld-manifests", out)
        self.assertEqual(
            self.read_state()["variables"]["workload_plan"]["units"][0]
            ["status"], "skipped")

    async def test_a_discarded_unit_is_named_in_the_success_summary(self):
        """One unit skipped mid-run, one still done: the run advances and
        says which unit's output it threw away."""
        self.write_state(make_plan([make_unit(),
                                    make_unit("wkld-identity")]))

        async def skipping_worker(prompt, model):
            state = self.read_state()
            for unit in state["variables"]["workload_plan"]["units"]:
                if unit["unit_id"] == "wkld-identity":
                    unit["status"] = "skipped"
            self.bucket.blob(f"workloads/{COMPONENT}/state.json") \
                .upload_from_string(json.dumps(state))
            return json.dumps(self.GOOD_RESULT)

        with patch.object(translator, "_run_worker", skipping_worker):
            out = await tools.run_workload_translation()
        self.assertIn("SUCCESS", out)
        self.assertIn("Discarded", out)
        self.assertIn("wkld-identity (skipped)", out)
        units = {u["unit_id"]: u["status"] for u in
                 self.read_state()["variables"]["workload_plan"]["units"]}
        self.assertEqual(units, {"wkld-manifests": "done",
                                 "wkld-identity": "skipped"})

    async def test_cas_produces_a_single_winner(self):
        self.write_state(make_plan([make_unit()]))
        state_blob_name = f"workloads/{COMPONENT}/state.json"
        real_blob = self.bucket.blob

        def racing_blob(name):
            blob = real_blob(name)
            if name == state_blob_name:
                real_upload = blob.upload_from_string

                def race_once(data, **kwargs):
                    if kwargs.get("if_generation_match") is not None \
                            and not getattr(race_once, "raced", False):
                        race_once.raced = True
                        raise exceptions.PreconditionFailed("raced")
                    return real_upload(data, **kwargs)
                blob.upload_from_string = race_once
            return blob

        with patch.object(self.bucket, "blob", side_effect=racing_blob):
            out = await tools.run_workload_translation()
        self.assertIn("ERROR", out)
        self.assertIn("claimed this component first", out)

    async def test_superseded_token_discards_the_run(self):
        self.write_state(make_plan([make_unit()]))

        async def usurping_worker(prompt, model):
            # While this run is fanned out, a second run claims token 2.
            state = self.read_state()
            state["variables"]["workload_run"] = 2
            self.bucket.blob(f"workloads/{COMPONENT}/state.json") \
                .upload_from_string(json.dumps(state))
            return json.dumps(self.GOOD_RESULT)

        with patch.object(translator, "_run_worker", usurping_worker):
            out = await tools.run_workload_translation()
        self.assertIn("ERROR", out)
        self.assertIn("superseded", out)
        # The incremental blob DID land (reusable data for the next run).
        self.assertTrue(self.bucket.blob(
            f"workloads/{COMPONENT}/units/wkld-manifests.json").exists())
        state = self.read_state()
        self.assertEqual(
            state["variables"]["workload_plan"]["units"][0]["status"],
            "planned")

    async def test_reuse_skips_workers_and_advances(self):
        plan = make_plan([make_unit()])
        self.write_state(plan, variables={"workload_run": 1})
        self.bucket.blob(
            f"workloads/{COMPONENT}/units/wkld-manifests.json") \
            .upload_from_string(json.dumps({
                "unit": {**make_unit(), "status": "done"},
                "result": self.GOOD_RESULT, "run": 1,
                "exports_generations": dict(GENERATIONS)}))

        async def exploding_worker(prompt, model):
            raise AssertionError("no worker may run — the blob is reusable")

        with patch.object(translator, "_run_worker", exploding_worker):
            out = await tools.run_workload_translation()
        self.assertIn("SUCCESS", out)
        self.assertIn("1 reused", out)
        self.assertEqual(self.read_state()["current_state"],
                         "STATE_WKLD_REVIEW")

    async def test_stale_exports_blob_is_not_reused(self):
        """The staleness arm's core: conjunct (d) refuses a blob stamped
        against different exports generations, so the unit re-runs."""
        plan = make_plan([make_unit()])
        self.write_state(plan, variables={"workload_run": 1})
        self.bucket.blob(
            f"workloads/{COMPONENT}/units/wkld-manifests.json") \
            .upload_from_string(json.dumps({
                "unit": {**make_unit(), "status": "done"},
                "result": self.GOOD_RESULT, "run": 1,
                "exports_generations": dict(GENERATIONS, discovery=99)}))
        ran = []

        async def counting_worker(prompt, model):
            ran.append(1)
            return json.dumps(self.GOOD_RESULT)

        with patch.object(translator, "_run_worker", counting_worker):
            out = await tools.run_workload_translation()
        self.assertIn("SUCCESS", out)
        self.assertEqual(len(ran), 1)  # re-translated, NOT reused
        self.assertIn("0 reused", out)

    async def test_claimant_is_enforced(self):
        self.write_state(make_plan([make_unit()]))
        with patch.object(state_mgr, "get_authenticated_user_email",
                          return_value="dev-b@x.com"):
            out = await tools.run_workload_translation()
        self.assertIn("ERROR", out)
        self.assertIn("claimed by dev-a@x.com", out)

    async def test_wrong_state_is_refused(self):
        self.write_state(make_plan([make_unit()]), current="STATE_WKLD_REVIEW")
        out = await tools.run_workload_translation()
        self.assertIn("ERROR: Invalid state", out)

    async def test_incremental_persist_lands_before_the_final_write(self):
        """The on_result callback writes the blob the moment the worker
        finishes — assert it is already in the bucket DURING the fan-out."""
        self.write_state(make_plan([make_unit()]))
        observed = {}

        async def worker_then_check(prompt, model):
            return json.dumps(self.GOOD_RESULT)

        real_persist = {}

        async def spying_translate_all(units, worker_inputs, render_ctx=None,
                                       concurrency=None, on_result=None,
                                       post_pass=None):
            unit = units[0]
            result = post_pass(unit, dict(self.GOOD_RESULT))
            await on_result(unit, result, None)
            observed["blob_exists_mid_run"] = self.bucket.blob(
                f"workloads/{COMPONENT}/units/wkld-manifests.json").exists()
            return {"results": {unit["unit_id"]: result}, "errors": {}}

        with patch.object(translator, "translate_all", spying_translate_all):
            out = await tools.run_workload_translation()
        self.assertIn("SUCCESS", out)
        self.assertTrue(observed["blob_exists_mid_run"])


GATEWAY_GENERATIONS = {"discovery": 3, "translation": 2, "deployment": 0}
GATEWAY_EXPORTS_DOC = {
    **EXPORTS_DOC,
    "gateway": {"name": "shared-gateway", "namespace": "gateway-infra"},
    "generated_at": "2026-08-15T01:00:00+00:00",
    "generations": GATEWAY_GENERATIONS,
}

ING_DOC = ("apiVersion: networking.k8s.io/v1\nkind: Ingress\nmetadata:\n"
           "  name: web\n  namespace: shop\nspec:\n  rules:\n"
           "  - host: shop.acme.example\n    http:\n      paths:\n"
           "      - path: /\n        pathType: Prefix\n        backend:\n"
           "          service: {name: frontend, port: {number: 80}}\n")


def routing_unit(status="parked"):
    unit = make_unit("wkld-routing", status=status)
    unit["inputs"]["documents"] = [
        {"path": "k8s/ing.yaml", "doc_index": 0, "kind": "Ingress",
         "namespace": "shop", "name": "web", "classification": "portable",
         "rendered_from": None}]
    unit["inputs"]["ingress_facts"] = [
        {"label": "web", "namespace": "shop",
         "rules": [{"host": "shop.acme.example", "paths": [
             {"path": "/", "path_type": "Prefix", "service": "frontend",
              "port_number": 80, "port_name": None}]}],
         "default_backend": False, "tls": False, "ingress_class_name": None,
         "annotations": ["alb.ingress.kubernetes.io/scheme"]}]
    return unit


class UnparkAtTranslateEntryTest(_FanOutBase):
    """M4 §2.3: parked routing units unpark when exports now has a gateway,
    the plan is re-stamped, and old-generation blobs honestly re-run."""

    def setUp(self):
        super().setUp()
        with open(os.path.join(self.root, "k8s", "ing.yaml"), "w") as f:
            f.write(ING_DOC)

    def publish_gateway_exports(self):
        self.bucket.blob("exports.json").upload_from_string(
            json.dumps(GATEWAY_EXPORTS_DOC))

    def read_plan_blob(self):
        return json.loads(self.bucket.blob(
            f"workloads/{COMPONENT}/plan.json").download_as_text())

    async def test_unpark_restamps_and_invalidates_old_blobs(self):
        self.publish_gateway_exports()
        # A previous run's blob, valid against the OLD stamp: without the
        # unpark it would be reused; the re-stamp must fail it on (d).
        self.bucket.blob(
            f"workloads/{COMPONENT}/units/wkld-manifests.json") \
            .upload_from_string(json.dumps({
                "unit": {"unit_id": "wkld-manifests", "status": "done"},
                "result": {"files": [{"path": "a.yaml", "content": APP_DOC}],
                           "tradeoffs": "t"},
                "run": 1, "exports_generations": dict(GENERATIONS)}))
        self.write_state(make_plan([make_unit(), routing_unit("parked")]),
                         variables={"workload_run": 1})
        prompts = []

        async def spy_worker(prompt, model):
            prompts.append(prompt)
            return json.dumps(self.GOOD_RESULT)

        with patch.object(translator, "_run_worker", spy_worker):
            out = await tools.run_workload_translation()
        self.assertIn("SUCCESS", out)
        self.assertIn("Unparked at translate entry: wkld-routing", out)
        self.assertNotIn("Reused", out)  # conjunct (d) fired on the re-stamp
        self.assertEqual(len(prompts), 2)  # BOTH units re-ran
        plan = self.read_plan_blob()
        self.assertEqual(plan["exports_stamp"]["generations"],
                         GATEWAY_GENERATIONS)
        statuses = {u["unit_id"]: u["status"] for u in plan["units"]}
        self.assertEqual(statuses["wkld-routing"], "done")
        routing = json.loads(self.bucket.blob(
            f"workloads/{COMPONENT}/units/wkld-routing.json")
            .download_as_text())
        self.assertEqual(routing["exports_generations"], GATEWAY_GENERATIONS)
        state = self.read_state()
        self.assertEqual(state["current_state"], "STATE_WKLD_REVIEW")
        self.assertTrue(any("Unparked wkld-routing" in h
                            for h in state["history"]))

    async def test_null_gateway_keeps_the_parked_exclusion(self):
        # exports.json still has gateway: None (the setUp default).
        self.write_state(make_plan([make_unit(), routing_unit("parked")]))
        with patch.object(translator, "_run_worker",
                          self.worker_returning(self.GOOD_RESULT)):
            out = await tools.run_workload_translation()
        self.assertIn("SUCCESS", out)
        self.assertNotIn("Unparked", out)
        self.assertIn("Parked", out)
        state = self.read_state()
        statuses = {u["unit_id"]: u["status"]
                    for u in state["variables"]["workload_plan"]["units"]}
        self.assertEqual(statuses["wkld-routing"], "parked")

    async def test_unpark_write_conflict_claims_nothing(self):
        self.publish_gateway_exports()
        self.write_state(make_plan([routing_unit("parked")]))
        state_path = f"workloads/{COMPONENT}/state.json"
        real_blob = self.bucket.blob
        armed = {"on": True}

        def flaky_blob(name):
            blob = real_blob(name)
            if name == state_path:
                real_upload = blob.upload_from_string

                def upload(data, **kwargs):
                    if armed["on"] and kwargs.get("if_generation_match"):
                        armed["on"] = False
                        raise exceptions.PreconditionFailed("injected")
                    return real_upload(data, **kwargs)
                blob.upload_from_string = upload
            return blob

        with patch.object(self.bucket, "blob", side_effect=flaky_blob):
            with patch.object(translator, "_run_worker",
                              self.worker_returning(self.GOOD_RESULT)):
                out = await tools.run_workload_translation()
        self.assertIn("ERROR", out)
        self.assertIn("unparking", out)
        self.assertIn("Nothing was claimed", out)
        state = self.read_state()
        self.assertEqual(state["current_state"], "STATE_WKLD_TRANSLATE")
        self.assertNotIn("workload_run_active", state["variables"])
        self.assertEqual(state["variables"].get("workload_run", 0), 0)

    async def test_unpark_requeues_done_units_so_they_really_re_run(self):
        """A `done` unit is not pending, so re-stamping alone strands it.

        Without the demotion the plan would advertise the new generations
        while wkld-manifests' blob still carried the old ones — and validate
        would bounce the component to REVIEW forever.
        """
        self.publish_gateway_exports()
        self.bucket.blob(
            f"workloads/{COMPONENT}/units/wkld-manifests.json") \
            .upload_from_string(json.dumps({
                "unit": {"unit_id": "wkld-manifests", "status": "done"},
                "result": {"files": [{"path": "a.yaml", "content": APP_DOC}],
                           "tradeoffs": "t"},
                "run": 1, "exports_generations": dict(GENERATIONS)}))
        self.write_state(
            make_plan([make_unit(status="done"), routing_unit("parked")]),
            variables={"workload_run": 1})
        prompts = []

        async def spy_worker(prompt, model):
            prompts.append(prompt)
            return json.dumps(self.GOOD_RESULT)

        with patch.object(translator, "_run_worker", spy_worker):
            out = await tools.run_workload_translation()
        self.assertIn("SUCCESS", out)
        self.assertIn("Re-queued", out)
        self.assertIn("wkld-manifests", out.split("Re-queued")[1])
        self.assertEqual(len(prompts), 2)  # the done unit re-ran too
        blob = json.loads(self.bucket.blob(
            f"workloads/{COMPONENT}/units/wkld-manifests.json")
            .download_as_text())
        self.assertEqual(blob["exports_generations"], GATEWAY_GENERATIONS)

    async def test_unpark_refuses_when_exports_moved_beyond_the_gateway(self):
        """Only `gateway` may have moved; anything else needs a re-plan.

        The step can rebuild wkld-routing's brief (it persists its ingress
        facts) but no other family's — so re-stamping the whole plan would
        certify briefs it never looked at.
        """
        self.bucket.blob("exports.json").upload_from_string(json.dumps({
            **GATEWAY_EXPORTS_DOC,
            "storage_class_menu": ["standard-rwo", "premium-rwo"]}))
        self.write_state(make_plan([make_unit(), routing_unit("parked")]))
        with patch.object(translator, "_run_worker",
                          self.worker_returning(self.GOOD_RESULT)):
            out = await tools.run_workload_translation()
        self.assertIn("Unpark DECLINED", out)
        self.assertIn("approve_workload_translation(action='replan')", out)
        self.assertNotIn("The unpark above persisted", out)
        statuses = {u["unit_id"]: u["status"]
                    for u in self.read_state()["variables"]
                    ["workload_plan"]["units"]}
        self.assertEqual(statuses["wkld-routing"], "parked")

    async def test_a_held_lease_refuses_before_anything_is_unparked(self):
        """The lease check runs FIRST: a refused call mutates nothing.

        Unparking before the check re-stamped the plan under a fan-out that
        was still writing blobs at the pre-unpark generations, and then
        returned an error that never mentioned the mutation.
        """
        self.publish_gateway_exports()
        plan = make_plan([make_unit(), routing_unit("parked")])
        self.bucket.blob(f"workloads/{COMPONENT}/plan.json") \
            .upload_from_string(json.dumps(plan))
        self.write_state(plan, variables={
            "workload_run_active": {"run": 7, "claimed_at": time.time(),
                                    "claimant": "dev-a@x.com"}})
        with patch.object(translator, "_run_worker",
                          self.worker_returning(self.GOOD_RESULT)):
            out = await tools.run_workload_translation()
        self.assertIn("ERROR", out)
        self.assertNotIn("Unparked", out)
        self.assertNotIn("DECLINED", out)
        self.assertIn("nothing was unparked", out)
        state = self.read_state()
        self.assertEqual(state["history"], [])
        statuses = {u["unit_id"]: u["status"]
                    for u in state["variables"]["workload_plan"]["units"]}
        self.assertEqual(statuses["wkld-routing"], "parked")
        on_disk = json.loads(self.bucket.blob(
            f"workloads/{COMPONENT}/plan.json").download_as_text())
        self.assertEqual(on_disk, plan)


if __name__ == "__main__":
    unittest.main()
