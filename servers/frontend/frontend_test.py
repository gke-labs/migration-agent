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

"""Deterministic tests for the review frontend.

Run from the repo root:
    PYTHONPATH=. python -m unittest servers.frontend.frontend_test
"""

import json
import os
import tempfile
import unittest
from unittest import mock

import yaml
from starlette.testclient import TestClient

from servers.dag import state_management
from servers.dag.fake_gcs import FakeStorageClient
from servers.frontend import ledger_view
from servers.frontend.server import GcsLedger, make_app

REPO_ROOT = os.path.realpath(os.path.join(os.path.dirname(__file__), "..", ".."))
DAG_PATH = os.path.join(REPO_ROOT, "servers", "dag", "platform_dag.json")

with open(DAG_PATH) as f:
    REAL_DAG = json.load(f)


class DagViewTest(unittest.TestCase):
    def setUp(self):
        self.view = ledger_view.build_dag_view(REAL_DAG)
        self.by_name = {n["name"]: n for n in self.view["nodes"]}

    def test_all_states_present_once_starting_at_start_state(self):
        names = [n["name"] for n in self.view["nodes"]]
        self.assertEqual(names[0], REAL_DAG["start_state"])
        self.assertEqual(sorted(names), sorted(REAL_DAG["states"].keys()))
        self.assertEqual(len(names), len(set(names)))
        self.assertEqual([n["index"] for n in self.view["nodes"]], list(range(len(names))))

    def test_every_transition_becomes_an_edge(self):
        expected = sum(len(s.get("transitions") or {}) for s in REAL_DAG["states"].values())
        self.assertEqual(len(self.view["edges"]), expected)

    def test_back_self_and_forward_edges_classified(self):
        edges = {(e["from"], e["trigger"]): e for e in self.view["edges"]}
        amend = edges[("STATE_ASSESSMENT", "on_amend_scope")]
        self.assertTrue(amend["back"])
        self.assertFalse(amend["self"])

        pending = edges[("STATE_CREATE_SSM_REPOSITORY", "on_pending")]
        self.assertTrue(pending["self"])

        forward = edges[("STATE_DISCOVERY", "on_tool_call_received")]
        self.assertFalse(forward["back"])
        self.assertFalse(forward["self"])

    def test_every_node_has_a_phase_and_phases_are_contiguous(self):
        for node in self.view["nodes"]:
            self.assertNotEqual(node["phase"], "other", f"{node['name']} has no phase")
        spans = self.view["phases"]
        self.assertEqual(spans[0]["start"], 0)
        for previous, current in zip(spans, spans[1:]):
            self.assertEqual(current["start"], previous["end"] + 1)
        self.assertEqual(spans[-1]["end"], len(self.view["nodes"]) - 1)

    def test_each_phase_forms_a_single_band(self):
        # The render decline path (STATE_DISCOVERY_RENDER_DECLINED) only
        # rejoins earlier territory, so a plain depth-first walk reaches it
        # last and would strand a second "discovery" band after the graph's
        # tail. The relocation pass must keep every phase to one band.
        phases = [span["phase"] for span in self.view["phases"]]
        self.assertEqual(len(phases), len(set(phases)),
                         f"a phase is split across bands: {phases}")

    def test_rejoining_branch_drawn_next_to_its_branch_point(self):
        by_name = {n["name"]: n for n in self.view["nodes"]}
        approval = by_name["STATE_DISCOVERY_RENDER_APPROVAL"]["index"]
        declined = by_name["STATE_DISCOVERY_RENDER_DECLINED"]["index"]
        self.assertEqual(declined, approval + 1)

    def test_phase_fallback_when_dag_omits_phase(self):
        dag = json.loads(json.dumps(REAL_DAG))
        for state in dag["states"].values():
            state.pop("phase", None)
        view = ledger_view.build_dag_view(dag)
        by_name = {n["name"]: n for n in view["nodes"]}
        self.assertEqual(by_name["STATE_LZ_DESIGN"]["phase"], "landingzone")
        # Translation states fall back to landingzone too: the merged phase spans
        # the whole target build, so STATE_TRANSLATION_* is one band with STATE_LZ_*.
        self.assertEqual(by_name["STATE_TRANSLATION_RUNNING"]["phase"], "landingzone")
        self.assertEqual(by_name["STATE_ASSESSMENT"]["phase"], "assessment")
        self.assertEqual(by_name["STATE_DISCOVERY_SCOPING"]["phase"], "discovery")
        self.assertEqual(by_name["STATE_CONFIGURE_REPOSITORIES"]["phase"], "setup")

    def test_display_names(self):
        # keep_upper keeps SSM uppercase; ordinary words are capitalized.
        self.assertEqual(
            self.by_name["STATE_ELICIT_SSM_CREATION_APPROVAL"]["display_name"],
            "Elicit SSM Creation Approval",
        )
        self.assertEqual(self.by_name["STATE_DISCOVERY_SCOPING"]["display_name"], "Discovery Scoping")

class HistoryAndRolesTest(unittest.TestCase):
    def test_parse_visited(self):
        history = [
            "<harness note>",
            "Transitioned STATE_DISCOVERY -> STATE_DISCOVERY_SCOPING via discover_configuration_files",
            "Transitioned STATE_DISCOVERY_SCOPING -> STATE_DISCOVERY_RUNNING via confirm_discovery_scope",
        ]
        self.assertEqual(
            ledger_view.parse_visited(history),
            ["STATE_DISCOVERY", "STATE_DISCOVERY_SCOPING", "STATE_DISCOVERY_RUNNING"],
        )
        self.assertEqual(ledger_view.parse_visited(None), [])

    def test_parse_visited_covers_hitl_and_server_transitions(self):
        # HITL and INTERNAL_TASK transitions log without a " via <tool>" suffix.
        history = [
            "Interactive HITL response: predeploy approved",
            "Transitioned STATE_PREDEPLOY_REVIEW -> STATE_PREDEPLOY_COMMIT",
            "Action 'commit_predeploy' returned 'on_success': committed locally",
            "Transitioned STATE_PREDEPLOY_COMMIT -> STATE_PREDEPLOY_COMPLETED",
            "Transitioned STATE_ASSESSMENT -> STATE_ARCHITECTURE_DESIGN via submit_assessment",
        ]
        self.assertEqual(
            ledger_view.parse_visited(history),
            ["STATE_PREDEPLOY_REVIEW", "STATE_PREDEPLOY_COMMIT", "STATE_PREDEPLOY_COMPLETED",
             "STATE_ASSESSMENT", "STATE_ARCHITECTURE_DESIGN"],
        )

    def test_resolve_roles_maps_platform_engineers(self):
        roles = ledger_view.resolve_roles(
            {"resolved_role": "platform_engineers", "workspace_name": "w", "gcp_project": "p"},
            {"roles": {"platform_engineers": ["eng@example.com"]}, "workspace_name": "w"},
            "eng@example.com",
        )
        self.assertEqual(roles["acting_role"], "platform")
        self.assertEqual(roles["registered_role"], "platform")

    def test_resolve_roles_unregistered_user(self):
        roles = ledger_view.resolve_roles(
            {"resolved_role": "platform"}, {"roles": {"admins": ["boss@example.com"]}}, "eve@example.com")
        self.assertIsNone(roles["registered_role"])


class BlobAllowListTest(unittest.TestCase):
    def test_registry_blobs_allowed(self):
        self.assertTrue(ledger_view.is_blob_allowed("platform/discovery/inventory.json"))
        self.assertTrue(ledger_view.is_blob_allowed("platform/discovery/readiness-report.md"))
        self.assertTrue(ledger_view.is_blob_allowed("platform/discovery/fragments/chunk-3.json"))
        self.assertTrue(ledger_view.is_blob_allowed("platform/translation/units/network.json"))
        self.assertTrue(ledger_view.is_blob_allowed("platform/translation/validation-report.json"))
        self.assertTrue(ledger_view.is_blob_allowed("platform/translation/comparisons.json"))
        self.assertTrue(ledger_view.is_blob_allowed(
            "platform/deployment/data-migration-runbook.md"))
        self.assertTrue(ledger_view.is_blob_allowed(
            "platform/deployment/runbooks/envs-prod--rds-orders-db--aws-db-instance-orders.md"))
        # The prefix itself is not a blob: `is_blob_allowed` requires
        # something after it, or `/api/blob` would serve a directory read.
        self.assertFalse(ledger_view.is_blob_allowed(
            "platform/deployment/runbooks/"))

    def test_a_prefix_artifact_keeps_its_own_renderer(self):
        """Every prefix artifact was JSON until the migration procedures
        arrived. The client branch that ignored `render` for prefix listings
        turned a markdown procedure into a dump of its own markup."""
        procedures = [a for a in ledger_view.ARTIFACT_REGISTRY
                      if a["id"] == "data-migration-procedures"]
        self.assertEqual(len(procedures), 1)
        self.assertEqual(procedures[0]["render"], "markdown")
        self.assertEqual(procedures[0]["source"]["prefix"],
                         "platform/deployment/runbooks/")

    def test_sensitive_and_malformed_paths_rejected(self):
        for path in (
            "platform/onboarding/state.json",   # exposed via /api/overview, never raw
            "workspace_registry.yaml",
            "platform_dag.json",
            "platform/discovery/fragments/../../onboarding/state.json",
            "/etc/passwd",
            "platform/discovery/fragments/",     # bare prefix, no object name
            "",
        ):
            self.assertFalse(ledger_view.is_blob_allowed(path), path)

    def test_every_state_artifact_source_is_well_formed(self):
        state_names = set(REAL_DAG["states"])
        for artifact in ledger_view.ARTIFACT_REGISTRY:
            self.assertEqual(len(artifact["source"]), 1, artifact["id"])
            for state in artifact["states"]:
                self.assertIn(state, state_names, f"{artifact['id']} references unknown {state}")

    def test_validation_report_is_primary_at_validate(self):
        ids = [a["id"] for a in ledger_view.artifacts_for_state("STATE_TRANSLATION_VALIDATE")]
        self.assertEqual(ids[0], "validation-report")

    def test_comparisons_is_primary_at_ship_approval(self):
        ids = [a["id"] for a in ledger_view.artifacts_for_state("STATE_TRANSLATION_APPROVED")]
        self.assertEqual(ids[0], "comparisons")

    def test_pull_request_shown_at_submit_and_completed(self):
        for state in ("STATE_TRANSLATION_SUBMIT_PR", "STATE_DEPLOYMENT_INIT",
                      "STATE_DEPLOYMENT_COMPLETED"):
            ids = [a["id"] for a in ledger_view.artifacts_for_state(state)]
            self.assertIn("pull-request", ids, state)

    def test_report_and_comparison_visible_at_the_states_review_rests_at(self):
        # run_generated_validation writes both blobs at VALIDATE, then either
        # raises the ship elicitation (ledger still reads VALIDATE) or lands at
        # REVIEW (validation failure / ship declined). Both must be reachable
        # there or the reviewer signs off blind.
        for state in ("STATE_TRANSLATION_VALIDATE", "STATE_TRANSLATION_REVIEW"):
            ids = [a["id"] for a in ledger_view.artifacts_for_state(state)]
            self.assertIn("validation-report", ids, state)
            self.assertIn("comparisons", ids, state)


class _LiveLedgerBase(unittest.TestCase):
    """Harness for the endpoint tests: the production GcsLedger reading an
    in-memory FakeStorageClient, with the session config, storage client, and
    identity resolution patched so no GCS bucket, credentials, or filesystem
    are touched. This is the one ledger backend the frontend has — the tests
    exercise the same read/write/CAS paths that run in production.
    """

    BUCKET = "test-ledger"
    USER_EMAIL = "pe@test"

    def setUp(self):
        self._fake = FakeStorageClient()
        self._bucket = self._fake.bucket(self.BUCKET)

        self._tmp = tempfile.TemporaryDirectory()
        config_path = os.path.join(self._tmp.name, "ledger_config.yaml")
        with open(config_path, "w") as f:
            yaml.safe_dump({
                "ledger_uri": "gs://" + self.BUCKET,
                "resolved_role": "platform",
                "workspace_name": "test-ws",
                "gcp_project": "test-project",
            }, f)

        self._patches = [
            # The scoped dir is pointed at an empty temp dir, so reads take the
            # legacy-fallback path to config_path (and never see a real
            # ~/.ledger_config.d entry for this cwd).
            mock.patch.object(state_management, "LEDGER_CONFIG_PATH", config_path),
            mock.patch.object(state_management, "LEDGER_CONFIG_DIR",
                              os.path.join(self._tmp.name, "ledger_config.d")),
            mock.patch("google.cloud.storage.Client",
                       new=lambda *a, **k: self._fake),
            mock.patch.object(state_management, "get_authenticated_user_email",
                              lambda *a, **k: self.USER_EMAIL),
        ]
        for p in self._patches:
            p.start()

    def tearDown(self):
        for p in self._patches:
            p.stop()
        self._tmp.cleanup()

    def _put(self, path, data):
        if not isinstance(data, str):
            data = json.dumps(data)
        self._bucket.blob(path).upload_from_string(data)

    def _delete(self, path):
        self._bucket.blob(path).delete()

    def _client(self, **kwargs):
        return TestClient(make_app(GcsLedger(), **kwargs))


class ApiTest(_LiveLedgerBase):
    def setUp(self):
        super().setUp()
        self._seed_base()
        self.client = self._client()

    def _seed_base(self):
        inventory = {"clusters": [{"name": "prod"}], "triggers": {"karpenter": True}}
        state = {
            "current_state": "STATE_ASSESSMENT",
            "history": [
                "Transitioned STATE_DISCOVERY -> STATE_DISCOVERY_SCOPING via discover_configuration_files",
                "Transitioned STATE_DISCOVERY_RUNNING -> STATE_ASSESSMENT via run_discovery_extraction",
            ],
            "variables": {
                "source_repo_url": "https://github.com/example/estate",
                "discovery_scope": {"exclude": ["vendor/"]},
                "discovery_inventory": inventory,
            },
        }
        self._put("platform_dag.json", REAL_DAG)
        self._put("workspace_registry.yaml", {
            "workspace_name": "test-ws", "gcp_project": "test-project",
            "roles": {"platform_engineers": ["eng@example.com"]},
        })
        self._put("platform/onboarding/state.json", state)
        self._put("platform/discovery/inventory.json", inventory)
        self._put("platform/discovery/readiness-report.md", "# Report\nAll good.")

    def test_overview(self):
        data = self.client.get("/api/overview").json()
        self.assertNotIn("error", data)
        self.assertEqual(data["current_state"], "STATE_ASSESSMENT")
        self.assertEqual(data["identity"]["mode"], "gcs")
        self.assertTrue(data["registry_ok"])
        self.assertIn("STATE_ASSESSMENT", data["visited"])
        self.assertEqual(data["dag"]["nodes"][0]["name"], REAL_DAG["start_state"])
        self.assertIn("discovery_inventory", data["variable_keys"])

    def test_state_detail_inlines_variables_and_serves_blobs(self):
        data = self.client.get("/api/state/STATE_ASSESSMENT").json()
        artifacts = {a["id"]: a for a in data["artifacts"]}
        self.assertEqual(artifacts["readiness-report"]["blob"],
                         "platform/discovery/readiness-report.md")
        self.assertTrue(artifacts["readiness-report"]["available"])
        self.assertTrue(artifacts["inventory"]["available"])
        self.assertEqual(artifacts["scope"]["content"], {"exclude": ["vendor/"]})

        data = self.client.get("/api/state/STATE_DISCOVERY_SCOPING").json()
        artifacts = {a["id"]: a for a in data["artifacts"]}
        self.assertEqual(artifacts["scope"]["content"], {"exclude": ["vendor/"]})
        self.assertTrue(artifacts["manifest"]["available"] is False)  # never written

    def test_state_detail_unknown_state(self):
        response = self.client.get("/api/state/STATE_NOPE")
        self.assertEqual(response.status_code, 404)

    def test_blob_endpoint_allowed_and_denied(self):
        ok = self.client.get("/api/blob", params={"path": "platform/discovery/readiness-report.md"})
        self.assertEqual(ok.status_code, 200)
        self.assertIn("Report", ok.json()["content"])

        denied = self.client.get("/api/blob", params={"path": "platform/onboarding/state.json"})
        self.assertEqual(denied.status_code, 403)

        missing = self.client.get("/api/blob", params={"path": "platform/discovery/fragments/ghost.json"})
        self.assertEqual(missing.status_code, 404)

    def test_extraction_progress_absent(self):
        data = self.client.get("/api/progress/extraction").json()
        self.assertEqual(data, {"available": False})

    def test_extraction_progress_merges_fragments_written_so_far(self):
        self._put("platform/discovery/extraction-progress.json",
                  {"total_chunks": 3, "chunk_ids": ["c1", "c2", "c3"], "reused": 1})
        fragment = {
            "clusters": [{"name": "prod", "region": "us-east-1"}],
            "triggers": {"karpenter": True},
        }
        other = {
            "clusters": [{"name": "staging"}],
            "triggers": {"gpu_tpu": True},
        }
        self._put("platform/discovery/fragments/c1.json", fragment)
        self._put("platform/discovery/fragments/c3.json", other)
        client = self._client()

        data = client.get("/api/progress/extraction").json()
        self.assertTrue(data["available"])
        self.assertEqual(data["total"], 3)
        self.assertEqual(data["done"], 2)  # c2 still running
        self.assertEqual(data["reused"], 1)
        names = {c["name"] for c in data["inventory"]["clusters"]}
        self.assertEqual(names, {"prod", "staging"})
        self.assertTrue(data["inventory"]["triggers"]["karpenter"])
        self.assertTrue(data["inventory"]["triggers"]["gpu_tpu"])

        # A fragment landing between polls shows up (write-once cache is per
        # path, so a new path is always picked up).
        self._put("platform/discovery/fragments/c2.json", {"clusters": [{"name": "dev"}]})
        data = client.get("/api/progress/extraction").json()
        self.assertEqual(data["done"], 3)
        self.assertIn("dev", {c["name"] for c in data["inventory"]["clusters"]})

    def test_extraction_progress_cache_dropped_when_a_new_run_starts(self):
        # A retry/amended run can rewrite the same content-addressed fragment
        # path with different worker output; a changed progress blob must
        # invalidate the per-path cache so the view never serves the old run.
        self._put("platform/discovery/extraction-progress.json",
                  {"total_chunks": 1, "chunk_ids": ["c1"], "reused": 0})
        self._put("platform/discovery/fragments/c1.json", {"clusters": [{"name": "old"}]})
        client = self._client()
        data = client.get("/api/progress/extraction").json()
        self.assertEqual({c["name"] for c in data["inventory"]["clusters"]}, {"old"})

        # Same fragment path, new content, same progress blob: cached (by design).
        self._put("platform/discovery/fragments/c1.json", {"clusters": [{"name": "new"}]})
        data = client.get("/api/progress/extraction").json()
        self.assertEqual({c["name"] for c in data["inventory"]["clusters"]}, {"old"})

        # A new run rewrites the progress blob -> cache dropped, fresh reads.
        self._put("platform/discovery/extraction-progress.json",
                  {"total_chunks": 1, "chunk_ids": ["c1"], "reused": 1})
        data = client.get("/api/progress/extraction").json()
        self.assertEqual({c["name"] for c in data["inventory"]["clusters"]}, {"new"})

    def test_extraction_progress_artifact_registered_for_running_state(self):
        data = self.client.get("/api/state/STATE_DISCOVERY_RUNNING").json()
        artifacts = {a["id"]: a for a in data["artifacts"]}
        progress = artifacts["extraction-progress"]
        self.assertEqual(progress["endpoint"], "/api/progress/extraction")
        self.assertTrue(progress["available"])
        self.assertEqual(progress["render"], "extraction-progress")

    def test_data_migration_step_shows_its_own_worklist(self):
        """The step exists to produce a worklist; a UI that cannot open it at
        that step is not showing the step. The runbook is primary because it is
        the subject — the discovery inventory is a click away in the same set."""
        data = self.client.get(
            "/api/state/STATE_DEPLOYMENT_DATA_MIGRATION").json()
        artifacts = {a["id"]: a for a in data["artifacts"]}

        self.assertIn("data-migration-runbook", artifacts)
        self.assertIn("data-migrations", artifacts)
        # PRIMARY_ARTIFACT sorts rather than labelling: first is primary.
        self.assertEqual(data["artifacts"][0]["id"], "data-migration-runbook")
        # ...and it is rendered as the Markdown it is. `render: "text"` has no
        # branch in renderBlobInto, so it falls through to a monospace dump of
        # the markup — headings, bullets, bold and the closed banner's
        # blockquote shown raw, at the one state where this is primary.
        self.assertEqual(artifacts["data-migration-runbook"]["render"],
                         "markdown")
        # The step sits between the two image states, and nothing it shows
        # becomes wrong there, so the existing deployment views come along.
        self.assertIn("inventory", artifacts)
        self.assertIn("replication-runbook", artifacts)
        # `keep-in-aws` is the step's documented exit and it writes the
        # corrections object, so this IS a state where the reviewer's own
        # decisions are being made — the criterion the entry states.
        self.assertIn("data-corrections", artifacts)

    def test_translation_progress_absent(self):
        data = self.client.get("/api/progress/translation").json()
        self.assertEqual(data, {"available": False})

    def test_translation_progress_counts_only_units_produced_this_run(self):
        # The run token is the crux: a unit revised after a prior run still has
        # its old blob on disk, and it must NOT count as done until this run
        # re-produces it — otherwise the bar jumps to full the instant the run
        # starts. Reused (already-done) units count immediately, by count.
        self._put("platform/translation/translation-progress.json", {
            "run": 2, "total_units": 3, "reused": 1,
            "pending_ids": ["network", "storage"],
        })
        # network finished this run (run == 2) -> done.
        self._put("platform/translation/units/network.json",
                  {"unit": {"unit_id": "network", "status": "done"},
                   "result": {"files": []}, "run": 2})
        # storage carries a stale blob from run 1 -> still translating.
        self._put("platform/translation/units/storage.json",
                  {"unit": {"unit_id": "storage", "status": "done"},
                   "result": {"files": []}, "run": 1})
        client = self._client()
        data = client.get("/api/progress/translation").json()
        self.assertTrue(data["available"])
        self.assertEqual(data["total"], 3)
        self.assertEqual(data["reused"], 1)
        self.assertEqual(data["done"], 2)  # reused(1) + network; storage stale
        self.assertEqual(data["done_ids"], ["network"])
        self.assertEqual(data["pending_ids"], ["network", "storage"])
        # A stale-but-not-errored unit is neither done nor failed.
        self.assertEqual(data["failed"], 0)
        self.assertEqual(data["error_ids"], [])

    def test_translation_progress_splits_failed_units_from_done(self):
        # A unit whose worker errored (status "error", or a null result) is a
        # failure, not a green success: it must land in error_ids/failed and
        # never inflate done/done_ids (which the progress bar builds on).
        self._put("platform/translation/translation-progress.json", {
            "run": 5, "total_units": 3, "reused": 0,
            "pending_ids": ["network", "storage", "compute"],
        })
        self._put("platform/translation/units/network.json",
                  {"unit": {"unit_id": "network", "status": "done"},
                   "result": {"files": []}, "run": 5})
        # storage failed outright: status "error".
        self._put("platform/translation/units/storage.json",
                  {"unit": {"unit_id": "storage", "status": "error", "error": "boom"},
                   "result": None, "run": 5})
        # compute has no result payload -> treated as an error, not done.
        self._put("platform/translation/units/compute.json",
                  {"unit": {"unit_id": "compute", "status": "done"},
                   "result": None, "run": 5})
        client = self._client()
        data = client.get("/api/progress/translation").json()
        self.assertEqual(data["done"], 1)  # only network
        self.assertEqual(data["done_ids"], ["network"])
        self.assertEqual(data["failed"], 2)
        self.assertEqual(data["error_ids"], ["compute", "storage"])
        self.assertEqual(data["pending_ids"], ["compute", "network", "storage"])

    def test_translation_progress_caches_finished_units_until_run_changes(self):
        # A finished unit blob is write-once within a run, so its terminal
        # status is cached per unit id (not re-downloaded every poll); a new
        # run token drops the whole cache so a re-produced unit is re-read.
        self._put("platform/translation/translation-progress.json", {
            "run": 1, "total_units": 1, "reused": 0, "pending_ids": ["network"],
        })
        self._put("platform/translation/units/network.json",
                  {"unit": {"unit_id": "network", "status": "done"},
                   "result": {"files": []}, "run": 1})
        client = self._client()
        data = client.get("/api/progress/translation").json()
        self.assertEqual(data["done_ids"], ["network"])
        self.assertEqual(data["failed"], 0)

        # Same run, blob flips to an error: served from cache (by design), so
        # the view still reports it done and never re-reads it.
        self._put("platform/translation/units/network.json",
                  {"unit": {"unit_id": "network", "status": "error", "error": "boom"},
                   "result": None, "run": 1})
        data = client.get("/api/progress/translation").json()
        self.assertEqual(data["done_ids"], ["network"])  # cached
        self.assertEqual(data["failed"], 0)

        # A new run rewrites the progress blob -> cache dropped, blob re-read
        # (now under the new run token) and correctly surfaces as failed.
        self._put("platform/translation/translation-progress.json", {
            "run": 2, "total_units": 1, "reused": 0, "pending_ids": ["network"],
        })
        self._put("platform/translation/units/network.json",
                  {"unit": {"unit_id": "network", "status": "error", "error": "boom"},
                   "result": None, "run": 2})
        data = client.get("/api/progress/translation").json()
        self.assertEqual(data["done_ids"], [])
        self.assertEqual(data["error_ids"], ["network"])
        self.assertEqual(data["failed"], 1)

    def test_translation_progress_artifact_is_primary_for_running_state(self):
        data = self.client.get("/api/state/STATE_TRANSLATION_RUNNING").json()
        artifacts = {a["id"]: a for a in data["artifacts"]}
        progress = artifacts["translation-progress"]
        self.assertEqual(progress["endpoint"], "/api/progress/translation")
        self.assertTrue(progress["available"])
        self.assertEqual(progress["render"], "translation-progress")
        # Shown first (default subtab) while translation runs.
        self.assertEqual(data["artifacts"][0]["id"], "translation-progress")


    def test_translation_review_lists_units_and_plan(self):
        plan_unit = {"unit_id": "network", "kind": "network", "status": "done",
                     "title": "VPC", "error": None, "feedback": None}
        state = {
            "current_state": "STATE_TRANSLATION_REVIEW",
            "history": [],
            "variables": {"translation_plan": {"units": [plan_unit]}},
        }
        self._put("platform/onboarding/state.json", state)
        unit_blob = {"unit": plan_unit, "run": 1,
                     "result": {"files": [{"path": "network.tf", "content": "x"}],
                                "tradeoffs": "t", "assumptions": [], "open_questions": []}}
        self._put("platform/translation/units/network.json", unit_blob)
        data = self.client.get("/api/state/STATE_TRANSLATION_REVIEW").json()
        artifacts = {a["id"]: a for a in data["artifacts"]}
        units = artifacts["translation-units"]
        self.assertTrue(units["available"])
        self.assertEqual(units["items"], ["platform/translation/units/network.json"])
        self.assertIn("caveat", units)
        plan = artifacts["translation-plan"]
        self.assertEqual(plan["content"]["units"][0]["status"], "done")
        # primary artifact for the review state leads the list
        self.assertEqual(data["artifacts"][0]["id"], "translation-units")

    def test_missing_state_yields_overview_error(self):
        self._delete("platform/onboarding/state.json")
        data = self._client().get("/api/overview").json()
        self.assertIn("error", data)
        self.assertIn("join_ledger", data["error"])

    def test_corrupt_state_is_not_misreported_as_missing(self):
        self._put("platform/onboarding/state.json", "{nope")
        data = self._client().get("/api/overview").json()
        self.assertIn("not valid JSON", data["error"])
        self.assertNotIn("join_ledger", data["error"])

    def test_non_dict_registry_does_not_crash_and_flags_registry(self):
        self._put("workspace_registry.yaml", "- just\n- a\n- list\n")
        response = self._client().get("/api/overview")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertNotIn("error", data)
        self.assertFalse(data["registry_ok"])
        self.assertIsNone(data["roles"]["registered_role"])

    def test_backend_failures_surface_as_structured_json_not_500(self):
        class ExplodingLedger:
            mode = "gcs"

            def identity(self):
                return {"mode": "gcs", "user_email": None, "acting_role": "unknown",
                        "ledger_uri": None, "identity_error": None}

            def read_text(self, path):
                raise RuntimeError("token expired")

            def read_text_and_generation(self, path):
                raise RuntimeError("token expired")

        client = TestClient(make_app(ExplodingLedger()), raise_server_exceptions=False)
        response = client.get("/api/overview")
        self.assertEqual(response.status_code, 503)
        data = response.json()
        self.assertIn("Ledger access failed", data["error"])
        self.assertIn("token expired", data["error"])
        self.assertEqual(data["identity"]["mode"], "gcs")

    def test_host_header_validation_blocks_dns_rebinding(self):
        client = self._client(allowed_hosts=["127.0.0.1", "localhost", "testserver"])
        ok = client.get("/api/overview")
        self.assertEqual(ok.status_code, 200)
        rebound = client.get("/api/overview", headers={"Host": "evil.attacker.example"})
        self.assertEqual(rebound.status_code, 400)


class LiveLedgerApiTest(_LiveLedgerBase):
    """The production GcsLedger over an in-memory bucket."""

    USER_EMAIL = "pe@local.test"

    def setUp(self):
        super().setUp()
        self._put("platform_dag.json", REAL_DAG)
        self._put("workspace_registry.yaml", {
            "workspace_name": "test-ws", "gcp_project": "test-project",
            "roles": {"platform_engineers": ["pe@local.test"]},
        })
        self._put("platform/onboarding/state.json", {
            "current_state": "STATE_DISCOVERY", "history": [], "variables": {},
        })
        self._put("platform/discovery/readiness-report.md", "# Local\n")
        self.client = self._client()

    def test_overview_and_blob_from_live_ledger(self):
        data = self.client.get("/api/overview").json()
        self.assertNotIn("error", data)
        self.assertEqual(data["current_state"], "STATE_DISCOVERY")
        self.assertEqual(data["identity"]["mode"], "gcs")
        self.assertEqual(data["identity"]["user_email"], "pe@local.test")
        self.assertEqual(data["identity"]["acting_role"], "platform")
        self.assertIsInstance(data["state_generation"], int)

        blob = self.client.get(
            "/api/blob", params={"path": "platform/discovery/readiness-report.md"}
        )
        self.assertEqual(blob.status_code, 200)
        self.assertIn("Local", blob.json()["content"])
