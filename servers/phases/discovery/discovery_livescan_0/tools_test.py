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

"""Tool-layer tests for discover_and_dump_all_clusters (STATE_DISCOVERY_LIVE).

Runs the real tool over FakeGCS through authorize_and_rehydrate: registry,
session config and platform onboarding state are seeded; no real GCS,
credentials or home files are touched. The deterministic walk
(live_discovery.run_live_discovery over the eks_auth seams) is stubbed — its
own logic is covered by the aws_live / k8s_live / live_csv tests — so these
tests exercise what the tool owns: state-guarding, the skip contract, the
dependency/credential preflight, the schema check, persistence under
LIVE_PREFIX, and the generation-matched advance.
"""

import asyncio
import copy
import json
import os
import shutil
import tempfile
import threading
import unittest
from unittest.mock import patch

from google.api_core import exceptions

import servers.dag.state_management as state_mgr
from servers.dag import fake_gcs
from servers.phases.discovery.discovery_livescan_0 import tools

REGISTRY = {
    "workspace_name": "ws-plat", "gcp_project": "proj",
    "roles": {"platform_engineers": ["eng@x.com"]},
}

_DAG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         "..", "..", "..", "dag", "platform_dag.json")

# What the stubbed walk returns: one cluster, one CSV per table, a summary.
# Schema-valid, so the tool's schema check passes on it (a violation is a
# note, tested apart) and a drift in the contract fails here first.
STUB_SUMMARY = {
    "regions_scanned": 1, "regions_unreachable": [],
    "clusters_found": 2, "clusters_walked": 1,
    "clusters_unreachable": ["staging"], "clusters_using_karpenter": 1,
    "workload_totals": {"Deployment": 4},
    "note_count": 1,
}
STUB_RESULT = {
    "ir": {"live_ir_version": "2.0", "regions": ["us-east-1"],
           "clusters": [{"name": "prod", "region": "us-east-1"}],
           "notes": ["one region was slow"], "summary": STUB_SUMMARY},
    "tables": {name: f"{name}\nrow\n" for name in
               ("clusters", "nodegroups", "workloads", "autoscaling",
                "networking", "identity", "storage", "config", "images")},
}


class _LiveToolBase(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        # Cleanups, not a tearDown: a setUp that fails after this line still
        # hands the module its globals back and removes its directory.
        self.addCleanup(self._restore_state_module, state_mgr.LEDGER_CONFIG_PATH,
                        state_mgr.LEDGER_CONFIG_DIR, state_mgr.gcs_client)
        config_dir = tempfile.mkdtemp(prefix="live-tools-")
        self.addCleanup(shutil.rmtree, config_dir, ignore_errors=True)
        state_mgr.LEDGER_CONFIG_PATH = os.path.join(config_dir, "ledger_config.yaml")
        state_mgr.LEDGER_CONFIG_DIR = os.path.join(config_dir, "ledger_config.d")
        self.fake = fake_gcs.FakeStorageClient()
        state_mgr.gcs_client = self.fake
        self.bucket = self.fake.bucket("plat-ledger")
        self.bucket.blob("workspace_registry.yaml").upload_from_string(
            json.dumps(REGISTRY))
        with open(os.path.normpath(_DAG_PATH)) as f:
            self.bucket.blob("platform_dag.json").upload_from_string(f.read())
        self.write_state()
        state_mgr.write_local_config("gs://plat-ledger", "platform", "ws-plat",
                                     "proj")
        self._email = patch.object(state_mgr, "get_authenticated_user_email",
                                   return_value="eng@x.com")
        self._email.start()
        self.addCleanup(self._email.stop)

    @staticmethod
    def _restore_state_module(config_path, config_dir, client):
        (state_mgr.LEDGER_CONFIG_PATH, state_mgr.LEDGER_CONFIG_DIR,
         state_mgr.gcs_client) = config_path, config_dir, client

    def write_state(self, current="STATE_DISCOVERY_LIVE", variables=None):
        state = {"current_state": current, "history": [],
                 "variables": variables or {}}
        self.bucket.blob("platform/onboarding/state.json").upload_from_string(
            json.dumps(state))

    def read_state(self):
        return json.loads(self.bucket.blob(
            "platform/onboarding/state.json").download_as_text())

    def _patch_walk(self, result=STUB_RESULT, side_effect=None):
        """Stubs the whole engine seam: deps present, creds resolve, walk runs."""
        stack = [
            patch.object(tools.eks_auth, "missing_dependencies",
                         return_value=[]),
            patch.object(tools.eks_auth, "make_client_factory",
                         return_value=lambda service, region: None),
            patch.object(tools.eks_auth, "caller_identity",
                         return_value={"arn": "arn:aws:iam::1:user/eng"}),
            patch.object(tools.live_discovery, "run_live_discovery",
                         side_effect=side_effect,
                         **({} if side_effect else {"return_value": result})),
        ]
        for p in stack:
            p.start()
            self.addCleanup(p.stop)


class StateGuardTest(_LiveToolBase):

    async def test_wrong_state_is_refused(self):
        self.write_state(current="STATE_DISCOVERY")
        out = await tools.discover_and_dump_all_clusters(regions=["us-east-1"])
        self.assertIn("ERROR: Invalid state", out)
        # State is untouched.
        self.assertEqual(self.read_state()["current_state"], "STATE_DISCOVERY")

    async def test_no_regions_is_an_actionable_error(self):
        out = await tools.discover_and_dump_all_clusters()
        self.assertIn("ERROR", out)
        self.assertIn("region", out.lower())
        self.assertEqual(self.read_state()["current_state"],
                         "STATE_DISCOVERY_LIVE")


class SkipTest(_LiveToolBase):

    async def test_skip_without_reason_is_refused(self):
        out = await tools.discover_and_dump_all_clusters(skip=True)
        self.assertIn("ERROR", out)
        self.assertIn("skip_reason", out)
        self.assertEqual(self.read_state()["current_state"],
                         "STATE_DISCOVERY_LIVE")

    async def test_skip_with_reason_advances_and_records(self):
        out = await tools.discover_and_dump_all_clusters(
            skip=True, skip_reason="repos-only engagement")
        self.assertIn("SUCCESS", out)
        self.assertNotIn("were removed", out)   # nothing stale to clear
        state = self.read_state()
        self.assertEqual(state["current_state"], "STATE_DISCOVERY")
        record = state["variables"]["live_discovery"]
        self.assertEqual(record["status"], "skipped")
        self.assertEqual(record["skip_reason"], "repos-only engagement")
        # No IR was written on the skip path.
        self.assertFalse(self.bucket.blob(tools.LIVE_IR_BLOB).exists())

    async def test_skip_removes_an_earlier_scan_so_it_is_not_read_as_this_run(self):
        # A previous engagement (or a re-entered state) left a record under
        # the live prefix; skipping must not leave it standing as the estate.
        for name in (tools.LIVE_IR_BLOB, f"{tools.LIVE_PREFIX}nodes.csv",
                     f"{tools.LIVE_PREFIX}clusters.csv"):
            self.bucket.blob(name).upload_from_string("stale")
        self.bucket.blob("platform/discovery/other.json").upload_from_string("x")
        out = await tools.discover_and_dump_all_clusters(
            skip=True, skip_reason="repos-only engagement")
        self.assertIn("SUCCESS", out)
        self.assertIn("3 object(s)", out)
        self.assertEqual(self.read_state()["current_state"], "STATE_DISCOVERY")
        self.assertEqual(list(self.bucket.list_blobs(prefix=tools.LIVE_PREFIX)),
                         [])
        # Only the live prefix is touched.
        self.assertTrue(self.bucket.blob("platform/discovery/other.json").exists())


class DependencyAndCredentialPreflightTest(_LiveToolBase):

    async def test_missing_dependencies_reports_install_line(self):
        with patch.object(tools.eks_auth, "missing_dependencies",
                          return_value=["boto3", "kubernetes"]):
            out = await tools.discover_and_dump_all_clusters(
                regions=["us-east-1"])
        self.assertIn("ERROR", out)
        self.assertIn("pip install boto3 kubernetes", out)
        self.assertEqual(self.read_state()["current_state"],
                         "STATE_DISCOVERY_LIVE")

    async def test_preflight_asks_the_first_scanned_region(self):
        # A credential in another partition cannot reach sts.us-east-1; the
        # preflight goes where the scan is about to go.
        self._patch_walk()
        await tools.discover_and_dump_all_clusters(
            regions=["cn-north-1", "cn-northwest-1"])
        tools.eks_auth.caller_identity.assert_called_once()
        self.assertEqual(tools.eks_auth.caller_identity.call_args.kwargs,
                         {"region": "cn-north-1"})

    async def test_credential_failure_leaves_state_in_place(self):
        with patch.object(tools.eks_auth, "missing_dependencies",
                          return_value=[]), \
             patch.object(tools.eks_auth, "make_client_factory",
                          return_value=lambda s, r: None), \
             patch.object(tools.eks_auth, "caller_identity",
                          side_effect=RuntimeError("ExpiredToken")):
            out = await tools.discover_and_dump_all_clusters(
                regions=["us-east-1"])
        self.assertIn("ERROR", out)
        self.assertIn("ExpiredToken", out)
        self.assertEqual(self.read_state()["current_state"],
                         "STATE_DISCOVERY_LIVE")
        self.assertFalse(self.bucket.blob(tools.LIVE_IR_BLOB).exists())

    async def test_an_unreachable_first_region_is_asked_past(self):
        # A mistyped first region has no STS endpoint; that is the region's
        # failure, not the credential's, and the next region is asked.
        class EndpointConnectionError(Exception):
            pass

        self._patch_walk()
        tools.eks_auth.caller_identity.side_effect = [
            EndpointConnectionError(
                'Could not connect to the endpoint URL: "https://sts.eu-west1.amazonaws.com/"'),
            {"arn": "arn:aws:iam::1:user/eng"}]
        out = await tools.discover_and_dump_all_clusters(
            regions=["eu-west1", "us-east-1"])
        self.assertIn("SUCCESS", out)
        self.assertEqual(
            [c.kwargs["region"]
             for c in tools.eks_auth.caller_identity.call_args_list],
            ["eu-west1", "us-east-1"])
        call = tools.live_discovery.run_live_discovery.call_args
        regions = (call.kwargs["regions"] if "regions" in call.kwargs
                   else call.args[1])
        self.assertEqual(list(regions), ["eu-west1", "us-east-1"])
        self.assertEqual(self.read_state()["current_state"], "STATE_DISCOVERY")

    async def test_no_reachable_region_leaves_state_in_place(self):
        class EndpointConnectionError(Exception):
            pass

        self._patch_walk()
        tools.eks_auth.caller_identity.side_effect = EndpointConnectionError(
            "Could not connect to the endpoint URL")
        out = await tools.discover_and_dump_all_clusters(
            regions=["eu-west1", "us-east1"])
        self.assertIn("ERROR", out)
        self.assertIn("No requested region could be reached", out)
        self.assertIn("eu-west1", out)
        self.assertIn("us-east1", out)
        self.assertNotIn("Could not authenticate", out)
        tools.live_discovery.run_live_discovery.assert_not_called()
        self.assertEqual(self.read_state()["current_state"],
                         "STATE_DISCOVERY_LIVE")
        self.assertFalse(self.bucket.blob(tools.LIVE_IR_BLOB).exists())

    async def test_a_refused_credential_is_not_retried_in_the_next_region(self):
        self._patch_walk()
        tools.eks_auth.caller_identity.side_effect = RuntimeError("ExpiredToken")
        out = await tools.discover_and_dump_all_clusters(
            regions=["us-east-1", "us-west-2"])
        self.assertIn("Could not authenticate", out)
        tools.eks_auth.caller_identity.assert_called_once()
        self.assertEqual(self.read_state()["current_state"],
                         "STATE_DISCOVERY_LIVE")

    async def test_a_region_the_account_has_not_enabled_is_asked_past(self):
        # STS in a region the account has not opted into answers
        # InvalidClientTokenId, as it would to a key that does not exist;
        # the next region tells the two apart.
        class ClientError(Exception):
            def __init__(self):
                super().__init__("An error occurred (InvalidClientTokenId) "
                                 "when calling the GetCallerIdentity operation")
                self.response = {"Error": {
                    "Code": "InvalidClientTokenId",
                    "Message": "The security token included in the request "
                               "is invalid"}}

        self._patch_walk()
        tools.eks_auth.caller_identity.side_effect = [
            ClientError(), {"arn": "arn:aws:iam::1:user/eng"}]
        out = await tools.discover_and_dump_all_clusters(
            regions=["ap-east-1", "us-east-1"])
        self.assertIn("SUCCESS", out)
        self.assertEqual(
            [c.kwargs["region"]
             for c in tools.eks_auth.caller_identity.call_args_list],
            ["ap-east-1", "us-east-1"])
        self.assertEqual(self.read_state()["current_state"], "STATE_DISCOVERY")

    async def test_every_region_refusing_the_key_is_the_credentials_failure(self):
        class ClientError(Exception):
            def __init__(self):
                super().__init__("An error occurred (InvalidClientTokenId)")
                self.response = {"Error": {"Code": "InvalidClientTokenId"}}

        self._patch_walk()
        tools.eks_auth.caller_identity.side_effect = ClientError()
        out = await tools.discover_and_dump_all_clusters(
            regions=["ap-east-1", "me-south-1"])
        self.assertIn("Could not authenticate", out)
        self.assertIn("InvalidClientTokenId", out)
        self.assertIn("has not enabled", out)
        self.assertEqual(len(tools.eks_auth.caller_identity.call_args_list), 2)
        tools.live_discovery.run_live_discovery.assert_not_called()
        self.assertEqual(self.read_state()["current_state"],
                         "STATE_DISCOVERY_LIVE")

    async def test_an_unreachable_region_is_named_beside_the_refusal(self):
        # A mistyped region ahead of one that refuses the key: the refusal
        # is the verdict, and the typo is not lost behind it.
        class EndpointConnectionError(Exception):
            pass

        class ClientError(Exception):
            def __init__(self):
                super().__init__("An error occurred (InvalidClientTokenId)")
                self.response = {"Error": {"Code": "InvalidClientTokenId"}}

        self._patch_walk()
        tools.eks_auth.caller_identity.side_effect = [
            EndpointConnectionError("Could not connect to the endpoint URL"),
            ClientError()]
        out = await tools.discover_and_dump_all_clusters(
            regions=["eu-west1", "ap-east-1"])
        self.assertIn("Could not authenticate", out)
        self.assertIn("InvalidClientTokenId", out)
        self.assertIn("has not enabled", out)
        self.assertIn("1 requested region(s) could not be reached at all", out)
        self.assertIn("eu-west1 (Could not connect to the endpoint URL)", out)
        self.assertEqual(
            [c.kwargs["region"]
             for c in tools.eks_auth.caller_identity.call_args_list],
            ["eu-west1", "ap-east-1"])
        tools.live_discovery.run_live_discovery.assert_not_called()
        self.assertEqual(self.read_state()["current_state"],
                         "STATE_DISCOVERY_LIVE")


class HappyPathTest(_LiveToolBase):

    async def test_scan_persists_ir_and_tables_and_advances(self):
        self._patch_walk()
        out = await tools.discover_and_dump_all_clusters(regions=["us-east-1"])
        self.assertIn("SUCCESS", out)
        # Summary lines the operator acts on.
        self.assertIn("staging", out)                 # unreachable cluster named
        self.assertIn("Karpenter", out)
        self.assertIn("Coverage notes (relay verbatim)", out)
        self.assertIn("one region was slow", out)

        # IR persisted, named for its schema, with provenance stamped in
        # and no schema complaint.
        self.assertEqual(tools.LIVE_IR_BLOB,
                         "platform/discovery/live/live_discovery.json")
        ir = json.loads(
            self.bucket.blob(tools.LIVE_IR_BLOB).download_as_text())
        self.assertEqual(ir["scanned_by"], "arn:aws:iam::1:user/eng")
        self.assertEqual(ir["clusters"], STUB_RESULT["ir"]["clusters"])
        self.assertNotIn("does not match its schema", out)
        # One CSV per table under the live prefix, and nothing else.
        self.assertEqual(
            sorted(b.name for b in self.bucket.list_blobs(prefix=tools.LIVE_PREFIX)),
            sorted([tools.LIVE_IR_BLOB] + [f"{tools.LIVE_PREFIX}{name}.csv"
                                           for name in STUB_RESULT["tables"]]))
        for name in ("clusters", "workloads", "images"):
            self.assertTrue(
                self.bucket.blob(f"{tools.LIVE_PREFIX}{name}.csv").exists())

        # State advanced and the outcome recorded.
        state = self.read_state()
        self.assertEqual(state["current_state"], "STATE_DISCOVERY")
        record = state["variables"]["live_discovery"]
        self.assertEqual(record["status"], "completed")
        self.assertIsNone(record["skip_reason"])
        self.assertEqual(record["summary"], STUB_SUMMARY)

    async def test_scope_parameters_reach_the_walk(self):
        captured = {}

        def walk(*args, **kwargs):
            captured.update(kwargs)
            return STUB_RESULT

        self._patch_walk(side_effect=walk)
        out = await tools.discover_and_dump_all_clusters(
            regions=["us-east-1"], cluster_names=["prod"],
            namespaces=["shop", "billing"])
        self.assertIn("SUCCESS", out)
        self.assertEqual(captured["cluster_names"], ["prod"])
        self.assertEqual(captured["namespaces"], ["shop", "billing"])

    async def test_aws_profile_reaches_the_client_factory(self):
        captured = {}

        def factory(profile_name=None):
            captured["profile_name"] = profile_name
            return lambda service, region: None

        self._patch_walk()
        with patch.object(tools.eks_auth, "make_client_factory", factory):
            out = await tools.discover_and_dump_all_clusters(
                regions=["us-east-1"], aws_profile="customer-ro")
        self.assertIn("SUCCESS", out)
        self.assertEqual(captured["profile_name"], "customer-ro")

    async def test_walk_failure_leaves_state_and_writes_nothing(self):
        self._patch_walk(side_effect=RuntimeError("boom mid-walk"))
        out = await tools.discover_and_dump_all_clusters(regions=["us-east-1"])
        self.assertIn("ERROR", out)
        self.assertIn("boom mid-walk", out)
        self.assertEqual(self.read_state()["current_state"],
                         "STATE_DISCOVERY_LIVE")
        self.assertFalse(self.bucket.blob(tools.LIVE_IR_BLOB).exists())
        self.assertEqual(list(self.bucket.list_blobs(prefix=tools.LIVE_PREFIX)), [])


class PersistOrderTest(_LiveToolBase):

    async def test_the_ir_is_written_last(self):
        # A reader that finds the IR can trust every table beside it is this
        # run's: it is the final write. Generations on the fake bucket
        # increase monotonically.
        self._patch_walk()
        await tools.discover_and_dump_all_clusters(regions=["us-east-1"])

        def generation(name):
            blob = self.bucket.blob(name)
            blob.reload()
            return blob.generation

        tables = [generation(f"{tools.LIVE_PREFIX}{name}.csv")
                  for name in STUB_RESULT["tables"]]
        self.assertGreater(generation(tools.LIVE_IR_BLOB), max(tables))


class SchemaCheckTest(_LiveToolBase):

    async def test_violation_is_noted_and_the_scan_still_lands(self):
        # A walk whose IR has drifted from the contract: the scan is kept,
        # the graph advances, and the violation rides along as a note — in
        # the response and, durably, in the persisted IR.
        drifted = copy.deepcopy(STUB_RESULT)
        drifted["ir"]["regions"] = "us-east-1"
        self._patch_walk(result=drifted)
        out = await tools.discover_and_dump_all_clusters(regions=["us-east-1"])
        self.assertIn("SUCCESS", out)
        self.assertIn("does not match its schema", out)
        self.assertIn("regions", out)
        ir = json.loads(
            self.bucket.blob(tools.LIVE_IR_BLOB).download_as_text())
        self.assertTrue(any("does not match its schema" in note
                            for note in ir["notes"]))
        self.assertEqual(ir["summary"]["note_count"], len(ir["notes"]))
        self.assertEqual(self.read_state()["current_state"], "STATE_DISCOVERY")

    async def test_validation_that_cannot_run_still_lands_the_scan(self):
        # The validator itself failing — schema file missing, jsonschema
        # absent, malformed schema — must never discard the only record of
        # the estate. It becomes a note and the scan still lands and advances.
        self._patch_walk(result=copy.deepcopy(STUB_RESULT))
        with patch.object(tools.live_schema, "validate_live_ir",
                          side_effect=FileNotFoundError("schema file gone")):
            out = await tools.discover_and_dump_all_clusters(
                regions=["us-east-1"])
        self.assertIn("SUCCESS", out)
        self.assertIn("could not run", out)
        ir = json.loads(
            self.bucket.blob(tools.LIVE_IR_BLOB).download_as_text())
        self.assertTrue(any("could not run" in note for note in ir["notes"]))
        self.assertEqual(ir["summary"]["note_count"], len(ir["notes"]))
        self.assertEqual(self.read_state()["current_state"], "STATE_DISCOVERY")


class ConcurrentUpdateTest(_LiveToolBase):
    """The scan reads the state under a generation, re-reads it before
    touching the live prefix, and advances under it at the end. A writer
    landing during the walk is caught by the re-read; one landing between
    the re-read and the advance by the advance."""

    async def test_a_writer_during_the_walk_leaves_the_prefix_untouched(self):
        # A concurrent scan finished (and wrote its record) while this walk
        # ran. This scan lost: it must not clear the winner's objects and
        # write its own under a state that is the winner's.
        theirs = (tools.LIVE_IR_BLOB, f"{tools.LIVE_PREFIX}clusters.csv")

        def their_scan_lands_then_return(*args, **kwargs):
            for name in theirs:
                self.bucket.blob(name).upload_from_string("theirs")
            self.write_state(current="STATE_DISCOVERY")  # new generation
            return STUB_RESULT

        self._patch_walk(side_effect=their_scan_lands_then_return)
        out = await tools.discover_and_dump_all_clusters(regions=["us-east-1"])
        self.assertIn("ERROR: Concurrent update conflict", out)
        self.assertIn("moved the migration state while this scan ran", out)
        self.assertIn(f"Nothing was written or removed under {tools.LIVE_PREFIX}",
                      out)
        self.assertIn("If it still stands at STATE_DISCOVERY_LIVE", out)
        for name in theirs:
            self.assertEqual(self.bucket.blob(name).download_as_text(), "theirs")
        self.assertEqual(sorted(b.name for b in
                                self.bucket.list_blobs(prefix=tools.LIVE_PREFIX)),
                         sorted(theirs))
        # The winner's state stands.
        self.assertEqual(self.read_state()["current_state"], "STATE_DISCOVERY")

    async def test_a_writer_between_the_re_read_and_the_advance_is_reported(self):
        # The residual window: the state moved after _persist confirmed the
        # generation and cleared the prefix, before the advance. The IR is
        # on disk under the other writer's state, and the message says so.
        real_clear = tools._clear_live_prefix

        def bump_then_clear(bucket):
            self.write_state()  # new generation on the state record
            return real_clear(bucket)

        self._patch_walk()
        with patch.object(tools, "_clear_live_prefix", bump_then_clear):
            out = await tools.discover_and_dump_all_clusters(
                regions=["us-east-1"])
        self.assertIn("ERROR", out)
        self.assertIn("Concurrent update conflict", out)
        self.assertIn("another writer moved the graph first", out)
        self.assertIn(f"Live IR was written under {tools.LIVE_PREFIX}", out)
        self.assertIn("If it still stands at STATE_DISCOVERY_LIVE", out)
        # The IR is on disk even though the graph did not move.
        self.assertTrue(self.bucket.blob(tools.LIVE_IR_BLOB).exists())
        # State did not advance.
        self.assertEqual(self.read_state()["current_state"],
                         "STATE_DISCOVERY_LIVE")

    async def test_a_state_deleted_during_the_walk_is_a_conflict(self):
        # A reset removed state.json while the walk ran: the generation the
        # scan read no longer stands. That is the concurrent-update case,
        # not a save failure — nothing is cleared, nothing is written, and
        # the message names the cause.
        def their_reset_lands_then_return(*args, **kwargs):
            self.bucket.blob(tools._STATE_BLOB).delete()
            return STUB_RESULT

        self._patch_walk(side_effect=their_reset_lands_then_return)
        self.bucket.blob(f"{tools.LIVE_PREFIX}stale.csv").upload_from_string("old")
        out = await tools.discover_and_dump_all_clusters(regions=["us-east-1"])
        self.assertIn("ERROR: Concurrent update conflict", out)
        self.assertIn("state.json is gone: it was deleted while this scan ran",
                      out)
        self.assertIn(f"Nothing was written or removed under {tools.LIVE_PREFIX}",
                      out)
        self.assertEqual([b.name for b in
                          self.bucket.list_blobs(prefix=tools.LIVE_PREFIX)],
                         [f"{tools.LIVE_PREFIX}stale.csv"])
        self.assertFalse(self.bucket.blob(tools._STATE_BLOB).exists())

    async def test_a_state_that_cannot_be_re_read_saves_nothing(self):
        # The first read of the state (authorize_and_rehydrate) succeeds;
        # the re-read before the prefix is touched fails. Nothing is
        # cleared or written on a generation that could not be confirmed.
        walked = []
        self._patch_walk(side_effect=lambda *a, **k: walked.append(1) or STUB_RESULT)
        real_reload = fake_gcs.FakeBlob.reload

        def refuse_state(blob, *args, **kwargs):
            if walked and blob.name == tools._STATE_BLOB:
                raise RuntimeError("bucket said no")
            return real_reload(blob, *args, **kwargs)

        self.bucket.blob(f"{tools.LIVE_PREFIX}stale.csv").upload_from_string("old")
        with patch.object(fake_gcs.FakeBlob, "reload", refuse_state):
            out = await tools.discover_and_dump_all_clusters(
                regions=["us-east-1"])
        self.assertIn("ERROR: Live discovery ran but its results could not be "
                      "saved (RuntimeError: bucket said no)", out)
        self.assertIn("Nothing was cleared or written", out)
        self.assertNotIn("partial output", out)
        self.assertEqual([b.name for b in
                          self.bucket.list_blobs(prefix=tools.LIVE_PREFIX)],
                         [f"{tools.LIVE_PREFIX}stale.csv"])
        self.assertEqual(self.read_state()["current_state"],
                         "STATE_DISCOVERY_LIVE")


class PrefixOrderTest(_LiveToolBase):

    async def test_a_skip_that_loses_the_race_removes_nothing(self):
        # A concurrent scan lands between this caller's read of the state and
        # its skip: the generation-matched write fails, and the scan's
        # results under the live prefix must stay — the prefix is cleared
        # only once the skip has actually landed.
        for name in (tools.LIVE_IR_BLOB, f"{tools.LIVE_PREFIX}clusters.csv"):
            self.bucket.blob(name).upload_from_string("theirs")
        original = tools.authorize_and_rehydrate

        def read_then_lose_the_race(*args, **kwargs):
            result = original(*args, **kwargs)
            self.write_state()  # someone else moved the state first
            return result

        with patch.object(tools, "authorize_and_rehydrate",
                          side_effect=read_then_lose_the_race):
            out = await tools.discover_and_dump_all_clusters(
                skip=True, skip_reason="repos-only engagement")
        self.assertIn("ERROR", out)
        self.assertIn("Concurrent update conflict", out)
        self.assertIn("Nothing was scanned or removed", out)
        self.assertEqual(
            sorted(b.name for b in self.bucket.list_blobs(prefix=tools.LIVE_PREFIX)),
            sorted([tools.LIVE_IR_BLOB, f"{tools.LIVE_PREFIX}clusters.csv"]))
        self.assertEqual(self.read_state()["current_state"],
                         "STATE_DISCOVERY_LIVE")

    async def test_a_rescan_removes_what_an_earlier_run_left_under_the_prefix(self):
        # After a rewind, an earlier run's extra table or IR beside
        # this run's fresh tables would read as one run's estate.
        self.bucket.blob(f"{tools.LIVE_PREFIX}stale.csv").upload_from_string("old")
        self.bucket.blob(tools.LIVE_IR_BLOB).upload_from_string("old ir")
        self._patch_walk()
        out = await tools.discover_and_dump_all_clusters(regions=["us-east-1"])
        self.assertIn("SUCCESS", out)
        self.assertFalse(self.bucket.blob(f"{tools.LIVE_PREFIX}stale.csv").exists())
        self.assertNotEqual(
            self.bucket.blob(tools.LIVE_IR_BLOB).download_as_text(), "old ir")


class SkipClearFailureTest(_LiveToolBase):

    async def test_a_skip_whose_clear_fails_still_reports_the_skip(self):
        # The skip has landed by the time the prefix is cleared; a failure
        # there is reported on the same success, with what was left behind,
        # rather than raised as if the skip had not happened.
        self.bucket.blob(f"{tools.LIVE_PREFIX}stale.csv").upload_from_string("old")
        with patch.object(tools, "_clear_live_prefix",
                          side_effect=RuntimeError("bucket said no")):
            out = await tools.discover_and_dump_all_clusters(
                skip=True, skip_reason="repos-only engagement")
        self.assertIn("SUCCESS", out)
        # The clear failed before it could list: the response asserts no
        # objects it never saw.
        self.assertIn("could not be checked (RuntimeError: bucket said no)", out)
        self.assertNotIn("object(s) under", out)
        self.assertIn("Delete them by hand", out)
        self.assertTrue(self.bucket.blob(f"{tools.LIVE_PREFIX}stale.csv").exists())
        self.assertEqual(self.read_state()["current_state"], "STATE_DISCOVERY")

    async def test_a_listing_failure_is_reported_as_unchecked(self):
        with patch.object(fake_gcs.FakeBucket, "list_blobs",
                          side_effect=RuntimeError("listing refused")):
            out = await tools.discover_and_dump_all_clusters(
                skip=True, skip_reason="repos-only engagement")
        self.assertIn("SUCCESS", out)
        self.assertIn("Whether an earlier scan left objects under", out)
        self.assertIn("could not be checked (RuntimeError: listing refused)", out)
        self.assertNotIn("object(s) under", out)
        self.assertEqual(self.read_state()["current_state"], "STATE_DISCOVERY")


class PartialClearTest(_LiveToolBase):

    async def test_a_clear_that_fails_part_way_reports_what_it_removed(self):
        # Two stale objects; the second delete fails. The skip still lands,
        # the response says one was removed and the rest could not be, so
        # the message matches the bucket rather than claiming nothing moved.
        self.bucket.blob(f"{tools.LIVE_PREFIX}a.csv").upload_from_string("old")
        self.bucket.blob(f"{tools.LIVE_PREFIX}b.csv").upload_from_string("old")
        real_delete = fake_gcs.FakeBlob.delete
        deleted = []

        def flaky_delete(blob, *args, **kwargs):
            if deleted:
                raise RuntimeError("bucket said no")
            deleted.append(blob.name)
            return real_delete(blob, *args, **kwargs)

        with patch.object(fake_gcs.FakeBlob, "delete", flaky_delete):
            out = await tools.discover_and_dump_all_clusters(
                skip=True, skip_reason="repos-only engagement")
        self.assertIn("SUCCESS", out)
        self.assertIn("An earlier scan's 1 object(s) under", out)
        self.assertIn("were removed.", out)
        # One of them is still there, so the response does not claim that
        # nothing reads as this run's estate.
        self.assertNotIn("so nothing reads as this run's estate", out)
        self.assertIn("The other 1 object(s) of that scan under", out)
        self.assertIn("could not be removed (RuntimeError: bucket said no)", out)
        remaining = sorted(b.name for b in
                           self.bucket.list_blobs(prefix=tools.LIVE_PREFIX))
        self.assertEqual(len(remaining), 1)
        self.assertEqual(self.read_state()["current_state"], "STATE_DISCOVERY")

    async def test_a_delete_that_fails_first_reports_what_the_listing_saw(self):
        self.bucket.blob(f"{tools.LIVE_PREFIX}a.csv").upload_from_string("old")
        self.bucket.blob(f"{tools.LIVE_PREFIX}b.csv").upload_from_string("old")
        with patch.object(fake_gcs.FakeBlob, "delete",
                          side_effect=RuntimeError("bucket said no")):
            out = await tools.discover_and_dump_all_clusters(
                skip=True, skip_reason="repos-only engagement")
        self.assertIn("SUCCESS", out)
        self.assertIn("An earlier scan's 2 object(s) under", out)
        self.assertIn("could not be removed (RuntimeError: bucket said no)", out)
        self.assertNotIn("were removed", out)

    def test_the_ir_goes_first_so_a_failure_never_leaves_it_standing(self):
        # Alphabetically the IR (live_discovery.json) sits after the a…i
        # tables; a delete order that followed the listing would leave it
        # standing over tables already gone.
        for name in ("autoscaling.csv", "clusters.csv", "images.csv", "nodes.csv"):
            self.bucket.blob(f"{tools.LIVE_PREFIX}{name}").upload_from_string("old")
        self.bucket.blob(tools.LIVE_IR_BLOB).upload_from_string("old ir")
        real_delete = fake_gcs.FakeBlob.delete
        deleted = []

        def flaky_delete(blob, *args, **kwargs):
            if deleted:
                raise RuntimeError("bucket said no")
            deleted.append(blob.name)
            return real_delete(blob, *args, **kwargs)

        with patch.object(fake_gcs.FakeBlob, "delete", flaky_delete):
            with self.assertRaises(tools.PrefixClearError) as ctx:
                tools._clear_live_prefix(self.bucket)
        self.assertEqual(deleted, [tools.LIVE_IR_BLOB])
        self.assertFalse(self.bucket.blob(tools.LIVE_IR_BLOB).exists())
        self.assertEqual((ctx.exception.removed, ctx.exception.found), (1, 5))

    def test_the_clear_raises_with_the_count_already_removed(self):
        self.bucket.blob(f"{tools.LIVE_PREFIX}a.csv").upload_from_string("old")
        self.bucket.blob(f"{tools.LIVE_PREFIX}b.csv").upload_from_string("old")
        real_delete = fake_gcs.FakeBlob.delete
        deleted = []

        def flaky_delete(blob, *args, **kwargs):
            if deleted:
                raise RuntimeError("bucket said no")
            deleted.append(blob.name)
            return real_delete(blob, *args, **kwargs)

        with patch.object(fake_gcs.FakeBlob, "delete", flaky_delete):
            with self.assertRaises(tools.PrefixClearError) as ctx:
                tools._clear_live_prefix(self.bucket)
        self.assertEqual(ctx.exception.removed, 1)
        self.assertEqual(ctx.exception.found, 2)
        self.assertEqual(str(ctx.exception), "RuntimeError: bucket said no")
        self.assertIsInstance(ctx.exception.cause, RuntimeError)

    def test_a_blob_already_gone_counts_as_removed(self):
        # A concurrent skip or scan deleted it between the listing and the
        # delete: the prefix ends up empty either way, which is the point.
        self.bucket.blob(f"{tools.LIVE_PREFIX}a.csv").upload_from_string("old")
        self.bucket.blob(f"{tools.LIVE_PREFIX}b.csv").upload_from_string("old")
        real_delete = fake_gcs.FakeBlob.delete

        def vanished_delete(blob, *args, **kwargs):
            if blob.name.endswith("a.csv"):
                raise exceptions.NotFound("gone")
            return real_delete(blob, *args, **kwargs)

        with patch.object(fake_gcs.FakeBlob, "delete", vanished_delete):
            self.assertEqual(tools._clear_live_prefix(self.bucket), 2)
        self.assertFalse(self.bucket.blob(f"{tools.LIVE_PREFIX}b.csv").exists())


class LibraryLogReleaseTest(_LiveToolBase):
    """The hold on the client libraries' loggers lasts exactly as long as
    the scan: released after a walk that ran, one that failed, an
    authentication that failed after the first client was built — and,
    when the request is cancelled, not before the walk it started is
    over."""

    async def test_the_release_follows_a_walk_that_ran(self):
        self._patch_walk()
        with patch.object(tools.eks_auth, "release_library_logs") as release:
            out = await tools.discover_and_dump_all_clusters(regions=["us-east-1"])
        self.assertIn("SUCCESS", out)
        self.assertEqual(release.call_count, 1)

    async def test_the_release_follows_a_walk_that_failed(self):
        self._patch_walk(side_effect=RuntimeError("cluster went away"))
        with patch.object(tools.eks_auth, "release_library_logs") as release:
            out = await tools.discover_and_dump_all_clusters(regions=["us-east-1"])
        self.assertIn("ERROR: Live discovery failed after authentication", out)
        self.assertEqual(release.call_count, 1)

    async def test_the_release_follows_an_authentication_that_failed(self):
        self._patch_walk()
        with patch.object(tools.eks_auth, "caller_identity",
                          side_effect=RuntimeError("no credentials")), \
             patch.object(tools.eks_auth, "release_library_logs") as release:
            out = await tools.discover_and_dump_all_clusters(regions=["us-east-1"])
        self.assertIn("ERROR: Could not authenticate", out)
        self.assertEqual(release.call_count, 1)

    async def test_a_cancelled_request_releases_only_once_the_walk_ends(self):
        # asyncio.to_thread cannot interrupt its worker: an MCP cancel ends
        # the coroutine at once while the thread goes on signing requests
        # for minutes. The release must follow the walk, not the request —
        # released early, botocore's DEBUG trace of every later signature
        # would reach the server log.
        walk_started = threading.Event()
        walk_may_end = threading.Event()

        def blocking_walk(*args, **kwargs):
            walk_started.set()
            walk_may_end.wait(timeout=10)
            return STUB_RESULT

        self._patch_walk(side_effect=blocking_walk)
        with patch.object(tools.eks_auth, "release_library_logs") as release:
            request = asyncio.ensure_future(
                tools.discover_and_dump_all_clusters(regions=["us-east-1"]))
            self.assertTrue(await asyncio.to_thread(walk_started.wait, 10))
            request.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await request
            # The request is gone; the walk is still running.
            self.assertEqual(release.call_count, 0)
            walk_may_end.set()
            for _ in range(500):
                if release.call_count:
                    break
                await asyncio.sleep(0.01)
        self.assertEqual(release.call_count, 1)
        # A cancelled request writes nothing.
        self.assertFalse(self.bucket.blob(tools.LIVE_IR_BLOB).exists())
        self.assertEqual(self.read_state()["current_state"],
                         "STATE_DISCOVERY_LIVE")


class RegionCoverageTest(_LiveToolBase):
    """No requested region listed its clusters: a failure that keeps the
    state. One denied among others that listed: a line on the success."""

    async def test_no_listable_region_is_an_error_that_keeps_the_state(self):
        result = copy.deepcopy(STUB_RESULT)
        result["ir"].update({
            "regions": [], "clusters": [],
            "notes": ["region eu-west1: not scanned — Could not connect to "
                      "the endpoint URL"]})
        result["ir"]["summary"].update({
            "regions_scanned": 0, "regions_unreachable": ["eu-west1"],
            "clusters_found": 0, "clusters_walked": 0,
            "clusters_unreachable": [], "clusters_using_karpenter": 0,
            "workload_totals": {},
            "note_count": 1})
        self._patch_walk(result=result)
        out = await tools.discover_and_dump_all_clusters(regions=["eu-west1"])
        self.assertTrue(out.startswith(
            "ERROR: Live discovery could not list EKS clusters in any "
            "requested region"), out)
        self.assertIn("eu-west1", out)
        self.assertIn("state stays here", out)
        self.assertEqual(self.read_state()["current_state"],
                         "STATE_DISCOVERY_LIVE")
        self.assertEqual(list(self.bucket.list_blobs(prefix=tools.LIVE_PREFIX)),
                         [])

    async def test_a_listed_region_with_no_clusters_still_completes(self):
        # An empty estate in a region that answered is a finding, not a
        # failure: only a region that could not be listed keeps the state.
        result = copy.deepcopy(STUB_RESULT)
        result["ir"].update({"regions": ["us-east-1"], "clusters": []})
        result["ir"]["summary"].update({
            "regions_scanned": 1, "regions_unreachable": [],
            "clusters_found": 0, "clusters_walked": 0,
            "clusters_unreachable": [], "clusters_using_karpenter": 0,
            "workload_totals": {},
            "note_count": 1})
        self._patch_walk(result=result)
        out = await tools.discover_and_dump_all_clusters(regions=["us-east-1"])
        self.assertTrue(out.startswith(
            "SUCCESS: Live discovery complete — 0 "), out)
        self.assertEqual(self.read_state()["current_state"], "STATE_DISCOVERY")

    async def test_a_region_denied_among_others_is_a_line_on_the_success(self):
        result = copy.deepcopy(STUB_RESULT)
        result["ir"]["notes"].append("region eu-west1: not scanned — denied")
        result["ir"]["summary"]["regions_unreachable"] = ["eu-west1"]
        result["ir"]["summary"]["note_count"] = 2
        self._patch_walk(result=result)
        out = await tools.discover_and_dump_all_clusters(
            regions=["us-east-1", "eu-west1"])
        self.assertIn("SUCCESS", out)
        self.assertIn("across 1 region(s)", out)
        self.assertIn("1 requested region(s) could not be listed", out)
        self.assertIn("eu-west1", out)
        self.assertEqual(self.read_state()["current_state"], "STATE_DISCOVERY")


class ScanClearFailureTest(_LiveToolBase):
    """A clear that stops part-way on the scanned path: the message carries
    the bucket's counts, nothing of the scan is written, the state stays."""

    async def test_the_counts_reach_the_caller(self):
        self.bucket.blob(f"{tools.LIVE_PREFIX}a.csv").upload_from_string("old")
        self.bucket.blob(f"{tools.LIVE_PREFIX}b.csv").upload_from_string("old")
        before = self.read_state()["current_state"]
        real_delete = fake_gcs.FakeBlob.delete

        def refused_delete(blob, *args, **kwargs):
            if blob.name.endswith("b.csv"):
                raise exceptions.Forbidden("denied")
            return real_delete(blob, *args, **kwargs)

        self._patch_walk()
        with patch.object(fake_gcs.FakeBlob, "delete", refused_delete):
            out = await tools.discover_and_dump_all_clusters(regions=["us-east-1"])
        self.assertIn("ERROR: Live discovery ran but its results could not "
                      "be saved: clearing the earlier estate", out)
        self.assertIn("1 of its 2 object(s) removed", out)
        self.assertIn("Forbidden", out)
        self.assertNotIn("Traceback", out)
        self.assertFalse(self.bucket.blob(tools.LIVE_IR_BLOB).exists())
        self.assertTrue(self.bucket.blob(f"{tools.LIVE_PREFIX}b.csv").exists())
        self.assertEqual(self.read_state()["current_state"], before)

    async def test_a_listing_that_fails_says_so(self):
        self._patch_walk()
        before = self.read_state()["current_state"]
        with patch.object(tools, "_clear_live_prefix",
                          side_effect=tools.PrefixClearError(
                              0, RuntimeError("bucket said no"))):
            out = await tools.discover_and_dump_all_clusters(regions=["us-east-1"])
        self.assertIn("its listing failed, nothing removed", out)
        self.assertIn("RuntimeError: bucket said no", out)
        self.assertEqual(self.read_state()["current_state"], before)


class StateWriteFailureTest(_LiveToolBase):
    """A state write the bucket refuses (not a generation conflict) is
    reported on both paths with what stands: nothing on the skip path,
    the Live IR on the scanned path."""

    def _refuse_state(self):
        real = fake_gcs.FakeBlob.upload_from_string

        def refused(blob, *args, **kwargs):
            if blob.name == tools._STATE_BLOB:
                raise exceptions.Forbidden("denied")
            return real(blob, *args, **kwargs)

        return patch.object(fake_gcs.FakeBlob, "upload_from_string", refused)

    async def test_the_skip_path_reports_nothing_removed(self):
        self.bucket.blob(f"{tools.LIVE_PREFIX}a.csv").upload_from_string("old")
        before = self.read_state()["current_state"]
        with self._refuse_state():
            out = await tools.discover_and_dump_all_clusters(
                skip=True, skip_reason="repos-only")
        self.assertIn("ERROR: Saving the migration state failed (Forbidden", out)
        self.assertIn("Nothing was scanned or removed", out)
        self.assertTrue(self.bucket.blob(f"{tools.LIVE_PREFIX}a.csv").exists())
        self.assertEqual(self.read_state()["current_state"], before)

    async def test_the_scanned_path_reports_what_was_written(self):
        self._patch_walk()
        before = self.read_state()["current_state"]
        with self._refuse_state():
            out = await tools.discover_and_dump_all_clusters(regions=["us-east-1"])
        self.assertIn("ERROR: Saving the migration state failed (Forbidden", out)
        self.assertIn(f"The Live IR was written under {tools.LIVE_PREFIX}", out)
        self.assertIn(f"state stays at {before}", out)
        self.assertTrue(self.bucket.blob(tools.LIVE_IR_BLOB).exists())
        self.assertEqual(self.read_state()["current_state"], before)


class TwentyThirdReviewRoundToolTest(_LiveToolBase):
    """The twenty-third round's tool-level fixes: a blank region name, a
    table write the bucket refuses after the clear, the remedy named for a
    DAG that predates the step, and the lost race's scan-after-scan
    reading."""

    async def test_a_blank_region_name_is_refused_before_anything_runs(self):
        self._patch_walk()
        for regions in ([""], ["us-east-1", " "], [None]):
            out = await tools.discover_and_dump_all_clusters(regions=regions)
            self.assertIn("ERROR: Every region must be a non-empty AWS region "
                          "name", out)
            self.assertIn(repr(regions), out)
        tools.live_discovery.run_live_discovery.assert_not_called()
        self.assertEqual(self.read_state()["current_state"],
                         "STATE_DISCOVERY_LIVE")

    async def test_a_table_write_refused_after_the_clear_says_what_stands(self):
        self._patch_walk()
        self.bucket.blob(f"{tools.LIVE_PREFIX}old.csv").upload_from_string("old")
        real = fake_gcs.FakeBlob.upload_from_string

        def refused(blob, *args, **kwargs):
            if blob.name.startswith(tools.LIVE_PREFIX) and blob.name.endswith(".csv"):
                raise exceptions.Forbidden("denied")
            return real(blob, *args, **kwargs)

        with patch.object(fake_gcs.FakeBlob, "upload_from_string", refused):
            out = await tools.discover_and_dump_all_clusters(regions=["us-east-1"])
        self.assertIn("ERROR: Live discovery ran but its results could not be "
                      "saved (Forbidden", out)
        self.assertIn(f"earlier estate under {tools.LIVE_PREFIX} was cleared "
                      "before the writes began", out)
        self.assertIn("tables without its IR", out)
        self.assertIn("state has not moved", out)
        self.assertIn("Re-run discover_and_dump_all_clusters", out)
        self.assertFalse(self.bucket.blob(f"{tools.LIVE_PREFIX}old.csv").exists())
        self.assertEqual(self.read_state()["current_state"],
                         "STATE_DISCOVERY_LIVE")

    async def test_a_dag_without_the_transition_names_the_upgrade_tool(self):
        self._patch_walk()
        dag = json.loads(self.bucket.blob("platform_dag.json").download_as_text())
        # A DAG that predates the step's tool transition (the validator
        # wants a non-empty block, so an unrelated outcome stands in).
        dag["states"]["STATE_DISCOVERY_LIVE"]["transitions"] = {
            "on_skip": "STATE_DISCOVERY"}
        self.bucket.blob("platform_dag.json").upload_from_string(json.dumps(dag))
        out = await tools.discover_and_dump_all_clusters(regions=["us-east-1"])
        self.assertTrue(out.startswith("ERROR:"), out)
        self.assertIn("upgrade_ledger_dags", out)
        tools.live_discovery.run_live_discovery.assert_not_called()

    async def test_the_lost_race_message_reads_a_second_scan_as_the_estate(self):
        self._patch_walk()
        real_clear = tools._clear_live_prefix

        def bump_then_clear(*args, **kwargs):
            self.write_state()      # another writer, same state, new generation
            return real_clear(*args, **kwargs)

        with patch.object(tools, "_clear_live_prefix", bump_then_clear):
            out = await tools.discover_and_dump_all_clusters(regions=["us-east-1"])
        self.assertIn("If it still stands at STATE_DISCOVERY_LIVE", out)
        self.assertIn("after a skip, these objects are not its estate", out)
        self.assertIn("after another scan of the same clusters", out)
        self.assertIn("serves the later steps as the winner's would", out)


if __name__ == "__main__":
    unittest.main()
