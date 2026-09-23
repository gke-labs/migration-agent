"""Unit tests for the per-cwd session cache (P0c) in state_management.

Covers scoped-path resolution, the legacy read fallback, write targeting,
cross-cwd isolation, and the workspace_name mismatch guard in
authorize_and_rehydrate. No GCS, credentials, or real home directory is
touched: paths are redirected into a TemporaryDirectory and the storage
client is a MagicMock.
"""

import datetime
import hashlib
import json
import os
import tempfile
import unittest
from unittest import mock

import yaml
from google.auth import compute_engine
from google.auth.compute_engine import _metadata

import servers.dag.state_management as sm


class ScopedPathTest(unittest.TestCase):

    def test_path_is_sha256_prefix_of_cwd_under_config_dir(self):
        cwd = "/work/session-a"
        expected_name = hashlib.sha256(cwd.encode("utf-8")).hexdigest()[:16] + ".yaml"
        self.assertEqual(
            sm.scoped_config_path(cwd),
            os.path.join(sm.LEDGER_CONFIG_DIR, expected_name),
        )

    def test_distinct_cwds_get_distinct_paths(self):
        self.assertNotEqual(
            sm.scoped_config_path("/work/session-a"),
            sm.scoped_config_path("/work/session-b"),
        )

    def test_defaults_to_process_cwd(self):
        with mock.patch.object(os, "getcwd", return_value="/work/session-a"), \
             mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("GKMA_SESSION_CWD", None)
            self.assertEqual(sm.scoped_config_path(),
                             sm.scoped_config_path("/work/session-a"))

    def test_env_var_does_not_override_process_cwd(self):
        # GKMA_SESSION_CWD is the frontend's bridge and is interpreted by the
        # frontend process only (servers/frontend/server.py passes it in as
        # the explicit cwd). The DAG server must ignore it: a value exported
        # in a user's shell would otherwise collapse every server started
        # from that shell onto one shared scoped file — the machine-global
        # clobber P0c removes.
        with mock.patch.object(os, "getcwd", return_value="/repo/root"), \
             mock.patch.dict(os.environ, {"GKMA_SESSION_CWD": "/work/session-a"}):
            self.assertEqual(sm.scoped_config_path(),
                             sm.scoped_config_path("/repo/root"))

    def test_explicit_cwd_argument_selects_the_scope(self):
        # The frontend pins itself to the spawning session by passing that
        # session's cwd explicitly.
        with mock.patch.object(os, "getcwd", return_value="/repo/root"):
            self.assertNotEqual(sm.scoped_config_path("/work/session-a"),
                                sm.scoped_config_path())


class _CacheDirBase(unittest.TestCase):
    """Redirects both cache locations into a temp dir and fixes the cwd."""

    CWD = "/work/session-a"

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self._patches = [
            mock.patch.object(sm, "LEDGER_CONFIG_DIR",
                              os.path.join(self._tmp.name, "ledger_config.d")),
            mock.patch.object(sm, "LEDGER_CONFIG_PATH",
                              os.path.join(self._tmp.name, "ledger_config.yaml")),
            mock.patch.object(os, "getcwd", return_value=self.CWD),
        ]
        for p in self._patches:
            p.start()
        os.environ.pop("GKMA_SESSION_CWD", None)

    def tearDown(self):
        for p in self._patches:
            p.stop()
        self._tmp.cleanup()

    def write(self, workspace="ws-a", cwd=None):
        if cwd is not None:
            with mock.patch.object(os, "getcwd", return_value=cwd):
                sm.write_local_config("gs://bucket-" + workspace, "platform",
                                      workspace, "proj")
        else:
            sm.write_local_config("gs://bucket-" + workspace, "platform",
                                  workspace, "proj")

    def write_legacy(self, workspace="ws-legacy"):
        with open(sm.LEDGER_CONFIG_PATH, "w") as f:
            yaml.safe_dump({"ledger_uri": "gs://legacy", "resolved_role": "platform",
                            "workspace_name": workspace, "gcp_project": "proj"}, f)


class ReadWriteScopingTest(_CacheDirBase):

    def test_write_goes_to_the_scoped_path_only(self):
        self.write(workspace="ws-a")
        self.assertTrue(os.path.exists(sm.scoped_config_path(self.CWD)))
        self.assertFalse(os.path.exists(sm.LEDGER_CONFIG_PATH))

    def test_read_prefers_the_scoped_file_over_legacy(self):
        self.write_legacy(workspace="ws-legacy")
        self.write(workspace="ws-a")
        self.assertEqual(sm.read_local_config()["workspace_name"], "ws-a")

    def test_read_falls_back_to_legacy_when_scoped_absent(self):
        self.write_legacy(workspace="ws-legacy")
        self.assertEqual(sm.read_local_config()["workspace_name"], "ws-legacy")

    def test_legacy_fallback_is_adopted_one_shot(self):
        # The legacy file records no cwd, so it would serve as a default
        # session for every directory forever. The first read adopts it into
        # the reader's scoped path and deletes the global file.
        self.write_legacy(workspace="ws-legacy")
        first = sm.read_local_config()
        self.assertEqual(first["workspace_name"], "ws-legacy")
        self.assertFalse(os.path.exists(sm.LEDGER_CONFIG_PATH))
        scoped = sm.scoped_config_path(self.CWD)
        self.assertTrue(os.path.exists(scoped))
        # The session keeps working from its scoped copy...
        self.assertEqual(sm.read_local_config(), first)
        # ...and a DIFFERENT cwd no longer inherits the legacy session.
        with mock.patch.object(os, "getcwd", return_value="/work/other"):
            with self.assertRaisesRegex(ValueError, "join_ledger"):
                sm.read_local_config()

    def test_read_without_any_cache_raises(self):
        with self.assertRaisesRegex(ValueError, "join_ledger"):
            sm.read_local_config()

    def test_malformed_cache_file_is_a_clean_error_not_none(self):
        # An interrupted write leaves an empty/truncated file; yaml then
        # yields None (or a scalar), which must become an actionable re-join
        # instruction, not an AttributeError at some .get() call site.
        scoped = sm.scoped_config_path(self.CWD)
        os.makedirs(os.path.dirname(scoped), exist_ok=True)
        for content in ("", "null", "- just\n- a\n- list\n"):
            with self.subTest(content=content):
                with open(scoped, "w") as f:
                    f.write(content)
                with self.assertRaisesRegex(ValueError, "join_ledger"):
                    sm.read_local_config()

    def test_sessions_in_different_cwds_do_not_clobber_each_other(self):
        # The P0c bug: pre-fix, the second join overwrote the first session's
        # cache. Scoped caches keep both, keyed by cwd.
        self.write(workspace="ws-a", cwd="/work/session-a")
        self.write(workspace="ws-b", cwd="/work/session-b")
        with mock.patch.object(os, "getcwd", return_value="/work/session-a"):
            self.assertEqual(sm.read_local_config()["workspace_name"], "ws-a")
        with mock.patch.object(os, "getcwd", return_value="/work/session-b"):
            self.assertEqual(sm.read_local_config()["workspace_name"], "ws-b")


class MismatchGuardTest(_CacheDirBase):
    """authorize_and_rehydrate refuses a cache naming the wrong workspace."""

    STATE = {"current_state": "STATE_DISCOVERY", "history": [], "variables": {}}

    def setUp(self):
        super().setUp()
        self._saved_client = sm.gcs_client
        self._email = mock.patch.object(
            sm, "get_authenticated_user_email", return_value="pe@test")
        self._email.start()

    def tearDown(self):
        self._email.stop()
        sm.gcs_client = self._saved_client
        super().tearDown()

    def wire(self, registry: dict):
        sm.gcs_client = mock.MagicMock()
        bucket = sm.gcs_client.bucket.return_value
        registry_blob, state_blob = mock.MagicMock(), mock.MagicMock()
        registry_blob.download_as_text.return_value = json.dumps(registry)
        state_blob.download_as_text.return_value = json.dumps(self.STATE)
        state_blob.generation = 7
        bucket.blob.side_effect = lambda path: (
            state_blob if path == "platform/onboarding/state.json" else registry_blob)

    def test_cached_workspace_must_match_the_registry(self):
        self.write(workspace="ws-cached")
        self.wire({"workspace_name": "ws-registry",
                   "roles": {"platform_engineers": ["pe@test"]}})
        with self.assertRaises(ValueError) as ctx:
            sm.authorize_and_rehydrate(None)
        # The hard error names both workspaces.
        self.assertIn("ws-cached", str(ctx.exception))
        self.assertIn("ws-registry", str(ctx.exception))

    def test_matching_workspace_passes(self):
        self.write(workspace="ws-a")
        self.wire({"workspace_name": "ws-a",
                   "roles": {"platform_engineers": ["pe@test"]}})
        state, generation, config = sm.authorize_and_rehydrate(None)
        self.assertEqual(state["current_state"], "STATE_DISCOVERY")
        self.assertEqual(generation, 7)
        self.assertEqual(config["workspace_name"], "ws-a")
        # The resolved identity rides on the in-memory session config for
        # mutations that act in the user's name (the PR commits).
        self.assertEqual(config["user_email"], "pe@test")

    def test_registry_without_workspace_name_skips_the_guard(self):
        # Compatibility: mock/old registries that never declare a workspace
        # name are not rejected — the guard needs both names to compare.
        self.write(workspace="ws-a")
        self.wire({"roles": {"platform_engineers": ["pe@test"]}})
        state, _, _ = sm.authorize_and_rehydrate(None)
        self.assertEqual(state["current_state"], "STATE_DISCOVERY")

    def test_stale_legacy_cache_is_caught_by_the_guard(self):
        # A pre-scoping legacy cache left behind by another workspace: the
        # fallback still reads it, but the guard stops it from acting.
        self.write_legacy(workspace="ws-old")
        self.wire({"workspace_name": "ws-new",
                   "roles": {"platform_engineers": ["pe@test"]}})
        with self.assertRaisesRegex(ValueError, "ws-old.*ws-new"):
            sm.authorize_and_rehydrate(None)


class WorkloadRehydrateTest(_CacheDirBase):
    """authorize_and_rehydrate_workload: the developer-path twin and the ONE
    claimant check (spec v2 decision 11)."""

    REGISTRY = {"workspace_name": "ws-a",
                "roles": {"developers": ["dev-a@x.com"], "admins": ["adm@x.com"]}}

    def setUp(self):
        super().setUp()
        self._saved_client = sm.gcs_client
        self._email = mock.patch.object(
            sm, "get_authenticated_user_email", return_value="dev-a@x.com")
        self._email.start()

    def tearDown(self):
        self._email.stop()
        sm.gcs_client = self._saved_client
        super().tearDown()

    def write_dev(self, component="orders-component", workspace="ws-a"):
        sm.write_local_config("gs://bucket-" + workspace, "developers",
                              workspace, "proj", component=component)

    def state(self, claimant="dev-a@x.com", current="STATE_WKLD_SCOPE"):
        return {"current_state": current, "history": [],
                "variables": {"component": "orders-component",
                              "claim": {"claimant": claimant, "claimed_at": "T0"}}}

    def wire(self, registry, workload_state=None, workload_error=None):
        sm.gcs_client = mock.MagicMock()
        bucket = sm.gcs_client.bucket.return_value
        registry_blob, state_blob = mock.MagicMock(), mock.MagicMock()
        registry_blob.download_as_text.return_value = json.dumps(registry)
        if workload_error is not None:
            state_blob.reload.side_effect = workload_error
        else:
            state_blob.download_as_text.return_value = json.dumps(workload_state)
            state_blob.generation = 11
        bucket.blob.side_effect = lambda path: (
            state_blob if path.startswith("workloads/") else registry_blob)

    def test_developer_role_is_accepted(self):
        self.write_dev()
        self.wire(self.REGISTRY, self.state())
        state, generation, config = sm.authorize_and_rehydrate_workload(None)
        self.assertEqual(state["current_state"], "STATE_WKLD_SCOPE")
        self.assertEqual(generation, 11)
        self.assertEqual(config["component"], "orders-component")

    def test_claimant_enforcement_names_the_claimant(self):
        self.write_dev()
        self.wire(self.REGISTRY, self.state(claimant="dev-b@x.com"))
        with self.assertRaises(PermissionError) as ctx:
            sm.authorize_and_rehydrate_workload(None)
        self.assertIn("dev-b@x.com", str(ctx.exception))
        self.assertIn("reclaim_component=True", str(ctx.exception))

    def test_enforce_claimant_false_allows_reading(self):
        self.write_dev()
        self.wire(self.REGISTRY, self.state(claimant="dev-b@x.com"))
        state, _, _ = sm.authorize_and_rehydrate_workload(
            None, enforce_claimant=False)
        self.assertEqual(state["current_state"], "STATE_WKLD_SCOPE")

    def test_component_absent_from_session_errors_naming_rejoin(self):
        sm.write_local_config("gs://bucket-ws-a", "developers", "ws-a", "proj")
        self.wire(self.REGISTRY, self.state())
        with self.assertRaisesRegex(ValueError, "join_ledger"):
            sm.authorize_and_rehydrate_workload(None)

    def test_uninitialized_component_errors_naming_join(self):
        from google.api_core import exceptions as gexc
        self.write_dev()
        self.wire(self.REGISTRY, workload_error=gexc.NotFound("nope"))
        with self.assertRaisesRegex(ValueError, "not initialized"):
            sm.authorize_and_rehydrate_workload(None)

    def test_forbidden_returns_the_admin_binding_instruction(self):
        from google.api_core import exceptions as gexc
        self.write_dev()
        self.wire(self.REGISTRY, workload_error=gexc.Forbidden("403"))
        with self.assertRaises(PermissionError) as ctx:
            sm.authorize_and_rehydrate_workload(None)
        text = str(ctx.exception)
        self.assertIn("managed-folder", text)
        self.assertIn("gcloud storage managed-folders", text)
        self.assertIn("workloads/orders-component/", text)

    def test_p0c_mismatch_guard_fires_on_the_workload_path(self):
        self.write_dev(workspace="ws-cached")
        self.wire(self.REGISTRY, self.state())  # registry says ws-a
        with self.assertRaisesRegex(ValueError, "ws-cached.*ws-a"):
            sm.authorize_and_rehydrate_workload(None)

    def test_missing_claimant_is_a_hard_stop_not_a_pass(self):
        # state.json is writable ledger data: "nobody recorded a claim" must
        # not mean "anybody may mutate". A hand-edited or partially written
        # state with no claim key gets a refusal naming the repair path.
        self.write_dev()
        state = {"current_state": "STATE_WKLD_SCOPE", "history": [],
                 "variables": {"component": "orders-component"}}
        self.wire(self.REGISTRY, state)
        with self.assertRaises(PermissionError) as ctx:
            sm.authorize_and_rehydrate_workload(None)
        self.assertIn("no claimant", str(ctx.exception))
        self.assertIn("join_ledger", str(ctx.exception))

    def test_missing_claimant_still_readable_without_enforcement(self):
        self.write_dev()
        state = {"current_state": "STATE_WKLD_SCOPE", "history": [],
                 "variables": {"component": "orders-component"}}
        self.wire(self.REGISTRY, state)
        got, _, _ = sm.authorize_and_rehydrate_workload(
            None, enforce_claimant=False)
        self.assertEqual(got["current_state"], "STATE_WKLD_SCOPE")

    def test_component_mismatch_between_state_and_session_is_refused(self):
        # The authorized object and the object the tools mutate must be the
        # SAME component: a state.json naming a different variables.component
        # would steer every write path at a component the claimant check
        # never looked at.
        self.write_dev(component="orders-component")
        state = self.state()
        state["variables"]["component"] = "other-component"
        self.wire(self.REGISTRY, state)
        with self.assertRaisesRegex(ValueError, "other-component"):
            sm.authorize_and_rehydrate_workload(None)

    def test_authorized_component_is_normalized_onto_the_config(self):
        # Tools take their write paths from config["component"]; the
        # rehydrate hands back exactly what it authorized.
        self.write_dev()
        self.wire(self.REGISTRY, self.state())
        _, _, config = sm.authorize_and_rehydrate_workload(None)
        self.assertEqual(config["component"], "orders-component")


class _FakeMetadataCredentials:
    """Stand-in for google.auth.compute_engine.Credentials.

    service_account_email is the alias "default" until the first refresh,
    after which it becomes the real address (or stays "default" when
    resolved_email is None, mimicking a metadata server that never resolves).
    """

    def __init__(self, resolved_email):
        self._resolved_email = resolved_email
        self.service_account_email = "default"
        self.valid = False
        self.token = None
        self.refresh_calls = 0

    def refresh(self, request):
        self.refresh_calls += 1
        self.valid = True
        self.token = "metadata-token"
        if self._resolved_email:
            self.service_account_email = self._resolved_email


class _FakeTokeninfoResponse:

    def __init__(self, payload):
        self._payload = json.dumps(payload).encode()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def read(self):
        return self._payload


class AuthenticatedEmailTest(unittest.TestCase):
    """get_authenticated_user_email must never hand back the metadata alias
    "default" as an identity."""

    def resolve(self, credentials, tokeninfo=None, env=None):
        env = dict(env or {})
        with mock.patch("google.auth.default",
                        return_value=(credentials, "proj")), \
             mock.patch.object(sm, "Request"), \
             mock.patch("urllib.request.urlopen",
                        return_value=_FakeTokeninfoResponse(tokeninfo or {})) \
                 as urlopen, \
             mock.patch.dict(os.environ, env, clear=False):
            if "GKE_MIGRATION_USER_EMAIL" not in env:
                os.environ.pop("GKE_MIGRATION_USER_EMAIL", None)
            email = sm.get_authenticated_user_email()
        return email, urlopen

    def test_metadata_creds_resolve_real_email_after_refresh(self):
        creds = _FakeMetadataCredentials("vm-sa@proj.iam.gserviceaccount.com")
        email, urlopen = self.resolve(creds)
        self.assertEqual(email, "vm-sa@proj.iam.gserviceaccount.com")
        self.assertEqual(creds.refresh_calls, 1)
        urlopen.assert_not_called()

    def test_service_account_key_email_returned_without_refresh(self):
        creds = mock.Mock()
        creds.service_account_email = "key-sa@proj.iam.gserviceaccount.com"
        creds.valid = False
        email, urlopen = self.resolve(creds)
        self.assertEqual(email, "key-sa@proj.iam.gserviceaccount.com")
        creds.refresh.assert_not_called()
        urlopen.assert_not_called()

    def test_real_compute_engine_credentials_resolve_via_metadata(self):
        """Same contract as above, proven against the real
        google.auth.compute_engine.Credentials class rather than the fake:
        only the two metadata-server calls its refresh makes are mocked."""
        creds = compute_engine.Credentials()
        self.assertEqual(creds.service_account_email, "default")
        with mock.patch.object(
                _metadata, "get_service_account_info",
                return_value={"email": "vm-sa@proj.iam.gserviceaccount.com",
                              "scopes": ["https://www.googleapis.com/auth/cloud-platform"]}) \
                    as get_info, \
             mock.patch.object(
                _metadata, "get_service_account_token",
                return_value=("metadata-token",
                              datetime.datetime.now(datetime.timezone.utc)
                              + datetime.timedelta(hours=1))) as get_token, \
             mock.patch.object(creds, "refresh", wraps=creds.refresh) as refresh:
            email, urlopen = self.resolve(creds)
        self.assertEqual(email, "vm-sa@proj.iam.gserviceaccount.com")
        self.assertEqual(refresh.call_count, 1)
        get_info.assert_called_once()
        get_token.assert_called_once()
        urlopen.assert_not_called()

    def test_unresolved_default_alias_falls_back_to_env(self):
        """Defensive coverage of the sentinel path, not a real metadata-server
        mode: the real library raises RefreshError when the metadata response
        carries no email, so a credential that refreshes successfully yet still
        reports "default" cannot occur with google-auth. The fake models it so
        the alias is provably never returned even then."""
        creds = _FakeMetadataCredentials(resolved_email=None)
        email, urlopen = self.resolve(
            creds, tokeninfo={"aud": "x"},
            env={"GKE_MIGRATION_USER_EMAIL": "ci@example.com"})
        self.assertEqual(email, "ci@example.com")
        self.assertEqual(creds.refresh_calls, 1)
        urlopen.assert_called_once()

    def test_unresolved_default_alias_without_env_raises(self):
        creds = _FakeMetadataCredentials(resolved_email=None)
        with self.assertRaises(PermissionError):
            self.resolve(creds, tokeninfo={"aud": "x"})


if __name__ == "__main__":
    unittest.main()
