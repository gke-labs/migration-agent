import unittest
from unittest.mock import patch, AsyncMock, MagicMock
import os
import copy
import json
import shutil
import tempfile
from types import SimpleNamespace
from google.api_core import exceptions
import testing_env  # before main: launcher off, per-process session-config dir
import main

# Keep the whole module hermetic: several test classes write the session cache
# in setUp before any per-class redirect ran, so the real ~/.ledger_config.yaml
# and ~/.ledger_config.d must never be touched. The paths live in a directory
# private to this process (testing_env), so concurrent checkouts running the
# suite cannot delete each other's cached session. Individual setUps re-assign
# the same values for clarity.
main.LEDGER_CONFIG_PATH = testing_env.LEDGER_CONFIG_PATH
main.state_mgr.LEDGER_CONFIG_PATH = testing_env.LEDGER_CONFIG_PATH
main.state_mgr.LEDGER_CONFIG_DIR = testing_env.LEDGER_CONFIG_DIR
from server.dag_validation import DagValidationError
from server import blocker_criteria
from servers.dag import dispatch
from servers.phases.assessment.assessment_review_1 import tools as review_tools
from servers.phases.assessment.assessment_blockers_2 import tools as blocker_tools
from servers.phases.discovery.discovery_init_1.files import find_configuration_files
from servers.phases.discovery import discovery_datareview_3
from servers.phases.discovery.discovery_init_1 import overrides as overrides_lib
from servers.phases.deployment import datamigration as datamigration_lib
from servers.phases.deployment import runbooks as runbooks_lib
from servers.phases.discovery.discovery_init_1 import consumers as consumers_lib
from servers.phases.deployment import actions as deployment_actions
from servers.phases.deployment import replication
from servers.phases.deployment.deployment_provision_1.tools import (
    mark_replication_complete,
    abandon_image_replication,
    prepare_image_deployment,
)
from servers.phases.landingzone import actions as lz_actions
from servers.phases.translation.translation_translate_1.tools import UNIT_BLOB_PREFIX
from servers.dag.server import exports as exports_lib

# A landing-zone design declaring its own registry, as the deployment scan sees it.
AR_TF = (
    'resource "google_artifact_registry_repository" "containers" {\n'
    '  project       = "shared-artifacts"\n'
    '  location      = "us-central1"\n'
    '  repository_id = "test-workspace"\n'
    '  format        = "DOCKER"\n'
    '  docker_config {\n'
    '    immutable_tags = false\n'
    '  }\n'
    '}\n'
)

# Every inventory fixture carries the full trigger set: partial sets are legal
# for worker fragments, but a complete analysis names all four.
ALL_TRIGGERS = {
    "karpenter": True,
    "privileged_daemonsets": False,
    "gpu_tpu": False,
    "vpc_peering": True,
}


class MainTest(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        main.LEDGER_CONFIG_PATH = testing_env.LEDGER_CONFIG_PATH
        # Redirect the state_management module's paths too: read_local_config and
        # write_local_config use them, so tests must not touch ~/.ledger_config*.
        main.state_mgr.LEDGER_CONFIG_PATH = testing_env.LEDGER_CONFIG_PATH
        main.state_mgr.LEDGER_CONFIG_DIR = testing_env.LEDGER_CONFIG_DIR
        shutil.rmtree(main.state_mgr.LEDGER_CONFIG_DIR, ignore_errors=True)
        if os.path.exists(main.LEDGER_CONFIG_PATH):
            os.remove(main.LEDGER_CONFIG_PATH)
        main.gcs_client = None
        main.current_state = "STATE_CONFIGURATION_GATHERING"
            
    def setup_mock_gcs(self, mock_gcs_client_class, registry_yaml=None, state_dict=None, state_not_found=False):
        mock_gcs_client = MagicMock()
        mock_gcs_client_class.return_value = mock_gcs_client
        mock_bucket = MagicMock()
        mock_gcs_client.bucket.return_value = mock_bucket
        
        mock_registry_blob = MagicMock()
        mock_state_blob = MagicMock()
        mock_dag_blob = MagicMock()
        mock_inventory_blob = MagicMock()

        # Load real static DAG from execution directory
        dir_path = os.path.dirname(os.path.realpath(__file__))
        dag_path = os.path.join(dir_path, "platform_dag.json")
        with open(dag_path, "r") as f:
             mock_dag_blob.download_as_text.return_value = f.read()

        if registry_yaml:
            mock_registry_blob.download_as_text.return_value = registry_yaml

        if state_dict:
            mock_state_blob.download_as_text.return_value = json.dumps(state_dict)
            mock_state_blob.generation = 123

        if state_not_found:
            mock_state_blob.reload.side_effect = exceptions.NotFound("Not Found")

        # No inventory exists until a test writes one; tests that need a
        # round-trip install an upload_side_effect on this blob.
        mock_inventory_blob.reload.side_effect = exceptions.NotFound("Not Found")
        # Exposed as an attribute rather than in the return tuple so existing
        # 5-way unpacking keeps working.
        self.mock_inventory_blob = mock_inventory_blob

        # exports.json and the discovery manifest start absent, like the
        # inventory. Exposed as attributes for the same reason.
        mock_exports_blob = MagicMock()
        mock_exports_blob.reload.side_effect = exceptions.NotFound("Not Found")
        mock_exports_blob.download_as_text.side_effect = exceptions.NotFound("Not Found")
        self.mock_exports_blob = mock_exports_blob
        mock_manifest_blob = MagicMock()
        mock_manifest_blob.download_as_text.side_effect = exceptions.NotFound("Not Found")
        self.mock_manifest_blob = mock_manifest_blob

        # The data review's corrections object. Absent until a review records
        # something — and it MUST be its own blob rather than falling through
        # to the catch-all: the scan refuses to run against an unreadable one,
        # since scanning without it would silently drop every correction.
        mock_overrides_blob = MagicMock()
        mock_overrides_blob.reload.side_effect = exceptions.NotFound("Not Found")
        mock_overrides_blob.download_as_text.side_effect = exceptions.NotFound("Not Found")
        self.mock_overrides_blob = mock_overrides_blob

        # What the scan derived before any correction was replayed. The review
        # rebuilds the section from it, so the round-trip is always installed:
        # every scan writes it, and an amend refuses without it.
        mock_scan_baseline_blob = MagicMock()
        mock_scan_baseline_blob.download_as_text.side_effect = exceptions.NotFound("Not Found")

        def baseline_upload(data_str, **kwargs):
            mock_scan_baseline_blob.download_as_text.side_effect = None
            mock_scan_baseline_blob.download_as_text.return_value = data_str
        mock_scan_baseline_blob.upload_from_string.side_effect = baseline_upload
        self.mock_scan_baseline_blob = mock_scan_baseline_blob

        # What has actually moved. Its own blob for the same reason the
        # corrections object has one: every reporting tool refuses outright
        # when it is unreadable, so it must not fall through to the catch-all.
        mock_migrations_blob = MagicMock()
        mock_migrations_blob.download_as_text.side_effect = exceptions.NotFound(
            "Not Found")

        def migrations_upload(data_str, **kwargs):
            mock_migrations_blob.download_as_text.side_effect = None
            mock_migrations_blob.download_as_text.return_value = data_str
            # Incremented, as the other round-trips are: a fixed generation
            # makes every if_generation_match pass unconditionally.
            mock_migrations_blob.generation += 1
        mock_migrations_blob.generation = 20
        mock_migrations_blob.upload_from_string.side_effect = migrations_upload
        self.mock_migrations_blob = mock_migrations_blob

        mock_dm_runbook_blob = MagicMock()
        self.mock_dm_runbook_blob = mock_dm_runbook_blob

        # The per-service adapted runbooks. A real bucket lists them back, and
        # the worklist marks an entry "adapted" from that listing — so they get
        # a store rather than the catch-all blob, which would report every
        # service as adapted the moment any one of them was.
        self.rendered_runbooks = {}

        # One mock per path, kept, so a generation survives between the
        # existence check and the guarded write — the overwrite refusal and
        # the if_generation_match are both unobservable against a fresh mock
        # each call.
        self.rendered_runbook_blobs = {}

        def rendered_blob(path):
            blob = self.rendered_runbook_blobs.get(path)
            if blob is not None:
                return blob
            blob = MagicMock()
            blob.generation = None
            blob.reload.side_effect = exceptions.NotFound("Not Found")

            def upload(data_str, **kwargs):
                self.rendered_runbooks[path] = data_str
                blob.reload.side_effect = None
                blob.generation = (blob.generation or 30) + 1
            blob.upload_from_string.side_effect = upload
            self.rendered_runbook_blobs[path] = blob
            return blob

        def list_blobs(bucket_or_name, prefix=None, **kwargs):
            return [SimpleNamespace(name=name)
                    for name in sorted(self.rendered_runbooks)
                    if prefix is None or name.startswith(prefix)]
        mock_gcs_client.list_blobs.side_effect = list_blobs

        def blob_side_effect(path):
            if path == datamigration_lib.MIGRATIONS_BLOB:
                return mock_migrations_blob
            elif path == datamigration_lib.RUNBOOK_BLOB:
                return mock_dm_runbook_blob
            elif path.startswith(runbooks_lib.RENDERED_PREFIX):
                return rendered_blob(path)
            elif path == "platform/onboarding/state.json":
                return mock_state_blob
            elif path == "platform_dag.json":
                return mock_dag_blob
            elif path == main.state_mgr.INVENTORY_BLOB_PATH:
                return mock_inventory_blob
            elif path == exports_lib.EXPORTS_BLOB:
                return mock_exports_blob
            elif path == exports_lib.MANIFEST_BLOB:
                return mock_manifest_blob
            elif path == discovery_datareview_3.tools.OVERRIDES_BLOB:
                return mock_overrides_blob
            elif path == discovery_datareview_3.tools.SCAN_BASELINE_BLOB:
                return mock_scan_baseline_blob
            else:
                return mock_registry_blob

        mock_bucket.blob.side_effect = blob_side_effect
        return mock_gcs_client, mock_bucket, mock_state_blob, mock_registry_blob, mock_dag_blob

    def tearDown(self):
        shutil.rmtree(main.state_mgr.LEDGER_CONFIG_DIR, ignore_errors=True)
        if os.path.exists(main.LEDGER_CONFIG_PATH):
            os.remove(main.LEDGER_CONFIG_PATH)
        main.gcs_client = None
        main.state_mgr.gcs_client = None

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    def test_check_role_compatibility(self, mock_gcs_client_class, mock_get_email):
        # Test hierarchy comparison
        main.state_mgr.check_role_compatibility("platform", "admins")  # Ok
        main.state_mgr.check_role_compatibility("platform", "platform")  # Ok
        with self.assertRaises(PermissionError):
            main.state_mgr.check_role_compatibility("admins", "platform")  # Denial

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_join_ledger_success_first_run(self, mock_gcs_client_class, mock_get_email):
        mock_get_email.return_value = "platform-user@google.com"
        
        mock_registry_yaml = """
workspace_name: "test-workspace"
gcp_project: "test-project"
roles:
  admins:
    - "admin-user@google.com"
  platform_engineers:
    - "platform-user@google.com"
"""
        _, _, mock_state_blob, _, _ = self.setup_mock_gcs(
            mock_gcs_client_class, 
            registry_yaml=mock_registry_yaml,
            state_not_found=True
        )
        
        res = await main.join_ledger(ledger_uri="gs://test-ledger-bucket")
        self.assertIn("Successfully joined ledger", res)
        self.assertIn("Assumed Role: platform", res)
        self.assertIn("Current Onboarding State: STATE_CONFIGURE_REPOSITORIES", res)
        
        config = main.state_mgr.read_local_config()
        self.assertEqual(config["ledger_uri"], "gs://test-ledger-bucket")
        self.assertEqual(config["resolved_role"], "platform")
        self.assertEqual(config["workspace_name"], "test-workspace")
        self.assertEqual(config["gcp_project"], "test-project")
        
        mock_state_blob.upload_from_string.assert_called_once()
        uploaded_data = json.loads(mock_state_blob.upload_from_string.call_args[0][0])
        self.assertEqual(uploaded_data["current_state"], "STATE_CONFIGURE_REPOSITORIES")

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_join_ledger_success_admin(self, mock_gcs_client_class, mock_get_email):
        mock_get_email.return_value = "admin-user@google.com"
        
        mock_registry_yaml = """
workspace_name: "test-workspace"
gcp_project: "test-project"
roles:
  admins:
    - "admin-user@google.com"
  platform_engineers:
    - "platform-user@google.com"
"""
        _, _, mock_state_blob, _, _ = self.setup_mock_gcs(
            mock_gcs_client_class, 
            registry_yaml=mock_registry_yaml
        )
        
        res = await main.join_ledger(ledger_uri="gs://test-ledger-bucket")
        self.assertIn("Successfully joined ledger", res)
        self.assertIn("Assumed Role: admin", res)
        self.assertIn("No admin onboarding tasks pending", res)
        
        config = main.state_mgr.read_local_config()
        self.assertEqual(config["ledger_uri"], "gs://test-ledger-bucket")
        self.assertEqual(config["resolved_role"], "admins")
        
        mock_state_blob.upload_from_string.assert_not_called()

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_join_ledger_denied_user(self, mock_gcs_client_class, mock_get_email):
        mock_get_email.return_value = "unregistered-user@google.com"
        
        self.setup_mock_gcs(
            mock_gcs_client_class, 
            registry_yaml="workspace_name: test\nroles:\n  admins: []"
        )
        
        res = await main.join_ledger("gs://test-bucket")
        self.assertIn("ERROR: Access Denied", res)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_join_ledger_reconfigure_denied_for_platform(self, mock_gcs_client_class, mock_get_email):
        mock_get_email.return_value = "platform-user@google.com"
        
        mock_registry_yaml = """
workspace_name: "test-workspace"
roles:
  platform:
    - "platform-user@google.com"
"""
        self.setup_mock_gcs(
            mock_gcs_client_class, 
            registry_yaml=mock_registry_yaml,
            state_dict={"current_state": "STATE_COMPLETED"}
        )
        
        res = await main.join_ledger("gs://test-bucket", reconfigure=True)
        self.assertEqual(res, "ERROR: Access Denied: Only administrators are authorized to reset onboarding configurations.")

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_join_ledger_explicit_role_deescalation(self, mock_gcs_client_class, mock_get_email):
        # An admin may join under a lower role. This exercises the `role`
        # branch of join_ledger, where the compatibility check is called: the
        # call must resolve through state_mgr, not a bare name (which raised
        # NameError and aborted every de-escalation attempt).
        mock_get_email.return_value = "admin-user@google.com"

        mock_registry_yaml = """
workspace_name: "test-workspace"
gcp_project: "test-project"
roles:
  admins:
    - "admin-user@google.com"
  platform_engineers:
    - "platform-user@google.com"
"""
        self.setup_mock_gcs(
            mock_gcs_client_class,
            registry_yaml=mock_registry_yaml,
            state_not_found=True,
        )

        res = await main.join_ledger(ledger_uri="gs://test-ledger-bucket", role="platform")
        self.assertIn("Successfully joined ledger", res)
        self.assertIn("Assumed Role: platform", res)
        self.assertIn("Current Onboarding State: STATE_CONFIGURE_REPOSITORIES", res)

        config = main.state_mgr.read_local_config()
        self.assertEqual(config["resolved_role"], "platform")

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_join_ledger_explicit_role_escalation_refused(self, mock_gcs_client_class, mock_get_email):
        # A platform engineer asking for admin is an escalation: the
        # compatibility check raises PermissionError, which join_ledger must
        # surface as a clean ERROR rather than crash on.
        mock_get_email.return_value = "platform-user@google.com"

        mock_registry_yaml = """
workspace_name: "test-workspace"
gcp_project: "test-project"
roles:
  admins:
    - "admin-user@google.com"
  platform_engineers:
    - "platform-user@google.com"
"""
        self.setup_mock_gcs(
            mock_gcs_client_class,
            registry_yaml=mock_registry_yaml,
            state_not_found=True,
        )

        res = await main.join_ledger(ledger_uri="gs://test-ledger-bucket", role="admins")
        self.assertIn("ERROR", res)
        self.assertNotIn("Successfully joined ledger", res)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    @patch("main.git_client.verify_git_path_exists")
    @patch("main.git_client.verify_git_write_permission")
    async def test_configure_repositories_success_non_ssm(self, mock_git_write, mock_git_path, mock_gcs_client_class, mock_get_email):
        main.state_mgr.write_local_config("gs://test-bucket", "platform", "test-workspace", "test-project")
        mock_get_email.return_value = "platform-user@google.com"
        
        _, _, mock_state_blob, _, _ = self.setup_mock_gcs(
            mock_gcs_client_class, 
            registry_yaml="roles:\n  platform: ['platform-user@google.com']",
            state_dict={"current_state": "STATE_CONFIGURE_REPOSITORIES", "history": [], "variables": {}}
        )
        
        mock_git_path.return_value = True
        mock_git_write.return_value = True
        
        mock_ctx = MagicMock()
        res = await main.configure_repositories(
            ctx=mock_ctx,
            source_repo_url="sso://source-repo",
            source_branch="main",
            source_path="/src",
            target_repo_url="sso://target-repo",
            target_branch="main",
            target_path="/target"
        )
        
        # Discovery now opens with the live AWS scan; the onboarding drain
        # parks at that first AGENT_TASK rather than the static index.
        self.assertIn("Current State: STATE_DISCOVERY_LIVE", res)
        self.assertIn("Source paths and target connection verified.", res)

        mock_state_blob.upload_from_string.assert_called_once()
        saved_state = json.loads(mock_state_blob.upload_from_string.call_args[0][0])
        self.assertEqual(saved_state["current_state"], "STATE_DISCOVERY_LIVE")

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    @patch("main.git_client.verify_git_path_exists")
    @patch("main.git_client.verify_git_write_permission")
    @patch("main.ssm_client.SSMClient")
    async def test_configure_repositories_lro_polling_lifecycle(self, mock_ssm_client_class, mock_git_write, mock_git_path, mock_gcs_client_class, mock_get_email):
        main.state_mgr.write_local_config("gs://test-bucket", "platform", "test-workspace", "test-project", ssm_poll_timeout=0.0)
        mock_get_email.return_value = "platform-user@google.com"
        
        # Setup mock GCS
        state_container = {"state": {"current_state": "STATE_CONFIGURE_REPOSITORIES", "history": [], "variables": {}}}
        
        mock_gcs, mock_bucket, mock_state_blob, mock_registry_blob, mock_dag_blob = self.setup_mock_gcs(
            mock_gcs_client_class, 
            registry_yaml="roles:\n  platform: ['platform-user@google.com']",
            state_dict=state_container["state"]
        )
        
        # Intercept GCS state uploads so we can simulate multi-step invocations reading back updated state
        def upload_side_effect(data_str, **kwargs):
            state_container["state"] = json.loads(data_str)
            mock_state_blob.download_as_text.return_value = data_str
            
        mock_state_blob.upload_from_string.side_effect = upload_side_effect
        
        mock_git_path.return_value = True
        mock_git_write.return_value = False
        
        mock_ssm_client = MagicMock()
        mock_ssm_client_class.return_value = mock_ssm_client
        
        # Initial check returns repository doesn't exist, instance doesn't exist
        mock_ssm_client.get_repository_by_details.return_value = None
        mock_ssm_client.get_instance_by_details.return_value = None
        
        # Trigger mock LRO operations
        mock_ssm_client.trigger_create_instance_by_details.return_value = "projects/test-project/locations/us-central1/operations/inst-123"
        mock_ssm_client.trigger_create_repository_by_details.return_value = "projects/test-project/locations/us-central1/operations/repo-456"
        
        # Setup mock Context returning approved = True for elicitation
        mock_ctx = MagicMock()
        mock_ctx.request_id = "test-request-id"
        mock_ctx.request_context.meta = None
        mock_res = MagicMock()
        mock_res.action = "accept"
        mock_res.content = {"approved": True}
        async def mock_send_request(*args, **kwargs):
            return mock_res
        mock_ctx.request_context.session.send_request.side_effect = mock_send_request
        
        # --- STEP 1: INITIAL CALL (triggers instance LRO) ---
        res = await main.configure_repositories(
            ctx=mock_ctx,
            source_repo_url="sso://source-repo",
            source_branch="main",
            source_path="/src",
            target_branch="main",
            target_path="/target",
            ssm_instance="my-ssm-instance",
            ssm_location="us-central1",
            ssm_repository="my-ssm-repo"
        )
        
        self.assertIn("Current State: STATE_CREATE_SSM_REPOSITORY", res)
        self.assertIn("Started GSSM instance creation. LRO: projects/test-project/locations/us-central1/operations/inst-123", res)
        mock_ssm_client.trigger_create_instance_by_details.assert_called_once_with("test-project", "us-central1", "my-ssm-instance")
        
        # --- STEP 2: POLL (Instance still creating) ---
        # Mock polling: LRO not done
        mock_ssm_client.get_operation_status.return_value = (False, None, None)
        
        res = await main.configure_repositories(
            ctx=mock_ctx,
            source_repo_url="sso://source-repo",
            source_branch="main",
            source_path="/src",
            target_branch="main",
            target_path="/target",
            ssm_instance="my-ssm-instance",
            ssm_location="us-central1",
            ssm_repository="my-ssm-repo"
        )
        
        self.assertIn("Current State: STATE_CREATE_SSM_REPOSITORY", res)
        self.assertIn("GSSM instance is still provisioning. LRO: projects/test-project/locations/us-central1/operations/inst-123", res)
        mock_ssm_client.get_operation_status.assert_called_with("us-central1", "projects/test-project/locations/us-central1/operations/inst-123")
        
        # --- STEP 3: POLL (Instance finishes, repository LRO triggers) ---
        # Mock polling: Instance LRO finishes (succeeded), repository details still return None (needs creation)
        mock_ssm_client.get_operation_status.side_effect = [(True, None, MagicMock())]
        mock_ssm_client.get_repository_by_details.return_value = None
        
        res = await main.configure_repositories(
            ctx=mock_ctx,
            source_repo_url="sso://source-repo",
            source_branch="main",
            source_path="/src",
            target_branch="main",
            target_path="/target",
            ssm_instance="my-ssm-instance",
            ssm_location="us-central1",
            ssm_repository="my-ssm-repo"
        )
        
        self.assertIn("Current State: STATE_CREATE_SSM_REPOSITORY", res)
        self.assertIn("GSSM instance is active. Started GSSM repository creation. LRO: projects/test-project/locations/us-central1/operations/repo-456", res)
        mock_ssm_client.trigger_create_repository_by_details.assert_called_once_with("test-project", "us-central1", "my-ssm-instance", "my-ssm-repo")
        
        # --- STEP 4: POLL (Repository creation finishes -> COMPLETED) ---
        # Mock polling: Repository LRO finishes (succeeded). Repository details return mock repo with git_https
        mock_ssm_client.get_operation_status.side_effect = [(True, None, MagicMock())]
        
        mock_repo = MagicMock()
        mock_repo.uris.git_https = "https://resolved-git-https.git"
        mock_ssm_client.get_repository_by_details.return_value = mock_repo
        
        res = await main.configure_repositories(
            ctx=mock_ctx,
            source_repo_url="sso://source-repo",
            source_branch="main",
            source_path="/src",
            target_branch="main",
            target_path="/target",
            ssm_instance="my-ssm-instance",
            ssm_location="us-central1",
            ssm_repository="my-ssm-repo"
        )
        self.assertIn("Current State: STATE_DISCOVERY_LIVE", res)
        self.assertIn("Source paths and target connection verified.", res)
        self.assertEqual(state_container["state"]["variables"]["target_repo_url"], "https://resolved-git-https.git")

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    @patch("main.git_client.verify_git_path_exists")
    @patch("main.git_client.verify_git_write_permission")
    @patch("main.ssm_client.SSMClient")
    async def test_configure_repositories_triggers_hitl_rejected(self, mock_ssm_client_class, mock_git_write, mock_git_path, mock_gcs_client_class, mock_get_email):
        main.state_mgr.write_local_config("gs://test-bucket", "platform", "test-workspace", "test-project")
        mock_get_email.return_value = "platform-user@google.com"
        
        self.setup_mock_gcs(
            mock_gcs_client_class, 
            registry_yaml="roles:\n  platform: ['platform-user@google.com']",
            state_dict={"current_state": "STATE_CONFIGURE_REPOSITORIES", "history": [], "variables": {}}
        )
        
        mock_git_path.return_value = True
        mock_git_write.return_value = False
        
        mock_ssm_client = MagicMock()
        mock_ssm_client_class.return_value = mock_ssm_client
        mock_ssm_client.get_repository_by_details.return_value = None
        
        # Setup mock Context returning approved = False
        mock_ctx = MagicMock()
        mock_ctx.request_id = "test-request-id"
        mock_ctx.request_context.meta = None
        mock_res = MagicMock()
        mock_res.action = "accept"
        mock_res.content = {"approved": False}
        async def mock_send_request(*args, **kwargs):
            return mock_res
        mock_ctx.request_context.session.send_request.side_effect = mock_send_request
        
        res = await main.configure_repositories(
            ctx=mock_ctx,
            source_repo_url="sso://source-repo",
            source_branch="main",
            source_path="/src",
            target_branch="main",
            target_path="/target",
            ssm_instance="my-ssm-instance",
            ssm_location="us-central1",
            ssm_repository="my-ssm-repo"
        )
        
        self.assertIn("Current State: STATE_CONFIGURE_REPOSITORIES", res)
        # The drain now accumulates messages, so the rejection follows the
        # mutation note that raised the approval rather than replacing it.
        self.assertIn("User rejected SSM repository creation.", res)
        # The flow creates SSM repos through the LRO path; assert against the
        # method production actually calls (the old create_repository_by_details
        # is unreachable, so asserting on it was trivially, permanently true).
        mock_ssm_client.trigger_create_repository_by_details.assert_not_called()

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    @patch("main.git_client.verify_git_path_exists")
    @patch("main.git_client.verify_git_write_permission")
    @patch("main.ssm_client.SSMClient")
    async def test_configure_repositories_instance_exists_creating_polling(self, mock_ssm_client_class, mock_git_write, mock_git_path, mock_gcs_client_class, mock_get_email):
        main.state_mgr.write_local_config("gs://test-bucket", "platform", "test-workspace", "test-project", ssm_poll_timeout=0.0)
        mock_get_email.return_value = "platform-user@google.com"
        
        state_container = {"state": {"current_state": "STATE_CONFIGURE_REPOSITORIES", "history": [], "variables": {}}}
        
        mock_gcs, mock_bucket, mock_state_blob, mock_registry_blob, mock_dag_blob = self.setup_mock_gcs(
            mock_gcs_client_class, 
            registry_yaml="roles:\n  platform: ['platform-user@google.com']",
            state_dict=state_container["state"]
        )
        
        def upload_side_effect(data_str, **kwargs):
            state_container["state"] = json.loads(data_str)
            mock_state_blob.download_as_text.return_value = data_str
            
        mock_state_blob.upload_from_string.side_effect = upload_side_effect
        
        mock_git_path.return_value = True
        mock_git_write.return_value = False
        
        mock_ssm_client = MagicMock()
        mock_ssm_client_class.return_value = mock_ssm_client
        
        # 1. Registration check: repo doesn't exist, instance exists but is CREATING
        mock_ssm_client.get_repository_by_details.return_value = None
        
        mock_instance = MagicMock()
        mock_instance.state = main.securesourcemanager_v1.Instance.State.CREATING
        mock_ssm_client.get_instance_by_details.return_value = mock_instance
        
        # Setup mock Context returning approved = True for elicitation
        mock_ctx = MagicMock()
        mock_ctx.request_id = "test-request-id"
        mock_ctx.request_context.meta = None
        mock_res = MagicMock()
        mock_res.action = "accept"
        mock_res.content = {"approved": True}
        async def mock_send_request(*args, **kwargs):
            return mock_res
        mock_ctx.request_context.session.send_request.side_effect = mock_send_request
        
        # --- STEP 1: INITIAL CALL (Transitions to STATE_CREATE_SSM_REPOSITORY, detects instance is CREATING, returns on_pending with polling message) ---
        res = await main.configure_repositories(
            ctx=mock_ctx,
            source_repo_url="sso://source-repo",
            source_branch="main",
            source_path="/src",
            target_branch="main",
            target_path="/target",
            ssm_instance="my-ssm-instance",
            ssm_location="us-central1",
            ssm_repository="my-ssm-repo"
        )
        
        self.assertIn("Current State: STATE_CREATE_SSM_REPOSITORY", res)
        self.assertIn("GSSM instance is still provisioning.", res)
        self.assertNotIn("LRO:", res) # No LRO name when direct polling
        
        # Verify variables: ssm_instance_needed = True, ssm_polling_instance = True
        variables = state_container["state"]["variables"]
        self.assertTrue(variables.get("ssm_instance_needed"))
        self.assertTrue(variables.get("ssm_polling_instance"))
        self.assertNotIn("ssm_creation_lro", variables)
        
        # --- STEP 2: POLL (Instance becomes ACTIVE) ---
        # Next call: Mock instance becomes ACTIVE, and repository LRO triggers
        mock_instance_active = MagicMock()
        mock_instance_active.state = main.securesourcemanager_v1.Instance.State.ACTIVE
        mock_ssm_client.get_instance_by_details.return_value = mock_instance_active
        mock_ssm_client.trigger_create_repository_by_details.return_value = "projects/test-project/locations/us-central1/operations/repo-456"
        
        res = await main.configure_repositories(
            ctx=mock_ctx,
            source_repo_url="sso://source-repo",
            source_branch="main",
            source_path="/src",
            target_branch="main",
            target_path="/target",
            ssm_instance="my-ssm-instance",
            ssm_location="us-central1",
            ssm_repository="my-ssm-repo"
        )
        
        self.assertIn("Current State: STATE_CREATE_SSM_REPOSITORY", res)
        self.assertIn("GSSM instance is active. Started GSSM repository creation. LRO: projects/test-project/locations/us-central1/operations/repo-456", res)
        
        # Verify variables: ssm_instance_needed = False, ssm_polling_instance was popped
        variables = state_container["state"]["variables"]
        self.assertFalse(variables.get("ssm_instance_needed"))
        self.assertNotIn("ssm_polling_instance", variables)
        self.assertEqual(variables.get("ssm_creation_lro"), "projects/test-project/locations/us-central1/operations/repo-456")

    @patch("main.storage.Client")
    def test_write_platform_state_machine(self, mock_gcs_client_class):
        mock_gcs_client = MagicMock()
        mock_gcs_client_class.return_value = mock_gcs_client
        mock_bucket = MagicMock()
        mock_gcs_client.bucket.return_value = mock_bucket
        
        mock_dag_blob = MagicMock()
        mock_state_blob = MagicMock()
        
        def blob_side_effect(path):
            if path == "platform_dag.json":
                return mock_dag_blob
            elif path == "platform/onboarding/state.json":
                return mock_state_blob
            return MagicMock()
            
        mock_bucket.blob.side_effect = blob_side_effect
        
        main.state_mgr.gcs_client = mock_gcs_client
        main.write_platform_state_machine("gs://my-bucket", "ws1")
        
        # Verify platform_dag.json was uploaded
        mock_dag_blob.upload_from_string.assert_called_once()
        dag_data = mock_dag_blob.upload_from_string.call_args[0][0]
        self.assertIn("gke-migration-platform", dag_data)
        
        # Verify platform/onboarding/state.json was initialized and uploaded
        mock_state_blob.upload_from_string.assert_called_once()
        state_data = mock_state_blob.upload_from_string.call_args[0][0]
        state_dict = json.loads(state_data)
        self.assertEqual(state_dict["current_state"], "STATE_CONFIGURE_REPOSITORIES")
        self.assertEqual(state_dict["history"], [])
        self.assertEqual(state_dict["variables"], {})
        self.assertEqual(mock_state_blob.upload_from_string.call_args[1].get("content_type"), "application/json")

    # The old test_full_discovery_to_landing_zone_flow was removed rather than
    # repaired: it drove approve_discovery_inventory and write_readiness_report,
    # both deleted when the extraction pipeline and the merged assessment review
    # landed (f255019, a43e3e6). Discovery is covered per-step above; what it
    # alone exercised — the landing-zone design chain — is covered here.
    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_landing_zone_design_chain(self, mock_gcs_client_class, mock_get_email):
        mock_get_email.return_value = "platform-user@google.com"

        state_container = {"state": {
            "current_state": "STATE_LZ_DESIGN",
            "history": [],
            "variables": {
                "target_repo_url": "sso://target-repo",
                "target_branch": "main",
                "target_path": "/target",
            },
        }}
        main.state_mgr.write_local_config(
            ledger_uri="gs://test-ledger-bucket",
            resolved_role="platform",
            workspace_name="test-workspace",
            gcp_project="test-project",
        )
        mock_gcs_client, _, mock_state_blob, _, _ = self.setup_mock_gcs(
            mock_gcs_client_class,
            registry_yaml="roles:\n  platform_engineers:\n    - 'platform-user@google.com'",
            state_dict=state_container["state"],
        )
        main.state_mgr.gcs_client = mock_gcs_client

        def upload_side_effect(data_str, **kwargs):
            state_container["state"] = json.loads(data_str)
            mock_state_blob.download_as_text.return_value = data_str
        mock_state_blob.upload_from_string.side_effect = upload_side_effect

        # Finalizing before all four decisions are recorded refuses and names
        # the missing ones.
        res = await main.finalize_landing_zone_design(ctx=MagicMock())
        self.assertIn("ERROR: Cannot finalize", res)
        self.assertIn("karpenter", res)

        # The first resolve_lz_decision opens the workspace: it allocates the
        # branch and target clone path before recording the decision.
        await main.resolve_lz_decision("karpenter", "GKE_STANDARD_NAP")
        self.assertEqual(state_container["state"]["current_state"], "STATE_LZ_DESIGN")
        variables = state_container["state"]["variables"]
        self.assertIn("lz_branch_uuid", variables)
        self.assertIn("lz_branch_name", variables)
        self.assertIn("target_clone_path", variables)

        res = await main.resolve_lz_decision("karpenter", "NOT_A_CHOICE")
        self.assertIn("ERROR: Invalid choice", res)

        await main.resolve_lz_decision("privileged_daemonsets", "GKE_STANDARD")
        await main.resolve_lz_decision("gpu_tpu", "GKE_STANDARD_SPECIALIZED")
        await main.resolve_lz_decision("vpc_peering", "PUBLIC_AUTHORIZED_NETS")

        decisions = state_container["state"]["variables"]["lz_decisions"]
        self.assertEqual(decisions["karpenter"], "GKE_STANDARD_NAP")
        self.assertEqual(decisions["vpc_peering"], "PUBLIC_AUTHORIZED_NETS")

        res = await main.finalize_landing_zone_design(ctx=MagicMock())
        self.assertEqual(state_container["state"]["current_state"], "STATE_LZ_TRANSLATION_PLAN")
        self.assertIn("Landing zone design recorded", res)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_finalize_refuses_decisions_that_imply_two_modes(self, mock_gcs_client_class, mock_get_email):
        # coverage-guards G0 step 3: two recorded choices implying different
        # cluster modes are refused at record time, naming the ids. Without
        # usable triggers every recorded choice votes (the reply says so);
        # with triggers, a default on an unfired trigger is mute (row 4).
        mock_get_email.return_value = "platform-user@google.com"
        state_container = {"state": {
            "current_state": "STATE_LZ_DESIGN", "history": [],
            "variables": {"target_repo_url": "sso://target-repo", "target_branch": "main",
                          "target_path": "/target"},
        }}
        main.state_mgr.write_local_config(
            ledger_uri="gs://test-ledger-bucket", resolved_role="platform",
            workspace_name="test-workspace", gcp_project="test-project")
        mock_gcs_client, _, mock_state_blob, _, _ = self.setup_mock_gcs(
            mock_gcs_client_class,
            registry_yaml="roles:\n  platform_engineers:\n    - 'platform-user@google.com'",
            state_dict=state_container["state"])
        main.state_mgr.gcs_client = mock_gcs_client

        def upload_side_effect(data_str, **kwargs):
            state_container["state"] = json.loads(data_str)
            mock_state_blob.download_as_text.return_value = data_str
        mock_state_blob.upload_from_string.side_effect = upload_side_effect

        res = await main.resolve_lz_decision("karpenter", "GKE_AUTOPILOT")
        self.assertIn("WARNING: the discovery inventory carries no usable `triggers`", res)
        await main.resolve_lz_decision("privileged_daemonsets", "GKE_STANDARD")
        await main.resolve_lz_decision("gpu_tpu", "GKE_STANDARD_SPECIALIZED")
        await main.resolve_lz_decision("vpc_peering", "PUBLIC_AUTHORIZED_NETS")
        res = await main.finalize_landing_zone_design(ctx=MagicMock())
        self.assertIn("ERROR: Cannot finalize", res)
        self.assertIn("karpenter=GKE_AUTOPILOT", res)
        self.assertIn("privileged_daemonsets=GKE_STANDARD", res)
        self.assertIn("triggers unavailable", res)
        self.assertEqual(state_container["state"]["current_state"], "STATE_LZ_DESIGN")

        # With triggers recorded, the defaulted GKE_STANDARD on an unfired
        # privileged_daemonsets trigger is mute and the pair finalizes.
        state_container["state"]["variables"]["discovery_inventory"] = {
            "triggers": {"karpenter": True, "privileged_daemonsets": False,
                         "gpu_tpu": False, "vpc_peering": False}}
        mock_state_blob.download_as_text.return_value = json.dumps(state_container["state"])
        res = await main.finalize_landing_zone_design(ctx=MagicMock())
        self.assertEqual(state_container["state"]["current_state"], "STATE_LZ_TRANSLATION_PLAN", res)

    # An ECR image pinned by tag and digest, one by tag only, and one
    # Docker Hub bystander replication must leave alone.
    ECR_HOST = "123456789012.dkr.ecr.us-east-1.amazonaws.com"
    REPLICATION_INVENTORY = {
        "images": [
            {"ref": f"{ECR_HOST}/payments/api:1.4.2", "registry": "ecr",
             "repository": f"{ECR_HOST}/payments/api", "tag": "1.4.2",
             "digest": "sha256:" + "a" * 64, "pinned_by": "tag_and_digest",
             "provenance": [{"kind": "literal", "file": "k8s/deploy.yaml"}]},
            {"ref": f"{ECR_HOST}/web:2.0", "registry": "ecr",
             "repository": f"{ECR_HOST}/web", "tag": "2.0",
             "digest": None, "pinned_by": "tag",
             "provenance": [{"kind": "literal", "file": "k8s/web.yaml"}]},
            {"ref": "busybox:stable", "registry": "dockerhub",
             "repository": "busybox", "tag": "stable", "digest": None,
             "pinned_by": "tag",
             "provenance": [{"kind": "literal", "file": "k8s/web.yaml"}]},
        ],
        "render_targets": [
            {"id": "t1", "type": "helm", "root": "charts/web",
             "status": "unrendered_declined"},
        ],
    }

    def _setup_deployment_flow(self, mock_gcs_client_class, elicit_action="accept",
                               elicit_content=None, inventory=None, variables=None):
        """Fixture for the deployment segment: authenticated platform user
        parked at STATE_DEPLOYMENT_INIT, state and inventory round-trips
        installed, the replication elicitation answering as configured, and
        the runbook blob captured. Returns (state_container,
        inventory_container, runbook_container, mock_ctx)."""
        state_container = {"state": {
            "current_state": "STATE_DEPLOYMENT_INIT", "history": [],
            "variables": variables or {}}}
        main.state_mgr.write_local_config(
            "gs://test-bucket", "platform", "test-workspace", "test-project")
        mock_gcs_client, mock_bucket, mock_state_blob, _, _ = self.setup_mock_gcs(
            mock_gcs_client_class,
            registry_yaml="roles:\n  platform: ['platform-user@google.com']",
            state_dict=state_container["state"])
        main.state_mgr.gcs_client = mock_gcs_client

        def upload_side_effect(data_str, **kwargs):
            state_container["state"] = json.loads(data_str)
            mock_state_blob.download_as_text.return_value = data_str
        mock_state_blob.upload_from_string.side_effect = upload_side_effect

        inventory_container = self._install_inventory_roundtrip()
        if inventory is not None:
            self.mock_inventory_blob.upload_from_string(json.dumps(inventory))

        runbook_container = {}
        runbook_blob = MagicMock()

        def runbook_upload(data_str, **kwargs):
            runbook_container["text"] = data_str
        runbook_blob.upload_from_string.side_effect = runbook_upload
        inner_side_effect = mock_bucket.blob.side_effect

        def blob_side_effect(path):
            if path == replication.RUNBOOK_BLOB_PATH:
                return runbook_blob
            return inner_side_effect(path)
        mock_bucket.blob.side_effect = blob_side_effect

        mock_ctx = MagicMock()
        mock_ctx.request_id = "test-request-id"
        mock_ctx.request_context.meta = None
        mock_res = MagicMock()
        mock_res.action = elicit_action
        mock_res.content = elicit_content

        async def mock_send_request(*args, **kwargs):
            return mock_res
        mock_ctx.request_context.session.send_request.side_effect = mock_send_request

        return state_container, inventory_container, runbook_container, mock_ctx

    # ---- CL 4: the data migration step -------------------------------

    DATA_ENTRIES = [
        {"service": "rds", "identifier": "orders-db",
         "address": "aws_db_instance.orders", "disposition": "migrate",
         "evidence": ["envs/prod/data.tf"], "notes": [],
         "consumers": [{"workload": "orders", "kind": "helm_release",
                        "namespace": "orders", "source_path": None,
                        "detection": "terraform_wiring",
                        "evidence": "envs/prod/data.tf"}]},
        {"service": "s3", "identifier": "exports",
         "address": "aws_s3_bucket.exports", "disposition": "migrate",
         "evidence": ["envs/prod/data.tf"], "notes": [], "consumers": []},
        {"service": "elasticache", "identifier": "sessions",
         "address": "aws_elasticache_cluster.sessions",
         "disposition": "rebuild", "evidence": ["envs/prod/data.tf"],
         "notes": [], "consumers": []},
    ]

    def _at_data_migration(self, mock_gcs_client_class, entries=None,
                           with_baseline=True):
        """Parks a platform user in the data migration step with a section.

        `with_baseline` installs the scan baseline, which is what decides
        whether `keep-in-aws` — the exit from an unmovable service — can be
        recorded. Default True: that is what any workspace scanned since the
        data review shipped has, and the refusal to close only makes sense
        while the exit works.
        """
        entry_list = [dict(e) for e in
                      (self.DATA_ENTRIES if entries is None else entries)]
        inventory = {"schema_version": "1.0",
                     "data_dependencies": entry_list}
        state_container, inventory_container, _, mock_ctx = \
            self._setup_deployment_flow(mock_gcs_client_class,
                                        inventory=inventory)
        # MERGED, not replaced. Replacing dropped
        # `artifact_registry_destinations`, which made every exports
        # publication from this step emit `artifact_registry: null` — and an
        # assertion that a ref is absent from an always-empty map cannot fail.
        state = dict(state_container["state"],
                     current_state="STATE_DEPLOYMENT_DATA_MIGRATION",
                     variables=dict(
                         state_container["state"]["variables"],
                         workspace_name="test-workspace",
                         # Set by provisioning, which this fixture skips.
                         # Without it the exports slice publishes
                         # artifact_registry: null and every assertion about
                         # the image map is vacuous.
                         artifact_registry_destinations=[
                             {"url": self.AR_DEST_URL,
                              "project": "test-project",
                              "location": "us-central1"}]))
        (main.state_mgr.gcs_client.bucket("test-bucket")
         .blob("platform/onboarding/state.json")
         .upload_from_string(json.dumps(state)))
        if with_baseline:
            from servers.phases.discovery import discovery_datareview_3
            (main.state_mgr.gcs_client.bucket("test-bucket")
             .blob(discovery_datareview_3.tools.SCAN_BASELINE_BLOB)
             .upload_from_string(json.dumps(
                 {"data_dependencies": entry_list, "truncated": [],
                  "excluded": [], "corrections_replayed": []})))
        return state_container, inventory_container, mock_ctx

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_worklist_names_what_is_owed_and_who_waits_for_it(
            self, mock_gcs_client_class, mock_get_email):
        """Only `migrate` is owed. An empty cache is a working cache, so the
        rebuild entry is not work anybody is waiting on."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)

        out = await t.list_data_migrations(ctx=mock_ctx)

        self.assertNotIn("ERROR", out)
        self.assertIn("2 graded 'migrate'", out)
        self.assertIn("rds orders-db", out)
        self.assertIn("used by: orders", out)
        self.assertIn("Cloud SQL", out)
        self.assertNotIn("sessions", out)
        # And the runbook is written where the operator can find it.
        self.assertIn(datamigration_lib.RUNBOOK_BLOB, out)
        runbook = self.mock_dm_runbook_blob.upload_from_string.call_args[0][0]
        self.assertIn("Data migration runbook", runbook)
        self.assertIn("landing zone Terraform has to be applied", runbook)

    # ---- CL 8: the per-service runbooks ------------------------------

    RENDERED = ("# Orders database — RDS PostgreSQL to Cloud SQL\n\n"
                "Every command here is yours to run.\n" + "Step one. " * 200)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_listing_offers_help_only_where_there_is_a_procedure(
            self, mock_gcs_client_class, mock_get_email):
        """The offer is printed per service, and only for the ones a runbook
        exists for. Offering help with a service the tool then declines is
        worse than not offering — and the agent cannot tell which is which
        without being told."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        entries = [dict(self.DATA_ENTRIES[0], engine="postgres"),
                   dict(self.DATA_ENTRIES[1]),
                   {"service": "dynamodb", "identifier": "sessions",
                    "address": "aws_dynamodb_table.sessions",
                    "disposition": "migrate",
                    "evidence": ["envs/prod/data.tf"], "notes": [],
                    "consumers": []}]
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class,
                                                 entries=entries)

        out = await t.list_data_migrations(ctx=mock_ctx)

        self.assertIn('offer help: get_data_migration_runbook('
                      'address="aws_db_instance.orders")', out)
        self.assertIn("orders-db (RDS, postgres) → Cloud SQL", out)
        self.assertIn("exports (S3) → Cloud Storage", out)
        # The DynamoDB entry is owed — a human graded it `migrate` — and gets
        # no offer, because there is nothing behind one.
        self.assertIn("dynamodb sessions", out)
        self.assertNotIn('address="aws_dynamodb_table.sessions") — ', out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_runbook_arrives_with_the_facts_and_as_a_template(
            self, mock_gcs_client_class, mock_get_email):
        """What separates a rendered runbook from the shipped one is the
        estate's own facts, so they are handed over with it — and the
        procedure arrives as a template, because an agent that is given a
        finished document has nothing left to adapt."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        entries = [dict(self.DATA_ENTRIES[0], engine="postgres",
                        engine_version="15.4", allocated_storage=2048)]
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class,
                                                 entries=entries)

        out = await t.get_data_migration_runbook(
            address="aws_db_instance.orders", ctx=mock_ctx)

        self.assertNotIn("ERROR", out)
        self.assertIn("RDS for PostgreSQL", out)
        self.assertIn("engine version: 15.4", out)
        self.assertIn("declared size (GiB): 2048", out)
        self.assertIn("used by: orders", out)
        self.assertIn("<SOURCE_HOST>", out)
        self.assertIn("save_data_migration_runbook", out)
        # The rule that outlives every procedure in the set.
        self.assertIn("yours to run", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_an_rds_without_an_engine_asks_rather_than_guessing(
            self, mock_gcs_client_class, mock_get_email):
        """PostgreSQL is the majority engine, and handing an Oracle instance a
        pglogical procedure is the failure this refusal exists to prevent."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)

        out = await t.get_data_migration_runbook(
            address="aws_db_instance.orders", ctx=mock_ctx)

        self.assertIn("ERROR", out)
        self.assertIn("does not state an engine", out)
        self.assertIn("Do not pick the common one", out)
        self.assertNotIn("pglogical", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_service_with_no_procedure_says_which_kind_of_no(
            self, mock_gcs_client_class, mock_get_email):
        """"Not found" sends an agent looking for the nearest-looking file.
        The reason is the answer: a re-platform is a decision, not a gap."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        entries = [{"service": "dynamodb", "identifier": "sessions",
                    "address": "aws_dynamodb_table.sessions",
                    "disposition": "migrate",
                    "evidence": ["envs/prod/data.tf"], "notes": [],
                    "consumers": []}]
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class,
                                                 entries=entries)

        out = await t.get_data_migration_runbook(
            address="aws_dynamodb_table.sessions", ctx=mock_ctx)

        self.assertIn("ERROR", out)
        self.assertIn("re-platform", out)
        self.assertIn("keep-in-aws", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_runbook_for_a_service_nothing_waits_on_says_so(
            self, mock_gcs_client_class, mock_get_email):
        """Moving a `rebuild` cache is the operator's call, and the tool
        answers — but an agent that reports it as progress against the gate is
        telling the estate something untrue."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)

        out = await t.get_data_migration_runbook(
            address="aws_elasticache_cluster.sessions", ctx=mock_ctx)

        self.assertNotIn("ERROR", out)
        self.assertIn("graded 'rebuild'", out)
        self.assertIn("does not hold the step open", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_saving_writes_the_adapted_copy_and_the_worklist_finds_it(
            self, mock_gcs_client_class, mock_get_email):
        """The artifact is for whoever runs the move in three weeks, so the
        worklist has to point at it rather than back at the shipped template."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        entries = [dict(self.DATA_ENTRIES[0], engine="postgres")]
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class,
                                                 entries=entries)

        out = await t.save_data_migration_runbook(
            address="aws_db_instance.orders", content=self.RENDERED,
            ctx=mock_ctx)

        path = ("platform/deployment/runbooks/"
                "envs-prod--rds-orders-db--aws-db-instance-orders.md")
        self.assertIn("SUCCESS", out)
        self.assertIn(path, out)
        self.assertEqual(self.rendered_runbooks[path], self.RENDERED.strip())
        # A plan is not a report: the move is still owed until somebody says
        # otherwise, and the response says which call does that.
        self.assertIn("mark_data_service_migrated", out)

        # Redrawn by the save itself, not only by the next listing — the
        # instructions tell the agent not to re-list, so a worklist that only
        # catches up on the next call is one nobody triggers.
        worklist = self.mock_dm_runbook_blob.upload_from_string.call_args[0][0]
        self.assertIn(f"`{path}` — adapted for this estate", worklist)

        listing = await t.list_data_migrations(ctx=mock_ctx)
        self.assertIn(f"runbook already adapted: {path}", listing)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_offer_names_the_directory_when_the_address_repeats(
            self, mock_gcs_client_class, mock_get_email):
        """The printed call has to be runnable as printed. A dev/prod pair
        shares an address, `_target` refuses it, and an agent told never to
        construct an address has nothing else to try."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        entries = [dict(self.DATA_ENTRIES[0], engine="postgres",
                        evidence=["envs/prod/data.tf"]),
                   dict(self.DATA_ENTRIES[0], engine="postgres",
                        evidence=["envs/dev/data.tf"])]
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class,
                                                 entries=entries)

        out = await t.list_data_migrations(ctx=mock_ctx)

        self.assertIn('offer help: get_data_migration_runbook('
                      'address="aws_db_instance.orders", '
                      'directory="envs/prod")', out)
        self.assertIn('directory="envs/dev")', out)
        # And the call the listing printed is one the tool accepts.
        answer = await t.get_data_migration_runbook(
            address="aws_db_instance.orders", directory="envs/prod",
            ctx=mock_ctx)
        self.assertNotIn("ERROR", answer)
        # The hand-out's own substitution has to narrow too. This is the line
        # that decides whether the tool calls INSIDE the saved runbook are
        # runnable weeks later by someone with no session — the templates
        # carry `<TARGET_ARGS>` precisely so this value fills them.
        self.assertIn('<TARGET_ARGS> in the template is: '
                      'address="aws_db_instance.orders", '
                      'directory="envs/prod"', answer)
        self.assertIn('save_data_migration_runbook(address='
                      '"aws_db_instance.orders", directory="envs/prod"',
                      answer)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_handout_substitution_is_plain_when_nothing_repeats(
            self, mock_gcs_client_class, mock_get_email):
        """The other half: a unique address must not be dressed up with
        arguments the operator would then have to justify."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        entries = [dict(self.DATA_ENTRIES[0], engine="postgres")]
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class,
                                                 entries=entries)

        out = await t.get_data_migration_runbook(
            address="aws_db_instance.orders", ctx=mock_ctx)

        self.assertIn('<TARGET_ARGS> in the template is: '
                      'address="aws_db_instance.orders" —', out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_an_rds_with_no_engine_is_offered_a_question(
            self, mock_gcs_client_class, mock_get_email):
        """`engine = var.db_engine` is the ordinary shape for a module-sourced
        database. Suppressing the offer tells the operator their PostgreSQL is
        a dead end when one question produces the procedure."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)

        out = await t.list_data_migrations(ctx=mock_ctx)

        self.assertIn("offer help: ask which engine this runs", out)
        self.assertIn("get_data_migration_runbook(", out)
        worklist = self.mock_dm_runbook_blob.upload_from_string.call_args[0][0]
        self.assertIn("Name the engine", worklist)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_naming_the_engine_produces_the_runbook(
            self, mock_gcs_client_class, mock_get_email):
        """The exit from the question. Without `engine=`, an agent that asks
        the operator, gets an answer and re-calls as instructed receives the
        identical refusal — nothing else in the repository can record an
        engine, so the offer would send it round a loop with no way out."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)

        refused = await t.get_data_migration_runbook(
            address="aws_db_instance.orders", ctx=mock_ctx)
        self.assertIn("ERROR", refused)
        self.assertIn('engine="postgres"', refused)

        out = await t.get_data_migration_runbook(
            address="aws_db_instance.orders", engine="postgres", ctx=mock_ctx)

        self.assertNotIn("ERROR", out)
        self.assertIn("RDS for PostgreSQL", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_an_engine_the_operator_names_wrongly_is_not_a_second_loop(
            self, mock_gcs_client_class, mock_get_email):
        """An answer outside the four is an escalation, not a reason to ask
        the same question again."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)

        out = await t.get_data_migration_runbook(
            address="aws_db_instance.orders", engine="oracle-se2",
            ctx=mock_ctx)

        self.assertIn("ERROR", out)
        # The attempted-engine arm, which names the accepted values. It sits
        # before the unsupported-engine arm on purpose: that one is for what
        # the DECLARATION says, and this one for what a human just typed.
        self.assertIn("is not one of the engines Cloud SQL runs", out)
        self.assertIn("postgres, mysql, mariadb, sqlserver", out)
        self.assertNotIn("Ask the operator which engine", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_declared_engine_is_not_overridden_by_an_argument(
            self, mock_gcs_client_class, mock_get_email):
        """The declaration is what the migration is planned against. A human
        contradicting it is a discovery finding, not a routing decision."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        entries = [dict(self.DATA_ENTRIES[0], engine="postgres")]
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class,
                                                 entries=entries)

        out = await t.get_data_migration_runbook(
            address="aws_db_instance.orders", engine="mysql", ctx=mock_ctx)

        self.assertIn("ERROR", out)
        self.assertIn("already records its engine as 'postgres'", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_runbook_written_where_none_ships_is_still_findable(
            self, mock_gcs_client_class, mock_get_email):
        """`_no_runbook` says the plan is the team's to write and `save`
        accepts it. Every surface then checked for a shipped template first,
        so the file existed and nothing ever mentioned it again."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        entries = [{"service": "dynamodb", "identifier": "sessions",
                    "address": "aws_dynamodb_table.sessions",
                    "disposition": "migrate",
                    "evidence": ["envs/prod/data.tf"], "notes": [],
                    "consumers": []}]
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class,
                                                 entries=entries)
        await t.save_data_migration_runbook(
            address="aws_dynamodb_table.sessions", content=self.RENDERED,
            ctx=mock_ctx)
        path = ("platform/deployment/runbooks/"
                "envs-prod--dynamodb-sessions--aws-dynamodb-table-sessions.md")

        listing = await t.list_data_migrations(ctx=mock_ctx)
        self.assertIn(f"runbook already adapted: {path}", listing)
        worklist = self.mock_dm_runbook_blob.upload_from_string.call_args[0][0]
        self.assertIn(f"`{path}` — adapted for this estate", worklist)
        # And the refusal still refuses, but names the file rather than
        # sending the next session off to write a second one.
        handout = await t.get_data_migration_runbook(
            address="aws_dynamodb_table.sessions", ctx=mock_ctx)
        self.assertIn("ERROR", handout)
        self.assertIn(path, handout)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_concurrent_write_refuses_rather_than_winning(
            self, mock_gcs_client_class, mock_get_email):
        """The generation guard's own arm. Two sessions adapting at once must
        not both believe they wrote the file."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        entries = [dict(self.DATA_ENTRIES[0], engine="postgres")]
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class,
                                                 entries=entries)
        path = ("platform/deployment/runbooks/"
                "envs-prod--rds-orders-db--aws-db-instance-orders.md")
        blob = (main.state_mgr.gcs_client.bucket("test-bucket").blob(path))
        blob.upload_from_string.side_effect = exceptions.PreconditionFailed(
            "generation mismatch")

        out = await t.save_data_migration_runbook(
            address="aws_db_instance.orders", content=self.RENDERED,
            ctx=mock_ctx)

        self.assertIn("ERROR", out)
        self.assertIn("another session wrote", out)
        self.assertNotIn(path, self.rendered_runbooks)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_second_session_does_not_overwrite_the_first_runbook(
            self, mock_gcs_client_class, mock_get_email):
        """The step is parked for weeks and "ask once" is per session, so the
        next session is offered the same help over a document the operator
        already worked through. What it carries — the endpoint, the agreed
        window — is not recoverable from a session that has ended."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        entries = [dict(self.DATA_ENTRIES[0], engine="postgres")]
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class,
                                                 entries=entries)
        first = self.RENDERED + "\nEndpoint orders.abc123.rds.amazonaws.com\n"
        await t.save_data_migration_runbook(
            address="aws_db_instance.orders", content=first, ctx=mock_ctx)
        path = ("platform/deployment/runbooks/"
                "envs-prod--rds-orders-db--aws-db-instance-orders.md")

        second = await t.save_data_migration_runbook(
            address="aws_db_instance.orders", content=self.RENDERED,
            ctx=mock_ctx)

        self.assertIn("ERROR", second)
        self.assertIn("already holds an adapted runbook", second)
        self.assertIn("replace=True", second)
        self.assertEqual(self.rendered_runbooks[path], first.strip())

        # And the template hand-out says so too, rather than letting the agent
        # rebuild from nothing and discover it at the save.
        handout = await t.get_data_migration_runbook(
            address="aws_db_instance.orders", ctx=mock_ctx)
        self.assertIn("AN ADAPTED RUNBOOK ALREADY EXISTS", handout)
        self.assertIn(path, handout)

        # Superseding it is allowed, once it is deliberate.
        third = await t.save_data_migration_runbook(
            address="aws_db_instance.orders",
            content=self.RENDERED + "\nsuperseded\n", replace=True,
            ctx=mock_ctx)
        self.assertIn("SUCCESS", third)
        self.assertIn("superseded", self.rendered_runbooks[path])

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_an_unresolved_placeholder_is_refused_by_name(
            self, mock_gcs_client_class, mock_get_email):
        """A stand-in in a document an operator runs from is a command that
        fails at best and hits the wrong resource at worst — the same reason
        `mark_replication_complete` refuses `<AR_DESTINATION>`."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        entries = [dict(self.DATA_ENTRIES[0], engine="postgres")]
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class,
                                                 entries=entries)

        out = await t.save_data_migration_runbook(
            address="aws_db_instance.orders",
            content=self.RENDERED + "\nconnect to <SOURCE_HOST> as <JOB>\n",
            ctx=mock_ctx)

        self.assertIn("ERROR", out)
        self.assertIn("<SOURCE_HOST>", out)
        self.assertIn("<JOB>", out)
        self.assertEqual(self.rendered_runbooks, {})

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_summary_is_not_accepted_as_a_runbook(
            self, mock_gcs_client_class, mock_get_email):
        """The artifact is opened weeks later with no session behind it. A
        summary is not a smaller version of the right thing."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        entries = [dict(self.DATA_ENTRIES[0], engine="postgres")]
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class,
                                                 entries=entries)

        out = await t.save_data_migration_runbook(
            address="aws_db_instance.orders",
            content="Use DMS to move the orders database to Cloud SQL.",
            ctx=mock_ctx)

        self.assertIn("ERROR", out)
        self.assertIn("too short", out)
        self.assertEqual(self.rendered_runbooks, {})

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_runbook_tools_refuse_outside_the_step(
            self, mock_gcs_client_class, mock_get_email):
        """Both are step tools, like every other tool in this package: at the
        terminal there is by construction nothing left to prepare."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        state_container, _, mock_ctx = self._at_data_migration(
            mock_gcs_client_class)
        state = dict(state_container["state"],
                     current_state="STATE_DEPLOYMENT_COMPLETED")
        (main.state_mgr.gcs_client.bucket("test-bucket")
         .blob("platform/onboarding/state.json")
         .upload_from_string(json.dumps(state)))

        for out in (await t.get_data_migration_runbook(
                        address="aws_db_instance.orders", ctx=mock_ctx),
                    await t.save_data_migration_runbook(
                        address="aws_db_instance.orders",
                        content=self.RENDERED, ctx=mock_ctx)):
            self.assertIn("ERROR", out)
            self.assertIn("Invalid state", out)
        self.assertEqual(self.rendered_runbooks, {})

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_an_ambiguous_address_is_refused_before_anything_is_written(
            self, mock_gcs_client_class, mock_get_email):
        """A dev and a prod database declaring one address: rendering the
        procedure against the wrong one of the pair puts the prod endpoint in
        the dev runbook, or the reverse."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        entries = [dict(self.DATA_ENTRIES[0], engine="postgres",
                        evidence=["envs/prod/data.tf"]),
                   dict(self.DATA_ENTRIES[0], engine="postgres",
                        evidence=["envs/dev/data.tf"])]
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class,
                                                 entries=entries)

        out = await t.get_data_migration_runbook(
            address="aws_db_instance.orders", ctx=mock_ctx)
        saved = await t.save_data_migration_runbook(
            address="aws_db_instance.orders", content=self.RENDERED,
            ctx=mock_ctx)

        for answer in (out, saved):
            self.assertIn("ERROR", answer)
            self.assertIn("names 2 data services", answer)
        self.assertEqual(self.rendered_runbooks, {})

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_listing_carries_the_arguments_the_tools_take(
            self, mock_gcs_client_class, mock_get_email):
        """A user says "the orders database is done"; the tool takes
        address="aws_db_instance.orders". The listing is the only thing that
        can connect the two — without the address on it the agent has to
        invent one, and a guessed address either refuses or matches a
        different entry."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)

        out = await t.list_data_migrations(ctx=mock_ctx)

        self.assertIn("address: aws_db_instance.orders", out)
        self.assertIn('mark_data_service_migrated(address="aws_db_instance.orders"',
                      out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_identifier_axis_settles_one_of_a_renamed_pair(
            self, mock_gcs_client_class, mock_get_email):
        """The finest of the three axes, and the only one with nothing
        executing it: deleting the filter left the whole suite green. Printing
        `identifier=` is not the same as HONOURING it — the printed call has
        to be accepted, and it has to settle the entry it names rather than
        its sibling. Recording "this moved" against the wrong half of a pair
        is the failure the three-axis key exists to prevent."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        entries = [
            {"service": "rds", "identifier": name,
             "address": "aws_db_instance.orders", "disposition": "migrate",
             "evidence": ["envs/prod/data.tf"], "notes": [], "consumers": []}
            for name in ("orders-db", "orders-db-renamed")]
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class, entries)

        out = await t.mark_data_service_migrated(
            address="aws_db_instance.orders", directory="envs/prod",
            identifier="orders-db-renamed", target="orders-gcp",
            ctx=mock_ctx)

        self.assertNotIn("ERROR", out)
        document = json.loads(
            self.mock_migrations_blob.upload_from_string.call_args[0][0])
        self.assertEqual([r["identifier"] for r in document["migrations"]],
                         ["orders-db-renamed"])
        # The sibling is untouched and still holds the step open.
        listing = await t.list_data_migrations(ctx=mock_ctx)
        self.assertIn("1 still owed", listing)
        close = await t.complete_data_migration(ctx=mock_ctx)
        self.assertIn("ERROR", close)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_listing_names_the_identifier_when_the_directory_repeats(
            self, mock_gcs_client_class, mock_get_email):
        """`key_of` keys on three axes because an `_override.tf` that renames
        leaves two entries sharing address AND directory. Printing a call that
        names only the first two is printing a call that refuses — and the
        instructions tell the agent to use it verbatim."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        entries = [
            {"service": "rds", "identifier": name,
             "address": "aws_db_instance.orders", "disposition": "migrate",
             "evidence": ["envs/prod/data.tf"], "notes": [], "consumers": []}
            for name in ("orders-db", "orders-db-renamed")]
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class, entries)

        out = await t.list_data_migrations(ctx=mock_ctx)

        self.assertIn('identifier="orders-db"', out)
        self.assertIn('identifier="orders-db-renamed"', out)

        # And the refusal does not offer one value twice as a disambiguator.
        refusal = await t.mark_data_service_migrated(
            address="aws_db_instance.orders", ctx=mock_ctx)
        self.assertIn("ERROR", refusal)
        self.assertNotIn("'envs/prod', 'envs/prod'", refusal)
        self.assertIn("identifier=<one of", refusal)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_listing_adds_the_directory_when_an_address_repeats(
            self, mock_gcs_client_class, mock_get_email):
        """Two root modules declaring one address is the case where reporting
        the wrong one says a database has moved when it has not. The call the
        listing prints has to be unambiguous on its own."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        entries = [
            {"service": "rds", "identifier": "orders-db",
             "address": "aws_db_instance.orders", "disposition": "migrate",
             "evidence": [f"envs/{env}/data.tf"], "notes": [], "consumers": []}
            for env in ("dev", "prod")]
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class, entries)

        out = await t.list_data_migrations(ctx=mock_ctx)

        self.assertIn('directory="envs/dev"', out)
        self.assertIn('directory="envs/prod"', out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_an_unattributed_service_does_not_read_as_unused(
            self, mock_gcs_client_class, mock_get_email):
        """The scan under-detects on purpose and the review may have left it
        unattributed. "Nothing uses this" is a claim nobody made."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)

        out = await t.list_data_migrations(ctx=mock_ctx)

        self.assertIn("nobody the scan could attribute", out)
        self.assertIn("unattributed is not unused", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_reporting_a_move_settles_it_and_survives_a_reload(
            self, mock_gcs_client_class, mock_get_email):
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)

        out = await t.mark_data_service_migrated(
            address="aws_db_instance.orders", target="orders-db-gcp",
            note="DMS continuous, cut over Tuesday", ctx=mock_ctx)

        self.assertNotIn("ERROR", out)
        self.assertIn("migrated at orders-db-gcp", out)
        self.assertIn("1 data service(s) still owe a move", out)

        # Read back through a second call, not from the first one's memory.
        out = await t.list_data_migrations(ctx=mock_ctx)
        self.assertIn("Already migrated", out)
        self.assertIn("orders-db-gcp", out)
        self.assertIn("1 still owed", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_migrating_is_reported_but_does_not_settle(
            self, mock_gcs_client_class, mock_get_email):
        """A status report is not a completion — a component must not ship
        against a database that is still copying."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)

        await t.mark_data_service_migrating(
            address="aws_db_instance.orders", note="DMS job running, 40GB to go",
            ctx=mock_ctx)

        out = await t.list_data_migrations(ctx=mock_ctx)
        self.assertIn("0 reported migrated, 2 still owed", out)
        self.assertIn("in_progress", out)
        self.assertIn("40GB to go", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_two_root_modules_at_one_address_are_refused_not_guessed(
            self, mock_gcs_client_class, mock_get_email):
        """Recording "this moved" against the wrong one of a dev/prod pair
        would let a component ship against a database still in AWS."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        entries = [
            {"service": "rds", "identifier": "orders-db",
             "address": "aws_db_instance.orders", "disposition": "migrate",
             "evidence": [f"envs/{env}/data.tf"], "notes": [], "consumers": []}
            for env in ("dev", "prod")]
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class, entries)

        out = await t.mark_data_service_migrated(
            address="aws_db_instance.orders", ctx=mock_ctx)

        self.assertIn("ERROR", out)
        self.assertIn("names 2 data services", out)
        self.assertIn("envs/dev", out)
        self.assertIn("envs/prod", out)

        # Naming the directory resolves it, and settles only that one.
        out = await t.mark_data_service_migrated(
            address="aws_db_instance.orders", directory="envs/prod",
            ctx=mock_ctx)
        self.assertNotIn("ERROR", out)
        self.assertIn("1 data service(s) still owe a move", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_an_unknown_address_names_what_the_section_records(
            self, mock_gcs_client_class, mock_get_email):
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)

        out = await t.mark_data_service_migrated(
            address="aws_db_instance.typo", ctx=mock_ctx)

        self.assertIn("ERROR", out)
        self.assertIn("aws_db_instance.orders", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_reporting_a_service_that_was_not_owed_says_so(
            self, mock_gcs_client_class, mock_get_email):
        """Never harmful to record, but the operator may be working from a
        stale list or have meant a different entry."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)

        out = await t.mark_data_service_migrated(
            address="aws_elasticache_cluster.sessions", ctx=mock_ctx)

        self.assertNotIn("ERROR", out)
        self.assertIn("graded 'rebuild', not 'migrate'", out)
        self.assertIn("Recorded anyway", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_leaving_with_work_outstanding_is_refused(
            self, mock_gcs_client_class, mock_get_email):
        """A terminal state carries no instructions, so a workspace that moved
        on with databases outstanding would have nothing left to tell the next
        session they exist — and nothing to point at the tool for reporting
        them. Parking here is what keeps the work discoverable weeks later."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        state_container, _, mock_ctx = self._at_data_migration(
            mock_gcs_client_class)
        await t.mark_data_service_migrated(
            address="aws_db_instance.orders", ctx=mock_ctx)

        out = await t.complete_data_migration(ctx=mock_ctx)

        self.assertIn("ERROR", out)
        self.assertIn("s3 exports", out)
        self.assertIn("keep-in-aws", out)
        self.assertEqual(state_container["state"]["current_state"],
                         "STATE_DEPLOYMENT_DATA_MIGRATION")

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_migrated_service_kept_in_aws_is_still_reported(
            self, mock_gcs_client_class, mock_get_email):
        """`settled` filtered through `gating`, so a service reported migrated
        and then recorded keep-in-aws fell in a gap: still in the section so
        not stale, no longer gating so not settled. The only record that it
        moved lived on where nothing would surface it again — which is what
        both DESIGN and the stale_records docstring promise cannot happen."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        self._install_overrides_roundtrip()
        await t.mark_data_service_migrated(
            address="aws_s3_bucket.exports", target="gs://exports",
            ctx=mock_ctx)

        await discovery_datareview_3.tools.annotate_data_dependency(
            address="aws_s3_bucket.exports", disposition="keep-in-aws",
            note="kept after all", ctx=mock_ctx)

        out = await t.list_data_migrations(ctx=mock_ctx)
        self.assertIn("Already migrated", out)
        self.assertIn("gs://exports", out)
        # ...and it says the grade, so "migrated" does not read as "owed".
        self.assertIn("not owed here", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_service_that_was_never_owed_is_still_read_back(
            self, mock_gcs_client_class, mock_get_email):
        """The same hole, reached the other way: mark_data_service_migrated
        accepts a non-migrate service on purpose, and that record was
        write-only."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)

        await t.mark_data_service_migrated(
            address="aws_elasticache_cluster.sessions", target="memorystore-x",
            ctx=mock_ctx)

        out = await t.list_data_migrations(ctx=mock_ctx)
        self.assertIn("elasticache sessions", out)
        self.assertIn("memorystore-x", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_reported_move_republishes_the_developer_data_gate(
            self, mock_gcs_client_class, mock_get_email):
        """The report is the event that releases every component held on
        that database, and exports.json is the only way it reaches a
        developer session (platform/* is 403 there)."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        exports = self._install_exports_capture()

        out = await t.mark_data_service_migrated(
            address="aws_db_instance.orders", target="orders-cloudsql",
            ctx=mock_ctx)

        self.assertNotIn("ERROR", out)
        self.assertIn("doc", exports, "the data slice was never published")
        gate = exports["doc"]["data_gate"]
        self.assertTrue(gate["scanned"])
        by_id = {s["identifier"]: s for s in gate["services"]}
        self.assertEqual(by_id["orders-db"]["status"], "migrated")
        self.assertIsNone(by_id["exports"]["status"],
                          "the sibling is still owed and must still gate")
        self.assertEqual(by_id["orders-db"]["consumers"][0]["workload"],
                         "orders", "the gate cannot attribute without them")

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_close_republishes_even_though_the_step_is_over(
            self, mock_gcs_client_class, mock_get_email):
        """The close is the biggest release of developer ship gates there
        is. Unlike the runbook refresh this one has no liveness guard: a
        closed step that withheld the publish would leave every component
        held on a migration that finished."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        for address in ("aws_db_instance.orders", "aws_s3_bucket.exports"):
            await t.mark_data_service_migrated(address=address, ctx=mock_ctx)
        exports = self._install_exports_capture()

        out = await t.complete_data_migration(ctx=mock_ctx)

        self.assertIn("SUCCESS", out)
        gate = exports["doc"]["data_gate"]
        self.assertEqual(
            [s["status"] for s in gate["services"]
             if s["disposition"] == "migrate"], ["migrated", "migrated"])

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_keep_in_aws_at_the_step_releases_the_developer_gate(
            self, mock_gcs_client_class, mock_get_email):
        """The escape hatch. Without the republish, flipping an unmovable
        service to keep-in-aws would settle the platform step and leave the
        component that uses it held for ever — the deadlock this gate's
        design set out to avoid."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        self._install_overrides_roundtrip()
        exports = self._install_exports_capture()

        out = await discovery_datareview_3.tools.annotate_data_dependency(
            address="aws_db_instance.orders", disposition="keep-in-aws",
            note="no Cloud SQL equivalent for this engine", ctx=mock_ctx)

        self.assertNotIn("ERROR", out)
        gate = exports["doc"]["data_gate"]
        by_id = {s["identifier"]: s for s in gate["services"]}
        self.assertEqual(by_id["orders-db"]["disposition"], "keep-in-aws",
                         "a kept service stops being owed, and the developer "
                         "side has to learn it from the same call")

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_abandoning_republishes_so_developers_stop_being_told_to_copy(
            self, mock_gcs_client_class, mock_get_email):
        """image_map admits only replicated and self_service, so an abandoned
        entry has to LEAVE the map — otherwise workload translation keeps
        telling developers to "finish the replication" for a copy the platform
        team cancelled."""
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        exports = self._install_exports_capture()
        abandoned = "1234.dkr.ecr.us-east-1.amazonaws.com/api:v1"
        kept = "1234.dkr.ecr.us-east-1.amazonaws.com/web:v2"
        inventory = self._with_failed_copy("self_service")
        # A sibling that MUST stay in the map. Without it, "the abandoned ref
        # is absent" is also true of a map that is empty for unrelated
        # reasons, and the assertion cannot fail.
        inventory["images"].append(
            {"ref": kept, "registry": "ecr", "repository": "web", "tag": "v2",
             "pinned_by": "tag",
             "provenance": [{"kind": "literal", "file": "k8s/web.yaml"}],
             "replication": {"status": "replicated",
                             "destination": "us-central1-docker.pkg.dev/p/r/web:v2",
                             "verified_by": "user_asserted"}})
        (main.state_mgr.gcs_client.bucket("test-bucket")
         .blob(main.state_mgr.INVENTORY_BLOB_PATH)
         .upload_from_string(json.dumps(inventory)))

        out = await abandon_image_replication(
            refs=[abandoned], reason="retired", ctx=mock_ctx)

        self.assertNotIn("ERROR", out)
        self.assertIn("doc", exports, "exports were never republished")
        image_map = ((exports["doc"].get("artifact_registry") or {})
                     .get("image_map") or {})
        self.assertIn(kept, image_map, "the map was empty for another reason")
        self.assertNotIn(abandoned, image_map)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_an_annotation_on_a_settled_service_does_not_claim_the_gate(
            self, mock_gcs_client_class, mock_get_email):
        """The gate is keyed on the grade AND the recorded outcome —
        `outstanding` drops a `migrate` entry that has been reported migrated
        — so a sentence keyed on the grade alone told the operator the close
        was blocked on a service that was already settled, and offered a
        durable mapping change as the remedy."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        self._install_overrides_roundtrip()
        await t.mark_data_service_migrated(
            address="aws_db_instance.orders", target="orders-db-gcp",
            ctx=mock_ctx)

        out = await discovery_datareview_3.tools.annotate_data_dependency(
            address="aws_db_instance.orders", note="cutover done 02:00",
            ctx=mock_ctx)

        self.assertIn("already been reported migrated", out)
        self.assertNotIn("still owed", out)
        self.assertNotIn("keep refusing", out)
        # And it agrees with the close, which is the thing it describes.
        close = await t.complete_data_migration(ctx=mock_ctx)
        self.assertNotIn("orders", close.split("still owe a move")[-1][:120])

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_transient_baseline_read_does_not_read_as_absence(
            self, mock_gcs_client_class, mock_get_email):
        """`complete_data_migration` splits a failed read from a genuine
        absence because one is retryable and the other sends the operator to
        an irreversible close. `_amend` collapsed them and told an operator
        whose read had blipped that the service could not be excused."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        self._install_overrides_roundtrip()
        (main.state_mgr.gcs_client.bucket("test-bucket")
         .blob(discovery_datareview_3.tools.SCAN_BASELINE_BLOB)
         .download_as_text.side_effect) = exceptions.ServiceUnavailable(
             "backend error")

        out = await discovery_datareview_3.tools.annotate_data_dependency(
            address="aws_db_instance.orders", disposition="keep-in-aws",
            note="not movable", ctx=mock_ctx)

        self.assertIn("ERROR", out)
        self.assertIn("try again", out)
        self.assertNotIn("there is no way to excuse this service", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_close_refusal_names_every_unconfirmed_copy(
            self, mock_gcs_client_class, mock_get_email):
        """It is the message that offers `abandon_image_replication` as the
        exit, and that tool needs each ref spelled. Naming an exit while
        withholding its argument is not an exit."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        images = [{"ref": f"1234.dkr.ecr.us-east-1.amazonaws.com/svc{i}:v1",
                   "registry": "ecr", "repository": f"svc{i}", "tag": "v1",
                   "pinned_by": "tag",
                   "provenance": [{"kind": "literal", "file": "k8s/a.yaml"}],
                   "replication": {"status": "self_service"}}
                  for i in range(8)]
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        (main.state_mgr.gcs_client.bucket("test-bucket")
         .blob(main.state_mgr.INVENTORY_BLOB_PATH)
         .upload_from_string(json.dumps(
             {"schema_version": "1.0",
              "data_dependencies": [dict(e) for e in self.DATA_ENTRIES],
              "images": images})))

        out = await t.complete_data_migration(ctx=mock_ctx)

        self.assertIn("abandon_image_replication", out)
        for image in images:
            self.assertIn(image["ref"], out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_runbook_grades_a_settled_service_that_was_not_owed(
            self, mock_gcs_client_class, mock_get_email):
        """"This moved and was owed" and "this moved and then the customer
        decided to keep it" are both true and only the first is progress
        against the gate. The chat listing said which; the durable artifact —
        the one a platform engineer opens to decide whether a component may
        ship — did not, and then stated in prose that such services "are not
        listed above" while listing one."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)

        await t.mark_data_service_migrated(
            address="aws_elasticache_cluster.sessions",
            target="memorystore-sessions", ctx=mock_ctx)
        await t.list_data_migrations(ctx=mock_ctx)

        runbook = self.mock_dm_runbook_blob.upload_from_string.call_args[0][0]
        self.assertIn("already reported migrated", runbook)
        self.assertIn("graded 'rebuild', not owed here", runbook)
        # ...and the paragraph beneath no longer contradicts the block above.
        self.assertNotIn("not listed above", runbook)
        # The same paragraph enumerates the grades. `undecided` is one the
        # scan really assigns, and a closed list of four told a reader an
        # `undecided` entry must be owed — while it is not listed as owed
        # either.
        self.assertIn("`keep-in-aws` and `undecided` do not", runbook)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_every_unconfirmed_copy_is_named(
            self, mock_gcs_client_class, mock_get_email):
        """`abandon_image_replication` has no bulk form — it needs each ref —
        so a truncated list leaves no way to spell the exit for a copy that
        was not printed, and the only reachable alternative asserts they were
        all copied."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        images = [{"ref": f"1234.dkr.ecr.us-east-1.amazonaws.com/svc{i}:v1",
                   "registry": "ecr", "repository": f"svc{i}", "tag": "v1",
                   "pinned_by": "tag",
                   "provenance": [{"kind": "literal", "file": "k8s/a.yaml"}],
                   "replication": {"status": "self_service"}}
                  for i in range(8)]
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        (main.state_mgr.gcs_client.bucket("test-bucket")
         .blob(main.state_mgr.INVENTORY_BLOB_PATH)
         .upload_from_string(json.dumps(
             {"schema_version": "1.0",
              "data_dependencies": [dict(e) for e in self.DATA_ENTRIES],
              "images": images})))

        out = await t.list_data_migrations(ctx=mock_ctx)

        for image in images:
            self.assertIn(image["ref"], out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_conflict_at_the_step_names_a_reachable_listing(
            self, mock_gcs_client_class, mock_get_email):
        """The correction is durable but the section was not rewritten, so
        the operator is told to re-read it — with the name of a tool that
        refuses where they stand. The two refusals beside this one were made
        state-conditional; this arm was not."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        self._install_overrides_roundtrip()
        (main.state_mgr.gcs_client.bucket("test-bucket")
         .blob(main.state_mgr.INVENTORY_BLOB_PATH)
         .upload_from_string.side_effect) = exceptions.PreconditionFailed(
             "generation mismatch")

        out = await discovery_datareview_3.tools.annotate_data_dependency(
            address="aws_db_instance.orders", disposition="keep-in-aws",
            note="not movable", ctx=mock_ctx)

        self.assertIn("recorded and durable", out)
        self.assertIn("list_data_migrations()", out)
        self.assertNotIn("list_data_dependencies()", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_read_that_fails_on_the_second_look_is_not_an_absence(
            self, mock_gcs_client_class, mock_get_email):
        """Grading the baseline is a SECOND download: `_open_review` reads it
        once and `scan_baseline_state` reads it again. The first can succeed
        with something falsy — a `{}` an operator uploaded following the
        repair advice — and the second fail, which is a transport failure
        arriving a moment after the branch that catches those. Calling that
        absence would send an operator to restore an object that is there,
        over a blip."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        self._install_overrides_roundtrip()
        blob = (main.state_mgr.gcs_client.bucket("test-bucket")
                .blob(discovery_datareview_3.tools.SCAN_BASELINE_BLOB))
        reads = {"n": 0}

        def fails_on_the_second_look(*_a, **_k):
            reads["n"] += 1
            if reads["n"] == 1:
                return "{}"
            raise exceptions.ServiceUnavailable("backend error")

        blob.download_as_text.side_effect = fails_on_the_second_look

        out = await discovery_datareview_3.tools.annotate_data_dependency(
            address="aws_db_instance.orders", disposition="keep-in-aws",
            note="engine change not approved", ctx=mock_ctx)

        self.assertIn("could not be read", out)
        self.assertIn("try again", out)
        # The absent arm's own wording, so this cannot pass by naming a
        # string no branch emits.
        self.assertNotIn("has been removed", out)
        self.assertNotIn("ONE-WAY DOOR", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_exit_refusal_names_which_baseline_case_it_is(
            self, mock_gcs_client_class, mock_get_email):
        """Absent and corrupt used to produce a byte-identical refusal, and
        they need different repairs: one is replaced from the copy the scan
        wrote, the other has to be restored — the scan writes the baseline in
        the same call that writes the section, so an absent one was removed.
        Nothing else here can tell them apart: the listing never mentions the
        baseline, it is not a review-UI artifact, and the agent may not run a
        CLI to look. If this message does not say which, nothing does."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"

        _, _, mock_ctx = self._at_data_migration(
            mock_gcs_client_class, with_baseline=False)
        self._install_overrides_roundtrip()
        (main.state_mgr.gcs_client.bucket("test-bucket")
         .blob(discovery_datareview_3.tools.SCAN_BASELINE_BLOB)
         .download_as_text.side_effect) = exceptions.NotFound("Not Found")
        absent = await discovery_datareview_3.tools.annotate_data_dependency(
            address="aws_db_instance.orders", disposition="keep-in-aws",
            note="engine change not approved", ctx=mock_ctx)

        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        self._install_overrides_roundtrip()
        (main.state_mgr.gcs_client.bucket("test-bucket")
         .blob(discovery_datareview_3.tools.SCAN_BASELINE_BLOB)
         .upload_from_string("{not json"))
        corrupt = await discovery_datareview_3.tools.annotate_data_dependency(
            address="aws_db_instance.orders", disposition="keep-in-aws",
            note="engine change not approved", ctx=mock_ctx)

        self.assertNotEqual(absent, corrupt)
        self.assertIn("this ledger is damaged", absent)
        self.assertIn("does not exist", absent)
        self.assertIn("has been removed", absent)
        # Restoring, not replacing: there is no good copy to copy from.
        self.assertIn("object versioning", absent)
        self.assertIn("join_ledger --reconfigure", absent)
        # Accurately: a reconfigure resets the walk, it does not throw the
        # ledger away. Claiming otherwise sends an operator looking for
        # backups of artifacts that are still there.
        self.assertIn("no artifact or recorded decision is discarded", absent)
        self.assertNotIn("Replace the baseline", absent)
        # And no close is offered for either.
        self.assertNotIn("caveat", absent)

        self.assertIn("is not readable as a scan baseline", corrupt)
        self.assertIn("Repair or replace", corrupt)
        self.assertNotIn("caveat", corrupt)
        self.assertIn("will refuse too", corrupt)
        self.assertNotIn("ONE-WAY DOOR", corrupt)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_broken_baseline_blocks_only_the_exit(
            self, mock_gcs_client_class, mock_get_email):
        """The claim the step's instructions make to the agent, as behaviour:
        the baseline is what `keep-in-aws` is replayed over, and NOTHING else
        here reads it. So a corrupt one costs the exit and nothing else — the
        services can still be reported, and once they have all moved the step
        closes cleanly, with no caveat and no baseline.

        The instructions said the opposite ("neither reported nor excused"),
        which removes the only thing that still works and leaves the operator
        with nothing to do but repair the ledger."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        state_container, _, mock_ctx = self._at_data_migration(
            mock_gcs_client_class)
        (main.state_mgr.gcs_client.bucket("test-bucket")
         .blob(discovery_datareview_3.tools.SCAN_BASELINE_BLOB)
         .upload_from_string("{not json"))

        # The exit is gone...
        excuse = await discovery_datareview_3.tools.annotate_data_dependency(
            address="aws_db_instance.orders", disposition="keep-in-aws",
            note="cannot move", ctx=mock_ctx)
        self.assertIn("ERROR", excuse)
        # ...and the close refuses, rather than taking the caveated path.
        refusal = await t.complete_data_migration(ctx=mock_ctx)
        self.assertIn("not readable as a scan baseline", refusal)
        self.assertNotIn("SUCCESS", refusal)

        # ...but reporting was available the whole time.
        for address in ("aws_db_instance.orders", "aws_s3_bucket.exports"):
            out = await t.mark_data_service_migrated(
                address=address, ctx=mock_ctx)
            self.assertNotIn("ERROR", out)

        out = await t.complete_data_migration(ctx=mock_ctx)

        self.assertIn("SUCCESS", out)
        self.assertNotIn("with a caveat", out)
        self.assertEqual(state_container["state"]["current_state"],
                         "STATE_DEPLOYMENT_COMPLETED")
        # The audit trail for the terminal transition: who left the step, and
        # the transition itself. Both were deletable with the suite green.
        history = "\n".join(state_container["state"]["history"])
        self.assertIn("Data migration step left by platform-user@google.com",
                      history)
        self.assertIn("2 migrated", history)
        self.assertIn("Transitioned STATE_DEPLOYMENT_DATA_MIGRATION -> "
                      "STATE_DEPLOYMENT_COMPLETED via complete_data_migration",
                      history)
        # ...and no count that can only ever be zero.
        self.assertNotIn("still owed", history)
        self.assertNotIn(
            "outstanding",
            json.dumps(state_container["state"]["variables"]))

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_an_absent_baseline_stops_the_close_rather_than_caveating_it(
            self, mock_gcs_client_class, mock_get_email):
        """This is the case that used to CLOSE. An earlier revision argued that
        refusing would wedge a workspace on a service it could neither report
        nor excuse, so it completed the segment and stamped what stayed
        outstanding. But the scan writes the baseline in the same call that
        writes the section, so a ledger carrying data services and no baseline
        has had the object removed — and reporting a migration complete over
        that tells every application team the platform side is finished on the
        strength of a ledger nobody can vouch for.

        It refuses, with the damage named, and says the object was removed
        rather than offering a replacement there is no copy of."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        state_container, _, mock_ctx = self._at_data_migration(
            mock_gcs_client_class, with_baseline=False)
        (main.state_mgr.gcs_client.bucket("test-bucket")
         .blob(discovery_datareview_3.tools.SCAN_BASELINE_BLOB)
         .download_as_text.side_effect) = exceptions.NotFound("Not Found")

        out = await t.complete_data_migration(ctx=mock_ctx)

        self.assertIn("this ledger is damaged", out)
        self.assertIn("has been removed", out)
        # The expensive repair, named only here — and described accurately:
        # a reconfigure resets the walk, it does not discard the ledger.
        self.assertIn("has to come back", out)
        self.assertIn("object versioning", out)
        self.assertIn("join_ledger --reconfigure", out)
        self.assertIn("no artifact or recorded decision is discarded", out)
        self.assertNotIn("Replace " + discovery_datareview_3.tools
                         .SCAN_BASELINE_BLOB, out)
        # No close of any kind, and nothing that reads as one.
        self.assertNotIn("SUCCESS", out)
        self.assertNotIn("caveat", out)
        self.assertEqual(state_container["state"]["current_state"],
                         "STATE_DEPLOYMENT_DATA_MIGRATION")
        self.assertNotIn("closed_without_the_exit",
                         json.dumps(state_container["state"]["variables"]))
        # Reporting still works, and clears the way out honestly.
        for address in ("aws_db_instance.orders", "aws_s3_bucket.exports"):
            await t.mark_data_service_migrated(address=address, ctx=mock_ctx)
        self.assertIn("SUCCESS", await t.complete_data_migration(ctx=mock_ctx))

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_unreadable_refusal_says_what_still_works(
            self, mock_gcs_client_class, mock_get_email):
        """The step will not close over a damaged ledger, so this message is
        where the operator stops — and saying the owed services "can be
        neither reported nor excused" removed the ordinary way forward, since
        reporting never touches the baseline. A workspace whose services do
        move still closes cleanly."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        (main.state_mgr.gcs_client.bucket("test-bucket")
         .blob(discovery_datareview_3.tools.SCAN_BASELINE_BLOB)
         .upload_from_string("{not json"))

        out = await t.complete_data_migration(ctx=mock_ctx)

        self.assertIn("this ledger is damaged", out)
        self.assertIn("cannot be EXCUSED", out)
        self.assertIn("can still be REPORTED", out)
        self.assertIn("not readable as a scan baseline", out)
        # No close is offered, and none is reachable.
        self.assertNotIn("SUCCESS", out)
        self.assertNotIn("DELETING", out)
        # ...but reporting really does work, with that object still in place,
        # and once everything owed has moved the step closes cleanly.
        for address in ("aws_db_instance.orders", "aws_s3_bucket.exports"):
            report = await t.mark_data_service_migrated(
                address=address, ctx=mock_ctx)
            self.assertNotIn("ERROR", report)
        closed = await t.complete_data_migration(ctx=mock_ctx)
        self.assertIn("SUCCESS", closed)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_unreadable_refusal_names_the_images_too(
            self, mock_gcs_client_class, mock_get_email):
        """The ABSENT path has a branch for this; UNREADABLE had none, so an
        operator who settled every database met a second refusal they had not
        been warned about."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        ref = "1234.dkr.ecr.us-east-1.amazonaws.com/api:v1"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        (main.state_mgr.gcs_client.bucket("test-bucket")
         .blob(discovery_datareview_3.tools.SCAN_BASELINE_BLOB)
         .upload_from_string("{not json"))
        (main.state_mgr.gcs_client.bucket("test-bucket")
         .blob(main.state_mgr.INVENTORY_BLOB_PATH)
         .upload_from_string(json.dumps(
             {"schema_version": "1.0",
              "data_dependencies": [dict(e) for e in self.DATA_ENTRIES],
              "images": [{"ref": ref, "registry": "ecr", "repository": "api",
                          "tag": "v1", "pinned_by": "tag",
                          "provenance": [{"kind": "literal",
                                          "file": "k8s/api.yaml"}],
                          "replication": {"status": "self_service"}}]})))

        out = await t.complete_data_migration(ctx=mock_ctx)

        self.assertIn(ref, out)
        self.assertIn("abandon_image_replication", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_report_redraws_the_durable_worklist(
            self, mock_gcs_client_class, mock_get_email):
        """The runbook is this state's PRIMARY artifact and the instructions
        tell the agent not to re-list, so nothing else would redraw it: a
        platform engineer opening it after a report was told the database
        that had just landed was still owed."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        await t.list_data_migrations(ctx=mock_ctx)

        await t.mark_data_service_migrated(
            address="aws_db_instance.orders", target="cloudsql-orders",
            ctx=mock_ctx)

        runbook = self.mock_dm_runbook_blob.upload_from_string.call_args[0][0]
        self.assertIn("1 service(s) still owe a move", runbook)
        self.assertIn("already reported migrated", runbook)
        self.assertIn("cloudsql-orders", runbook)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_keep_in_aws_redraws_the_durable_worklist(
            self, mock_gcs_client_class, mock_get_email):
        """The step's documented exit changes what is owed, and it arrives
        from the review module — the one correction nothing inside the
        deployment package sees."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        self._install_overrides_roundtrip()
        await t.list_data_migrations(ctx=mock_ctx)

        await discovery_datareview_3.tools.annotate_data_dependency(
            address="aws_db_instance.orders", disposition="keep-in-aws",
            note="the licence does not transfer", ctx=mock_ctx)

        runbook = self.mock_dm_runbook_blob.upload_from_string.call_args[0][0]
        self.assertIn("1 service(s) still owe a move", runbook)
        self.assertNotIn("### rds orders-db", runbook)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_review_refusal_also_keeps_an_empty_directory(
            self, mock_gcs_client_class, mock_get_email):
        """Round 11 fixed the deployment `_target` and recorded that the
        review's sibling was already written `is not None`. Nothing held it
        there — and `annotate_data_dependency` at the data migration step goes
        through this one, where `directory=""` is the value the step's own
        printed call teaches an operator to type."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        entries = [{"service": "rds", "identifier": "orders-db",
                    "address": "aws_db_instance.orders",
                    "disposition": "migrate",
                    "evidence": ["envs/dev/data.tf"], "notes": [],
                    "consumers": []}]
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class, entries)
        self._install_overrides_roundtrip()

        out = await discovery_datareview_3.tools.annotate_data_dependency(
            address="aws_db_instance.orders", directory="",
            disposition="keep-in-aws", note="x", ctx=mock_ctx)

        self.assertIn("ERROR", out)
        self.assertIn("directory=''", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_root_declared_entry_is_told_why_it_matched_nothing(
            self, mock_gcs_client_class, mock_get_email):
        """`reporting_call` emits `directory=""` for a root-declared entry on
        purpose. Dropping a falsy directory from the refusal printed "no data
        service is recorded at X" and then listed X, leaving the filter that
        rejected the match the one thing unmentioned."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        entries = [{"service": "rds", "identifier": "orders-db",
                    "address": "aws_db_instance.orders",
                    "disposition": "migrate",
                    "evidence": ["envs/dev/data.tf"], "notes": [],
                    "consumers": []}]
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class, entries)

        out = await t.mark_data_service_migrated(
            address="aws_db_instance.orders", directory="", ctx=mock_ctx)

        self.assertIn("ERROR", out)
        self.assertIn("in directory ''", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_disambiguator_offers_only_axes_that_separate(
            self, mock_gcs_client_class, mock_get_email):
        """"directory=<one of 'envs/prod', 'envs/prod'>" is not a
        disambiguator, it is noise the caller has to see through. Both guards
        mutated away with the whole suite green."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        # Two entries, one directory: only the identifier separates them.
        renamed = [
            {"service": "rds", "identifier": name,
             "address": "aws_db_instance.orders", "disposition": "migrate",
             "evidence": ["envs/prod/data.tf"], "notes": [], "consumers": []}
            for name in ("orders-db", "orders-db-renamed")]
        _, _, mock_ctx = self._at_data_migration(
            mock_gcs_client_class, renamed)

        out = await t.mark_data_service_migrated(
            address="aws_db_instance.orders", ctx=mock_ctx)

        self.assertIn("identifier=<one of", out)
        self.assertNotIn("directory=<one of", out)

        # Two entries, one identifier: only the directory separates them.
        two_envs = [
            {"service": "rds", "identifier": "orders-db",
             "address": "aws_db_instance.orders", "disposition": "migrate",
             "evidence": [f"envs/{env}/data.tf"], "notes": [], "consumers": []}
            for env in ("dev", "prod")]
        _, _, mock_ctx = self._at_data_migration(
            mock_gcs_client_class, two_envs)

        out = await t.mark_data_service_migrated(
            address="aws_db_instance.orders", ctx=mock_ctx)

        self.assertIn("directory=<one of", out)
        self.assertNotIn("identifier=<one of", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_an_empty_ledger_refuses_rather_than_raising(
            self, mock_gcs_client_class, mock_get_email):
        """Without the guard the tool raises AttributeError out of a call
        whose contract is that a failure comes back as an ERROR string."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        (main.state_mgr.gcs_client.bucket("test-bucket")
         .blob(main.state_mgr.INVENTORY_BLOB_PATH)
         .download_as_text.side_effect) = exceptions.NotFound("Not Found")

        out = await t.list_data_migrations(ctx=mock_ctx)

        self.assertIn("ERROR", out)
        self.assertIn("No discovery inventory", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_dropping_baseline_refusal_does_not_promise_the_close(
            self, mock_gcs_client_class, mock_get_email):
        """A baseline that drops entries is PRESENT, so it grades UNREADABLE
        and the close refuses too. This refusal used to promise a close that
        would not happen; now it states the repair, and warns against the
        deletion that used to be an escape and now destroys the only record of
        what the scan found."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        self._install_overrides_roundtrip()
        (main.state_mgr.gcs_client.bucket("test-bucket")
         .blob(discovery_datareview_3.tools.SCAN_BASELINE_BLOB)
         .upload_from_string(json.dumps({"data_dependencies": []})))

        out = await discovery_datareview_3.tools.annotate_data_dependency(
            address="aws_s3_bucket.exports", disposition="keep-in-aws",
            note="staying put", ctx=mock_ctx)

        self.assertIn("refuses too", out)
        self.assertIn("Do not delete it", out)
        # And what it says about the close is what the close does.
        close = await t.complete_data_migration(ctx=mock_ctx)
        self.assertIn("ERROR", close)
        self.assertIn("this ledger is damaged", close)
        self.assertNotIn("SUCCESS", close)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_an_excused_in_flight_move_stays_reportable(
            self, mock_gcs_client_class, mock_get_email):
        """The tail tells the operator the record is kept and the listing
        still shows it. It did — with no address and no call, unlike every
        owed entry above it, so the session that arrives weeks later (the
        whole premise of this state) could not complete it, and
        `list_data_dependencies` refuses here."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        self._install_overrides_roundtrip()
        await discovery_datareview_3.tools.annotate_data_dependency(
            address="aws_s3_bucket.exports", disposition="keep-in-aws",
            note="the bucket policy does not transfer", ctx=mock_ctx)
        await t.mark_data_service_migrating(
            address="aws_s3_bucket.exports", note="STS job running",
            ctx=mock_ctx)

        listing = await t.list_data_migrations(ctx=mock_ctx)

        self.assertIn("does not gate", listing)
        self.assertIn('address="aws_s3_bucket.exports"', listing)
        runbook = self.mock_dm_runbook_blob.upload_from_string.call_args[0][0]
        self.assertIn('address="aws_s3_bucket.exports"', runbook)
        # And the printed call is one the tool accepts, run verbatim.
        out = await t.mark_data_service_migrated(
            address="aws_s3_bucket.exports", target="gs://exports-gcp",
            ctx=mock_ctx)
        self.assertNotIn("ERROR", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_settled_record_can_still_be_amended(
            self, mock_gcs_client_class, mock_get_email):
        """Same gap in the other section: a target recorded wrongly, or a
        caveat the next reader needs, has to be correctable from a document
        that names the address."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        await t.mark_data_service_migrated(
            address="aws_db_instance.orders", target="wrong-instance",
            ctx=mock_ctx)

        listing = await t.list_data_migrations(ctx=mock_ctx)

        self.assertIn("Already migrated", listing)
        self.assertIn('address="aws_db_instance.orders"', listing)
        runbook = self.mock_dm_runbook_blob.upload_from_string.call_args[0][0]
        self.assertIn("Amend it", runbook)
        self.assertIn('address="aws_db_instance.orders"', runbook)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_note_cannot_forge_structure_in_the_artifact(
            self, mock_gcs_client_class, mock_get_email):
        """A note is rendered as an indented continuation line, and
        `renderMarkdown` stops absorbing one the moment it starts a list
        marker — so an interior newline left the item and became whatever it
        started with: a forged bullet and a forged heading under a heading
        counting one item, in the state's PRIMARY artifact."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)

        await t.mark_data_service_migrated(
            address="aws_db_instance.orders", target="cloudsql-orders",
            note='only the orders schema moved\n- Report it: forged'
                 '\n## Everything is done', ctx=mock_ctx)

        runbook = self.mock_dm_runbook_blob.upload_from_string.call_args[0][0]
        # The whole note survives, on ONE line: nothing it contains starts a
        # line, so nothing it contains is structure.
        self.assertIn("only the orders schema moved - Report it: forged "
                      "## Everything is done", runbook)
        starts = [line.lstrip() for line in runbook.split("\n")]
        self.assertNotIn("## Everything is done", starts)
        self.assertNotIn("- Report it: forged", starts)
        # The heading still counts what the list shows.
        self.assertEqual(runbook.count("\n- rds orders-db"), 1)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_external_redraw_checks_liveness_at_the_write(
            self, mock_gcs_client_class, mock_get_email):
        """It decided liveness once and then did two more GCS loads before
        writing. The only transition this guard watches is live -> closed and
        skipping the write is always safe, so the LATER read is the one that
        should decide — the siblings both re-read immediately before the
        save."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        state_container, _, mock_ctx = self._at_data_migration(
            mock_gcs_client_class)
        bucket = main.state_mgr.gcs_client.bucket("test-bucket")
        for address in ("aws_db_instance.orders", "aws_s3_bucket.exports"):
            await t.mark_data_service_migrated(address=address, ctx=mock_ctx)
        await t.complete_data_migration(ctx=mock_ctx)
        closed = self.mock_dm_runbook_blob.upload_from_string.call_args[0][0]
        self.assertIn("left the data migration step", closed)

        # The other session's first state read predates the close.
        live = json.dumps(dict(state_container["state"],
                               current_state="STATE_DEPLOYMENT_DATA_MIGRATION"))
        now_closed = json.dumps(state_container["state"])
        reads = {"n": 0}

        def closes_between_the_two_reads(*_a, **_k):
            reads["n"] += 1
            return live if reads["n"] == 1 else now_closed

        (bucket.blob("platform/onboarding/state.json")
         .download_as_text.side_effect) = closes_between_the_two_reads

        t.refresh_runbook_from_ledger(bucket)

        self.assertGreaterEqual(reads["n"], 2, "liveness read only once")
        self.assertEqual(
            self.mock_dm_runbook_blob.upload_from_string.call_args[0][0],
            closed)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_external_redraw_says_when_it_could_not_tell(
            self, mock_gcs_client_class, mock_get_email):
        """The third caller of the three-verdict split, and the only one whose
        UNKNOWN arm nothing asserted."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        self._at_data_migration(mock_gcs_client_class)
        bucket = main.state_mgr.gcs_client.bucket("test-bucket")
        (bucket.blob("platform/onboarding/state.json")
         .download_as_text.side_effect) = exceptions.ServiceUnavailable(
             "backend error")

        warning = t.refresh_runbook_from_ledger(bucket)

        self.assertIn("WARNING", warning)
        self.assertIn("could not be read", warning)
        self.mock_dm_runbook_blob.upload_from_string.assert_not_called()

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_no_match_refusal_names_every_axis_it_filtered_on(
            self, mock_gcs_client_class, mock_get_email):
        """The identifier clause was untested in both directions, and the
        directory clause only in the empty-string case: a call passing neither
        must not read "in directory 'None'"."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)

        out = await t.mark_data_service_migrated(
            address="aws_db_instance.orders", identifier="orders-db-typo",
            ctx=mock_ctx)

        self.assertIn("with identifier 'orders-db-typo'", out)
        self.assertNotIn("in directory", out)

        # Neither axis supplied: the refusal names neither.
        out = await t.mark_data_service_migrated(
            address="aws_db_instance.ordrs", ctx=mock_ctx)
        self.assertNotIn("in directory", out)
        self.assertNotIn("with identifier", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_an_empty_section_says_so_rather_than_listing_nothing(
            self, mock_gcs_client_class, mock_get_email):
        """"The section records: ." is not an answer."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(
            mock_gcs_client_class, entries=[])

        out = await t.mark_data_service_migrated(
            address="aws_db_instance.orders", ctx=mock_ctx)

        self.assertIn("nothing at all", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_chat_listing_reports_stale_records_too(
            self, mock_gcs_client_class, mock_get_email):
        """The runbook's copy of this paragraph is tested and the listing's is
        not — the split that has produced four findings in this CL."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        await t.mark_data_service_migrated(
            address="aws_db_instance.orders", target="cloudsql-orders",
            ctx=mock_ctx)
        # The scan runs again and the resource is gone from the estate.
        (main.state_mgr.gcs_client.bucket("test-bucket")
         .blob(main.state_mgr.INVENTORY_BLOB_PATH)
         .upload_from_string(json.dumps(
             {"schema_version": "1.0",
              "data_dependencies": [dict(e) for e in self.DATA_ENTRIES
                                    if e["address"] != "aws_db_instance.orders"]})))

        out = await t.list_data_migrations(ctx=mock_ctx)

        self.assertIn("the current section does not carry", out)
        self.assertIn("orders-db", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_whitespace_note_is_reported_as_cleared(
            self, mock_gcs_client_class, mock_get_email):
        """`clean_value` runs in two places on purpose. Only the recorder's
        copy was tested — with the tool's gone, the stored field is cleared
        while the response says nothing was."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        await t.mark_data_service_migrated(
            address="aws_db_instance.orders", note="wrong service",
            ctx=mock_ctx)

        out = await t.mark_data_service_migrated(
            address="aws_db_instance.orders", note="   ", ctx=mock_ctx)

        self.assertIn("Cleared:", out)
        listing = await t.list_data_migrations(ctx=mock_ctx)
        self.assertNotIn("wrong service", listing)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_an_unreadable_state_is_not_reported_as_a_close(
            self, mock_gcs_client_class, mock_get_email):
        """The freshness check folded "could not read it" into "it closed", and
        the listing turned that into a claim about the WORKSPACE: it announced
        that the step had closed and then printed two services still owed. A
        503 on the second state read is a transport failure that clears on the
        next call."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        state_container, _, mock_ctx = self._at_data_migration(
            mock_gcs_client_class)
        state_blob = (main.state_mgr.gcs_client.bucket("test-bucket")
                      .blob("platform/onboarding/state.json"))
        live = json.dumps(state_container["state"])
        reads = {"n": 0}

        def fails_after_the_gate_check(*_a, **_k):
            reads["n"] += 1
            if reads["n"] == 1:
                return live
            raise exceptions.ServiceUnavailable("backend error")

        state_blob.download_as_text.side_effect = fails_after_the_gate_check

        out = await t.list_data_migrations(ctx=mock_ctx)

        self.assertIn("2 still owed", out)
        # Case-folded: the closed footer begins with a capital, and the
        # lowercase check passed against it. And positively: a failed state
        # read is not a close, so the listing still INSTRUCTS.
        self.assertNotIn("has closed", out.lower())
        self.assertIn("could not be read", out)
        self.assertIn("    report it: ", out)
        self.assertIn("should not move after all", out)
        # The artifact is still not overwritten: not knowing is not a licence
        # to write over the record of a close either.
        self.mock_dm_runbook_blob.upload_from_string.assert_not_called()

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_report_says_when_it_could_not_redraw_the_worklist(
            self, mock_gcs_client_class, mock_get_email):
        """The refresh exists so the terminal artifact's "written as it stood
        when the step closed" stays true, and a silent skip is the one case
        that makes it false again. Two of the three callers of the freshness
        check said nothing."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        state_container, _, mock_ctx = self._at_data_migration(
            mock_gcs_client_class)
        state_blob = (main.state_mgr.gcs_client.bucket("test-bucket")
                      .blob("platform/onboarding/state.json"))
        live = json.dumps(state_container["state"])
        reads = {"n": 0}

        def fails_after_the_gate_check(*_a, **_k):
            reads["n"] += 1
            if reads["n"] == 1:
                return live
            raise exceptions.ServiceUnavailable("backend error")

        state_blob.download_as_text.side_effect = fails_after_the_gate_check

        out = await t.mark_data_service_migrated(
            address="aws_db_instance.orders", target="cloudsql-orders",
            ctx=mock_ctx)

        # The outcome is durable; only the redraw was skipped, and it says so.
        self.assertIn("Recorded:", out)
        self.assertIn("WARNING", out)
        self.assertIn("could not be read", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_an_owed_service_is_never_in_the_not_gated_section(
            self, mock_gcs_client_class, mock_get_email):
        """`in_flight_not_owed` is the only view that surfaces an in-progress
        outcome on a non-`migrate` entry, and its whole correctness is the
        disposition clause. Without it a `migrate` service appears twice: once
        as owed and once under a heading asserting nothing waits on it, with
        "(graded 'migrate')" printed inside the "does not gate" section."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        await t.mark_data_service_migrating(
            address="aws_db_instance.orders", note="DMS at 60%", ctx=mock_ctx)

        out = await t.list_data_migrations(ctx=mock_ctx)

        # Still owed, and only that.
        self.assertIn("2 still owed", out)
        self.assertNotIn("does not gate", out)
        runbook = self.mock_dm_runbook_blob.upload_from_string.call_args[0][0]
        # The heading, not the phrase: the "Not owed here" paragraph names
        # that section, so the phrase alone is in every runbook.
        self.assertNotIn("## 1 move(s) in progress", runbook)
        # The control: a non-migrate entry with the same status IS surfaced,
        # so the assertions above are not passing on a section that never
        # renders.
        await t.mark_data_service_migrating(
            address="aws_elasticache_cluster.sessions", note="RDB export",
            ctx=mock_ctx)
        out = await t.list_data_migrations(ctx=mock_ctx)
        self.assertIn("does not gate", out)
        self.assertIn("sessions", out.split("does not gate")[1])

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_move_in_flight_survives_being_excused(
            self, mock_gcs_client_class, mock_get_email):
        """Round 1 closed this hole for `migrated` by taking the disposition
        filter out of `settled`. `in_progress` was left behind: outstanding
        drops it on the disposition, settled on the status, stale_records
        because the entry is still in the section — three views, three
        reasons, union empty. And this is the status where the AWS-side
        resource is still live."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        self._install_overrides_roundtrip()
        caveat = "DMS job dms-orders-1 at 60%, cutover Tue 02:00 UTC"
        await t.mark_data_service_migrating(
            address="aws_db_instance.orders", note=caveat, ctx=mock_ctx)

        out = await discovery_datareview_3.tools.annotate_data_dependency(
            address="aws_db_instance.orders", disposition="keep-in-aws",
            note="the licence does not transfer", ctx=mock_ctx)

        # The tail says the report is still there.
        self.assertIn("IN PROGRESS", out)
        listing = await t.list_data_migrations(ctx=mock_ctx)
        self.assertIn(caveat, listing)
        self.assertIn("does not gate", listing)
        # NAMED, not just given a call: this is the view whose argument is
        # that somebody is mid-cutover on a live resource.
        self.assertIn("- rds orders-db (graded 'keep-in-aws')", listing)
        runbook = self.mock_dm_runbook_blob.upload_from_string.call_args[0][0]
        self.assertIn(caveat, runbook)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_move_in_flight_on_a_service_never_owed_is_read_back(
            self, mock_gcs_client_class, mock_get_email):
        """The other route in. The tool answers "Recorded anyway" and the
        record was then unreachable from every view — including the terminal
        artifact, after a close that reported nothing outstanding."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        caveat = "RDB export running, do not delete the AWS cluster"
        await t.mark_data_service_migrating(
            address="aws_elasticache_cluster.sessions", note=caveat,
            ctx=mock_ctx)
        for address in ("aws_db_instance.orders", "aws_s3_bucket.exports"):
            await t.mark_data_service_migrated(address=address, ctx=mock_ctx)

        out = await t.complete_data_migration(ctx=mock_ctx)

        self.assertIn("SUCCESS", out)
        runbook = self.mock_dm_runbook_blob.upload_from_string.call_args[0][0]
        self.assertIn("sessions", runbook)
        self.assertIn(caveat, runbook)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_report_racing_a_close_does_not_reopen_the_record(
            self, mock_gcs_client_class, mock_get_email):
        """Round 14 fixed the two EXTERNAL callers. `_report` and the listing
        still rendered from the state `_open` read at the top of the call, so
        a report whose own gate check passed before somebody else's close
        landed put the live document back over the terminal's record —
        against a UI caveat promising nothing rewrites it afterwards.

        The race is spelled with the state read flipping between the two: the
        tool's own read says the step is live, the redraw's says it has
        closed. That is exactly the window, and the only way to reach it."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        state_container, _, mock_ctx = self._at_data_migration(
            mock_gcs_client_class)
        state_blob = (main.state_mgr.gcs_client.bucket("test-bucket")
                      .blob("platform/onboarding/state.json"))
        live = json.dumps(state_container["state"])
        closed_state = json.dumps(dict(
            state_container["state"],
            current_state="STATE_DEPLOYMENT_COMPLETED"))
        reads = {"n": 0}

        def flips_after_the_gate_check(*_a, **_k):
            reads["n"] += 1
            return live if reads["n"] == 1 else closed_state

        state_blob.download_as_text.side_effect = flips_after_the_gate_check

        out = await t.mark_data_service_migrated(
            address="aws_db_instance.orders", target="cloudsql-orders",
            ctx=mock_ctx)

        # The outcome is recorded — it is durable and this call was legitimate
        # when it started — and the operator is told the artifact will not
        # show it. The tail does not then describe a gate that is gone.
        self.assertNotIn("ERROR", out)
        self.assertIn("closed the data migration step while this call", out)
        self.assertNotIn("still owe a move", out)
        self.assertNotIn("hold the step open", out)
        self.assertIn("no longer open", out)
        # The artifact at the terminal is not touched.
        self.mock_dm_runbook_blob.upload_from_string.assert_not_called()

        # Same for the listing, which writes the runbook directly — and it
        # must not announce the close and then hand out four calls that
        # refuse there, the invariant the runbook renderer already keeps.
        reads["n"] = 0
        listing = await t.list_data_migrations(ctx=mock_ctx)
        self.assertNotIn("ERROR", listing)
        self.assertIn("was not rewritten", listing)
        self.mock_dm_runbook_blob.upload_from_string.assert_not_called()
        self.assertIn("rds orders-db", listing)  # still named, as a record
        self.assertNotIn("    report it: ", listing)
        self.assertNotIn("    amend it: ", listing)
        self.assertNotIn("report with:", listing)
        self.assertNotIn("should not move after all", listing)
        self.assertIn("none of the tools this listing would normally offer",
                      listing)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_stale_caller_cannot_reopen_the_closed_record(
            self, mock_gcs_client_class, mock_get_email):
        """A caller rehydrates its state at the top of its own call and may
        hold it while another session closes the step. Gating the redraw on
        that cached copy restored the LIVE document over the record of what
        the close left behind — the banner gone, the reporting calls back and
        `keep-in-aws` re-offered at a terminal where it refuses. Round 13
        fixed that by ordering; this is the same thing through a stale read,
        so the freshness test belongs to the writer, not its callers."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        bucket = main.state_mgr.gcs_client.bucket("test-bucket")
        for address in ("aws_db_instance.orders", "aws_s3_bucket.exports"):
            await t.mark_data_service_migrated(address=address, ctx=mock_ctx)
        await t.complete_data_migration(ctx=mock_ctx)
        closed = self.mock_dm_runbook_blob.upload_from_string.call_args[0][0]
        self.assertIn("left the data migration step", closed)

        # The other session, still holding its pre-close view.
        warning = t.refresh_runbook_from_ledger(bucket)

        # Not silent: the outcome is durable but will never appear in the
        # terminal's artifact, and the operator has no reason to look.
        self.assertIn("closed the data migration step while this call", warning)
        self.assertEqual(
            self.mock_dm_runbook_blob.upload_from_string.call_args[0][0],
            closed)


    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    @patch("servers.phases.deployment.actions._default_session")
    async def test_settling_a_copy_redraws_the_worklist(
            self, mock_session_fn, mock_gcs_client_class, mock_get_email):
        """Round 12 put the image copies in the artifact so it and the gate
        would agree. The two tools that settle one never redrew it, so it went
        on listing a copy already reported — for as long as the databases take,
        with the instructions telling the agent not to re-list."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        mock_session_fn.return_value = self._ar_session()
        ref = "1234.dkr.ecr.us-east-1.amazonaws.com/api:v1"
        sibling = "1234.dkr.ecr.us-east-1.amazonaws.com/web:v1"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        (main.state_mgr.gcs_client.bucket("test-bucket")
         .blob(main.state_mgr.INVENTORY_BLOB_PATH)
         .upload_from_string(json.dumps(
             {"schema_version": "1.0",
              "data_dependencies": [dict(e) for e in self.DATA_ENTRIES],
              "images": [{"ref": r, "registry": "ecr", "repository": repo,
                          "tag": "v1", "pinned_by": "tag",
                          "provenance": [{"kind": "literal",
                                          "file": f"k8s/{repo}.yaml"}],
                          "replication": {"status": "self_service"}}
                         for r, repo in ((ref, "api"), (sibling, "web"))]})))
        await t.list_data_migrations(ctx=mock_ctx)
        self.assertIn(
            ref, self.mock_dm_runbook_blob.upload_from_string.call_args[0][0])

        await mark_replication_complete(refs=[ref], ctx=mock_ctx)

        runbook = self.mock_dm_runbook_blob.upload_from_string.call_args[0][0]
        self.assertNotIn(ref, runbook)
        # The sibling is the control: the redraw has to still RENDER the
        # copies, not just omit the settled one. Without it, a refresh that
        # dropped the image half entirely would pass the assertion above.
        self.assertIn(sibling, runbook)
        # And the gate agrees about both.
        close = await t.complete_data_migration(ctx=mock_ctx)
        self.assertNotIn(ref, close)
        self.assertIn(sibling, close)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    @patch("servers.phases.deployment.actions._default_session")
    async def test_abandoning_a_copy_redraws_the_worklist(
            self, mock_session_fn, mock_gcs_client_class, mock_get_email):
        """The same hole, reached the other way: an abandoned copy stayed in
        the artifact as work to do."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        mock_session_fn.return_value = self._ar_session()
        ref = "1234.dkr.ecr.us-east-1.amazonaws.com/api:v1"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        (main.state_mgr.gcs_client.bucket("test-bucket")
         .blob(main.state_mgr.INVENTORY_BLOB_PATH)
         .upload_from_string(json.dumps(
             {"schema_version": "1.0",
              "data_dependencies": [dict(e) for e in self.DATA_ENTRIES],
              "images": [{"ref": ref, "registry": "ecr", "repository": "api",
                          "tag": "v1", "pinned_by": "tag",
                          "provenance": [{"kind": "literal",
                                          "file": "k8s/api.yaml"}],
                          "replication": {"status": "self_service"}}]})))
        await t.list_data_migrations(ctx=mock_ctx)

        await abandon_image_replication(
            refs=[ref], reason="upstream base image is gone", ctx=mock_ctx)

        runbook = self.mock_dm_runbook_blob.upload_from_string.call_args[0][0]
        self.assertNotIn(ref, runbook)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_failed_close_leaves_no_it_has_left_claim(
            self, mock_gcs_client_class, mock_get_email):
        """The closed runbook asserts a fact about the STATE — "this workspace
        has left the data migration step" — so writing it before the state
        write left that claim in the ledger on every path where the write then
        failed, telling a review-UI reader the tools were dead while they
        still worked.

        The setup has to REACH the state write: a healthy baseline and
        everything owed reported. An earlier version of this test used a
        missing baseline, which now meets the damaged-ledger refusal and
        returns before the write, so all three of its assertions passed
        without the property being exercised at all."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        state_container, _, mock_ctx = self._at_data_migration(
            mock_gcs_client_class)
        for address in ("aws_db_instance.orders", "aws_s3_bucket.exports"):
            await t.mark_data_service_migrated(address=address, ctx=mock_ctx)
        state_blob = (main.state_mgr.gcs_client.bucket("test-bucket")
                      .blob("platform/onboarding/state.json"))
        writes_before = state_blob.upload_from_string.call_count
        state_blob.upload_from_string.side_effect = \
            exceptions.PreconditionFailed("generation mismatch")
        self.mock_dm_runbook_blob.upload_from_string.reset_mock()

        out = await t.complete_data_migration(ctx=mock_ctx)

        # The write was attempted — without this the rest is vacuous.
        self.assertGreater(state_blob.upload_from_string.call_count,
                           writes_before,
                           "the state write was never reached: " + out)
        self.assertIn("ERROR", out)
        self.assertIn("try again", out)
        self.assertEqual(state_container["state"]["current_state"],
                         "STATE_DEPLOYMENT_DATA_MIGRATION")
        # ...and no "has left the step" banner was written over a live step.
        self.mock_dm_runbook_blob.upload_from_string.assert_not_called()

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_clean_close_stops_instructing_too(
            self, mock_gcs_client_class, mock_get_email):
        """Round 12 fixed the caveated path only. The clean close is the path
        every successful migration takes, and it left an artifact at the
        terminal offering `keep-in-aws` — which refuses there — under a
        heading whose "these" named an empty list."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        for address in ("aws_db_instance.orders", "aws_s3_bucket.exports"):
            await t.mark_data_service_migrated(address=address, ctx=mock_ctx)

        out = await t.complete_data_migration(ctx=mock_ctx)

        self.assertIn("SUCCESS", out)
        runbook = self.mock_dm_runbook_blob.upload_from_string.call_args[0][0]
        self.assertIn("left the data migration step", runbook)
        self.assertIn("closed with nothing outstanding", runbook)
        self.assertNotIn("If one of these should not move after all", runbook)
        # ...and NOT the caveated reason, which never applied here.
        self.assertNotIn("scan baseline was absent", runbook)
        self.assertNotIn("Why these were not excused", runbook)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_closed_runbook_names_the_only_real_way_back(
            self, mock_gcs_client_class, mock_get_email):
        """It offered "re-run the deployment segment", which nothing can do:
        no writer sets a deployment state from the terminal, and the only
        reset is an admin reconfigure that restarts the platform walk."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        for address in ("aws_db_instance.orders", "aws_s3_bucket.exports"):
            await t.mark_data_service_migrated(address=address, ctx=mock_ctx)

        await t.complete_data_migration(ctx=mock_ctx)

        runbook = self.mock_dm_runbook_blob.upload_from_string.call_args[0][0]
        self.assertNotIn("re-run the deployment segment", runbook)
        self.assertIn("join_ledger --reconfigure", runbook)
        self.assertIn("restarts the platform walk", runbook)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_refresh_without_an_inventory_does_not_blank_the_artifact(
            self, mock_gcs_client_class, mock_get_email):
        """The outcome-store load is guarded and the inventory load was not,
        so a missing inventory would overwrite the state's primary artifact
        with "No data service is waiting to move"."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        state_container, _, _ = self._at_data_migration(mock_gcs_client_class)
        bucket = main.state_mgr.gcs_client.bucket("test-bucket")
        (bucket.blob(main.state_mgr.INVENTORY_BLOB_PATH)
         .download_as_text.side_effect) = exceptions.NotFound("Not Found")

        warning = t.refresh_runbook_from_ledger(bucket)

        self.assertIn("WARNING", warning)
        self.assertIn("inventory", warning)
        self.mock_dm_runbook_blob.upload_from_string.assert_not_called()


    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_report_under_an_unreadable_state_still_says_what_is_owed(
            self, mock_gcs_client_class, mock_get_email):
        """The report tail's closed-under-us arm compares against the exact
        warning; widening it to "any warning" left the suite green, and a
        report on a LIVE step whose second state read failed then answered
        "the step is no longer open" — a false claim about the workspace.
        The annotation half had a test; this half did not."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        state_container, _, mock_ctx = self._at_data_migration(
            mock_gcs_client_class)
        state_blob = (main.state_mgr.gcs_client.bucket("test-bucket")
                      .blob("platform/onboarding/state.json"))
        live = json.dumps(state_container["state"])
        reads = {"n": 0}

        def fails_after_the_gate_check(*_a, **_k):
            reads["n"] += 1
            if reads["n"] == 1:
                return live
            raise exceptions.ServiceUnavailable("backend error")

        state_blob.download_as_text.side_effect = fails_after_the_gate_check

        out = await t.mark_data_service_migrated(
            address="aws_db_instance.orders", ctx=mock_ctx)

        self.assertIn("could not be read", out)
        # The step is LIVE: the forward-looking sentence is the true one.
        self.assertIn("still owe a move", out)
        self.assertNotIn("no longer open", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_an_unreadable_state_keeps_the_lost_note_instruction(
            self, mock_gcs_client_class, mock_get_email):
        """The CLOSED direction is tested below; this is the other one. A
        failed state read leaves the step LIVE and the tool working, so
        withholding "pass note=" costs the operator the one thing that would
        let them keep the caveat they just lost."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        state_container, _, mock_ctx = self._at_data_migration(
            mock_gcs_client_class)
        await t.mark_data_service_migrating(
            address="aws_db_instance.orders", note="DMS at 60%", ctx=mock_ctx)
        state_blob = (main.state_mgr.gcs_client.bucket("test-bucket")
                      .blob("platform/onboarding/state.json"))
        live = json.dumps(state_container["state"])
        reads = {"n": 0}

        def fails_after_the_gate_check(*_a, **_k):
            reads["n"] += 1
            if reads["n"] == 1:
                return live
            raise exceptions.ServiceUnavailable("backend error")

        state_blob.download_as_text.side_effect = fails_after_the_gate_check

        out = await t.mark_data_service_migrated(
            address="aws_db_instance.orders", ctx=mock_ctx)

        self.assertIn("could not be read", out)
        self.assertIn("was not carried onto this one", out)
        self.assertIn("Pass note=", out)
        self.assertNotIn("no longer open", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    @patch("servers.phases.deployment.actions._default_session")
    async def test_an_unreadable_state_keeps_the_close_line(
            self, mock_session_fn, mock_gcs_client_class, mock_get_email):
        """The digest tip five lines below carries the same
        `!= _closed_under_us()` gate and IS pinned in this direction. These two
        were pinned for no-warning (round 37) and for CLOSED (round 38), but
        not for the third outcome: a warning that is not CLOSED. A failed state
        read is not evidence the step closed, so narrowing either gate to `not
        worklist_warning` would leave the operator who just settled the last
        copy told nothing, with the instructions handing control straight
        back — round 36 finding 2's harm. The existing unreadable-state test
        cannot see it: its fixture owes two data services, so the line is
        structurally unreachable there."""
        mock_get_email.return_value = "platform-user@google.com"
        mock_session_fn.return_value = self._ar_session()
        ref = "1234.dkr.ecr.us-east-1.amazonaws.com/api:v1"
        for settle in ("confirm", "abandon"):
            state_container, _, mock_ctx = self._at_data_migration(
                mock_gcs_client_class, entries=[])
            (main.state_mgr.gcs_client.bucket("test-bucket")
             .blob(main.state_mgr.INVENTORY_BLOB_PATH)
             .upload_from_string(json.dumps(
                 {"schema_version": "1.0", "data_dependencies": [],
                  "images": [{"ref": ref, "registry": "ecr",
                              "repository": "api", "tag": "v1",
                              "pinned_by": "tag",
                              "provenance": [{"kind": "literal",
                                              "file": "k8s/api.yaml"}],
                              "replication": {"status": "self_service"}}]})))
            state_blob = (main.state_mgr.gcs_client.bucket("test-bucket")
                          .blob("platform/onboarding/state.json"))
            live = json.dumps(state_container["state"])
            reads = {"n": 0}

            def fails_after_the_gate_check(*_a, **_k):
                reads["n"] += 1
                if reads["n"] == 1:
                    return live
                raise exceptions.ServiceUnavailable("backend error")

            state_blob.download_as_text.side_effect = (
                fails_after_the_gate_check)

            out = (await mark_replication_complete(refs=[ref], ctx=mock_ctx)
                   if settle == "confirm" else
                   await abandon_image_replication(
                       refs=[ref], reason="upstream gone", ctx=mock_ctx))

            with self.subTest(settle=settle):
                self.assertIn("could not be read", out)
                self.assertNotIn("closed the data migration step", out)
                self.assertIn("Nothing is outstanding now", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    @patch("servers.phases.deployment.actions._default_session")
    async def test_an_unreadable_state_keeps_the_digest_tip(
            self, mock_session_fn, mock_gcs_client_class, mock_get_email):
        """The other direction of the digest gate, and the one where the
        marker crosses a module boundary through `_closed_under_us()`. Two
        non-closed warnings reach here — a failed state read and a failed
        runbook save — and neither means the tool has stopped working."""
        mock_get_email.return_value = "platform-user@google.com"
        mock_session_fn.return_value = self._ar_session()
        state_container, _, mock_ctx = self._at_data_migration(
            mock_gcs_client_class)
        (main.state_mgr.gcs_client.bucket("test-bucket")
         .blob(main.state_mgr.INVENTORY_BLOB_PATH)
         .upload_from_string(json.dumps(self._with_failed_copy("self_service"))))
        state_blob = (main.state_mgr.gcs_client.bucket("test-bucket")
                      .blob("platform/onboarding/state.json"))
        live = json.dumps(state_container["state"])
        reads = {"n": 0}

        def fails_after_the_gate_check(*_a, **_k):
            reads["n"] += 1
            if reads["n"] == 1:
                return live
            raise exceptions.ServiceUnavailable("backend error")

        state_blob.download_as_text.side_effect = fails_after_the_gate_check

        out = await mark_replication_complete(ctx=mock_ctx)

        self.assertIn("Marked 1 image(s) replicated", out)
        self.assertIn("could not be read", out)
        self.assertIn("re-call with digests", out)
        self.assertNotIn("closed the data migration step", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_close_is_not_offered_over_an_unconfirmed_copy(
            self, mock_gcs_client_class, mock_get_email):
        """Round 36's footer is keyed on both populations. Keyed on the data
        alone it would print "Nothing is outstanding" six lines under an
        image copy it had just listed, handing the agent a call the close
        refuses naming that ref — round 26 finding 1 recreated in the line
        that fixed round 36 finding 2."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        ref = "1234.dkr.ecr.us-east-1.amazonaws.com/api:v1"
        _, _, mock_ctx = self._at_data_migration(
            mock_gcs_client_class, entries=[])
        (main.state_mgr.gcs_client.bucket("test-bucket")
         .blob(main.state_mgr.INVENTORY_BLOB_PATH)
         .upload_from_string(json.dumps(
             {"schema_version": "1.0", "data_dependencies": [],
              "images": [{"ref": ref, "registry": "ecr", "repository": "api",
                          "tag": "v1", "pinned_by": "tag",
                          "provenance": [{"kind": "literal",
                                          "file": "k8s/api.yaml"}],
                          "replication": {"status": "self_service"}}]})))

        out = await t.list_data_migrations(ctx=mock_ctx)

        self.assertIn(ref, out)
        self.assertNotIn("Nothing is outstanding", out)
        # ...and the close agrees, naming the ref.
        self.assertIn(ref, await t.complete_data_migration(ctx=mock_ctx))

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_closed_listing_does_not_offer_the_close(
            self, mock_gcs_client_class, mock_get_email):
        """The other gate on the same line. The only closed-listing test uses
        the default fixture, which owes two services — so `not owed` is False
        and this footer is unreachable there, leaving the newest member of
        the "a closed listing does not instruct" family unpinned."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        state_container, _, mock_ctx = self._at_data_migration(
            mock_gcs_client_class, entries=[])
        state_blob = (main.state_mgr.gcs_client.bucket("test-bucket")
                      .blob("platform/onboarding/state.json"))
        live = json.dumps(state_container["state"])
        closed_state = json.dumps(dict(
            state_container["state"],
            current_state="STATE_DEPLOYMENT_COMPLETED"))
        reads = {"n": 0}

        def flips_after_the_gate_check(*_a, **_k):
            reads["n"] += 1
            return live if reads["n"] == 1 else closed_state

        state_blob.download_as_text.side_effect = flips_after_the_gate_check

        out = await t.list_data_migrations(ctx=mock_ctx)

        self.assertIn("none of the tools this listing would normally offer",
                      out)
        self.assertNotIn("Nothing is outstanding", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    @patch("servers.phases.deployment.actions._default_session")
    async def test_settling_the_last_copy_names_the_close(
            self, mock_session_fn, mock_gcs_client_class, mock_get_email):
        """An image copy can be the LAST thing holding the step open. Round
        36 named the close in the data half only, so the tool that actually
        opened the gate said nothing — and the instructions tell the agent to
        hand back control and not re-list."""
        mock_get_email.return_value = "platform-user@google.com"
        mock_session_fn.return_value = self._ar_session()
        ref = "1234.dkr.ecr.us-east-1.amazonaws.com/api:v1"
        for settle in ("confirm", "abandon"):
            _, _, mock_ctx = self._at_data_migration(
                mock_gcs_client_class, entries=[])
            (main.state_mgr.gcs_client.bucket("test-bucket")
             .blob(main.state_mgr.INVENTORY_BLOB_PATH)
             .upload_from_string(json.dumps(
                 {"schema_version": "1.0", "data_dependencies": [],
                  "images": [{"ref": ref, "registry": "ecr",
                              "repository": "api", "tag": "v1",
                              "pinned_by": "tag",
                              "provenance": [{"kind": "literal",
                                              "file": "k8s/api.yaml"}],
                              "replication": {"status": "self_service"}}]})))

            out = (await mark_replication_complete(refs=[ref], ctx=mock_ctx)
                   if settle == "confirm" else
                   await abandon_image_replication(
                       refs=[ref], reason="upstream gone", ctx=mock_ctx))

            with self.subTest(settle=settle):
                self.assertNotIn("ERROR", out)
                self.assertIn("Nothing is outstanding now", out)
                self.assertIn("complete_data_migration()", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    @patch("servers.phases.deployment.actions._default_session")
    async def test_a_corrupt_outcome_store_withholds_the_close_line(
            self, mock_session_fn, mock_gcs_client_class, mock_get_email):
        """`_nothing_left_at_the_step` answers False when either object comes
        back unusable, and both failure arms were unpinned. The reachable one
        is a corrupt `data_migrations.json`: `load_migrations` returns
        (None, None) there, and the settle tool already warns the runbook could
        not be refreshed for that reason — so with the arm flipped the same
        response says the step can close, one line under that warning, and the
        close then refuses because the store is unreadable. A transport failure
        on the same object takes the `except` arm instead: `load_migrations`
        lets out anything that is neither NotFound nor a parse error."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        mock_session_fn.return_value = self._ar_session()
        ref = "1234.dkr.ecr.us-east-1.amazonaws.com/api:v1"
        for settle, damage in (("confirm", "corrupt"), ("abandon", "corrupt"),
                               ("confirm", "unreachable"),
                               ("abandon", "unreachable")):
            _, _, mock_ctx = self._at_data_migration(
                mock_gcs_client_class, entries=[])
            (main.state_mgr.gcs_client.bucket("test-bucket")
             .blob(main.state_mgr.INVENTORY_BLOB_PATH)
             .upload_from_string(json.dumps(
                 {"schema_version": "1.0", "data_dependencies": [],
                  "images": [{"ref": ref, "registry": "ecr",
                              "repository": "api", "tag": "v1",
                              "pinned_by": "tag",
                              "provenance": [{"kind": "literal",
                                              "file": "k8s/api.yaml"}],
                              "replication": {"status": "self_service"}}]})))
            store = (main.state_mgr.gcs_client.bucket("test-bucket")
                     .blob(datamigration_lib.MIGRATIONS_BLOB))
            if damage == "corrupt":
                store.upload_from_string("not json at all")
            else:
                store.reload.side_effect = exceptions.ServiceUnavailable(
                    "backend error")

            out = (await mark_replication_complete(refs=[ref], ctx=mock_ctx)
                   if settle == "confirm" else
                   await abandon_image_replication(
                       refs=[ref], reason="upstream gone", ctx=mock_ctx))

            with self.subTest(settle=settle, damage=damage):
                self.assertNotIn("Nothing is outstanding", out)
                # ...because the close does not agree that nothing is left.
                self.assertIn("ERROR",
                              await t.complete_data_migration(ctx=mock_ctx))

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    @patch("servers.phases.deployment.actions._default_session")
    async def test_settling_a_copy_with_databases_owed_does_not_name_the_close(
            self, mock_session_fn, mock_gcs_client_class, mock_get_email):
        """`_nothing_left_at_the_step` reads BOTH populations. Both tests that
        touch it used `entries=[]`, so its data half did no work — and without
        it an operator settling a copy is told the step can close while two
        databases are owed, which the very next close refuses naming."""
        mock_get_email.return_value = "platform-user@google.com"
        mock_session_fn.return_value = self._ar_session()
        ref = "1234.dkr.ecr.us-east-1.amazonaws.com/api:v1"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        (main.state_mgr.gcs_client.bucket("test-bucket")
         .blob(main.state_mgr.INVENTORY_BLOB_PATH)
         .upload_from_string(json.dumps(
             {"schema_version": "1.0",
              "data_dependencies": [dict(e) for e in self.DATA_ENTRIES],
              "images": [{"ref": ref, "registry": "ecr", "repository": "api",
                          "tag": "v1", "pinned_by": "tag",
                          "provenance": [{"kind": "literal",
                                          "file": "k8s/api.yaml"}],
                          "replication": {"status": "self_service"}}]})))

        out = await abandon_image_replication(
            refs=[ref], reason="upstream gone", ctx=mock_ctx)

        self.assertNotIn("ERROR", out)
        self.assertNotIn("Nothing is outstanding", out)
        # ...and the close agrees, naming the databases.
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        self.assertIn("still owe a move",
                      await t.complete_data_migration(ctx=mock_ctx))

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    @patch("servers.phases.deployment.actions._default_session")
    async def test_a_copy_settled_under_a_close_does_not_name_the_close(
            self, mock_session_fn, mock_gcs_client_class, mock_get_email):
        """Both new lines are gated on the closed-under-us warning, and
        neither gate was pinned — so the response could say the step is gone
        and then hand the agent a call that refuses at the terminal, two
        consecutive lines apart."""
        mock_get_email.return_value = "platform-user@google.com"
        mock_session_fn.return_value = self._ar_session()
        ref = "1234.dkr.ecr.us-east-1.amazonaws.com/api:v1"
        for settle in ("confirm", "abandon"):
            state_container, _, mock_ctx = self._at_data_migration(
                mock_gcs_client_class, entries=[])
            (main.state_mgr.gcs_client.bucket("test-bucket")
             .blob(main.state_mgr.INVENTORY_BLOB_PATH)
             .upload_from_string(json.dumps(
                 {"schema_version": "1.0", "data_dependencies": [],
                  "images": [{"ref": ref, "registry": "ecr",
                              "repository": "api", "tag": "v1",
                              "pinned_by": "tag",
                              "provenance": [{"kind": "literal",
                                              "file": "k8s/api.yaml"}],
                              "replication": {"status": "self_service"}}]})))
            state_blob = (main.state_mgr.gcs_client.bucket("test-bucket")
                          .blob("platform/onboarding/state.json"))
            live = json.dumps(state_container["state"])
            closed_state = json.dumps(dict(
                state_container["state"],
                current_state="STATE_DEPLOYMENT_COMPLETED"))
            reads = {"n": 0}

            def flips_after_the_gate_check(*_a, **_k):
                reads["n"] += 1
                return live if reads["n"] == 1 else closed_state

            state_blob.download_as_text.side_effect = flips_after_the_gate_check

            out = (await mark_replication_complete(refs=[ref], ctx=mock_ctx)
                   if settle == "confirm" else
                   await abandon_image_replication(
                       refs=[ref], reason="upstream gone", ctx=mock_ctx))

            with self.subTest(settle=settle):
                self.assertIn("closed the data migration step while this call",
                              out)
                self.assertNotIn("Nothing is outstanding", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    @patch("servers.phases.deployment.actions._default_session")
    async def test_settling_one_of_two_copies_does_not_name_the_close(
            self, mock_session_fn, mock_gcs_client_class, mock_get_email):
        """The control: the sentence is a claim about the whole step, not
        about the call."""
        mock_get_email.return_value = "platform-user@google.com"
        mock_session_fn.return_value = self._ar_session()
        first = "1234.dkr.ecr.us-east-1.amazonaws.com/api:v1"
        second = "1234.dkr.ecr.us-east-1.amazonaws.com/web:v1"
        _, _, mock_ctx = self._at_data_migration(
            mock_gcs_client_class, entries=[])
        (main.state_mgr.gcs_client.bucket("test-bucket")
         .blob(main.state_mgr.INVENTORY_BLOB_PATH)
         .upload_from_string(json.dumps(
             {"schema_version": "1.0", "data_dependencies": [],
              "images": [{"ref": r, "registry": "ecr", "repository": repo,
                          "tag": "v1", "pinned_by": "tag",
                          "provenance": [{"kind": "literal",
                                          "file": f"k8s/{repo}.yaml"}],
                          "replication": {"status": "self_service"}}
                         for r, repo in ((first, "api"), (second, "web"))]})))

        out = await abandon_image_replication(
            refs=[first], reason="upstream gone", ctx=mock_ctx)

        self.assertNotIn("ERROR", out)
        self.assertNotIn("Nothing is outstanding", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_settled_estate_is_told_how_to_close(
            self, mock_gcs_client_class, mock_get_email):
        """Nothing in this step's own output named `complete_data_migration`.
        Settle the last service with `annotate_data_dependency` and the tail
        said the close would proceed; settle the identical state with
        `mark_data_service_migrated` and the operator was told nothing — while
        the instructions tell the agent to hand back control and propose
        nothing. An estate with no `migrate` service and an agent-mediated
        replication meets that on arrival, and the walk parks for good."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        await t.mark_data_service_migrated(
            address="aws_db_instance.orders", ctx=mock_ctx)

        # The last owed service, settled: the gate has just opened.
        out = await t.mark_data_service_migrated(
            address="aws_s3_bucket.exports", ctx=mock_ctx)

        self.assertIn("Nothing else is owed", out)
        self.assertIn("complete_data_migration()", out)

        # And a listing over an estate that was never owed anything says so
        # too, rather than leaving the agent with nothing to act on.
        _, _, mock_ctx = self._at_data_migration(
            mock_gcs_client_class,
            entries=[{"service": "elasticache", "identifier": "sessions",
                      "address": "aws_elasticache_cluster.sessions",
                      "disposition": "rebuild",
                      "evidence": ["envs/prod/data.tf"], "notes": [],
                      "consumers": []}])
        listing = await t.list_data_migrations(ctx=mock_ctx)

        self.assertIn("Nothing is outstanding", listing)
        self.assertIn("complete_data_migration()", listing)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    @patch("servers.phases.deployment.actions._default_session")
    async def test_the_abandon_response_reports_what_it_did(
            self, mock_session_fn, mock_gcs_client_class, mock_get_email):
        """Three of the response's four lines could be deleted with the whole
        suite green, including round 28's worklist warning and the only place
        a failed exports republish surfaces. The sibling tool has both halves
        covered; this one — the CL's only new inventory-mutating tool — had
        one of four."""
        mock_get_email.return_value = "platform-user@google.com"
        mock_session_fn.return_value = self._ar_session()
        ref = "1234.dkr.ecr.us-east-1.amazonaws.com/api:v1"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        (main.state_mgr.gcs_client.bucket("test-bucket")
         .blob(main.state_mgr.INVENTORY_BLOB_PATH)
         .upload_from_string(json.dumps(self._with_failed_copy())))

        out = await abandon_image_replication(
            refs=[ref], reason="upstream base image is gone", ctx=mock_ctx)

        # It says what it did, echoes the required reason, and confirms the
        # republish that stops translation asking anyone to finish the copy.
        self.assertIn("1 image(s) will not be copied", out)
        self.assertIn(ref, out)
        self.assertIn("upstream base image is gone", out)
        self.assertIn("Exports refreshed", out)
        self.assertIn("stay in ECR", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    @patch("servers.phases.deployment.actions._default_session")
    async def test_a_failed_republish_on_abandon_is_reported(
            self, mock_session_fn, mock_gcs_client_class, mock_get_email):
        """The only place a failed `publish_completion_exports` surfaces. An
        abandonment that does not reach `image_map` leaves workload
        translation telling developers to finish a cancelled copy — round 1
        finding 4 — and this line is what says so."""
        mock_get_email.return_value = "platform-user@google.com"
        mock_session_fn.return_value = self._ar_session()
        ref = "1234.dkr.ecr.us-east-1.amazonaws.com/api:v1"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        (main.state_mgr.gcs_client.bucket("test-bucket")
         .blob(main.state_mgr.INVENTORY_BLOB_PATH)
         .upload_from_string(json.dumps(self._with_failed_copy())))
        (main.state_mgr.gcs_client.bucket("test-bucket")
         .blob(exports_lib.EXPORTS_BLOB)
         .upload_from_string.side_effect) = exceptions.ServiceUnavailable(
             "backend error")

        out = await abandon_image_replication(
            refs=[ref], reason="upstream gone", ctx=mock_ctx)

        self.assertIn("1 image(s) will not be copied", out)
        self.assertNotIn("Exports refreshed", out)
        self.assertIn("exports", out.lower())

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_closed_listing_does_not_promise_an_in_flight_report(
            self, mock_gcs_client_class, mock_get_email):
        """The last untested member of the family round 18, 31 and 32 each
        caught elsewhere: "report it when it lands" is a promise about the
        future, three lines above a footer saying nothing can be called. The
        runbook half has two tests; the chat listing had none, because no
        closed-listing test built an in-flight-not-owed entry."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        state_container, _, mock_ctx = self._at_data_migration(
            mock_gcs_client_class)
        self._install_overrides_roundtrip()
        await t.mark_data_service_migrating(
            address="aws_db_instance.orders", note="DMS at 60%", ctx=mock_ctx)
        await discovery_datareview_3.tools.annotate_data_dependency(
            address="aws_db_instance.orders", disposition="keep-in-aws",
            note="licence", ctx=mock_ctx)
        state_blob = (main.state_mgr.gcs_client.bucket("test-bucket")
                      .blob("platform/onboarding/state.json"))
        live = json.dumps(state_container["state"])
        closed_state = json.dumps(dict(
            state_container["state"],
            current_state="STATE_DEPLOYMENT_COMPLETED"))
        reads = {"n": 0}

        def flips_after_the_gate_check(*_a, **_k):
            reads["n"] += 1
            return live if reads["n"] == 1 else closed_state

        state_blob.download_as_text.side_effect = flips_after_the_gate_check

        out = await t.list_data_migrations(ctx=mock_ctx)

        # The in-flight record is still NAMED — the listing is a record now.
        self.assertIn("does not gate", out)
        self.assertIn("DMS at 60%", out)
        self.assertNotIn("report it when it lands", out)
        self.assertIn("none of the tools this listing would normally offer",
                      out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_closed_report_drops_its_re_call_instruction(
            self, mock_gcs_client_class, mock_get_email):
        """"Pass note= to record one" is an instruction to call a tool that
        refuses once the step has closed under the call."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        state_container, _, mock_ctx = self._at_data_migration(
            mock_gcs_client_class)
        await t.mark_data_service_migrating(
            address="aws_db_instance.orders", note="DMS at 60%", ctx=mock_ctx)
        state_blob = (main.state_mgr.gcs_client.bucket("test-bucket")
                      .blob("platform/onboarding/state.json"))
        live = json.dumps(state_container["state"])
        closed_state = json.dumps(dict(
            state_container["state"],
            current_state="STATE_DEPLOYMENT_COMPLETED"))
        reads = {"n": 0}

        def flips_after_the_gate_check(*_a, **_k):
            reads["n"] += 1
            return live if reads["n"] == 1 else closed_state

        state_blob.download_as_text.side_effect = flips_after_the_gate_check

        out = await t.mark_data_service_migrated(
            address="aws_db_instance.orders", ctx=mock_ctx)

        self.assertIn("was not carried onto this one", out)
        self.assertIn("no longer open", out)
        self.assertNotIn("Pass note=", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    @patch("servers.phases.deployment.actions._default_session")
    async def test_a_closed_confirmation_drops_the_digest_tip(
            self, mock_session_fn, mock_gcs_client_class, mock_get_email):
        """Same class on the image half: "re-call with digests=" names a tool
        that refuses at the terminal, printed under the warning saying so."""
        mock_get_email.return_value = "platform-user@google.com"
        mock_session_fn.return_value = self._ar_session()
        state_container, _, mock_ctx = self._at_data_migration(
            mock_gcs_client_class)
        (main.state_mgr.gcs_client.bucket("test-bucket")
         .blob(main.state_mgr.INVENTORY_BLOB_PATH)
         .upload_from_string(json.dumps(self._with_failed_copy("self_service"))))
        state_blob = (main.state_mgr.gcs_client.bucket("test-bucket")
                      .blob("platform/onboarding/state.json"))
        live = json.dumps(state_container["state"])
        closed_state = json.dumps(dict(
            state_container["state"],
            current_state="STATE_DEPLOYMENT_COMPLETED"))
        reads = {"n": 0}

        def flips_after_the_gate_check(*_a, **_k):
            reads["n"] += 1
            return live if reads["n"] == 1 else closed_state

        state_blob.download_as_text.side_effect = flips_after_the_gate_check

        out = await mark_replication_complete(ctx=mock_ctx)

        self.assertIn("Marked 1 image(s) replicated", out)
        self.assertIn("closed the data migration step while this call", out)
        self.assertNotIn("re-call with digests", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_closed_listing_header_is_past_tense(
            self, mock_gcs_client_class, mock_get_email):
        """The first line the agent relays. "N still owed" is a claim about a
        gate that no longer exists, under a footer saying the step closed."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        state_container, _, mock_ctx = self._at_data_migration(
            mock_gcs_client_class)
        state_blob = (main.state_mgr.gcs_client.bucket("test-bucket")
                      .blob("platform/onboarding/state.json"))
        live = json.dumps(state_container["state"])
        closed_state = json.dumps(dict(
            state_container["state"],
            current_state="STATE_DEPLOYMENT_COMPLETED"))
        reads = {"n": 0}

        def flips_after_the_gate_check(*_a, **_k):
            reads["n"] += 1
            return live if reads["n"] == 1 else closed_state

        state_blob.download_as_text.side_effect = flips_after_the_gate_check

        out = await t.list_data_migrations(ctx=mock_ctx)

        self.assertIn("as this session saw them", out)
        self.assertIn("2 unreported", out)
        self.assertNotIn("still owed", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_closed_listing_does_not_instruct_about_copies_either(
            self, mock_gcs_client_class, mock_get_email):
        """Round 31 gated the `report with:` line under the image paragraph
        and left the paragraph, which says "Run them and report with
        mark_replication_complete" three lines above a footer saying nothing
        can be called. No test built a closed listing with a copy in it."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        ref = "1234.dkr.ecr.us-east-1.amazonaws.com/api:v1"
        state_container, _, mock_ctx = self._at_data_migration(
            mock_gcs_client_class)
        (main.state_mgr.gcs_client.bucket("test-bucket")
         .blob(main.state_mgr.INVENTORY_BLOB_PATH)
         .upload_from_string(json.dumps(
             {"schema_version": "1.0",
              "data_dependencies": [dict(e) for e in self.DATA_ENTRIES],
              "images": [{"ref": ref, "registry": "ecr", "repository": "api",
                          "tag": "v1", "pinned_by": "tag",
                          "provenance": [{"kind": "literal",
                                          "file": "k8s/api.yaml"}],
                          "replication": {"status": "self_service"}}]})))
        state_blob = (main.state_mgr.gcs_client.bucket("test-bucket")
                      .blob("platform/onboarding/state.json"))
        live = json.dumps(state_container["state"])
        closed_state = json.dumps(dict(
            state_container["state"],
            current_state="STATE_DEPLOYMENT_COMPLETED"))
        reads = {"n": 0}

        def flips_after_the_gate_check(*_a, **_k):
            reads["n"] += 1
            return live if reads["n"] == 1 else closed_state

        state_blob.download_as_text.side_effect = flips_after_the_gate_check

        out = await t.list_data_migrations(ctx=mock_ctx)

        self.assertIn(ref, out)  # still named, as a record
        self.assertNotIn("Run them and report with", out)
        self.assertNotIn("report with:", out)
        self.assertIn("as this session saw it", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_last_report_does_not_say_nothing_is_owed_over_a_copy(
            self, mock_gcs_client_class, mock_get_email):
        """The close waits on two populations; this sentence was keyed on
        one. Reporting the last database said "Nothing else is owed" and the
        very next close refused, naming an image copy — the Acme fixture's
        exact shape, relayed verbatim."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        ref = "1234.dkr.ecr.us-east-1.amazonaws.com/api:v1"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        (main.state_mgr.gcs_client.bucket("test-bucket")
         .blob(main.state_mgr.INVENTORY_BLOB_PATH)
         .upload_from_string(json.dumps(
             {"schema_version": "1.0",
              "data_dependencies": [dict(e) for e in self.DATA_ENTRIES],
              "images": [{"ref": ref, "registry": "ecr", "repository": "api",
                          "tag": "v1", "pinned_by": "tag",
                          "provenance": [{"kind": "literal",
                                          "file": "k8s/api.yaml"}],
                          "replication": {"status": "self_service"}}]})))
        await t.mark_data_service_migrated(
            address="aws_db_instance.orders", ctx=mock_ctx)

        out = await t.mark_data_service_migrated(
            address="aws_s3_bucket.exports", ctx=mock_ctx)

        self.assertNotIn("Nothing else is owed", out)
        self.assertIn("No data service is still owed", out)
        self.assertIn(ref, out)
        # ...and what it says is what the close does.
        self.assertIn(ref, await t.complete_data_migration(ctx=mock_ctx))

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_worklist_artifact_carries_the_image_half_too(
            self, mock_gcs_client_class, mock_get_email):
        """The step's PRIMARY artifact is labelled "what still has to move".
        With nothing owed and a copy unconfirmed it said "No data service is
        waiting to move" and nothing else — at a state that will not close."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        ref = "1234.dkr.ecr.us-east-1.amazonaws.com/api:v1"
        entries = [{"service": "elasticache", "identifier": "sessions",
                    "address": "aws_elasticache_cluster.sessions",
                    "disposition": "rebuild",
                    "evidence": ["envs/prod/data.tf"], "notes": [],
                    "consumers": []}]
        _, _, mock_ctx = self._at_data_migration(
            mock_gcs_client_class, entries)
        (main.state_mgr.gcs_client.bucket("test-bucket")
         .blob(main.state_mgr.INVENTORY_BLOB_PATH)
         .upload_from_string(json.dumps(
             {"schema_version": "1.0", "data_dependencies": entries,
              "images": [{"ref": ref, "registry": "ecr", "repository": "api",
                          "tag": "v1", "pinned_by": "tag",
                          "provenance": [{"kind": "literal",
                                          "file": "k8s/api.yaml"}],
                          "replication": {"status": "self_service"}}]})))

        await t.list_data_migrations(ctx=mock_ctx)

        runbook = self.mock_dm_runbook_blob.upload_from_string.call_args[0][0]
        self.assertIn(ref, runbook)
        self.assertIn("also unconfirmed", runbook)
        self.assertIn("abandon_image_replication", runbook)
        # The step really does refuse to close on it, so the artifact and the
        # gate now agree.
        self.assertIn("ERROR", await t.complete_data_migration(ctx=mock_ctx))

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_failed_reload_for_the_correction_refresh_is_reported(
            self, mock_gcs_client_class, mock_get_email):
        """The correction is durable either way, so this cannot be an error —
        but silence leaves the operator reading a worklist that no longer
        matches what they just recorded, with no sign it is stale."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        self._install_overrides_roundtrip()
        self.mock_migrations_blob.reload.side_effect = \
            exceptions.ServiceUnavailable("backend error")

        out = await discovery_datareview_3.tools.annotate_data_dependency(
            address="aws_db_instance.orders", disposition="keep-in-aws",
            note="not movable", ctx=mock_ctx)

        self.assertNotIn("ERROR", out)
        self.assertIn("Annotation recorded", out)
        self.assertIn("WARNING", out)
        self.assertIn(datamigration_lib.RUNBOOK_BLOB, out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_an_unreadable_outcome_store_does_not_redraw_from_nothing(
            self, mock_gcs_client_class, mock_get_email):
        """`load_migrations` returns (None, None) for an object that exists
        and is not a record set. Rendering the worklist from that reports
        every service as unstarted, in the artifact that outlives the
        session."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        self._install_overrides_roundtrip()
        self.mock_migrations_blob.reload.side_effect = None
        self.mock_migrations_blob.download_as_text.side_effect = None
        self.mock_migrations_blob.download_as_text.return_value = "[]"

        out = await discovery_datareview_3.tools.annotate_data_dependency(
            address="aws_db_instance.orders", disposition="keep-in-aws",
            note="not movable", ctx=mock_ctx)

        self.assertNotIn("ERROR", out)
        self.assertIn("WARNING", out)
        self.assertIn(datamigration_lib.MIGRATIONS_BLOB, out)
        self.mock_dm_runbook_blob.upload_from_string.assert_not_called()


    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_failed_refresh_at_the_close_is_reported(
            self, mock_gcs_client_class, mock_get_email):
        """The refresh exists so the terminal artifact's "written as it stood
        when the step closed" stays true, and a silent failure is the one case
        that makes it false again — with nothing callable at the terminal to
        correct it."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        for address in ("aws_db_instance.orders", "aws_s3_bucket.exports"):
            await t.mark_data_service_migrated(address=address, ctx=mock_ctx)
        self.mock_dm_runbook_blob.upload_from_string.side_effect = \
            exceptions.ServiceUnavailable("backend error")

        out = await t.complete_data_migration(ctx=mock_ctx)

        self.assertIn("SUCCESS", out)
        self.assertIn(datamigration_lib.RUNBOOK_BLOB, out)
        self.assertIn("could not be", out.lower())

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_damaged_ledger_refusal_names_every_copy(
            self, mock_gcs_client_class, mock_get_email):
        """The refusal names both halves in one message rather than leaving
        the images for a second one the operator was not warned about, and
        `abandon_image_replication` needs each ref spelled."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        images = [{"ref": f"1234.dkr.ecr.us-east-1.amazonaws.com/svc{i}:v1",
                   "registry": "ecr", "repository": f"svc{i}", "tag": "v1",
                   "pinned_by": "tag",
                   "provenance": [{"kind": "literal", "file": "k8s/a.yaml"}],
                   "replication": {"status": "self_service"}}
                  for i in range(8)]
        _, _, mock_ctx = self._at_data_migration(
            mock_gcs_client_class, with_baseline=False)
        (main.state_mgr.gcs_client.bucket("test-bucket")
         .blob(discovery_datareview_3.tools.SCAN_BASELINE_BLOB)
         .download_as_text.side_effect) = exceptions.NotFound("Not Found")
        (main.state_mgr.gcs_client.bucket("test-bucket")
         .blob(main.state_mgr.INVENTORY_BLOB_PATH)
         .upload_from_string(json.dumps(
             {"schema_version": "1.0",
              "data_dependencies": [dict(e) for e in self.DATA_ENTRIES],
              "images": images})))

        out = await t.complete_data_migration(ctx=mock_ctx)

        self.assertIn("abandon_image_replication", out)
        for image in images:
            self.assertIn(image["ref"], out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_an_excuse_over_a_corrupt_store_does_not_promise_the_close(
            self, mock_gcs_client_class, mock_get_email):
        """The `keep-in-aws` arm reads the same store as the `migrate` arms
        and made the same two claims from it: that the close will proceed
        (it refuses on a corrupt store first), and that no move is in flight
        (which an unreadable store cannot say). Reachable because
        `_open_review` never loads the outcome store, so the annotation itself
        succeeds."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        self._install_overrides_roundtrip()
        self.mock_migrations_blob.reload.side_effect = None
        self.mock_migrations_blob.download_as_text.side_effect = None
        self.mock_migrations_blob.download_as_text.return_value = "[]"

        out = await discovery_datareview_3.tools.annotate_data_dependency(
            address="aws_db_instance.orders", disposition="keep-in-aws",
            note="licence", ctx=mock_ctx)

        self.assertIn("Annotation recorded", out)
        self.assertIn("repair or remove that object", out)
        self.assertNotIn("will close once nothing else", out)
        self.assertIn("cannot be told from here", out)
        # All four of `_open`'s callers, as round 29 fixed in the `migrate`
        # arm and did not carry here. "Cannot be told from here" sends the
        # operator to `list_data_migrations()` next, so naming only the close
        # walks them into an unannounced refusal from the message standing in
        # front of it.
        self.assertIn("complete_data_migration(), list_data_migrations() and "
                      "the two reporting tools all refuse", out)
        self.assertIn("recorded regardless", out)

        # A failed read, by contrast, is a blip and says so.
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        self._install_overrides_roundtrip()
        self.mock_migrations_blob.reload.side_effect = \
            exceptions.ServiceUnavailable("backend error")
        out = await discovery_datareview_3.tools.annotate_data_dependency(
            address="aws_db_instance.orders", disposition="keep-in-aws",
            note="licence", ctx=mock_ctx)
        self.assertIn("could not be read just now", out)
        self.assertNotIn("will close once nothing else", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_review_dropping_refusal_names_a_reachable_repair(
            self, mock_gcs_client_class, mock_get_email):
        """`scan_data_dependencies` needs the scan state, and nothing
        transitions from the review back to it — the class round 23 fixed at
        the deployment step, duplicated here by this CL's own new branch."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, _, _, mock_ctx = await self._reach_the_data_review(
            mock_gcs_client_class)
        (main.state_mgr.gcs_client.bucket("test-bucket")
         .blob(discovery_datareview_3.tools.SCAN_BASELINE_BLOB)
         .upload_from_string(json.dumps({"data_dependencies": []})))

        out = await discovery_datareview_3.tools.annotate_data_dependency(
            address="aws_s3_bucket.orders_exports", note="checked",
            ctx=mock_ctx)

        self.assertIn("does not describe the current section", out)
        self.assertIn("Replace the baseline", out)
        self.assertNotIn("Re-run scan_data_dependencies", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_an_annotation_racing_a_close_does_not_describe_the_gate(
            self, mock_gcs_client_class, mock_get_email):
        """Round 28's warning says the step closed under this call; the tail
        appended after it was keyed on the grade and the outcome store only,
        so an upgrade to `migrate` said "complete_data_migration() will
        refuse until it is reported" about a step that no longer exists."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        state_container, _, mock_ctx = self._at_data_migration(
            mock_gcs_client_class)
        self._install_overrides_roundtrip()
        state_blob = (main.state_mgr.gcs_client.bucket("test-bucket")
                      .blob("platform/onboarding/state.json"))
        live = json.dumps(state_container["state"])
        closed_state = json.dumps(dict(
            state_container["state"],
            current_state="STATE_DEPLOYMENT_COMPLETED"))
        reads = {"n": 0}

        def flips_after_the_gate_check(*_a, **_k):
            reads["n"] += 1
            return live if reads["n"] == 1 else closed_state

        state_blob.download_as_text.side_effect = flips_after_the_gate_check

        out = await discovery_datareview_3.tools.annotate_data_dependency(
            address="aws_elasticache_cluster.sessions", disposition="migrate",
            note="we decided to move it", ctx=mock_ctx)

        self.assertIn("Annotation recorded", out)
        self.assertIn("closed the data migration step while this call", out)
        self.assertNotIn("will refuse until", out)
        self.assertNotIn("NOW graded", out)
        self.assertIn("nothing will ask about this service again", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_corrupt_store_sentence_does_not_overclaim_the_refusal(
            self, mock_gcs_client_class, mock_get_email):
        """"Every tool at the step refuses" was false of three of the seven,
        including the one that had just printed it — and `keep-in-aws` is the
        documented exit, so it told an operator the exit was gone."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        self._install_overrides_roundtrip()
        self.mock_migrations_blob.reload.side_effect = None
        self.mock_migrations_blob.download_as_text.side_effect = None
        self.mock_migrations_blob.download_as_text.return_value = "[]"

        out = await discovery_datareview_3.tools.annotate_data_dependency(
            address="aws_db_instance.orders", note="checked", ctx=mock_ctx)

        self.assertIn("Annotation recorded", out)
        self.assertNotIn("every tool at the step refuses", out)
        # Exact against `_open`'s four callers.
        self.assertIn("complete_data_migration(), list_data_migrations() and "
                      "the two reporting tools refuse", out)
        self.assertIn("recorded regardless", out)
        # ...and it explains that durability without naming an operation the
        # operator did not perform. This arm needs the entry graded `migrate`
        # AFTER the correction, so the only ways in are a note-only annotation
        # (this one) and an upgrade to `migrate` — never `keep-in-aws`.
        self.assertNotIn("keep-in-aws", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_damaged_ledger_exits_do_not_promise_a_close_over_a_copy(
            self, mock_gcs_client_class, mock_get_email):
        """Round 26 stopped `_report` saying "nothing else is owed" over an
        unconfirmed copy; the two amend refusals still promised "closes
        cleanly once everything owed has moved" in the same shape."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        for baseline in (None, '{"data_dependencies": []}'):
            _, _, mock_ctx = self._at_data_migration(
                mock_gcs_client_class, with_baseline=baseline is not None)
            self._install_overrides_roundtrip()
            blob = (main.state_mgr.gcs_client.bucket("test-bucket")
                    .blob(discovery_datareview_3.tools.SCAN_BASELINE_BLOB))
            if baseline is None:
                blob.download_as_text.side_effect = exceptions.NotFound("x")
            else:
                blob.upload_from_string(baseline)

            out = await discovery_datareview_3.tools.annotate_data_dependency(
                address="aws_db_instance.orders", disposition="keep-in-aws",
                note="licence", ctx=mock_ctx)

            with self.subTest(baseline=baseline):
                self.assertIn("ERROR", out)
                self.assertIn("closes cleanly", out)
                self.assertIn("image copy is confirmed or abandoned", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_abandoning_under_a_concurrent_inventory_write_saves_nothing(
            self, mock_gcs_client_class, mock_get_email):
        """The only new inventory-mutating tool. Asserting the handler's
        message is not asserting the guard: the mock raises whatever
        precondition it is handed, so a call passing a freshly-read generation
        — a textbook lost update — produced the same ERROR. The generation
        actually sent has to be the one the read returned."""
        mock_get_email.return_value = "platform-user@google.com"
        _, inventory_container, mock_ctx = self._at_data_migration(
            mock_gcs_client_class)
        ref = "1234.dkr.ecr.us-east-1.amazonaws.com/api:v1"
        (main.state_mgr.gcs_client.bucket("test-bucket")
         .blob(main.state_mgr.INVENTORY_BLOB_PATH)
         .upload_from_string(json.dumps(self._with_failed_copy())))
        blob = (main.state_mgr.gcs_client.bucket("test-bucket")
                .blob(main.state_mgr.INVENTORY_BLOB_PATH))
        blob.upload_from_string.side_effect = exceptions.PreconditionFailed(
            "generation mismatch")
        # Every reload hands back a newer generation, as a bucket under
        # concurrent writes does.
        handed_out = []

        def advancing_generation(*_a, **_k):
            blob.generation = 100 + len(handed_out)
            handed_out.append(blob.generation)

        blob.reload.side_effect = advancing_generation

        out = await abandon_image_replication(
            refs=[ref], reason="upstream gone", ctx=mock_ctx)

        self.assertIn("ERROR", out)
        self.assertIn("Nothing was saved", out)
        self.assertIn("try again", out)
        self.assertNotEqual(
            inventory_container["inventory"]["images"][0]["replication"]
            .get("status"), "abandoned")
        # The write must carry the generation from the FIRST read, not one
        # fetched immediately before writing. The mock's generation advances
        # on every reload, so a re-read hands back a newer value — which is a
        # textbook lost update and is otherwise indistinguishable, because
        # the mock raises whatever precondition it is given.
        self.assertEqual(
            blob.upload_from_string.call_args.kwargs.get("if_generation_match"),
            handed_out[0],
            "the abandon write must carry the generation it originally read")

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_corrupt_outcome_store_is_not_called_a_blip(
            self, mock_gcs_client_class, mock_get_email):
        """`load_migrations` returns (None, None) for an object that is not a
        record set and raises for a failed read. Folding the first into
        "could not be read just now" told the operator to retry a repair."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        self._install_overrides_roundtrip()
        self.mock_migrations_blob.reload.side_effect = None
        self.mock_migrations_blob.download_as_text.side_effect = None
        self.mock_migrations_blob.download_as_text.return_value = "[]"

        out = await discovery_datareview_3.tools.annotate_data_dependency(
            address="aws_db_instance.orders", note="checked with the team",
            ctx=mock_ctx)

        self.assertIn("Repair or remove", out)
        self.assertIn(datamigration_lib.MIGRATIONS_BLOB, out)
        self.assertNotIn("just now", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_an_unreadable_outcome_store_is_said_rather_than_guessed(
            self, mock_gcs_client_class, mock_get_email):
        """The tail predicts the gate from the outcome store. When it cannot
        be read, asserting either answer is a claim made from nothing — and
        the agent relays the sentence verbatim."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        self._install_overrides_roundtrip()
        self.mock_migrations_blob.reload.side_effect = \
            exceptions.ServiceUnavailable("backend error")

        out = await discovery_datareview_3.tools.annotate_data_dependency(
            address="aws_db_instance.orders", note="checked with the team",
            ctx=mock_ctx)

        self.assertIn("could not be read", out)
        self.assertIn("list_data_migrations()", out)
        self.assertNotIn("keep refusing", out)
        self.assertNotIn("NOW graded", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_baseline_missing_one_of_a_pair_is_refused(
            self, mock_gcs_client_class, mock_get_email):
        """The guard's key is the three identity axes, and the only test that
        reached it used an EMPTY baseline — which any granularity catches.
        Keyed on the address alone it would see both entries as present and
        let the rebuild delete the one the baseline omits.

        Reachable by following this step's own "replace it with the copy the
        scan wrote" refusal with a copy from an older, narrower scan. The
        harvester falls back to the block address for a name built from
        variables, so a dev/prod pair really does read alike on two of the
        three axes."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        pair = [{"service": "rds", "identifier": "orders",
                 "address": "aws_db_instance.orders", "disposition": "migrate",
                 "evidence": [f"envs/{env}/main.tf"], "notes": [],
                 "consumers": []}
                for env in ("dev", "prod")]
        _, inventory_container, mock_ctx = self._at_data_migration(
            mock_gcs_client_class, pair)
        overrides = self._install_overrides_roundtrip()
        # The narrower scan: same address, same identifier, one directory.
        (main.state_mgr.gcs_client.bucket("test-bucket")
         .blob(discovery_datareview_3.tools.SCAN_BASELINE_BLOB)
         .upload_from_string(json.dumps({"data_dependencies": [pair[0]]})))

        out = await discovery_datareview_3.tools.annotate_data_dependency(
            address="aws_db_instance.orders", directory="envs/dev",
            disposition="keep-in-aws", note="staying put", ctx=mock_ctx)

        self.assertIn("ERROR", out)
        self.assertIn("does not describe the current section", out)
        # `envs/prod` is named as the entry that would have been deleted.
        self.assertIn("aws_db_instance.orders", out)
        # Nothing written: not the section, not the corrections document.
        self.assertEqual(
            len(inventory_container["inventory"]["data_dependencies"]), 2)
        self.assertNotIn("document", overrides)
        # And the close agrees rather than reporting a clean finish.
        close = await t.complete_data_migration(ctx=mock_ctx)
        self.assertIn("does not describe the current section", close)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_baseline_that_does_not_describe_the_section_is_refused(
            self, mock_gcs_client_class, mock_get_email):
        """`{"data_dependencies": []}` is truthy, well-formed, and the minimal
        thing an operator types when they follow this step's own "repair or
        replace it" refusal. The rebuild takes the baseline's entry list
        wholesale, so applying a correction against it DELETED the mapping a
        human signed off — silently, under a success message, at the one step
        with no re-scan and no second sign-off."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, inventory_container, mock_ctx = self._at_data_migration(
            mock_gcs_client_class)
        overrides = self._install_overrides_roundtrip()
        (main.state_mgr.gcs_client.bucket("test-bucket")
         .blob(discovery_datareview_3.tools.SCAN_BASELINE_BLOB)
         .upload_from_string(json.dumps({"data_dependencies": []})))

        out = await discovery_datareview_3.tools.annotate_data_dependency(
            address="aws_s3_bucket.exports", disposition="keep-in-aws",
            note="staying put", ctx=mock_ctx)

        self.assertIn("ERROR", out)
        self.assertIn("does not describe the current section", out)
        # The section is untouched, and so is the corrections document: the
        # refusal lands before the override is written.
        self.assertEqual(
            len(inventory_container["inventory"]["data_dependencies"]), 3)
        self.assertNotIn("document", overrides)

        # And the close does not read that baseline as a working exit. Both
        # branches refuse, so "ERROR" proves nothing: the grade decides WHICH
        # refusal, and grading it ok sends the operator to a `keep-in-aws`
        # that now refuses, naming neither the real problem nor its repair.
        close = await t.complete_data_migration(ctx=mock_ctx)
        self.assertIn("does not describe the current section", close)
        self.assertNotIn("SUCCESS", close)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_an_in_progress_note_does_not_survive_the_completion(
            self, mock_gcs_client_class, mock_get_email):
        """A note is written about the status it accompanies. Carrying "DMS
        job at 60%, cutover Tue" onto the completed record puts it in the
        runbook a platform engineer reads to decide whether a component may
        ship against that database."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        progress = "DMS job dms-orders-1 at 60%, cutover Tue 02:00 UTC"
        await t.mark_data_service_migrating(
            address="aws_db_instance.orders", note=progress, ctx=mock_ctx)

        # The call the listing prints: no note=, no target=.
        out = await t.mark_data_service_migrated(
            address="aws_db_instance.orders", target="orders-db-gcp",
            ctx=mock_ctx)

        self.assertNotIn(progress, out.split("was not carried")[0])
        self.assertIn("was not carried onto this one", out)
        listing = await t.list_data_migrations(ctx=mock_ctx)
        self.assertNotIn(progress, listing)
        self.assertIn("orders-db-gcp", listing)
        runbook = self.mock_dm_runbook_blob.upload_from_string.call_args[0][0]
        self.assertNotIn(progress, runbook)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_an_empty_string_clears_a_field_the_carry_forward_keeps(
            self, mock_gcs_client_class, mock_get_email):
        """Silence means "unchanged", so without this there is no call that
        takes a wrong note back out — and nothing at the terminal can edit the
        object either."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        await t.mark_data_service_migrated(
            address="aws_db_instance.orders", target="orders-db-gcp",
            note="wrong database, ignore this", ctx=mock_ctx)

        out = await t.mark_data_service_migrated(
            address="aws_db_instance.orders", note="", ctx=mock_ctx)

        self.assertIn("Cleared:", out)
        listing = await t.list_data_migrations(ctx=mock_ctx)
        self.assertNotIn("wrong database", listing)
        self.assertIn("orders-db-gcp", listing)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_re_reporting_keeps_the_target_and_note(
            self, mock_gcs_client_class, mock_get_email):
        """The call the listing prints carries no `target=`, so running it
        verbatim against an already-reported service erased one — along with
        the note this module calls "exactly what must not disappear before a
        component ships against that database"."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        caveat = "only the orders schema moved; audit tables still in RDS"
        await t.mark_data_service_migrated(
            address="aws_db_instance.orders", target="orders-db-gcp",
            note=caveat, ctx=mock_ctx)

        # The printed call, verbatim.
        out = await t.mark_data_service_migrated(
            address="aws_db_instance.orders", ctx=mock_ctx)

        self.assertIn("Kept from the earlier report", out)
        listing = await t.list_data_migrations(ctx=mock_ctx)
        self.assertIn("orders-db-gcp", listing)
        self.assertIn(caveat, listing)

        # A NEW value still replaces, and says so.
        out = await t.mark_data_service_migrated(
            address="aws_db_instance.orders", target="orders-db-gcp-2",
            ctx=mock_ctx)
        self.assertIn("Replaced:", out)
        self.assertIn("orders-db-gcp", out)


    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_terminal_accepts_no_assertions(
            self, mock_gcs_client_class, mock_get_email):
        """A terminal state means the workflow is finished, and a tool that
        accepts calls there says the opposite. Both assertion tools are
        confirmed from the step that waits for the work, and from nowhere
        else."""
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        state = dict(json.loads(
            main.state_mgr.gcs_client.bucket("test-bucket")
            .blob("platform/onboarding/state.json").download_as_text()),
            current_state="STATE_DEPLOYMENT_COMPLETED")
        (main.state_mgr.gcs_client.bucket("test-bucket")
         .blob("platform/onboarding/state.json")
         .upload_from_string(json.dumps(state)))

        self.assertIn("Invalid state",
                      await mark_replication_complete(ctx=mock_ctx))
        self.assertIn("Invalid state", await abandon_image_replication(
            refs=["x"], reason="y", ctx=mock_ctx))

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_an_annotation_on_a_never_owed_service_claims_no_transition(
            self, mock_gcs_client_class, mock_get_email):
        """"No longer owed" asserts a transition, and a note on a `rebuild`
        cache had nothing to transition from. Three cases, not two."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        self._install_overrides_roundtrip()

        out = await discovery_datareview_3.tools.annotate_data_dependency(
            address="aws_elasticache_cluster.sessions",
            note="checked with the team: nothing persists here", ctx=mock_ctx)

        self.assertNotIn("no longer owed", out)
        self.assertIn("does not wait on", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_an_unknown_address_points_at_a_reachable_listing(
            self, mock_gcs_client_class, mock_get_email):
        """`list_data_dependencies` refuses at this step, so naming it hands
        the operator an instruction they cannot follow."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        self._install_overrides_roundtrip()

        out = await discovery_datareview_3.tools.annotate_data_dependency(
            address="aws_db_instance.ordrs", disposition="keep-in-aws",
            note="typo", ctx=mock_ctx)

        self.assertIn("ERROR", out)
        self.assertIn("list_data_migrations()", out)
        self.assertNotIn("list_data_dependencies()", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_annotation_tail_describes_what_it_did(
            self, mock_gcs_client_class, mock_get_email):
        """The tail was keyed on the STATE the correction was made in, so every
        annotation at the deployment step claimed the service was no longer
        owed — including a note-only one that changed nothing, and an upgrade
        to `migrate` that made it owed for the first time. It is the only
        thing the agent sees, and it relays it."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        self._install_overrides_roundtrip()

        # A note that excuses nothing.
        out = await discovery_datareview_3.tools.annotate_data_dependency(
            address="aws_db_instance.orders",
            note="the DBA is scheduling the window", ctx=mock_ctx)
        self.assertNotIn("no longer owed", out)
        self.assertIn("still owed", out)

        # ...and one that does.
        out = await discovery_datareview_3.tools.annotate_data_dependency(
            address="aws_db_instance.orders", disposition="keep-in-aws",
            note="staying put", ctx=mock_ctx)
        self.assertIn("no longer owed", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_upgrading_a_service_at_the_step_says_it_is_now_owed(
            self, mock_gcs_client_class, mock_get_email):
        """"We will move it after all" is a legitimate decision, and it makes
        the service owed rather than excusing it — for the FIRST time. Saying
        it is "still owed" and the close "will keep refusing" describes a
        state of play the operator's own call just created, and the agent
        relays the sentence verbatim."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        entries = [{"service": "dynamodb", "identifier": "carts",
                    "address": "aws_dynamodb_table.carts",
                    "disposition": "escalate", "evidence": ["envs/prod/data.tf"],
                    "notes": [], "consumers": []}]
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class, entries)
        self._install_overrides_roundtrip()

        out = await discovery_datareview_3.tools.annotate_data_dependency(
            address="aws_dynamodb_table.carts", disposition="migrate",
            note="we decided to move it", ctx=mock_ctx)

        self.assertIn("NOW graded 'migrate'", out)
        self.assertIn("owed a move from here on", out)
        self.assertNotIn("still owed", out)
        self.assertNotIn("no longer owed", out)
        self.assertNotIn("keep refusing", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_baseline_refusal_names_a_reachable_route(
            self, mock_gcs_client_class, mock_get_email):
        """The review's wording sends the operator to `scan_data_dependencies`,
        which needs a state nothing transitions back to. Standing in front of
        a refusal, they were handed the one instruction they cannot follow."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(
            mock_gcs_client_class, with_baseline=False)

        out = await discovery_datareview_3.tools.annotate_data_dependency(
            address="aws_s3_bucket.exports", disposition="keep-in-aws",
            ctx=mock_ctx)

        self.assertIn("ERROR", out)
        self.assertNotIn("Re-run scan_data_dependencies", out)
        self.assertIn("complete_data_migration", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_printed_call_is_runnable_as_printed(
            self, mock_gcs_client_class, mock_get_email):
        """`reporting_call`'s docstring promises a reader following it verbatim
        gets an accepted call, and it carried the placeholder target the tool
        refuses."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)

        out = await t.list_data_migrations(ctx=mock_ctx)

        self.assertNotIn("<where it landed>", out)
        runbook = self.mock_dm_runbook_blob.upload_from_string.call_args[0][0]
        self.assertNotIn("<where it landed>", runbook)
        # The exact call the listing printed, run verbatim.
        printed = next(line.split("report it: ", 1)[1]
                       for line in out.splitlines() if "report it: " in line)
        self.assertEqual(
            printed, 'mark_data_service_migrated(address="aws_db_instance.orders")')
        self.assertNotIn("ERROR", await t.mark_data_service_migrated(
            address="aws_db_instance.orders", ctx=mock_ctx))

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_failed_runbook_refresh_is_reported_at_the_close(
            self, mock_gcs_client_class, mock_get_email):
        """The refresh exists so the terminal's artifact caveat is true. A
        silent failure is the one case that makes it false again, with nothing
        callable at the terminal to correct it."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        await t.mark_data_service_migrated(
            address="aws_db_instance.orders", ctx=mock_ctx)
        await t.mark_data_service_migrated(
            address="aws_s3_bucket.exports", ctx=mock_ctx)
        self.mock_dm_runbook_blob.upload_from_string.side_effect = RuntimeError(
            "bucket on fire")

        out = await t.complete_data_migration(ctx=mock_ctx)

        self.assertIn("SUCCESS", out)
        self.assertIn("WARNING", out)
        self.assertIn("could not be refreshed", out)


    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_an_unusable_baseline_stops_the_workflow(
            self, mock_gcs_client_class, mock_get_email):
        """`scan_baseline_state` tested `is not None` while `_amend` tests
        falsiness, so a baseline of `{}` read as "ok" while the exit refused —
        the two predicates have to agree about one object. They agree on
        refusing: the step stays put, which for a damaged ledger is the
        outcome, not a wedge to be escaped."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        (main.state_mgr.gcs_client.bucket("test-bucket")
         .blob(discovery_datareview_3.tools.SCAN_BASELINE_BLOB)
         .upload_from_string("{}"))

        out = await t.complete_data_migration(ctx=mock_ctx)

        # Refuses, and names the damage rather than failing silently.
        self.assertIn("ERROR", out)
        self.assertIn("this ledger is damaged", out)
        self.assertNotIn("SUCCESS", out)
        # Directly, because the close now reaches the same verdict by a
        # second route — `entries_a_rebuild_would_drop` also rejects `{}` —
        # so the assertion above stopped proving the predicate this test is
        # named after. Both are wanted; only one of them is tested here.
        self.assertEqual(
            discovery_datareview_3.tools.scan_baseline_state(
                main.state_mgr.gcs_client.bucket("test-bucket")),
            discovery_datareview_3.tools.BASELINE_UNREADABLE)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_runbook_is_refreshed_before_the_step_closes(
            self, mock_gcs_client_class, mock_get_email):
        """The runbook is registered at the terminal too, where its caveat
        promises it was "written as it stood when the step closed". Only the
        listing wrote it, so a reviewer opening the terminal's primary
        artifact was told two databases had not moved."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        await t.list_data_migrations(ctx=mock_ctx)   # writes "2 still owe"
        await t.mark_data_service_migrated(
            address="aws_db_instance.orders", ctx=mock_ctx)
        await t.mark_data_service_migrated(
            address="aws_s3_bucket.exports", ctx=mock_ctx)

        self.assertIn("SUCCESS", await t.complete_data_migration(ctx=mock_ctx))

        runbook = self.mock_dm_runbook_blob.upload_from_string.call_args[0][0]
        self.assertIn("No data service is waiting to move", runbook)
        self.assertNotIn("still owe a move", runbook)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_runbook_carries_a_status_note_and_stale_records(
            self, mock_gcs_client_class, mock_get_email):
        """The durable artifact was rendering strictly less than the ephemeral
        listing: an in-progress note is exactly what has to survive a session,
        and a stale record is what must not vanish from the count of what was
        done."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        _, inventory_container, mock_ctx = self._at_data_migration(
            mock_gcs_client_class)
        await t.mark_data_service_migrating(
            address="aws_db_instance.orders",
            note="DMS job dms-orders-1 at 60%, cutover Tue 02:00 UTC",
            ctx=mock_ctx)
        # A record whose entry the section no longer carries.
        await t.mark_data_service_migrated(
            address="aws_s3_bucket.exports", target="gs://exports",
            ctx=mock_ctx)
        inventory = dict(inventory_container["inventory"])
        inventory["data_dependencies"] = [
            e for e in inventory["data_dependencies"]
            if e["identifier"] != "exports"]
        (main.state_mgr.gcs_client.bucket("test-bucket")
         .blob(main.state_mgr.INVENTORY_BLOB_PATH)
         .upload_from_string(json.dumps(inventory)))

        await t.list_data_migrations(ctx=mock_ctx)

        runbook = self.mock_dm_runbook_blob.upload_from_string.call_args[0][0]
        self.assertIn("cutover Tue 02:00 UTC", runbook)
        self.assertIn("no entry in this scan", runbook)
        self.assertIn("gs://exports", runbook)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_corrupt_baseline_refuses_rather_than_closing(
            self, mock_gcs_client_class, mock_get_email):
        """A damaged ledger does not finish the workflow. Every unusable
        shape of the baseline refuses and names the repair; none of them
        closes, because a migration reported complete is a claim made to every
        application team and cannot rest on an artifact nobody can vouch
        for."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        for corrupt in ('{"data_dependencies": [null]}',
                        '{"data_dependencies": "x"}', 'not json'):
            state_container, _, mock_ctx = self._at_data_migration(
                mock_gcs_client_class)
            (main.state_mgr.gcs_client.bucket("test-bucket")
             .blob(discovery_datareview_3.tools.SCAN_BASELINE_BLOB)
             .upload_from_string(corrupt))

            with self.subTest(corrupt=corrupt):
                out = await t.complete_data_migration(ctx=mock_ctx)
                self.assertIn("ERROR", out)
                self.assertIn("this ledger is damaged", out)
                # The repair for an object that is PRESENT and wrong:
                # replace it. Not the absent one — telling an operator to
                # restart the platform walk over a re-uploadable file
                # contradicts what `_amend` says about the same object.
                self.assertIn("Replace " + discovery_datareview_3.tools
                              .SCAN_BASELINE_BLOB + " with the copy the scan "
                              "wrote", out)
                self.assertNotIn("join_ledger --reconfigure", out)
                self.assertNotIn("has to come back", out)
                self.assertNotIn("SUCCESS", out)
                self.assertEqual(state_container["state"]["current_state"],
                                 "STATE_DEPLOYMENT_DATA_MIGRATION")

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_placeholder_target_is_refused(
            self, mock_gcs_client_class, mock_get_email):
        """The printed call carries `target="<where it landed>"`, and the
        instructions say both to use that call and never to guess a target.
        Storing the stand-in verbatim puts it in the durable record of where
        customer data went."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)

        out = await t.mark_data_service_migrated(
            address="aws_db_instance.orders", target="<where it landed>",
            ctx=mock_ctx)

        self.assertIn("ERROR", out)
        self.assertIn("placeholder", out)
        # ...and omitting it entirely is the documented alternative.
        out = await t.mark_data_service_migrated(
            address="aws_db_instance.orders", ctx=mock_ctx)
        self.assertNotIn("ERROR", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_failure_survives_abandonment_and_repair(
            self, mock_gcs_client_class, mock_get_email):
        """failed -> abandoned -> replicated. Rebuilding `previous` from the
        current layer alone kept the human decision and dropped the server's
        reason for it, which is backwards."""
        mock_get_email.return_value = "platform-user@google.com"
        _, inventory_container, mock_ctx = self._at_data_migration(
            mock_gcs_client_class)
        ref = "1234.dkr.ecr.us-east-1.amazonaws.com/api:v1"
        (main.state_mgr.gcs_client.bucket("test-bucket")
         .blob(main.state_mgr.INVENTORY_BLOB_PATH)
         .upload_from_string(json.dumps(self._with_failed_copy())))

        await abandon_image_replication(
            refs=[ref], reason="not worth chasing", ctx=mock_ctx)
        await mark_replication_complete(refs=[ref], ctx=mock_ctx)

        record = inventory_container["inventory"]["images"][0]["replication"]
        self.assertEqual(record["status"], "replicated")
        self.assertEqual(record["previous"]["status"], "abandoned")
        self.assertIn("not worth chasing", record["previous"]["reason"])
        # Who decided, and when. A reason with nobody attached to it reads as
        # an oversight, which is the one thing abandonment must not look like.
        self.assertEqual(record["previous"]["abandoned_by"],
                         "platform-user@google.com")
        self.assertTrue(record["previous"]["abandoned_at"])
        # The server's own failure is still there, two layers down.
        self.assertIn("unauthorized", record["previous"]["error"])

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_transient_baseline_read_failure_refuses_rather_than_closes(
            self, mock_gcs_client_class, mock_get_email):
        """`load_scan_baseline` already turns absence and corruption into None,
        so the only thing an exception reports is a transport failure — and
        those are exactly the cases where the exit works. Closing is the branch
        that cannot be undone, so a retryable blip must not take it."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        state_container, _, mock_ctx = self._at_data_migration(
            mock_gcs_client_class)
        (main.state_mgr.gcs_client.bucket("test-bucket")
         .blob(discovery_datareview_3.tools.SCAN_BASELINE_BLOB)
         .download_as_text.side_effect) = exceptions.ServiceUnavailable(
             "backend error")

        out = await t.complete_data_migration(ctx=mock_ctx)

        self.assertIn("ERROR", out)
        self.assertIn("try again", out)
        self.assertNotIn("does not have", out)
        self.assertEqual(state_container["state"]["current_state"],
                         "STATE_DEPLOYMENT_DATA_MIGRATION")

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_listing_with_work_left_keeps_the_exit(
            self, mock_gcs_client_class, mock_get_email):
        """The mirror of the runbook's `test_a_runbook_with_work_left_keeps_the
        _exit`. The footer's three gates were pinned two out of three: rounds
        37's two findings covered `not copies` and `not closed_under_us`, and
        the positive test uses an estate with nothing owed, so `not owed` was
        the arm no test could see. Dropping it tells the agent the step can
        close in the same response that lists two services as owed."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)

        out = await t.list_data_migrations(ctx=mock_ctx)

        self.assertIn("2 still owed", out)
        self.assertNotIn("Nothing is outstanding", out)
        # ...and the close agrees, which is the whole point of the gate.
        self.assertIn("ERROR", await t.complete_data_migration(ctx=mock_ctx))

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_reporting_under_a_concurrent_outcome_write_saves_nothing(
            self, mock_gcs_client_class, mock_get_email):
        """Round 36 finding 4's test, on the other store. `data_migrations.json`
        is the only record of which data services have moved, and asserting the
        PreconditionFailed handler is not asserting the guard: the mock raises
        whatever precondition it is handed, so a write carrying a generation
        re-read just before it — the textbook lost update — produced the same
        ERROR. Two engineers reporting different services in overlapping calls
        would lose one record and both be told it was saved."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        from servers.phases.deployment import datamigration as dm
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        blob = (main.state_mgr.gcs_client.bucket("test-bucket")
                .blob(datamigration_lib.MIGRATIONS_BLOB))
        # The store has to already exist: absent, `load_migrations` returns
        # generation 0, which is the create-if-absent precondition rather than
        # a read the write has to match.
        blob.download_as_text.side_effect = None
        blob.download_as_text.return_value = json.dumps(dm.empty_document())
        blob.upload_from_string.side_effect = exceptions.PreconditionFailed(
            "generation mismatch")
        # Every reload hands back a newer generation, as a bucket under
        # concurrent writes does.
        handed_out = []

        def advancing_generation(*_a, **_k):
            blob.generation = 200 + len(handed_out)
            handed_out.append(blob.generation)

        blob.reload.side_effect = advancing_generation

        out = await t.mark_data_service_migrated(
            address="aws_db_instance.orders", ctx=mock_ctx)

        self.assertIn("ERROR", out)
        self.assertIn("Nothing was saved", out)
        self.assertTrue(handed_out)
        self.assertEqual(
            blob.upload_from_string.call_args.kwargs.get("if_generation_match"),
            handed_out[0],
            "the outcome write must carry the generation it originally read")

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_close_writes_state_against_the_generation_it_read(
            self, mock_gcs_client_class, mock_get_email):
        """The second half of the same class: the close's own state write. It
        is the transition to the terminal, so an unconditional write lands the
        platform walk on top of whatever another session did in between."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(
            mock_gcs_client_class, entries=[])
        state_blob = (main.state_mgr.gcs_client.bucket("test-bucket")
                      .blob("platform/onboarding/state.json"))
        first = state_blob.generation

        out = await t.complete_data_migration(ctx=mock_ctx)

        self.assertNotIn("ERROR", out)
        self.assertIsNotNone(first)
        self.assertEqual(
            state_blob.upload_from_string.call_args.kwargs.get(
                "if_generation_match"),
            first,
            "the close must write state against the generation it authorized "
            "on, not unconditionally")

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_offering_the_close_over_a_live_cutover_names_the_cost(
            self, mock_gcs_client_class, mock_get_email):
        """An in-flight move against a service the step does not gate
        satisfies "nothing owed, nothing unconfirmed", so every site that
        offers the close on that test offers it over a running cutover. Rounds
        17 and 18 established what the close then costs — the record can never
        be completed, because `mark_data_service_migrated` refuses at the
        terminal and nothing transitions back — and only the CLOSED runbook
        said so, which is saying it too late. Three live sites, one call
        sequence: the report itself, the listing, and the durable artifact."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        cache = [{"service": "elasticache", "identifier": "sessions",
                  "address": "aws_elasticache_cluster.sessions",
                  "disposition": "rebuild",
                  "evidence": ["envs/prod/data.tf"], "consumers": [],
                  "notes": []}]
        _, _, mock_ctx = self._at_data_migration(
            mock_gcs_client_class, entries=cache)

        opened = await t.mark_data_service_migrating(
            address="aws_elasticache_cluster.sessions",
            note="RDB export running, do not delete the AWS cluster",
            ctx=mock_ctx)
        listing = await t.list_data_migrations(ctx=mock_ctx)
        runbook = self.mock_dm_runbook_blob.upload_from_string.call_args[0][0]

        # The response that OPENS the cutover must not propose the close bare.
        self.assertIn("Nothing else is owed", opened)
        self.assertIn("frozen by that", opened)
        for where, text in (("listing", listing), ("runbook", runbook)):
            with self.subTest(where=where):
                self.assertIn("Nothing is outstanding", text)
                self.assertIn("frozen by that", text)
                self.assertIn("elasticache sessions",
                              text.split("frozen by that")[1])
        # ...and the close is still not GATED on it: the step never owed this
        # service, so it closes. Saying the cost is not the same as refusing.
        self.assertIn("SUCCESS", await t.complete_data_migration(ctx=mock_ctx))

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    @patch("servers.phases.deployment.actions._default_session")
    async def test_settling_the_last_copy_over_a_live_cutover_names_the_cost(
            self, mock_session_fn, mock_gcs_client_class, mock_get_email):
        """The two image tools are the fifth and sixth sites that offer the
        close, and the only two that offer it in the shape round 37 named — an
        image copy as the LAST thing holding the step open. Nothing about that
        shape says a database cutover is not running, and the instructions have
        the agent hand back control here rather than re-list, so this response
        is the surface the operator acts on."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        mock_session_fn.return_value = self._ar_session()
        ref = "1234.dkr.ecr.us-east-1.amazonaws.com/api:v1"
        cache = [{"service": "elasticache", "identifier": "sessions",
                  "address": "aws_elasticache_cluster.sessions",
                  "disposition": "rebuild",
                  "evidence": ["envs/prod/data.tf"], "consumers": [],
                  "notes": []}]
        for settle in ("confirm", "abandon"):
            _, _, mock_ctx = self._at_data_migration(
                mock_gcs_client_class, entries=cache)
            (main.state_mgr.gcs_client.bucket("test-bucket")
             .blob(main.state_mgr.INVENTORY_BLOB_PATH)
             .upload_from_string(json.dumps(
                 {"schema_version": "1.0",
                  "data_dependencies": [dict(e) for e in cache],
                  "images": [{"ref": ref, "registry": "ecr",
                              "repository": "api", "tag": "v1",
                              "pinned_by": "tag",
                              "provenance": [{"kind": "literal",
                                              "file": "k8s/api.yaml"}],
                              "replication": {"status": "self_service"}}]})))
            await t.mark_data_service_migrating(
                address="aws_elasticache_cluster.sessions",
                note="RDB export running", ctx=mock_ctx)

            out = (await mark_replication_complete(refs=[ref], ctx=mock_ctx)
                   if settle == "confirm" else
                   await abandon_image_replication(
                       refs=[ref], reason="upstream gone", ctx=mock_ctx))

            with self.subTest(settle=settle):
                self.assertIn("Nothing is outstanding now", out)
                self.assertIn("frozen by that", out)
                self.assertIn("elasticache sessions",
                              out.split("frozen by that")[1])

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_caveat_read_failure_does_not_undo_a_recorded_correction(
            self, mock_gcs_client_class, mock_get_email):
        """`_freeze_over_the_section` runs AFTER `_amend` has saved the rebuilt
        inventory, so the human's decision is already durable. A transient 503
        on its own re-read is the ordinary reason its failure arm exists, and
        without it the tool raises out of MCP with the correction recorded and
        the operator told it failed. Round 39 finding 1's class on the sibling
        `_nothing_left_at_the_step`, which was pinned; this one was not."""
        from servers.phases.discovery.discovery_datareview_3 import tools as r
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        blob = (main.state_mgr.gcs_client.bucket("test-bucket")
                .blob(main.state_mgr.INVENTORY_BLOB_PATH))
        saved = {}

        def fails_only_on_the_caveat_read(*_a, **_k):
            # Reads before the save serve the correction itself; the one after
            # it is the caveat's, and only that one blows up.
            if saved:
                raise exceptions.ServiceUnavailable("backend error")
            return blob._payload

        blob._payload = blob.download_as_text()

        def remember(data, **_k):
            saved["data"] = data
            blob._payload = data

        blob.download_as_text.side_effect = fails_only_on_the_caveat_read
        blob.upload_from_string.side_effect = remember

        out = await r.annotate_data_dependency(
            address="aws_db_instance.orders", disposition="keep-in-aws",
            note="stays in AWS", ctx=mock_ctx)

        self.assertNotIn("ERROR", out)
        self.assertIn("Annotation recorded", out)
        # The caveat is withheld, not guessed at, and nothing else is lost.
        self.assertNotIn("frozen by that", out)
        self.assertIn("no longer owed a move", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_missing_inventory_withholds_the_caveat_rather_than_raising(
            self, mock_gcs_client_class, mock_get_email):
        """The other arm of the same guard. `load_inventory` returns None for
        an object that is absent rather than unreadable, and `.get` on None is
        an AttributeError out of a tool whose durable work has landed."""
        from servers.phases.discovery.discovery_datareview_3 import tools as r
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        blob = (main.state_mgr.gcs_client.bucket("test-bucket")
                .blob(main.state_mgr.INVENTORY_BLOB_PATH))
        saved = {}
        blob._payload = blob.download_as_text()

        def gone_after_the_save(*_a, **_k):
            if saved:
                raise exceptions.NotFound("Not Found")
            return blob._payload

        def remember(data, **_k):
            saved["data"] = data

        blob.download_as_text.side_effect = gone_after_the_save
        blob.upload_from_string.side_effect = remember

        out = await r.annotate_data_dependency(
            address="aws_db_instance.orders", disposition="keep-in-aws",
            note="stays in AWS", ctx=mock_ctx)

        self.assertNotIn("ERROR", out)
        self.assertIn("Annotation recorded", out)
        self.assertNotIn("frozen by that", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    @patch("servers.phases.deployment.actions._default_session")
    async def test_a_caveat_read_failure_does_not_undo_a_settled_copy(
            self, mock_session_fn, mock_gcs_client_class, mock_get_email):
        """`_freeze_warning`'s two arms, on the image side. It runs after the
        inventory write that records the settlement, so a failure here would
        turn a durable outcome into an error — and it sits one line under
        `_nothing_left_at_the_step`, whose identical arms round 39 pinned.

        The failure is armed when that neighbour RETURNS rather than at a
        counted read: failing earlier makes the neighbour answer False, which
        suppresses the whole block and tests nothing. This is the only window
        the arms exist for, and it is a real one — the two functions make four
        reads between them and another session can lose the bucket in between.
        """
        from servers.phases.deployment.deployment_provision_1 import tools as p
        mock_get_email.return_value = "platform-user@google.com"
        mock_session_fn.return_value = self._ar_session()
        ref = "1234.dkr.ecr.us-east-1.amazonaws.com/api:v1"
        for damage, error in (("unreachable",
                               exceptions.ServiceUnavailable("backend error")),
                              ("absent", exceptions.NotFound("Not Found"))):
            _, _, mock_ctx = self._at_data_migration(
                mock_gcs_client_class, entries=[])
            blob = (main.state_mgr.gcs_client.bucket("test-bucket")
                    .blob(main.state_mgr.INVENTORY_BLOB_PATH))
            blob.upload_from_string(json.dumps(
                {"schema_version": "1.0", "data_dependencies": [],
                 "images": [{"ref": ref, "registry": "ecr",
                             "repository": "api", "tag": "v1",
                             "pinned_by": "tag",
                             "provenance": [{"kind": "literal",
                                             "file": "k8s/api.yaml"}],
                             "replication": {"status": "self_service"}}]}))
            armed, real = {}, p._nothing_left_at_the_step

            def arm_once_the_line_is_decided(bucket, _real=real):
                verdict = _real(bucket)
                armed["yes"] = True
                return verdict

            def fails_only_for_the_caveat(*_a, **_k):
                if armed:
                    raise error
                # The fixture's upload keeps this current, so the re-reads see
                # the mark this call just made rather than a stale snapshot.
                return blob.download_as_text.return_value

            blob.download_as_text.side_effect = fails_only_for_the_caveat

            with patch.object(p, "_nothing_left_at_the_step",
                              arm_once_the_line_is_decided):
                out = await mark_replication_complete(refs=[ref], ctx=mock_ctx)

            with self.subTest(damage=damage):
                self.assertIn("Marked 1 image(s) replicated", out)
                self.assertNotIn("ERROR", out)
                # The line it accompanies still lands; only the caveat is
                # withheld, because the caveat is the part that could not be
                # computed.
                self.assertIn("Nothing is outstanding now", out)
                self.assertNotIn("frozen by that", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_freeze_warning_spans_the_section_not_the_entry(
            self, mock_gcs_client_class, mock_get_email):
        """The ordinary sequence separates the two services: the one excused
        is what makes the close available, the one moving is what the close
        would strand. Keyed on the corrected entry, `_amend`'s tail went silent
        in exactly that case while `list_data_migrations` one call later said
        the opposite — two surfaces, one state, different answers, and the
        instructions tell the agent not to re-list."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        from servers.phases.discovery.discovery_datareview_3 import tools as r
        mock_get_email.return_value = "platform-user@google.com"
        both = list(self.DATA_ENTRIES[:1]) + [
            {"service": "elasticache", "identifier": "sessions",
             "address": "aws_elasticache_cluster.sessions",
             "disposition": "rebuild", "evidence": ["envs/prod/data.tf"],
             "consumers": [], "notes": []}]
        _, _, mock_ctx = self._at_data_migration(
            mock_gcs_client_class, entries=both)

        await t.mark_data_service_migrating(
            address="aws_elasticache_cluster.sessions",
            note="RDB export running", ctx=mock_ctx)
        # The call that OPENS the close, on the other service.
        out = await r.annotate_data_dependency(
            address="aws_db_instance.orders", disposition="keep-in-aws",
            note="stays in AWS", ctx=mock_ctx)

        self.assertIn("no longer owed a move", out)
        self.assertIn("frozen by that", out)
        self.assertIn("elasticache sessions", out.split("frozen by that")[1])
        # The moving service is not the corrected one, so the entry-scoped
        # clause must stay quiet — that is what makes this the population.
        self.assertNotIn("already reported IN PROGRESS", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    @patch("servers.phases.deployment.actions._default_session")
    async def test_settling_a_copy_under_a_close_says_what_was_frozen(
            self, mock_session_fn, mock_gcs_client_class, mock_get_email):
        """The fifth and sixth sites that report the close has been taken.
        Round 51 wired the three in the step's own module; these two are the
        same pair round 45 finding 1 found missing from the OFFER side. They
        correctly suppress the offer here — what was missing is the fact that
        the cutover can no longer be recorded as landed."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        mock_session_fn.return_value = self._ar_session()
        ref = "1234.dkr.ecr.us-east-1.amazonaws.com/api:v1"
        cache = [{"service": "elasticache", "identifier": "sessions",
                  "address": "aws_elasticache_cluster.sessions",
                  "disposition": "rebuild",
                  "evidence": ["envs/prod/data.tf"], "consumers": [],
                  "notes": []}]
        for settle in ("confirm", "abandon"):
            state_container, _, mock_ctx = self._at_data_migration(
                mock_gcs_client_class, entries=cache)
            (main.state_mgr.gcs_client.bucket("test-bucket")
             .blob(main.state_mgr.INVENTORY_BLOB_PATH)
             .upload_from_string(json.dumps(
                 {"schema_version": "1.0",
                  "data_dependencies": [dict(e) for e in cache],
                  "images": [{"ref": ref, "registry": "ecr",
                              "repository": "api", "tag": "v1",
                              "pinned_by": "tag",
                              "provenance": [{"kind": "literal",
                                              "file": "k8s/api.yaml"}],
                              "replication": {"status": "self_service"}}]})))
            await t.mark_data_service_migrating(
                address="aws_elasticache_cluster.sessions",
                note="RDB export running", ctx=mock_ctx)
            state_blob = (main.state_mgr.gcs_client.bucket("test-bucket")
                          .blob("platform/onboarding/state.json"))
            live = json.dumps(state_container["state"])
            closed = json.dumps(dict(
                state_container["state"],
                current_state="STATE_DEPLOYMENT_COMPLETED"))
            reads = {"n": 0}

            def closes_under_us(*_a, **_k):
                reads["n"] += 1
                return live if reads["n"] == 1 else closed

            state_blob.download_as_text.side_effect = closes_under_us

            out = (await mark_replication_complete(refs=[ref], ctx=mock_ctx)
                   if settle == "confirm" else
                   await abandon_image_replication(
                       refs=[ref], reason="upstream gone", ctx=mock_ctx))

            with self.subTest(settle=settle):
                self.assertIn("closed the data migration step while this call",
                              out)
                self.assertIn("can no longer be completed", out)
                self.assertIn("elasticache sessions", out)
                # The offer stays suppressed — that is the other half.
                self.assertNotIn("Nothing is outstanding now", out)
                self.assertNotIn("would be frozen", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_close_says_what_it_froze(
            self, mock_gcs_client_class, mock_get_email):
        """The cost was stated at every site that OFFERS the close and at none
        that reports it TAKEN — and that is the channel the operator is in at
        the moment it happens. "Nothing outstanding" is true, and on its own
        reads as "nothing left to do", which is not the same thing while a
        cutover is running. The closed runbook says it, but nothing here sends
        anyone there and the instructions have the agent hand back control
        rather than open artifacts."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        cache = [{"service": "elasticache", "identifier": "sessions",
                  "address": "aws_elasticache_cluster.sessions",
                  "disposition": "rebuild",
                  "evidence": ["envs/prod/data.tf"], "consumers": [],
                  "notes": []}]
        _, _, mock_ctx = self._at_data_migration(
            mock_gcs_client_class, entries=cache)
        await t.mark_data_service_migrating(
            address="aws_elasticache_cluster.sessions",
            note="RDB export running", ctx=mock_ctx)

        out = await t.complete_data_migration(ctx=mock_ctx)

        self.assertIn("SUCCESS", out)
        self.assertIn("can no longer be completed", out)
        self.assertIn("elasticache sessions", out)
        # Past tense: the choice is gone, so the conditional wording would
        # read as a warning there is still time to act on.
        self.assertNotIn("would be frozen", out)
        # And the pointer, which only THIS site can make: it has just written
        # the runbook from this same section, so it carries them.
        self.assertIn(datamigration_lib.RUNBOOK_BLOB + " lists them", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_clean_close_says_nothing_about_freezing(
            self, mock_gcs_client_class, mock_get_email):
        """The other direction: the sentence is keyed on the fourth view, not
        printed after every close."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(
            mock_gcs_client_class, entries=[])
        (main.state_mgr.gcs_client.bucket("test-bucket")
         .blob(main.state_mgr.INVENTORY_BLOB_PATH)
         .upload_from_string(json.dumps(
             {"schema_version": "1.0", "data_dependencies": [],
              "images": []})))

        out = await t.complete_data_migration(ctx=mock_ctx)

        self.assertIn("SUCCESS", out)
        self.assertNotIn("can no longer be completed", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_record_born_under_a_close_says_it_is_uncompletable(
            self, mock_gcs_client_class, mock_get_email):
        """Both closed-under-us arms. `CLOSED_UNDER_US` itself says the runbook
        does not reflect this call, so without this the record appears in NO
        document that says it can never be completed — and
        `list_data_migrations()` refuses now, so the operator cannot go and
        look for themselves."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        cache = {"service": "elasticache", "identifier": "sessions",
                 "address": "aws_elasticache_cluster.sessions",
                 "disposition": "rebuild",
                 "evidence": ["envs/prod/data.tf"], "consumers": [],
                 "notes": []}

        # (a) the report that records the move, under a concurrent close.
        state_container, _, mock_ctx = self._at_data_migration(
            mock_gcs_client_class, entries=[cache])
        state_blob = (main.state_mgr.gcs_client.bucket("test-bucket")
                      .blob("platform/onboarding/state.json"))
        live = json.dumps(state_container["state"])
        closed = json.dumps(dict(state_container["state"],
                                 current_state="STATE_DEPLOYMENT_COMPLETED"))
        reads = {"n": 0}

        def closes_under_us(*_a, **_k):
            reads["n"] += 1
            return live if reads["n"] == 1 else closed

        state_blob.download_as_text.side_effect = closes_under_us

        out = await t.mark_data_service_migrating(
            address="aws_elasticache_cluster.sessions",
            note="RDB export running", ctx=mock_ctx)

        self.assertIn("no longer open", out)
        self.assertIn("can no longer be completed", out)
        # NOT the runbook pointer. This record was created after the close
        # wrote that document, so it appears in no section of it — and
        # CLOSED_UNDER_US two lines below says exactly that.
        self.assertNotIn("lists them", out)
        self.assertIn("does not reflect this call", out)

        # (b) the correction, under a concurrent close, with a move already
        #     in flight on another service.
        state_container, _, mock_ctx = self._at_data_migration(
            mock_gcs_client_class,
            entries=list(self.DATA_ENTRIES[:1]) + [cache])
        self._install_overrides_roundtrip()
        await t.mark_data_service_migrating(
            address="aws_elasticache_cluster.sessions",
            note="RDB export running", ctx=mock_ctx)
        live = json.dumps(state_container["state"])
        closed = json.dumps(dict(state_container["state"],
                                 current_state="STATE_DEPLOYMENT_COMPLETED"))
        reads["n"] = 0
        state_blob = (main.state_mgr.gcs_client.bucket("test-bucket")
                      .blob("platform/onboarding/state.json"))
        state_blob.download_as_text.side_effect = closes_under_us

        out = await discovery_datareview_3.tools.annotate_data_dependency(
            address="aws_db_instance.orders", disposition="keep-in-aws",
            note="stays in AWS", ctx=mock_ctx)

        self.assertIn("closed while this call was running", out)
        self.assertIn("can no longer be completed", out)
        self.assertIn("elasticache sessions", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_correction_over_a_live_cutover_names_the_cost(
            self, mock_gcs_client_class, mock_get_email):
        """The fourth site: `_amend`'s tail acknowledged the live cutover and
        offered the close in the same sentence, without saying the close ends
        the ability to complete it. This is `in_flight_not_owed`'s own
        documented route in — report a move, then record keep-in-aws."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        from servers.phases.discovery.discovery_datareview_3 import tools as r
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)

        await t.mark_data_service_migrating(
            address="aws_db_instance.orders", note="dump running",
            ctx=mock_ctx)
        out = await r.annotate_data_dependency(
            address="aws_db_instance.orders", disposition="keep-in-aws",
            note="stays in AWS after all", ctx=mock_ctx)

        self.assertIn("already reported IN PROGRESS", out)
        self.assertIn("frozen by that", out)
        self.assertIn("callable only from the data migration step", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_missing_inventory_names_no_scan_at_the_migration_step(
            self, mock_gcs_client_class, mock_get_email):
        """The fourth message on this path to need the distinction the other
        three draw. `scan_data_dependencies` runs at STATE_DISCOVERY_DATA_SCAN,
        whose only in-edges are from scoping and the assessment; from the
        deployment step the only reachable states are itself and the terminal.
        The step's own `_open` gets this right by naming no repair."""
        from servers.phases.discovery.discovery_datareview_3 import tools as r
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        (main.state_mgr.gcs_client.bucket("test-bucket")
         .blob(main.state_mgr.INVENTORY_BLOB_PATH)
         .download_as_text.side_effect) = exceptions.NotFound("Not Found")

        out = await r.annotate_data_dependency(
            address="aws_db_instance.orders", disposition="keep-in-aws",
            note="cannot move", ctx=mock_ctx)

        self.assertIn("ERROR", out)
        self.assertIn("No inventory found", out)
        self.assertNotIn("scan_data_dependencies", out)
        self.assertIn("Nothing was changed", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_listing_with_nothing_owed_omits_the_keep_in_aws_exit(
            self, mock_gcs_client_class, mock_get_email):
        """Round 42's fix in the second renderer. "It stops being owed and
        stops gating" describes a transition nothing can undergo when the line
        above says 0 are owed — and the listing prints an address only against
        an owed or settled entry, so this offered a call whose one argument the
        operator has not been given, against instructions that say never to
        construct one."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        rebuild_only = [{"service": "sqs", "identifier": "jobs",
                         "address": "aws_sqs_queue.jobs",
                         "disposition": "rebuild",
                         "evidence": ["envs/prod/data.tf"], "consumers": [],
                         "notes": []}]
        _, _, mock_ctx = self._at_data_migration(
            mock_gcs_client_class, entries=rebuild_only)

        out = await t.list_data_migrations(ctx=mock_ctx)

        self.assertIn("0 still owed", out)
        self.assertNotIn("annotate_data_dependency", out)
        self.assertNotIn("stops being owed", out)
        # The close is still offered — that is the exit that applies here.
        self.assertIn("Nothing is outstanding", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_repeating_a_report_verbatim_replaces_nothing(
            self, mock_gcs_client_class, mock_get_email):
        """The printed reporting call is re-runnable, and an operator amending
        a note re-supplies the target — so the same value arriving twice is
        ordinary. Without the inequality the one sentence whose job is to say
        something changed fires over a value that did not."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)

        await t.mark_data_service_migrated(
            address="aws_db_instance.orders", target="cloudsql-orders",
            ctx=mock_ctx)
        out = await t.mark_data_service_migrated(
            address="aws_db_instance.orders", target="cloudsql-orders",
            ctx=mock_ctx)

        self.assertNotIn("Replaced:", out)
        # ...and a genuine change still says so.
        changed = await t.mark_data_service_migrated(
            address="aws_db_instance.orders", target="cloudsql-orders-2",
            ctx=mock_ctx)
        self.assertIn("Replaced: target was cloudsql-orders.", changed)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_both_renderers_name_the_declaring_directory(
            self, mock_gcs_client_class, mock_get_email):
        """With one declaration of an address, `ambiguity()` finds no repeat
        and `reporting_call` emits `address=` alone — so the root module
        appears nowhere else in either artifact. Both clauses survive deletion
        independently, the same "both renderers, neither asserted" shape as
        round 35 finding 4."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)

        listing = await t.list_data_migrations(ctx=mock_ctx)
        runbook = self.mock_dm_runbook_blob.upload_from_string.call_args[0][0]

        self.assertIn("(in envs/prod)", listing)
        self.assertIn("in `envs/prod`", runbook)

        # And the root-module wording, which round 11 fixed in `_target`'s
        # refusal for the same reason: an empty directory rendered bare reads
        # as a missing field rather than as the root.
        at_root = [dict(self.DATA_ENTRIES[0], evidence=["data.tf"])]
        _, _, root_ctx = self._at_data_migration(
            mock_gcs_client_class, entries=at_root)
        await t.list_data_migrations(ctx=root_ctx)
        root_runbook = (
            self.mock_dm_runbook_blob.upload_from_string.call_args[0][0])

        self.assertIn("at the repository root", root_runbook)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_no_refusal_enumerates_a_population_that_is_empty(
            self, mock_gcs_client_class, mock_get_email):
        """All three refusals in `complete_data_migration` are nothing but an
        enumeration of what is holding the step open, and each gates its two
        halves on those halves being non-empty. Every gate was pinned in the
        OFF direction only — widening any of them to `True` emits "0 container
        image copy/copies are unconfirmed: ." or "0 data service(s) still owe a
        move: ." inside the one message whose whole content is the list, and
        offers an exit for a population that does not exist."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        ref = "1234.dkr.ecr.us-east-1.amazonaws.com/api:v1"
        copy_boilerplate = "container image copy"
        data_boilerplate = "still owe a move"

        def _inventory(entries, images):
            (main.state_mgr.gcs_client.bucket("test-bucket")
             .blob(main.state_mgr.INVENTORY_BLOB_PATH)
             .upload_from_string(json.dumps(
                 {"schema_version": "1.0",
                  "data_dependencies": [dict(e) for e in entries],
                  "images": images})))

        image = [{"ref": ref, "registry": "ecr", "repository": "api",
                  "tag": "v1", "pinned_by": "tag",
                  "provenance": [{"kind": "literal", "file": "k8s/api.yaml"}],
                  "replication": {"status": "self_service"}}]

        # 1. The transient refusal, and 2. the damaged-ledger refusal: both
        #    fire on owed databases, and neither may invent a copy.
        for damage, expected in (("transient", "try again"),
                                 ("absent", "this ledger is damaged")):
            _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
            _inventory(self.DATA_ENTRIES, [])
            baseline = (main.state_mgr.gcs_client.bucket("test-bucket")
                        .blob(discovery_datareview_3.tools.SCAN_BASELINE_BLOB))
            if damage == "transient":
                baseline.download_as_text.side_effect = (
                    exceptions.ServiceUnavailable("backend error"))
            else:
                baseline.download_as_text.side_effect = exceptions.NotFound("x")

            out = await t.complete_data_migration(ctx=mock_ctx)

            with self.subTest(refusal=damage):
                self.assertIn(expected, out)
                self.assertNotIn(copy_boilerplate, out)
                self.assertNotIn("mark_replication_complete", out)

        # 3. The not-finished refusal's copies arm: owed only, no copy.
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        _inventory(self.DATA_ENTRIES, [])
        out = await t.complete_data_migration(ctx=mock_ctx)
        with self.subTest(refusal="not finished, data only"):
            self.assertIn(data_boilerplate, out)
            self.assertNotIn(copy_boilerplate, out)
            # And the reason the graph will not move on. This is the
            # most-hit refusal in the step, the instructions have the agent
            # relay it verbatim and stop, so this paragraph is the only place
            # the answer to "can't we finish and report the rest later?"
            # reaches the user at the moment they ask it. Every other clause
            # of these three refusals is pinned; this one closed them all and
            # could be deleted with the suite green.
            self.assertIn("The graph stays here on purpose", out)

        # 4. ...and its data arm: one copy outstanding, nothing owed.
        _, _, mock_ctx = self._at_data_migration(
            mock_gcs_client_class, entries=[])
        _inventory([], image)
        out = await t.complete_data_migration(ctx=mock_ctx)
        with self.subTest(refusal="not finished, copies only"):
            self.assertIn(copy_boilerplate, out)
            self.assertNotIn(data_boilerplate, out)
            self.assertNotIn("keep-in-aws", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_transient_baseline_refusal_names_the_copies_too(
            self, mock_gcs_client_class, mock_get_email):
        """The close has three refusals answering "what is still holding this
        step open", and the other two name both populations. This one named the
        data half only, so an operator acting on the enumeration rather than on
        "try again" settles the databases and meets the copy at the next
        refusal."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        ref = "1234.dkr.ecr.us-east-1.amazonaws.com/api:v1"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        (main.state_mgr.gcs_client.bucket("test-bucket")
         .blob(main.state_mgr.INVENTORY_BLOB_PATH)
         .upload_from_string(json.dumps(
             {"schema_version": "1.0",
              "data_dependencies": [dict(e) for e in self.DATA_ENTRIES],
              "images": [{"ref": ref, "registry": "ecr", "repository": "api",
                          "tag": "v1", "pinned_by": "tag",
                          "provenance": [{"kind": "literal",
                                          "file": "k8s/api.yaml"}],
                          "replication": {"status": "self_service"}}]})))
        (main.state_mgr.gcs_client.bucket("test-bucket")
         .blob(discovery_datareview_3.tools.SCAN_BASELINE_BLOB)
         .download_as_text.side_effect) = exceptions.ServiceUnavailable(
             "backend error")

        out = await t.complete_data_migration(ctx=mock_ctx)

        self.assertIn("try again", out)
        self.assertIn("still owed", out)
        self.assertIn(ref, out)
        self.assertIn("abandon_image_replication", out)
        # And the data half names its exit too. This is the refusal where the
        # BASELINE failed and `_report` never touches it, so "reporting still
        # works" is true and not deducible from the message's own subject.
        self.assertIn("mark_data_service_migrated", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_persistent_elasticache_gets_a_target(
            self, mock_gcs_client_class, mock_get_email):
        """The harvester upgrades a Redis with snapshot_retention_limit > 0 to
        `migrate` because it persists. The step held the workspace open on it
        while printing "no default target", and the phase knowledge document
        said it was not work at all."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        entries = [{"service": "elasticache", "identifier": "sessions",
                    "address": "aws_elasticache_replication_group.sessions",
                    "disposition": "migrate", "evidence": ["envs/prod/data.tf"],
                    "notes": [], "consumers": []}]
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class, entries)

        out = await t.list_data_migrations(ctx=mock_ctx)

        self.assertIn("Memorystore", out)
        self.assertNotIn("no default target", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_caveat_on_a_completed_move_is_still_shown(
            self, mock_gcs_client_class, mock_get_email):
        """"Only the orders schema moved; the audit tables are still in RDS" is
        exactly what must not disappear before a component ships against that
        database — and it was rendered only while the entry was still owed."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        caveat = "only the orders schema moved; audit tables still in RDS"

        await t.mark_data_service_migrated(
            address="aws_db_instance.orders", target="orders-db-gcp",
            note=caveat, ctx=mock_ctx)

        out = await t.list_data_migrations(ctx=mock_ctx)
        self.assertIn(caveat, out)
        runbook = self.mock_dm_runbook_blob.upload_from_string.call_args[0][0]
        self.assertIn(caveat, runbook)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_runbook_prints_a_call_the_tool_accepts(
            self, mock_gcs_client_class, mock_get_email):
        """The runbook is the step's PRIMARY review-UI artifact and the listing
        points at it by path. Round 1 fixed the chat listing's disambiguator
        and left the runbook's, so it printed calls the tool refuses — the
        root-directory case too, where `if directory` drops `directory=""`."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        entries = [
            {"service": "rds", "identifier": "orders-db",
             "address": "aws_db_instance.orders", "disposition": "migrate",
             "evidence": ["data.tf"], "notes": [], "consumers": []},
            {"service": "rds", "identifier": "orders-db-prod",
             "address": "aws_db_instance.orders", "disposition": "migrate",
             "evidence": ["envs/prod/data.tf"], "notes": [], "consumers": []},
        ]
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class, entries)
        await t.list_data_migrations(ctx=mock_ctx)
        runbook = self.mock_dm_runbook_blob.upload_from_string.call_args[0][0]

        # The root entry must carry directory="", not have it dropped.
        self.assertIn('directory=""', runbook)
        self.assertIn('directory="envs/prod"', runbook)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_header_counts_one_population(
            self, mock_gcs_client_class, mock_get_email):
        """Widening `settled` mixed two populations into three numbers that
        contradicted each other in the one line whose job is to say how much
        is left."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        await t.mark_data_service_migrated(
            address="aws_db_instance.orders", ctx=mock_ctx)
        await t.mark_data_service_migrated(
            address="aws_elasticache_cluster.sessions", ctx=mock_ctx)

        out = await t.list_data_migrations(ctx=mock_ctx)

        # 2 graded migrate; 1 of those done; 1 owed. The rebuild entry is
        # reported separately rather than folded into the same arithmetic.
        self.assertIn("2 graded 'migrate' — 1 reported migrated, 1 still owed",
                      out)
        self.assertIn("1 more moved that this step was not waiting on", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_keeping_in_aws_from_the_step_points_at_a_reachable_tool(
            self, mock_gcs_client_class, mock_get_email):
        """The success message was written for the review and told the operator
        to call confirm_data_dependencies, which refuses here."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        self._install_overrides_roundtrip()

        out = await discovery_datareview_3.tools.annotate_data_dependency(
            address="aws_s3_bucket.exports", disposition="keep-in-aws",
            note="staying put", ctx=mock_ctx)

        self.assertNotIn("ERROR", out)
        self.assertNotIn("confirm_data_dependencies", out)
        self.assertIn("complete_data_migration", out)



    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_step_closes_once_nothing_is_owed(
            self, mock_gcs_client_class, mock_get_email):
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        state_container, _, mock_ctx = self._at_data_migration(
            mock_gcs_client_class)
        await t.mark_data_service_migrated(
            address="aws_db_instance.orders", target="orders-db-gcp",
            ctx=mock_ctx)
        await t.mark_data_service_migrated(
            address="aws_s3_bucket.exports", target="gs://exports",
            ctx=mock_ctx)

        out = await t.complete_data_migration(ctx=mock_ctx)

        self.assertIn("SUCCESS", out)
        self.assertIn("nothing outstanding", out)
        # The count is written to four places on this path — the stamp, both
        # history lines and this sentence. The other three are pinned; the one
        # the operator actually reads was the unasserted copy, so it could
        # report a different number from the audit log with nothing noticing.
        self.assertIn("2 service(s) moved", out)
        self.assertEqual(state_container["state"]["current_state"],
                         "STATE_DEPLOYMENT_COMPLETED")
        stamp = state_container["state"]["variables"]["data_migration_review"]
        self.assertEqual(stamp["migrated"], 2)
        # No outstanding count on the stamp: the step cannot close with work
        # owed, so it could only ever have been zero.
        self.assertNotIn("outstanding", stamp)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_keeping_the_last_one_in_aws_also_closes_the_step(
            self, mock_gcs_client_class, mock_get_email):
        """The exit for a service that genuinely cannot move. Without it the
        refusal above would be a trap rather than a decision."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        state_container, _, mock_ctx = self._at_data_migration(
            mock_gcs_client_class)
        self._install_overrides_roundtrip()
        await t.mark_data_service_migrated(
            address="aws_db_instance.orders", ctx=mock_ctx)

        await discovery_datareview_3.tools.annotate_data_dependency(
            address="aws_s3_bucket.exports", disposition="keep-in-aws",
            note="no equivalent worth the rebuild", ctx=mock_ctx)
        out = await t.complete_data_migration(ctx=mock_ctx)

        self.assertIn("SUCCESS", out)
        self.assertEqual(state_container["state"]["current_state"],
                         "STATE_DEPLOYMENT_COMPLETED")

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_later_session_is_told_what_is_still_outstanding(
            self, mock_gcs_client_class, mock_get_email):
        """The whole reason the graph parks here. Six weeks and an operator
        change later, get_next_stage is the only thing that will mention the
        outstanding databases — a terminal would answer with one line."""
        mock_get_email.return_value = "platform-user@google.com"
        self._at_data_migration(mock_gcs_client_class)

        payload = main.get_next_stage()

        self.assertIn("STATE_DEPLOYMENT_DATA_MIGRATION", payload)
        # The step's instructions, and the phase knowledge it declares.
        self.assertIn("waiting state", payload)
        self.assertIn("list_data_migrations", payload)
        self.assertIn("Database Migration Service", payload)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    @patch("servers.phases.deployment.actions._default_session")
    async def test_image_copies_are_not_trapped_behind_the_databases(
            self, mock_session_fn, mock_gcs_client_class, mock_get_email):
        """mark_replication_complete was terminal-only. With the graph parking
        at the data migration step until every database moves, that would have
        left a self-service image copy unreportable for as long as a Postgres
        takes."""
        mock_get_email.return_value = "platform-user@google.com"
        mock_session_fn.return_value = self._ar_session()
        inventory = {
            "schema_version": "1.0",
            "data_dependencies": [dict(e) for e in self.DATA_ENTRIES],
            "images": [{"ref": "1234.dkr.ecr.us-east-1.amazonaws.com/api:v1",
                        "registry": "ecr", "repository": "api", "tag": "v1",
                        "pinned_by": "tag",
                        "provenance": [{"kind": "literal", "file": "k8s/api.yaml"}],
                        "replication": {"status": "self_service"}}]}
        _, _, mock_ctx = self._at_data_migration(
            mock_gcs_client_class, entries=None)
        (main.state_mgr.gcs_client.bucket("test-bucket")
         .blob(main.state_mgr.INVENTORY_BLOB_PATH)
         .upload_from_string(json.dumps(inventory)))

        out = await mark_replication_complete(ctx=mock_ctx)

        self.assertNotIn("Invalid state", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_unconfirmed_image_copies_also_hold_the_step_open(
            self, mock_gcs_client_class, mock_get_email):
        """mark_replication_complete is callable only from here now, so closing
        with copies unreported would strand them exactly the way closing with
        databases outstanding would."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        inventory = {
            "schema_version": "1.0",
            "data_dependencies": [],
            "images": [{"ref": "1234.dkr.ecr.us-east-1.amazonaws.com/api:v1",
                        "registry": "ecr", "repository": "api", "tag": "v1",
                        "pinned_by": "tag",
                        "provenance": [{"kind": "literal", "file": "k8s/api.yaml"}],
                        "replication": {"status": "self_service"}}]}
        (main.state_mgr.gcs_client.bucket("test-bucket")
         .blob(main.state_mgr.INVENTORY_BLOB_PATH)
         .upload_from_string(json.dumps(inventory)))

        out = await t.complete_data_migration(ctx=mock_ctx)

        self.assertIn("ERROR", out)
        self.assertIn("image copy", out)
        self.assertIn("api:v1", out)
        self.assertIn("mark_replication_complete", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_declined_replication_does_not_hold_the_step_open(
            self, mock_gcs_client_class, mock_get_email):
        """An image with no replication record was never planned or attempted
        — replication was declined and the images stay in ECR by decision.
        There is nothing to assert, so nothing is owed."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        inventory = {
            "schema_version": "1.0", "data_dependencies": [],
            "images": [{"ref": "1234.dkr.ecr.us-east-1.amazonaws.com/api:v1",
                        "registry": "ecr", "repository": "api", "tag": "v1",
                        "pinned_by": "tag",
                        "provenance": [{"kind": "literal", "file": "k8s/api.yaml"}]}]}
        (main.state_mgr.gcs_client.bucket("test-bucket")
         .blob(main.state_mgr.INVENTORY_BLOB_PATH)
         .upload_from_string(json.dumps(inventory)))

        out = await t.complete_data_migration(ctx=mock_ctx)

        self.assertIn("SUCCESS", out)

    def _with_failed_copy(self, status="replication_failed"):
        return {"schema_version": "1.0", "data_dependencies": [],
                "images": [{"ref": "1234.dkr.ecr.us-east-1.amazonaws.com/api:v1",
                            "registry": "ecr", "repository": "api", "tag": "v1",
                            "pinned_by": "tag",
                            "provenance": [{"kind": "literal",
                                            "file": "k8s/api.yaml"}],
                            "replication": {
                                "status": status,
                                "destination": "us-central1-docker.pkg.dev/p/r/api:v1",
                                **({"error": "unauthorized: authentication required"}
                                   if status == "replication_failed" else {})}}]}

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_flipping_a_failed_copy_saves(
            self, mock_gcs_client_class, mock_get_email):
        """Pre-existing, latent until this CL: flipping a FAILED copy writes a
        `previous` record, `replication` declares additionalProperties: false,
        and `previous` was not among its properties — so the save was rejected
        and the repair lost. The pure test covered the dict mutation; nothing
        drove the tool end to end with a failed entry, which is the only way
        schema validation runs."""
        mock_get_email.return_value = "platform-user@google.com"
        _, inventory_container, mock_ctx = self._at_data_migration(
            mock_gcs_client_class)
        ref = "1234.dkr.ecr.us-east-1.amazonaws.com/api:v1"
        (main.state_mgr.gcs_client.bucket("test-bucket")
         .blob(main.state_mgr.INVENTORY_BLOB_PATH)
         .upload_from_string(json.dumps(self._with_failed_copy())))

        out = await mark_replication_complete(refs=[ref], ctx=mock_ctx)

        self.assertNotIn("ERROR", out)
        record = inventory_container["inventory"]["images"][0]["replication"]
        self.assertEqual(record["status"], "replicated")
        self.assertEqual(record["verified_by"], "user_asserted")
        self.assertEqual(record["previous"]["status"], "replication_failed")
        self.assertIn("unauthorized", record["previous"]["error"])

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_an_image_copy_can_be_abandoned_and_stops_blocking(
            self, mock_gcs_client_class, mock_get_email):
        """Without this the step is a trap: a failed copy nobody is going to
        chase would hold the segment open forever, since there is no image
        equivalent of keep-in-aws."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        _, inventory_container, mock_ctx = self._at_data_migration(
            mock_gcs_client_class)
        (main.state_mgr.gcs_client.bucket("test-bucket")
         .blob(main.state_mgr.INVENTORY_BLOB_PATH)
         .upload_from_string(json.dumps(self._with_failed_copy())))
        self.assertIn("ERROR", await t.complete_data_migration(ctx=mock_ctx))

        out = await abandon_image_replication(
            refs=["1234.dkr.ecr.us-east-1.amazonaws.com/api:v1"],
            reason="the base image is retired; the team is rebuilding it",
            ctx=mock_ctx)

        self.assertNotIn("ERROR", out)
        self.assertIn("stay in ECR", out)
        self.assertIn("egress", out)
        record = inventory_container["inventory"]["images"][0]["replication"]
        self.assertEqual(record["status"], "abandoned")
        self.assertEqual(record["abandoned_by"], "platform-user@google.com")
        self.assertIn("retired", record["reason"])
        # The failure it came from is kept, not overwritten.
        self.assertEqual(record["previous"]["status"], "replication_failed")
        # ...and the step can close.
        self.assertIn("SUCCESS", await t.complete_data_migration(ctx=mock_ctx))

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_abandoning_requires_a_reason_and_a_ref(
            self, mock_gcs_client_class, mock_get_email):
        """An abandoned image leaves a workload pulling across clouds forever.
        The next reader has to be able to tell that from an oversight."""
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)

        self.assertIn("reason= is required", await abandon_image_replication(
            refs=["1234.dkr.ecr.us-east-1.amazonaws.com/api:v1"], reason="  ",
            ctx=mock_ctx))
        self.assertIn("no bulk form", await abandon_image_replication(
            refs=[], reason="whatever", ctx=mock_ctx))

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_replicated_image_cannot_be_abandoned(
            self, mock_gcs_client_class, mock_get_email):
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        (main.state_mgr.gcs_client.bucket("test-bucket")
         .blob(main.state_mgr.INVENTORY_BLOB_PATH)
         .upload_from_string(json.dumps(self._with_failed_copy("replicated"))))

        out = await abandon_image_replication(
            refs=["1234.dkr.ecr.us-east-1.amazonaws.com/api:v1"],
            reason="changed my mind", ctx=mock_ctx)

        self.assertIn("already replicated", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    @patch("servers.phases.deployment.actions._default_session")
    async def test_an_abandonment_survives_a_bulk_mark_carrying_its_digest(
            self, mock_session_fn, mock_gcs_client_class, mock_get_email):
        """The asymmetry is enforced twice — bulk target SELECTION and the
        per-ref guard — and only the first was covered. Supplying a digest
        pulls a ref into the bulk target set, so the second guard is the only
        thing left, and this is the call shape the tool's own response asks
        for: "verify with skopeo and re-call with digests", a call that
        carries no refs."""
        mock_get_email.return_value = "platform-user@google.com"
        mock_session_fn.return_value = self._ar_session()
        _, inventory_container, mock_ctx = self._at_data_migration(
            mock_gcs_client_class)
        abandoned = "1234.dkr.ecr.us-east-1.amazonaws.com/api:v1"
        sibling = "1234.dkr.ecr.us-east-1.amazonaws.com/web:v1"
        inventory = self._with_failed_copy()
        inventory["images"].append({
            "ref": sibling, "registry": "ecr", "repository": "web",
            "tag": "v1", "pinned_by": "tag",
            "provenance": [{"kind": "literal", "file": "k8s/web.yaml"}],
            "replication": {"status": "self_service"}})
        (main.state_mgr.gcs_client.bucket("test-bucket")
         .blob(main.state_mgr.INVENTORY_BLOB_PATH)
         .upload_from_string(json.dumps(inventory)))
        await abandon_image_replication(
            refs=[abandoned], reason="upstream base image is gone",
            ctx=mock_ctx)

        await mark_replication_complete(
            digests={abandoned: "sha256:" + "b" * 64,
                     sibling: "sha256:" + "c" * 64}, ctx=mock_ctx)

        records = {i["ref"]: i["replication"]
                   for i in inventory_container["inventory"]["images"]}
        self.assertEqual(records[abandoned]["status"], "abandoned")
        # The control: the bulk call did do its job for the entry that was
        # not abandoned, so the assertion above is not passing on an inert
        # call.
        self.assertEqual(records[sibling]["status"], "replicated")

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_an_abandoned_image_survives_a_bulk_mark(
            self, mock_gcs_client_class, mock_get_email):
        """A bulk mark_replication_complete() must not resurrect a decision
        somebody made on purpose — but naming the ref re-opens it, because
        changing your mind is allowed if it is deliberate."""
        mock_get_email.return_value = "platform-user@google.com"
        _, inventory_container, mock_ctx = self._at_data_migration(
            mock_gcs_client_class)
        ref = "1234.dkr.ecr.us-east-1.amazonaws.com/api:v1"
        (main.state_mgr.gcs_client.bucket("test-bucket")
         .blob(main.state_mgr.INVENTORY_BLOB_PATH)
         .upload_from_string(json.dumps(self._with_failed_copy())))
        await abandon_image_replication(
            refs=[ref], reason="not worth chasing", ctx=mock_ctx)

        await mark_replication_complete(ctx=mock_ctx)
        self.assertEqual(
            inventory_container["inventory"]["images"][0]["replication"]["status"],
            "abandoned")

        out = await mark_replication_complete(refs=[ref], ctx=mock_ctx)

        self.assertNotIn("ERROR", out)
        record = inventory_container["inventory"]["images"][0]["replication"]
        self.assertEqual(record["status"], "replicated")
        self.assertEqual(record["previous"]["status"], "abandoned")
        self.assertIn("not worth chasing", record["previous"]["reason"])

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_nothing_is_callable_at_the_terminal(
            self, mock_gcs_client_class, mock_get_email):
        """A terminal means the workflow is finished. A tool that accepts calls
        there says the opposite."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)
        (main.state_mgr.gcs_client.bucket("test-bucket")
         .blob(main.state_mgr.INVENTORY_BLOB_PATH)
         .upload_from_string(json.dumps(
             {"schema_version": "1.0", "data_dependencies": [], "images": []})))
        self.assertIn("SUCCESS", await t.complete_data_migration(ctx=mock_ctx))

        from servers.phases.discovery import discovery_datareview_3
        explained = []
        for out in (await t.list_data_migrations(ctx=mock_ctx),
                    await t.mark_data_service_migrated(
                        address="aws_db_instance.orders", ctx=mock_ctx),
                    await t.mark_data_service_migrating(
                        address="aws_db_instance.orders", ctx=mock_ctx),
                    await t.complete_data_migration(ctx=mock_ctx),
                    await mark_replication_complete(ctx=mock_ctx),
                    await abandon_image_replication(
                        refs=["x"], reason="y", ctx=mock_ctx),
                    # The third tool this CL reaches into the deployment
                    # segment, and the only one that writes a durable change
                    # to the signed-off mapping. Widening CORRECTABLE_STATES
                    # by the terminal left the whole suite green.
                    await discovery_datareview_3.tools.annotate_data_dependency(
                        address="aws_db_instance.orders",
                        disposition="keep-in-aws", note="late",
                        ctx=mock_ctx)):
            self.assertIn("ERROR", out)
            self.assertIn("Invalid state", out)
            # The explanatory sentence too, wherever there is one. Three of
            # these messages state the park condition, the agent relays them
            # verbatim from the terminal, and each used to give a different
            # half-account: keyed on the data half alone, an estate with no
            # `migrate` service reads "this state passes straight through"
            # while the step is in fact holding for unconfirmed image copies.
            # `annotate_data_dependency`'s refusal is the review module's
            # generic one, shared with states this sentence would be wrong
            # about, so it names the state and stops.
            if "parks in until" in out or "does not close until" in out:
                explained.append(out)
                self.assertIn("keep-in-aws", out)
                self.assertIn("container image copy", out)
        # Counted, so the loop above cannot pass by every message quietly
        # losing its explanation: `_open`'s, and the two in the provision
        # module. The four reporting tools share `_open`'s.
        self.assertEqual(len(explained), 6, "\n\n".join(explained))

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_worklist_names_the_image_copies_too(
            self, mock_gcs_client_class, mock_get_email):
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        _, inventory_container, mock_ctx = self._at_data_migration(
            mock_gcs_client_class)
        inventory = dict(inventory_container["inventory"])
        inventory["images"] = [
            {"ref": "1234.dkr.ecr.us-east-1.amazonaws.com/api:v1",
             "registry": "ecr", "repository": "api", "tag": "v1",
             "pinned_by": "tag",
             "provenance": [{"kind": "literal", "file": "k8s/api.yaml"}],
             "replication": {"status": "replication_failed"}}]
        (main.state_mgr.gcs_client.bucket("test-bucket")
         .blob(main.state_mgr.INVENTORY_BLOB_PATH)
         .upload_from_string(json.dumps(inventory)))

        out = await t.list_data_migrations(ctx=mock_ctx)

        self.assertIn("also waiting on you", out)
        self.assertIn("api:v1", out)
        self.assertIn("un-rewritten", out)
        # BOTH exits, positively. The closed listing's suppression of this
        # line is pinned at two sites and its presence at none, so the exit
        # for a copy nobody is going to make — the counterpart of
        # `keep-in-aws`, and half of what keeps this step a decision rather
        # than a trap — could leave the chat surface entirely. The runbook's
        # identical footer is pinned; the instructions tell the agent the
        # listing carries the exact arguments and not to invent one, so this
        # is the only place that call reaches it.
        self.assertIn("mark_replication_complete(refs=", out)
        self.assertIn("abandon_image_replication(refs=", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_malformed_migrations_object_is_refused_not_raised(
            self, mock_gcs_client_class, mock_get_email):
        """The record of what has moved is the only one there is; a reader
        that crashes on it takes every tool in the step with it."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)

        for corrupt in ('[]', '{"migrations": [null]}', '{"migrations": "x"}',
                        'not json at all'):
            (main.state_mgr.gcs_client.bucket("test-bucket")
             .blob(datamigration_lib.MIGRATIONS_BLOB)
             .upload_from_string(corrupt))
            with self.subTest(corrupt=corrupt):
                for out in (
                        await t.list_data_migrations(ctx=mock_ctx),
                        await t.mark_data_service_migrated(
                            address="aws_db_instance.orders", ctx=mock_ctx),
                        await t.complete_data_migration(ctx=mock_ctx)):
                    self.assertIn("ERROR", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_service_can_be_kept_in_aws_from_the_migration_step(
            self, mock_gcs_client_class, mock_get_email):
        """The deadlock fix. Without a way to record keep-in-aws from here, a
        service that turns out not to be movable holds its components up
        forever: annotate_data_dependency was reachable only from the review,
        and the only route back is amend_discovery_scope from the assessment,
        which is long past by the deployment segment."""
        from servers.phases.deployment.deployment_datamigration_2 import tools as t
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, inventory_container, mock_ctx = self._at_data_migration(
            mock_gcs_client_class)
        # The review's amend path rebuilds from the scan baseline, which
        # the harness installs.
        self._install_overrides_roundtrip()

        out = await discovery_datareview_3.tools.annotate_data_dependency(
            address="aws_s3_bucket.exports", disposition="keep-in-aws",
            note="no equivalent worth the rebuild; staying on S3",
            ctx=mock_ctx)

        self.assertNotIn("ERROR", out)
        entry = next(e for e in inventory_container["inventory"]
                     ["data_dependencies"] if e["identifier"] == "exports")
        self.assertEqual(entry["disposition"], "keep-in-aws")
        # ...and it stops being owed.
        out = await t.list_data_migrations(ctx=mock_ctx)
        self.assertIn("1 graded 'migrate'", out)
        self.assertNotIn("s3 exports", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_mapping_itself_stays_sealed_at_the_migration_step(
            self, mock_gcs_client_class, mock_get_email):
        """Only the disposition opens up. Rejecting or attaching a consumer
        here would move the attribution under a mapping the reviewer signed
        off and extraction froze."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, _, mock_ctx = self._at_data_migration(mock_gcs_client_class)

        for out in (
                await discovery_datareview_3.tools.reject_data_consumer(
                    address="aws_db_instance.orders", workload="orders",
                    reason="changed my mind", ctx=mock_ctx),
                await discovery_datareview_3.tools.attach_data_consumer(
                    address="aws_s3_bucket.exports", workload="orders",
                    ctx=mock_ctx),
                await discovery_datareview_3.tools.list_data_dependencies(
                    ctx=mock_ctx)):
            self.assertIn("ERROR", out)
            self.assertIn("Invalid state", out)

    @staticmethod
    def _ar_session(status_code=200):
        session = MagicMock()
        session.get.return_value = MagicMock(status_code=status_code, text="")
        session.post.return_value = MagicMock(status_code=200, text="")
        return session

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    @patch("servers.phases.deployment.actions._default_session")
    async def test_prepare_image_deployment_provisions_and_completes(
            self, mock_session_fn, mock_gcs_client_class, mock_get_email):
        mock_get_email.return_value = "platform-user@google.com"
        session = self._ar_session(status_code=404)
        mock_session_fn.return_value = session

        # No inventory seeded: replication has nothing to do and the drain
        # still reaches the terminal.
        state_container, _, _, mock_ctx = self._setup_deployment_flow(
            mock_gcs_client_class)

        res = await prepare_image_deployment(ctx=mock_ctx)

        # The drain ran check (404) then create, asked the replication
        # question, and parked at the terminal. Both mutations' messages are
        # in the reply: replication's must not erase provisioning's.
        # The drain now parks at the data migration step; the terminal
        # is one explicit call further on.
        self.assertEqual(state_container["state"]["current_state"],
                         "STATE_DEPLOYMENT_DATA_MIGRATION")
        self.assertIn("created", res)
        dests = state_container["state"]["variables"]["artifact_registry_destinations"]
        self.assertEqual(
            dests[0]["url"], "us-central1-docker.pkg.dev/test-project/test-workspace")
        session.post.assert_called_once()
        mock_ctx.request_context.session.send_request.assert_called_once()
        self.assertIn("nothing to replicate", res)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    @patch("servers.phases.deployment.actions._default_session")
    async def test_deployment_completion_publishes_exports(
            self, mock_session_fn, mock_gcs_client_class, mock_get_email):
        # Parking at the data migration step (self-service leg here) must
        # publish the deployment slice: destinations, the image map with
        # planned self-service refs, and the do-not-conflate project rule.
        mock_get_email.return_value = "platform-user@google.com"
        mock_session_fn.return_value = self._ar_session()
        state_container, _, _, mock_ctx = self._setup_deployment_flow(
            mock_gcs_client_class, elicit_content=None,
            inventory=self.REPLICATION_INVENTORY)
        captured = self._install_exports_capture()

        res = await prepare_image_deployment(ctx=mock_ctx)

        self.assertEqual(state_container["state"]["current_state"],
                         "STATE_DEPLOYMENT_DATA_MIGRATION")
        self.assertIn("exports.json refreshed (deployment fields)", res)
        doc = captured["doc"]
        dest_url = "us-central1-docker.pkg.dev/test-project/test-workspace"
        self.assertEqual(doc["artifact_registry"]["destinations"], [dest_url])
        image_map = doc["artifact_registry"]["image_map"]
        self.assertEqual(set(image_map),
                         {f"{self.ECR_HOST}/payments/api:1.4.2", f"{self.ECR_HOST}/web:2.0"},
                         "the Docker Hub bystander stays out of the map")
        self.assertEqual(image_map[f"{self.ECR_HOST}/web:2.0"],
                         {"dest_ref": f"{dest_url}/web:2.0", "status": "self_service"})
        # The only recorded project is the ledger metadata project (the
        # provisioning default), which must never be presented as the target.
        self.assertIsNone(doc["project"])
        self.assertIsNone(doc["workload_pool"])
        self.assertEqual(doc["generations"],
                         {"discovery": 0, "translation": 0, "deployment": 1,
                          "data": 0})

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    @patch("servers.phases.deployment.actions._default_session")
    async def test_deployment_exports_failure_is_nonfatal(
            self, mock_session_fn, mock_gcs_client_class, mock_get_email):
        mock_get_email.return_value = "platform-user@google.com"
        mock_session_fn.return_value = self._ar_session()
        state_container, _, _, mock_ctx = self._setup_deployment_flow(
            mock_gcs_client_class, elicit_content=None,
            inventory=self.REPLICATION_INVENTORY)
        self.mock_exports_blob.upload_from_string.side_effect = RuntimeError("boom")

        res = await prepare_image_deployment(ctx=mock_ctx)

        self.assertEqual(state_container["state"]["current_state"],
                         "STATE_DEPLOYMENT_DATA_MIGRATION")
        self.assertIn("WARNING: exports.json publication failed", res)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    @patch("servers.phases.deployment.actions._default_session")
    async def test_replication_self_service_is_the_bare_accept_default(
            self, mock_session_fn, mock_gcs_client_class, mock_get_email):
        mock_get_email.return_value = "platform-user@google.com"
        mock_session_fn.return_value = self._ar_session()

        # A bare accept (no form body) must mean self-service: the runbook is
        # written, the server touches nothing, and the ECR entries are marked.
        state_container, inventory_container, runbook_container, mock_ctx = (
            self._setup_deployment_flow(
                mock_gcs_client_class, elicit_content=None,
                inventory=self.REPLICATION_INVENTORY))

        res = await prepare_image_deployment(ctx=mock_ctx)

        # The drain now parks at the data migration step; the terminal
        # is one explicit call further on.
        self.assertEqual(state_container["state"]["current_state"],
                         "STATE_DEPLOYMENT_DATA_MIGRATION")
        self.assertIn(replication.RUNBOOK_BLOB_PATH, res)
        runbook = runbook_container["text"]
        # Digest-pinned source wins; destination is the default registry with
        # the source path; credential steps are present but commented.
        self.assertIn(f"docker://{self.ECR_HOST}/payments/api@sha256:", runbook)
        self.assertIn(
            "docker://us-central1-docker.pkg.dev/test-project/test-workspace/payments/api:1.4.2",
            runbook)
        self.assertIn("aws ecr get-login-password", runbook)
        self.assertNotIn("busybox", runbook)
        statuses = {i["ref"]: i.get("replication", {}).get("status")
                    for i in inventory_container["inventory"]["images"]}
        self.assertEqual(statuses[f"{self.ECR_HOST}/payments/api:1.4.2"], "self_service")
        self.assertEqual(statuses[f"{self.ECR_HOST}/web:2.0"], "self_service")
        self.assertIsNone(statuses["busybox:stable"])
        # The remainder is reported, never blocking.
        self.assertIn("1 non-ECR image(s)", res)
        self.assertIn("1 render target(s)", res)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    @patch("servers.phases.deployment.actions._default_session")
    @patch("servers.phases.deployment.replication.google.auth.default",
           return_value=(MagicMock(valid=True, token="tok"), "test-project"))
    @patch("servers.phases.deployment.replication.AuthorizedSession")
    @patch("servers.phases.deployment.replication.subprocess.run")
    @patch("servers.phases.deployment.replication.shutil.which", return_value="/usr/bin/skopeo")
    async def test_replication_agent_mediated_copies_with_local_credentials(
            self, mock_which, mock_sub_run, mock_auth_session_cls,
            mock_auth_default, mock_session_fn, mock_gcs_client_class, mock_get_email):
        mock_get_email.return_value = "platform-user@google.com"
        mock_session_fn.return_value = self._ar_session()
        mock_auth_session_cls.return_value = self._ar_session()
        mock_sub_run.return_value = MagicMock(returncode=0, stdout="", stderr="")

        state_container, inventory_container, runbook_container, mock_ctx = (
            self._setup_deployment_flow(
                mock_gcs_client_class, elicit_content={"mode": "agent_mediated"},
                inventory=self.REPLICATION_INVENTORY))

        res = await prepare_image_deployment(ctx=mock_ctx)

        # The drain now parks at the data migration step; the terminal
        # is one explicit call further on.
        self.assertEqual(state_container["state"]["current_state"],
                         "STATE_DEPLOYMENT_DATA_MIGRATION")
        self.assertIn("Replicated 2/2", res)
        # One inspect for the single ECR host, then one copy per ECR image —
        # and no login invocation anywhere.
        commands = [c.args[0] for c in mock_sub_run.call_args_list]
        self.assertEqual([c[1] for c in commands], ["inspect", "copy", "copy"])
        self.assertNotIn("login", {c[1] for c in commands})
        copies = [c for c in commands if c[1] == "copy"]
        for copy in copies:
            self.assertIn("--preserve-digests", copy)
            self.assertIn("oauth2accesstoken:tok", copy)
        self.assertIn(f"docker://{self.ECR_HOST}/payments/api@sha256:" + "a" * 64, copies[0])
        self.assertIn(
            "docker://us-central1-docker.pkg.dev/test-project/test-workspace/payments/api:1.4.2",
            copies[0])
        statuses = {i["ref"]: i.get("replication", {}).get("status")
                    for i in inventory_container["inventory"]["images"]}
        self.assertEqual(statuses[f"{self.ECR_HOST}/payments/api:1.4.2"], "replicated")
        self.assertEqual(statuses[f"{self.ECR_HOST}/web:2.0"], "replicated")
        self.assertIsNone(statuses["busybox:stable"])
        self.assertNotIn("text", runbook_container)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    @patch("servers.phases.deployment.actions._default_session")
    @patch("servers.phases.deployment.replication.shutil.which", return_value=None)
    async def test_replication_missing_skopeo_parks_for_retry(
            self, mock_which, mock_session_fn, mock_gcs_client_class, mock_get_email):
        mock_get_email.return_value = "platform-user@google.com"
        mock_session_fn.return_value = self._ar_session()

        state_container, inventory_container, _, mock_ctx = (
            self._setup_deployment_flow(
                mock_gcs_client_class, elicit_content={"mode": "agent_mediated"},
                inventory=self.REPLICATION_INVENTORY))

        res = await prepare_image_deployment(ctx=mock_ctx)

        # on_failure returns to the agent task: the user installs skopeo,
        # then calls prepare_image_deployment again.
        self.assertEqual(state_container["state"]["current_state"], "STATE_DEPLOYMENT_INIT")
        self.assertIn("skopeo is not installed", res)
        self.assertNotIn("replication",
                         inventory_container["inventory"]["images"][0])

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    @patch("servers.phases.deployment.actions._default_session")
    @patch("servers.phases.deployment.replication.google.auth.default",
           return_value=(MagicMock(), "test-project"))
    @patch("servers.phases.deployment.replication.AuthorizedSession")
    @patch("servers.phases.deployment.replication.subprocess.run")
    @patch("servers.phases.deployment.replication.shutil.which", return_value="/usr/bin/skopeo")
    async def test_replication_missing_ecr_credentials_reports_fix_command(
            self, mock_which, mock_sub_run, mock_auth_session_cls,
            mock_auth_default, mock_session_fn, mock_gcs_client_class, mock_get_email):
        mock_get_email.return_value = "platform-user@google.com"
        mock_session_fn.return_value = self._ar_session()
        mock_auth_session_cls.return_value = self._ar_session()
        mock_sub_run.return_value = MagicMock(
            returncode=1, stdout="", stderr="unauthorized: authentication required")

        state_container, _, _, mock_ctx = self._setup_deployment_flow(
            mock_gcs_client_class, elicit_content={"mode": "agent_mediated"},
            inventory=self.REPLICATION_INVENTORY)

        res = await prepare_image_deployment(ctx=mock_ctx)

        # The preflight names the missing credential and the command the USER
        # runs — the server checked and stopped, it never logged in.
        self.assertEqual(state_container["state"]["current_state"], "STATE_DEPLOYMENT_INIT")
        self.assertIn("aws ecr get-login-password --region us-east-1", res)
        self.assertIn("never sets up credentials", res)
        commands = [c.args[0] for c in mock_sub_run.call_args_list]
        self.assertEqual({c[1] for c in commands}, {"inspect"})

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    @patch("servers.phases.deployment.actions._default_session")
    async def test_replication_declined_skips_and_completes(
            self, mock_session_fn, mock_gcs_client_class, mock_get_email):
        mock_get_email.return_value = "platform-user@google.com"
        mock_session_fn.return_value = self._ar_session()

        state_container, inventory_container, runbook_container, mock_ctx = (
            self._setup_deployment_flow(
                mock_gcs_client_class, elicit_action="decline",
                inventory=self.REPLICATION_INVENTORY))

        res = await prepare_image_deployment(ctx=mock_ctx)

        # The drain now parks at the data migration step; the terminal
        # is one explicit call further on.
        self.assertEqual(state_container["state"]["current_state"],
                         "STATE_DEPLOYMENT_DATA_MIGRATION")
        self.assertIn("skipped", res)
        self.assertNotIn("text", runbook_container)
        self.assertNotIn("replication",
                         inventory_container["inventory"]["images"][0])

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    @patch("servers.phases.deployment.actions._default_session")
    async def test_replication_bare_accept_overrides_stale_agent_opt_in(
            self, mock_session_fn, mock_gcs_client_class, mock_get_email):
        mock_get_email.return_value = "platform-user@google.com"
        mock_session_fn.return_value = self._ar_session()

        # A previous run's explicit agent_mediated opt-in survived in the
        # ledger (e.g. its preflight failed and parked the graph). This run's
        # bare accept means the DEFAULT: self-service — the stale answer must
        # not be replayed into an AWS-reading mode the user did not choose.
        state_container, inventory_container, runbook_container, mock_ctx = (
            self._setup_deployment_flow(
                mock_gcs_client_class, elicit_content=None,
                inventory=self.REPLICATION_INVENTORY,
                variables={"elicitation_responses": {
                    "STATE_DEPLOYMENT_IMAGE_REPLICATION": {"mode": "agent_mediated"}}}))

        res = await prepare_image_deployment(ctx=mock_ctx)

        # The drain now parks at the data migration step; the terminal
        # is one explicit call further on.
        self.assertEqual(state_container["state"]["current_state"],
                         "STATE_DEPLOYMENT_DATA_MIGRATION")
        self.assertIn(replication.RUNBOOK_BLOB_PATH, res)
        self.assertIn("text", runbook_container)
        statuses = {i.get("replication", {}).get("status")
                    for i in inventory_container["inventory"]["images"]
                    if i["registry"] == "ecr"}
        self.assertEqual(statuses, {"self_service"})
        # Consumed once: nothing left to replay next run either.
        self.assertNotIn(
            "STATE_DEPLOYMENT_IMAGE_REPLICATION",
            state_container["state"]["variables"].get("elicitation_responses", {}))

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    @patch("servers.phases.deployment.actions._default_session")
    @patch("servers.phases.deployment.replication.google.auth.default",
           return_value=(MagicMock(valid=True, token="tok"), "test-project"))
    @patch("servers.phases.deployment.replication.AuthorizedSession")
    @patch("servers.phases.deployment.replication.subprocess.run")
    @patch("servers.phases.deployment.replication.shutil.which", return_value="/usr/bin/skopeo")
    async def test_replication_partial_failure_completes_and_reports(
            self, mock_which, mock_sub_run, mock_auth_session_cls,
            mock_auth_default, mock_session_fn, mock_gcs_client_class, mock_get_email):
        mock_get_email.return_value = "platform-user@google.com"
        mock_session_fn.return_value = self._ar_session()
        mock_auth_session_cls.return_value = self._ar_session()

        def run_side_effect(cmd, **kwargs):
            # The web image's copy fails; everything else succeeds.
            if cmd[1] == "copy" and any("web" in a for a in cmd):
                return MagicMock(returncode=1, stdout="", stderr="connection reset")
            return MagicMock(returncode=0, stdout="", stderr="")
        mock_sub_run.side_effect = run_side_effect

        state_container, inventory_container, _, mock_ctx = (
            self._setup_deployment_flow(
                mock_gcs_client_class, elicit_content={"mode": "agent_mediated"},
                inventory=self.REPLICATION_INVENTORY))

        res = await prepare_image_deployment(ctx=mock_ctx)

        # Partial success never blocks: the graph completes with the failure
        # named in the message and recorded on the entry.
        # The drain now parks at the data migration step; the terminal
        # is one explicit call further on.
        self.assertEqual(state_container["state"]["current_state"],
                         "STATE_DEPLOYMENT_DATA_MIGRATION")
        self.assertIn("Replicated 1/2", res)
        self.assertIn("connection reset", res)
        statuses = {i["ref"]: i.get("replication", {}).get("status")
                    for i in inventory_container["inventory"]["images"]}
        self.assertEqual(statuses[f"{self.ECR_HOST}/payments/api:1.4.2"], "replicated")
        self.assertEqual(statuses[f"{self.ECR_HOST}/web:2.0"], "replication_failed")

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    @patch("servers.phases.deployment.actions._default_session")
    @patch("servers.phases.deployment.replication.google.auth.default",
           return_value=(MagicMock(valid=True, token="tok"), "test-project"))
    @patch("servers.phases.deployment.replication.AuthorizedSession")
    @patch("servers.phases.deployment.replication.subprocess.run")
    @patch("servers.phases.deployment.replication.shutil.which", return_value="/usr/bin/skopeo")
    async def test_replication_nothing_copied_parks_for_retry(
            self, mock_which, mock_sub_run, mock_auth_session_cls,
            mock_auth_default, mock_session_fn, mock_gcs_client_class, mock_get_email):
        mock_get_email.return_value = "platform-user@google.com"
        mock_session_fn.return_value = self._ar_session()
        mock_auth_session_cls.return_value = self._ar_session()

        def run_side_effect(cmd, **kwargs):
            # Preflight inspect passes; every copy fails.
            if cmd[1] == "copy":
                return MagicMock(returncode=1, stdout="", stderr="blob upload failed")
            return MagicMock(returncode=0, stdout="", stderr="")
        mock_sub_run.side_effect = run_side_effect

        state_container, _, _, mock_ctx = self._setup_deployment_flow(
            mock_gcs_client_class, elicit_content={"mode": "agent_mediated"},
            inventory=self.REPLICATION_INVENTORY)

        exports_blob = (main.state_mgr.gcs_client.bucket("test-bucket")
                        .blob(exports_lib.EXPORTS_BLOB))
        exports_blob.upload_from_string.reset_mock()

        res = await prepare_image_deployment(ctx=mock_ctx)

        self.assertEqual(state_container["state"]["current_state"], "STATE_DEPLOYMENT_INIT")
        self.assertIn("Nothing copied", res)
        # `SEGMENT_RUN` was pinned positively — reverting it to the terminal
        # fails and emptying it fails — but not against widening. The publish
        # is keyed on where the walk PARKED, and a failed replication parks
        # back here for the retry: publishing from this park would rewrite the
        # whole deployment slice and bump generations.deployment on the
        # strength of a run that just failed, once per attempt.
        self.assertNotIn("Exports:", res)
        self.assertFalse(
            exports_blob.upload_from_string.called,
            "the exports slice must not be republished from the retry park")

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    @patch("servers.phases.deployment.actions._default_session")
    @patch("servers.phases.deployment.replication.shutil.which", return_value="/usr/bin/skopeo")
    async def test_replication_agent_mode_with_unresolved_destination_parks(
            self, mock_which, mock_session_fn, mock_gcs_client_class, mock_get_email):
        mock_get_email.return_value = "platform-user@google.com"
        mock_session_fn.return_value = self._ar_session()

        # The ledger records only a computed-values destination (url null,
        # DESIGN.md known issue 15): agent-mediated cannot resolve a target
        # and parks; the message sends the user to apply the PR first.
        state_container, _, _, mock_ctx = self._setup_deployment_flow(
            mock_gcs_client_class, elicit_content={"mode": "agent_mediated"},
            inventory=self.REPLICATION_INVENTORY,
            variables={"artifact_registry_destinations": [
                {"project": None, "location": None,
                 "repository": "app_repo", "url": None}]})

        res = await prepare_image_deployment(ctx=mock_ctx)

        self.assertEqual(state_container["state"]["current_state"], "STATE_DEPLOYMENT_INIT")
        self.assertIn("Apply the migration PR", res)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    @patch("servers.phases.deployment.actions._default_session")
    async def test_replication_runbook_placeholder_for_unresolved_destination(
            self, mock_session_fn, mock_gcs_client_class, mock_get_email):
        mock_get_email.return_value = "platform-user@google.com"
        mock_session_fn.return_value = self._ar_session()

        state_container, _, runbook_container, mock_ctx = (
            self._setup_deployment_flow(
                mock_gcs_client_class, elicit_content={"mode": "self_service"},
                inventory=self.REPLICATION_INVENTORY,
                variables={"artifact_registry_destinations": [
                    {"project": None, "location": None,
                     "repository": "app_repo", "url": None}]}))

        res = await prepare_image_deployment(ctx=mock_ctx)

        # Self-service still works without a resolvable destination: the
        # placeholder is used AND explained.
        # The drain now parks at the data migration step; the terminal
        # is one explicit call further on.
        self.assertEqual(state_container["state"]["current_state"],
                         "STATE_DEPLOYMENT_DATA_MIGRATION")
        runbook = runbook_container["text"]
        self.assertIn("docker://<AR_DESTINATION>/payments/api:1.4.2", runbook)
        self.assertIn("replace every <AR_DESTINATION>", runbook)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    @patch("servers.phases.deployment.actions._default_session")
    @patch("servers.phases.deployment.replication.time.sleep")
    @patch("servers.phases.deployment.replication.google.auth.default",
           return_value=(MagicMock(valid=True, token="tok"), "test-project"))
    @patch("servers.phases.deployment.replication.AuthorizedSession")
    @patch("servers.phases.deployment.replication.subprocess.run")
    @patch("servers.phases.deployment.replication.shutil.which", return_value="/usr/bin/skopeo")
    async def test_replication_destination_check_tolerates_settling_create(
            self, mock_which, mock_sub_run, mock_auth_session_cls,
            mock_auth_default, mock_sleep, mock_session_fn, mock_gcs_client_class,
            mock_get_email):
        mock_get_email.return_value = "platform-user@google.com"
        mock_session_fn.return_value = self._ar_session()
        mock_sub_run.return_value = MagicMock(returncode=0, stdout="", stderr="")

        # Provisioning creates the repository but does not poll the LRO; the
        # preflight's first existence check races it (404) and must retry
        # rather than park with misleading terraform advice.
        preflight_session = MagicMock()
        preflight_session.get.side_effect = [
            MagicMock(status_code=404, text=""), MagicMock(status_code=200, text="")]
        mock_auth_session_cls.return_value = preflight_session

        state_container, _, _, mock_ctx = self._setup_deployment_flow(
            mock_gcs_client_class, elicit_content={"mode": "agent_mediated"},
            inventory=self.REPLICATION_INVENTORY)

        res = await prepare_image_deployment(ctx=mock_ctx)

        # The drain now parks at the data migration step; the terminal
        # is one explicit call further on.
        self.assertEqual(state_container["state"]["current_state"],
                         "STATE_DEPLOYMENT_DATA_MIGRATION")
        self.assertIn("Replicated 2/2", res)
        mock_sleep.assert_called_once_with(replication.DEST_CHECK_RETRY_S)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    @patch("servers.phases.deployment.actions._default_session")
    @patch("servers.phases.deployment.replication.time.sleep")
    @patch("servers.phases.deployment.replication.google.auth.default",
           return_value=(MagicMock(valid=True, token="tok"), "test-project"))
    @patch("servers.phases.deployment.replication.AuthorizedSession")
    @patch("servers.phases.deployment.replication.subprocess.run")
    @patch("servers.phases.deployment.replication.shutil.which", return_value="/usr/bin/skopeo")
    async def test_replication_destination_persistently_missing_parks(
            self, mock_which, mock_sub_run, mock_auth_session_cls,
            mock_auth_default, mock_sleep, mock_session_fn, mock_gcs_client_class,
            mock_get_email):
        mock_get_email.return_value = "platform-user@google.com"
        mock_session_fn.return_value = self._ar_session()
        mock_sub_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        mock_auth_session_cls.return_value = self._ar_session(status_code=404)

        state_container, _, _, mock_ctx = self._setup_deployment_flow(
            mock_gcs_client_class, elicit_content={"mode": "agent_mediated"},
            inventory=self.REPLICATION_INVENTORY)

        res = await prepare_image_deployment(ctx=mock_ctx)

        self.assertEqual(state_container["state"]["current_state"], "STATE_DEPLOYMENT_INIT")
        self.assertIn("does not exist yet", res)
        # All retries were burned before parking.
        self.assertEqual(mock_sleep.call_count, replication.DEST_CHECK_ATTEMPTS - 1)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    @patch("servers.phases.deployment.actions._default_session")
    async def test_replication_multiple_destinations_first_wins_and_is_reported(
            self, mock_session_fn, mock_gcs_client_class, mock_get_email):
        mock_get_email.return_value = "platform-user@google.com"
        mock_session_fn.return_value = self._ar_session()

        state_container, _, runbook_container, mock_ctx = (
            self._setup_deployment_flow(
                mock_gcs_client_class, elicit_content={"mode": "self_service"},
                inventory=self.REPLICATION_INVENTORY,
                variables={"artifact_registry_destinations": [
                    {"project": "p1", "location": "us-central1",
                     "repository": "first", "url": "us-central1-docker.pkg.dev/p1/first"},
                    {"project": "p2", "location": "us-east1",
                     "repository": "second", "url": "us-east1-docker.pkg.dev/p2/second"}]}))

        res = await prepare_image_deployment(ctx=mock_ctx)

        # First-wins, never silent: the runbook targets the first resolved
        # destination and the reply names the one left out.
        # The drain now parks at the data migration step; the terminal
        # is one explicit call further on.
        self.assertEqual(state_container["state"]["current_state"],
                         "STATE_DEPLOYMENT_DATA_MIGRATION")
        self.assertIn("docker://us-central1-docker.pkg.dev/p1/first/payments/api:1.4.2",
                      runbook_container["text"])
        self.assertNotIn("p2/second", runbook_container["text"])
        self.assertIn("not targeted: us-east1-docker.pkg.dev/p2/second", res)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    @patch("servers.phases.deployment.actions._default_session")
    @patch("servers.phases.deployment.replication.google.auth.default",
           return_value=(MagicMock(valid=True, token="tok"), "test-project"))
    @patch("servers.phases.deployment.replication.AuthorizedSession")
    @patch("servers.phases.deployment.replication.subprocess.run")
    @patch("servers.phases.deployment.replication.shutil.which", return_value="/usr/bin/skopeo")
    async def test_replication_colliding_paths_are_disambiguated(
            self, mock_which, mock_sub_run, mock_auth_session_cls,
            mock_auth_default, mock_session_fn, mock_gcs_client_class, mock_get_email):
        mock_get_email.return_value = "platform-user@google.com"
        mock_session_fn.return_value = self._ar_session()
        mock_auth_session_cls.return_value = self._ar_session()
        mock_sub_run.return_value = MagicMock(returncode=0, stdout="", stderr="")

        # The same repository path and tag from two ECR accounts: mirrored
        # naively, both would land on one destination ref and the tag would
        # point at whichever copied last.
        host_a = "111111111111.dkr.ecr.us-east-1.amazonaws.com"
        host_b = "222222222222.dkr.ecr.eu-west-1.amazonaws.com"
        inventory = {"images": [
            {"ref": f"{h}/payments/api:1.4.2", "registry": "ecr",
             "repository": f"{h}/payments/api", "tag": "1.4.2", "digest": None,
             "pinned_by": "tag",
             "provenance": [{"kind": "literal", "file": "k8s/deploy.yaml"}]}
            for h in (host_a, host_b)
        ]}
        state_container, inventory_container, _, mock_ctx = (
            self._setup_deployment_flow(
                mock_gcs_client_class, elicit_content={"mode": "agent_mediated"},
                inventory=inventory))

        res = await prepare_image_deployment(ctx=mock_ctx)

        # The drain now parks at the data migration step; the terminal
        # is one explicit call further on.
        self.assertEqual(state_container["state"]["current_state"],
                         "STATE_DEPLOYMENT_DATA_MIGRATION")
        self.assertIn("Replicated 2/2", res)
        self.assertIn("Destination collision", res)
        destinations = {i["replication"]["destination"]
                        for i in inventory_container["inventory"]["images"]}
        self.assertEqual(len(destinations), 2)
        for dst in destinations:
            self.assertIn("-dkr-ecr-", dst)  # host-prefixed path
            self.assertTrue(dst.endswith("/payments/api:1.4.2"))

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    @patch("servers.phases.deployment.actions._default_session")
    @patch("servers.phases.deployment.replication.google.auth.default")
    @patch("servers.phases.deployment.replication.AuthorizedSession")
    @patch("servers.phases.deployment.replication.subprocess.run")
    @patch("servers.phases.deployment.replication.shutil.which", return_value="/usr/bin/skopeo")
    async def test_replication_refreshes_expired_credentials_per_image(
            self, mock_which, mock_sub_run, mock_auth_session_cls,
            mock_auth_default, mock_session_fn, mock_gcs_client_class, mock_get_email):
        mock_get_email.return_value = "platform-user@google.com"
        mock_session_fn.return_value = self._ar_session()
        mock_auth_session_cls.return_value = self._ar_session()
        mock_sub_run.return_value = MagicMock(returncode=0, stdout="", stderr="")

        # A copy run can outlive one access token: expired credentials must
        # be refreshed before each affected copy, not fetched once.
        credentials = MagicMock(valid=False, token="tok")
        mock_auth_default.return_value = (credentials, "test-project")

        state_container, _, _, mock_ctx = self._setup_deployment_flow(
            mock_gcs_client_class, elicit_content={"mode": "agent_mediated"},
            inventory=self.REPLICATION_INVENTORY)

        res = await prepare_image_deployment(ctx=mock_ctx)

        # The drain now parks at the data migration step; the terminal
        # is one explicit call further on.
        self.assertEqual(state_container["state"]["current_state"],
                         "STATE_DEPLOYMENT_DATA_MIGRATION")
        self.assertIn("Replicated 2/2", res)
        # Two ECR images, credentials never became valid: one refresh each.
        self.assertEqual(credentials.refresh.call_count, 2)

    AR_DEST_URL = "us-central1-docker.pkg.dev/test-project/test-workspace"

    def _self_service_inventory(self):
        """REPLICATION_INVENTORY after a self-service replicate_images run:
        both ECR entries carry the plan, the bystander carries nothing."""
        inventory = json.loads(json.dumps(self.REPLICATION_INVENTORY))
        for image in inventory["images"]:
            if image["registry"] == "ecr":
                image["replication"] = {"status": "self_service"}
        return inventory

    def _setup_deployment_at_the_waiting_step(self, mock_gcs_client_class, inventory,
                                    current_state="STATE_DEPLOYMENT_DATA_MIGRATION",
                                    extra_variables=None):
        """Fixture for the post-runbook path: authenticated platform user
        parked in the step the segment waits in, destination recorded,
        inventory seeded with replication outcomes, round-trips installed.

        Not the terminal: a terminal state means the workflow is finished, so
        nothing is callable there. Everything the server cannot observe for
        itself is confirmed from the data migration step."""
        state = {"current_state": current_state, "history": [],
                 "variables": {"artifact_registry_destinations": [
                     {"url": self.AR_DEST_URL, "project": "test-project",
                      "location": "us-central1",
                      "repository": "test-workspace"}]}}
        state["variables"].update(extra_variables or {})
        main.state_mgr.write_local_config(
            "gs://test-bucket", "platform", "test-workspace", "test-project")
        mock_gcs_client, _, _, _, _ = self.setup_mock_gcs(
            mock_gcs_client_class,
            registry_yaml="roles:\n  platform: ['platform-user@google.com']",
            state_dict=state)
        main.state_mgr.gcs_client = mock_gcs_client
        inventory_container = self._install_inventory_roundtrip()
        self.mock_inventory_blob.upload_from_string(json.dumps(inventory))
        return inventory_container

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_mark_replication_complete_flips_and_republishes(
            self, mock_gcs_client_class, mock_get_email):
        # The bulk path: the user says the runbook has been run, both
        # self-service entries flip to user-asserted replicated, the one
        # supplied digest is recorded, and the exports image_map republish
        # happens in the same call.
        mock_get_email.return_value = "platform-user@google.com"
        inventory_container = self._setup_deployment_at_the_waiting_step(
            mock_gcs_client_class, self._self_service_inventory())
        captured = self._install_exports_capture()
        api_ref = f"{self.ECR_HOST}/payments/api:1.4.2"
        digest = "sha256:" + "d" * 64

        res = await mark_replication_complete(digests={api_ref: digest})

        self.assertIn("Marked 2 image(s) replicated", res)
        self.assertIn("1 with a recorded content digest", res)
        outcomes = {i["ref"]: i.get("replication")
                    for i in inventory_container["inventory"]["images"]}
        self.assertEqual(outcomes[api_ref], {
            "status": "replicated",
            "destination": f"{self.AR_DEST_URL}/payments/api:1.4.2",
            "verified_by": "user_asserted", "content_digest": digest})
        self.assertEqual(outcomes[f"{self.ECR_HOST}/web:2.0"], {
            "status": "replicated",
            "destination": f"{self.AR_DEST_URL}/web:2.0",
            "verified_by": "user_asserted"},
            "no digest supplied for web, none recorded")
        self.assertIsNone(outcomes["busybox:stable"],
                          "the Docker Hub bystander stays untouched")
        image_map = captured["doc"]["artifact_registry"]["image_map"]
        self.assertEqual(image_map[api_ref]["status"], "replicated")
        self.assertEqual(image_map[api_ref]["content_digest"], digest)
        self.assertEqual(image_map[f"{self.ECR_HOST}/web:2.0"]["status"],
                         "replicated")
        self.assertIn("image_map now lists 2 replicated image(s)", res)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_mark_replication_complete_refuses_before_the_segment_runs(
            self, mock_gcs_client_class, mock_get_email):
        # Before the deployment segment has run there are no outcomes to
        # assert over; the tool refuses and nothing is written.
        mock_get_email.return_value = "platform-user@google.com"
        inventory_container = self._setup_deployment_at_the_waiting_step(
            mock_gcs_client_class, self._self_service_inventory(),
            current_state="STATE_DEPLOYMENT_INIT")

        res = await mark_replication_complete()

        self.assertIn("ERROR: Invalid state for mark_replication_complete", res)
        statuses = [i.get("replication", {}).get("status")
                    for i in inventory_container["inventory"]["images"]
                    if i["registry"] == "ecr"]
        self.assertEqual(statuses, ["self_service", "self_service"])

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_mark_replication_complete_partial_names_the_unknown(
            self, mock_gcs_client_class, mock_get_email):
        # A known ref is marked; an unknown one is named, never created.
        mock_get_email.return_value = "platform-user@google.com"
        inventory_container = self._setup_deployment_at_the_waiting_step(
            mock_gcs_client_class, self._self_service_inventory())
        api_ref = f"{self.ECR_HOST}/payments/api:1.4.2"
        ghost = f"{self.ECR_HOST}/ghost:9.9"

        res = await mark_replication_complete(refs=[api_ref, ghost])

        self.assertIn("Marked 1 image(s) replicated", res)
        self.assertIn(f"Not marked — {ghost}: not in the discovery inventory",
                      res)
        outcomes = {i["ref"]: i.get("replication", {}).get("status")
                    for i in inventory_container["inventory"]["images"]}
        self.assertEqual(outcomes[api_ref], "replicated")
        self.assertEqual(outcomes[f"{self.ECR_HOST}/web:2.0"], "self_service",
                         "the un-named self-service entry is left alone")
        self.assertNotIn(ghost, outcomes)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_mark_replication_complete_is_idempotent(
            self, mock_gcs_client_class, mock_get_email):
        # A second bulk call finds nothing left, records nothing, and does
        # not republish exports; re-naming a marked ref is calm.
        mock_get_email.return_value = "platform-user@google.com"
        self._setup_deployment_at_the_waiting_step(
            mock_gcs_client_class, self._self_service_inventory())
        captured = self._install_exports_capture()

        res = await mark_replication_complete()
        self.assertIn("Marked 2 image(s) replicated", res)
        captured.clear()

        res = await mark_replication_complete()
        self.assertIn("Nothing to mark", res)
        self.assertIn("exports.json unchanged", res)
        self.assertEqual(captured, {}, "no exports republish without a change")

        api_ref = f"{self.ECR_HOST}/payments/api:1.4.2"
        res = await mark_replication_complete(refs=[api_ref])
        self.assertIn("Nothing marked.", res)
        self.assertIn(f"Already replicated (left as recorded): {api_ref}", res)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_mark_replication_complete_records_a_digest_supplied_later(
            self, mock_gcs_client_class, mock_get_email):
        # The tool's own response tells the user to go verify with `skopeo
        # inspect` and re-call with the digest, so the re-call must save and
        # republish rather than report the entry as already done.
        mock_get_email.return_value = "platform-user@google.com"
        container = self._setup_deployment_at_the_waiting_step(
            mock_gcs_client_class, self._self_service_inventory())
        api_ref = f"{self.ECR_HOST}/payments/api:1.4.2"
        digest = "sha256:" + "d" * 64
        await mark_replication_complete()
        captured = self._install_exports_capture()

        res = await mark_replication_complete(digests={api_ref: digest})

        self.assertIn("Already replicated, record improved with the value(s) "
                      f"you supplied: {api_ref}", res)
        outcomes = {i["ref"]: i.get("replication") or {}
                    for i in container["inventory"]["images"]}
        self.assertEqual(outcomes[api_ref].get("content_digest"), digest)
        self.assertEqual(
            captured["doc"]["artifact_registry"]["image_map"][api_ref]
            ["content_digest"], digest, "the upgrade reaches consumers")

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_mark_replication_complete_without_the_clone_is_degraded(
            self, mock_gcs_client_class, mock_get_email):
        # The runbook can be run weeks later on a machine that never had the
        # target clone. Republishing the whole slice from there would null
        # out cluster/project; only the image map may be republished.
        mock_get_email.return_value = "platform-user@google.com"
        self._setup_deployment_at_the_waiting_step(
            mock_gcs_client_class, self._self_service_inventory(),
            extra_variables={"target_clone_path": "/no/such/clone"})
        captured = self._install_exports_capture()

        res = await mark_replication_complete()

        self.assertIn("Marked 2 image(s) replicated", res)
        self.assertIn("only the exports image_map was republished", res)
        notes = captured["doc"]["derivation_notes"]
        self.assertTrue(any("keep their last published values" in n
                            for n in notes))
        self.assertFalse(any(n.startswith("deployment: cluster") for n in notes),
                         "a field that was not recomputed gets no fresh note")

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_resolve_lz_decision_echoes_workspace_coordinates(self, mock_gcs_client_class, mock_get_email):
        # The design step allocates the target clone workspace on the first
        # decision. The agent's instructions have it read the branch and clone
        # path back from this tool's response before writing any HCL, so the
        # success message must carry them — on the allocating call and every
        # re-entry, not just leave them buried in state.
        mock_get_email.return_value = "platform-user@google.com"

        state_container = {
            "state": {
                "current_state": "STATE_LZ_DESIGN",
                "history": [],
                "variables": {"source_path": "/src"},
            }
        }
        main.state_mgr.write_local_config("gs://test-bucket", "platform", "test-workspace", "test-project")
        mock_gcs_client, _, mock_state_blob, _, _ = self.setup_mock_gcs(
            mock_gcs_client_class,
            registry_yaml="roles:\n  platform: ['platform-user@google.com']",
            state_dict=state_container["state"],
        )
        main.state_mgr.gcs_client = mock_gcs_client

        def upload_side_effect(data_str, **kwargs):
            state_container["state"] = json.loads(data_str)
            mock_state_blob.download_as_text.return_value = data_str
        mock_state_blob.upload_from_string.side_effect = upload_side_effect

        # First decision allocates the workspace; the response must echo it.
        res = await main.resolve_lz_decision("karpenter", "GKE_STANDARD_NAP")
        variables = state_container["state"]["variables"]
        self.assertIn(f"lz_branch_uuid: {variables['lz_branch_uuid']}", res)
        self.assertIn(f"lz_branch_name: {variables['lz_branch_name']}", res)
        self.assertIn(f"target_clone_path: {variables['target_clone_path']}", res)

        # Re-entry keeps the same coordinates and still reports them.
        res2 = await main.resolve_lz_decision("vpc_peering", "PUBLIC_AUTHORIZED_NETS")
        self.assertIn(f"lz_branch_name: {variables['lz_branch_name']}", res2)
        self.assertIn(f"target_clone_path: {variables['target_clone_path']}", res2)
        # No inventory anywhere: the network echo says so and offers only the
        # unchecked baseline, rather than failing the decision.
        self.assertIn("source_address_space: not recorded", res2)
        self.assertIn("proposed_target_ranges: nodes 10.0.0.0/22, pods 10.4.0.0/16, "
                      "services 10.20.0.0/20 (the baseline, not checked against the source)", res2)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_resolve_lz_decision_reads_the_ledger_when_state_lacks_the_section(
            self, mock_gcs_client_class, mock_get_email):
        # A workspace whose state variables carry an inventory written before
        # this section existed, while the ledger blob has since been re-scanned:
        # the echo must read the blob rather than report nothing.
        mock_get_email.return_value = "platform-user@google.com"
        state_container = {
            "state": {
                "current_state": "STATE_LZ_DESIGN",
                "history": [],
                "variables": {"source_path": "/src",
                              "discovery_inventory": {"clusters": [], "triggers": {}}},
            }
        }
        main.state_mgr.write_local_config("gs://test-bucket", "platform", "test-workspace", "test-project")
        mock_gcs_client, _, mock_state_blob, _, _ = self.setup_mock_gcs(
            mock_gcs_client_class,
            registry_yaml="roles:\n  platform: ['platform-user@google.com']",
            state_dict=state_container["state"],
        )
        main.state_mgr.gcs_client = mock_gcs_client

        def upload_side_effect(data_str, **kwargs):
            state_container["state"] = json.loads(data_str)
            mock_state_blob.download_as_text.return_value = data_str
        mock_state_blob.upload_from_string.side_effect = upload_side_effect
        ledger_inventory = {
            "clusters": [], "triggers": {},
            "address_space": {"vpcs": [{"name": "v", "address": "aws_vpc.v", "path": "vpc.tf",
                                        "form": "resource", "cidr": "10.0.0.0/16",
                                        "secondary_cidrs": [], "cluster_vpc": True,
                                        "evidence": []}],
                              "subnets": [], "clusters": [], "routes": [], "unresolved": []},
        }
        with patch.object(main.state_mgr, "load_inventory", return_value=(ledger_inventory, 3)):
            res = await main.resolve_lz_decision("karpenter", "GKE_STANDARD_NAP")
        self.assertIn("source_address_space: 10.0.0.0/16 (cluster VPC v)", res)
        # 10.0.0.0/16 covers the baseline nodes block, so nodes moves.
        self.assertIn("moved_off_the_baseline: nodes", res)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_resolve_lz_decision_echoes_the_source_ranges_and_a_proposal(
            self, mock_gcs_client_class, mock_get_email):
        # The design step's network default comes from this echo, not from the
        # knowledge document's baseline: with the acme estate's peered
        # 10.20.0.0/16 recorded, the services block has to move off
        # 10.20.0.0/20, and the reply has to say which ranges it kept clear of.
        mock_get_email.return_value = "platform-user@google.com"
        state_container = {
            "state": {
                "current_state": "STATE_LZ_DESIGN",
                "history": [],
                "variables": {
                    "source_path": "/src",
                    "discovery_inventory": {
                        "clusters": [], "triggers": {},
                        "address_space": {
                            "vpcs": [{"name": "acme_prod", "address": "aws_vpc.acme_prod",
                                      "path": "vpc.tf", "form": "resource",
                                      "cidr": "10.42.0.0/16", "secondary_cidrs": [],
                                      "cluster_vpc": True, "evidence": []}],
                            "subnets": [], "clusters": [],
                            "routes": [{"destination": "10.20.0.0/16",
                                        "via": "vpc_peering_connection",
                                        "address": "aws_route.peer", "path": "vpc.tf",
                                        "evidence": []}],
                            "unresolved": [{"address": "module.vpc", "path": "vpc.tf",
                                            "argument": "private_subnets",
                                            "expression": "local.private_subnets"}],
                        },
                    },
                },
            }
        }
        main.state_mgr.write_local_config("gs://test-bucket", "platform", "test-workspace", "test-project")
        mock_gcs_client, _, mock_state_blob, _, _ = self.setup_mock_gcs(
            mock_gcs_client_class,
            registry_yaml="roles:\n  platform: ['platform-user@google.com']",
            state_dict=state_container["state"],
        )
        main.state_mgr.gcs_client = mock_gcs_client

        def upload_side_effect(data_str, **kwargs):
            state_container["state"] = json.loads(data_str)
            mock_state_blob.download_as_text.return_value = data_str
        mock_state_blob.upload_from_string.side_effect = upload_side_effect

        res = await main.resolve_lz_decision("karpenter", "GKE_STANDARD_NAP")
        self.assertIn("source_address_space: 10.42.0.0/16 (cluster VPC acme_prod); "
                      "10.20.0.0/16 (routed via vpc_peering_connection)", res)
        self.assertIn("proposed_target_ranges: nodes 10.0.0.0/22, pods 10.4.0.0/16, "
                      "services 10.0.16.0/20", res)
        self.assertIn("moved_off_the_baseline: services", res)
        self.assertIn("unresolved_source_ranges: 1", res)

    def _install_inventory_roundtrip(self):
        """Makes writes to the inventory blob readable by subsequent loads."""
        inventory_container = {"generation": 6}

        def inventory_upload_side_effect(data_str, **kwargs):
            inventory_container["inventory"] = json.loads(data_str)
            self.mock_inventory_blob.reload.side_effect = None
            self.mock_inventory_blob.download_as_text.return_value = data_str
            # Incremented, as GCS does. A fixed generation makes every
            # optimistic-concurrency precondition in the code under test pass
            # unconditionally, so a tool that reads the section and writes a
            # claim about it later cannot be shown to notice the difference.
            inventory_container["generation"] += 1
            self.mock_inventory_blob.generation = inventory_container["generation"]

        self.mock_inventory_blob.upload_from_string.side_effect = inventory_upload_side_effect
        return inventory_container

    def _install_overrides_roundtrip(self):
        """Makes the data review's corrections readable by subsequent loads."""
        container = {"generation": 10}

        def overrides_upload_side_effect(data_str, **kwargs):
            container["document"] = json.loads(data_str)
            self.mock_overrides_blob.reload.side_effect = None
            self.mock_overrides_blob.download_as_text.side_effect = None
            self.mock_overrides_blob.download_as_text.return_value = data_str
            # Incremented, for the same reason the inventory's is: a fixed
            # generation makes every if_generation_match pass unconditionally,
            # so no test could observe a conflict on this blob either.
            container["generation"] += 1
            self.mock_overrides_blob.generation = container["generation"]

        self.mock_overrides_blob.upload_from_string.side_effect = overrides_upload_side_effect
        return container

    def _install_exports_capture(self):
        """Captures the exports.json document written by a publish hook.

        Each write reads back, so a tool that publishes more than one source
        in a single call — discovery extraction publishes the discovery slice
        and then the data_gate slice — leaves the ACCUMULATED document in
        `doc` rather than the last slice laid over an empty skeleton.
        `kwargs` is the FIRST write's: that is the one that has to be a
        create, and a later slice in the same call is legitimately an
        overwrite. `writes` is every document in order, for a test that cares
        which publish carried what.
        """
        captured = {"writes": []}

        def exports_upload_side_effect(data_str, **kwargs):
            captured["doc"] = json.loads(data_str)
            captured["writes"].append(captured["doc"])
            captured.setdefault("kwargs", kwargs)
            self.mock_exports_blob.reload.side_effect = None
            self.mock_exports_blob.download_as_text.side_effect = None
            self.mock_exports_blob.download_as_text.return_value = data_str
            self.mock_exports_blob.generation = len(captured["writes"])

        self.mock_exports_blob.upload_from_string.side_effect = exports_upload_side_effect
        return captured

    def _install_exports_roundtrip(self):
        """Like the capture, but written documents read back on the next call
        — what refresh_exports needs to see its own previous publish."""
        container = {}

        def exports_upload_side_effect(data_str, **kwargs):
            container["doc"] = json.loads(data_str)
            self.mock_exports_blob.reload.side_effect = None
            self.mock_exports_blob.download_as_text.side_effect = None
            self.mock_exports_blob.download_as_text.return_value = data_str
            self.mock_exports_blob.generation = 11

        self.mock_exports_blob.upload_from_string.side_effect = exports_upload_side_effect
        return container

    def _setup_review_state(self, mock_gcs_client_class, current_state="STATE_DISCOVERY_RUNNING"):
        """Common fixture: authenticated platform user parked mid-discovery,
        with saved state readable back (a write's transition must be
        observable by the next call)."""
        state_container = {"state": {
            "current_state": current_state, "history": [],
            "variables": {"source_path": "/src"}}}
        main.state_mgr.write_local_config("gs://test-bucket", "platform", "test-workspace", "test-project")
        mock_gcs_client, _, mock_state_blob, _, _ = self.setup_mock_gcs(
            mock_gcs_client_class,
            registry_yaml="roles:\n  platform: ['platform-user@google.com']",
            state_dict=state_container["state"]
        )
        main.state_mgr.gcs_client = mock_gcs_client

        def upload_side_effect(data_str, **kwargs):
            state_container["state"] = json.loads(data_str)
            mock_state_blob.download_as_text.return_value = data_str
        mock_state_blob.upload_from_string.side_effect = upload_side_effect
        return state_container, mock_state_blob

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_write_discovery_inventory_writes_blob_and_advances(self, mock_gcs_client_class, mock_get_email):
        # The manual alternative to run_discovery_extraction: the whole
        # agent-assembled inventory is written as given and the graph moves on
        # to the assessment.
        mock_get_email.return_value = "platform-user@google.com"
        state_container, _ = self._setup_review_state(mock_gcs_client_class)
        inventory_container = self._install_inventory_roundtrip()

        res = await main.write_discovery_inventory(
            {"clusters": [{"name": "eks-1"}], "triggers": ALL_TRIGGERS}
        )

        self.assertIn("Discovery inventory saved to ledger", res)
        self.assertEqual(state_container["state"]["current_state"], "STATE_ASSESSMENT")
        inventory = inventory_container["inventory"]
        self.assertEqual(inventory["clusters"], [{"name": "eks-1"}])
        self.assertEqual(inventory["triggers"], ALL_TRIGGERS)
        # The written inventory is also what the review reads back.
        self.assertEqual(
            state_container["state"]["variables"]["discovery_inventory"]["clusters"],
            [{"name": "eks-1"}])

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_write_discovery_inventory_wrong_state(self, mock_gcs_client_class, mock_get_email):
        mock_get_email.return_value = "platform-user@google.com"
        self._setup_review_state(mock_gcs_client_class, current_state="STATE_DISCOVERY")

        res = await main.write_discovery_inventory({"clusters": []})

        self.assertIn("ERROR: Invalid state for write_discovery_inventory: STATE_DISCOVERY", res)
        self.mock_inventory_blob.upload_from_string.assert_not_called()

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_write_discovery_inventory_precondition_conflict(self, mock_gcs_client_class, mock_get_email):
        # The conflict guard sits on the state save: a concurrent transition
        # must not be silently overwritten.
        mock_get_email.return_value = "platform-user@google.com"
        _, mock_state_blob = self._setup_review_state(mock_gcs_client_class)
        self._install_inventory_roundtrip()
        mock_state_blob.upload_from_string.side_effect = exceptions.PreconditionFailed("conflict")

        res = await main.write_discovery_inventory({"clusters": []})

        self.assertIn("ERROR: Concurrent update conflict", res)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_write_discovery_inventory_publishes_exports(self, mock_gcs_client_class, mock_get_email):
        # The manual inventory path is one of the two discovery persist
        # points, so it must publish the exports channel too.
        mock_get_email.return_value = "platform-user@google.com"
        state_container, mock_state_blob = self._setup_review_state(mock_gcs_client_class)
        self._install_inventory_roundtrip()
        captured = self._install_exports_capture()

        with tempfile.TemporaryDirectory() as tmp:
            with open(os.path.join(tmp, "app.yaml"), "w", encoding="utf-8") as f:
                f.write("apiVersion: v1\nkind: Service\nmetadata:\n"
                        "  name: s\n  namespace: web\n")
            state_container["state"]["variables"].update({
                "source_repo_url": "sso://estate", "source_branch": "main",
                "target_repo_url": "https://ssm/target.git", "target_branch": "main",
                "target_path": "/",
                "discovery_scope": {"root_dir": tmp, "excluded": ["private/"],
                                    "included": []},
            })
            mock_state_blob.download_as_text.return_value = json.dumps(state_container["state"])
            self.mock_manifest_blob.download_as_text.side_effect = None
            self.mock_manifest_blob.download_as_text.return_value = json.dumps(
                {"files": [{"path": "app.yaml", "size": 1, "kind": "k8s-manifest"},
                           {"path": "private/keep-out.yaml", "size": 1,
                            "kind": "k8s-manifest"}]})

            res = await main.write_discovery_inventory(
                {"triggers": ALL_TRIGGERS,
                 # A SCANNED section: without one the data slice is skipped
                 # rather than published (a vanished section must never lay
                 # "nobody looked" over a good slice), which is its own test.
                 "data_dependencies": [{"service": "rds", "identifier": "orders-db", "address": "aws_db_instance.orders", "disposition": "migrate", "evidence": ["envs/prod/data.tf"], "consumers": []}]})

        self.assertIn("SUCCESS", res)
        self.assertIn("exports.json refreshed", res)
        doc = captured["doc"]
        self.assertEqual(doc["source_repo"],
                         {"url": "sso://estate", "branch": "main", "path": "/src"})
        self.assertEqual(doc["target_repo"],
                         {"url": "https://ssm/target.git", "branch": "main", "path": "/"},
                         "the developer ship coordinates ride the exports channel: "
                         "developers cannot read platform/onboarding/state.json")
        self.assertEqual(doc["component_seed_index"]["app.yaml"],
                         {"kinds": ["Service"], "namespaces": ["web"],
                          "team_labels": [], "names": ["Service/s"]})
        self.assertNotIn("private/keep-out.yaml", doc["component_seed_index"],
                         "the confirmed scope's exclusions hold for the seed index too")
        self.assertEqual(doc["generations"], {"discovery": 1, "translation": 0,
                                              "deployment": 0, "data": 1},
                         "extraction publishes two slices: the discovery "
                         "fields and the data gate the reviewed section feeds")
        self.assertIsNone(doc["storage_class_menu"], "null discipline: nothing else populated")
        self.assertTrue(doc["data_gate"]["scanned"])
        self.assertEqual(doc["data_gate"]["services"][0]["identifier"],
                         "orders-db")
        self.assertEqual(captured["kwargs"].get("if_generation_match"), 0,
                         "a first publish must be a create, not an overwrite")
        self.assertEqual(state_container["state"]["current_state"], "STATE_ASSESSMENT")

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_write_discovery_inventory_exports_failure_is_nonfatal(
            self, mock_gcs_client_class, mock_get_email):
        # Best-effort contract: a failed exports publish warns in the
        # response but the discovery step itself still completes.
        mock_get_email.return_value = "platform-user@google.com"
        state_container, _ = self._setup_review_state(mock_gcs_client_class)
        self._install_inventory_roundtrip()
        self.mock_exports_blob.upload_from_string.side_effect = RuntimeError("bucket on fire")

        res = await main.write_discovery_inventory({"triggers": ALL_TRIGGERS})

        self.assertIn("SUCCESS", res)
        self.assertIn("WARNING: exports.json publication failed", res)
        self.assertEqual(state_container["state"]["current_state"], "STATE_ASSESSMENT")

    @patch("servers.phases.discovery.discovery_extract_3.tools.reporter.generate_report",
           new_callable=AsyncMock)
    @patch("servers.phases.discovery.discovery_extract_3.tools.merger.merge_fragments")
    @patch("servers.phases.discovery.discovery_extract_3.tools.extractor.extract_all",
           new_callable=AsyncMock)
    @patch("servers.phases.discovery.discovery_extract_3.tools.extractor.check_llm_auth")
    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_run_discovery_extraction_publishes_exports(
            self, mock_gcs_client_class, mock_get_email, mock_auth, mock_extract,
            mock_merge, mock_report):
        # The pipeline path is the other inventory-persist point; the hook
        # runs after inventory + report land and reports in the summary.
        mock_get_email.return_value = "platform-user@google.com"
        mock_auth.return_value = None
        mock_report.return_value = "# readiness"
        mock_merge.return_value = {
            "clusters": [], "triggers": dict(ALL_TRIGGERS),
            "data_dependencies": [{"service": "rds", "identifier": "orders-db", "address": "aws_db_instance.orders", "disposition": "migrate", "evidence": ["envs/prod/data.tf"], "consumers": []}]}

        async def fake_extract(chunks_with_content, schema, on_fragment=None):
            return {"fragments": {c["chunk_id"]: {"clusters": []}
                                  for c, _ in chunks_with_content}, "errors": {}}
        mock_extract.side_effect = fake_extract

        state_container, mock_state_blob = self._setup_review_state(mock_gcs_client_class)
        self._install_inventory_roundtrip()
        captured = self._install_exports_capture()

        with tempfile.TemporaryDirectory() as tmp:
            with open(os.path.join(tmp, "app.yaml"), "w", encoding="utf-8") as f:
                f.write("apiVersion: v1\nkind: Service\nmetadata:\n"
                        "  name: s\n  namespace: web\n")
            state_container["state"]["variables"].update({
                "source_repo_url": "sso://estate", "source_branch": "main",
                "discovery_scope": {"root_dir": tmp, "excluded": [], "included": []},
            })
            mock_state_blob.download_as_text.return_value = json.dumps(state_container["state"])
            self.mock_manifest_blob.download_as_text.side_effect = None
            self.mock_manifest_blob.download_as_text.return_value = json.dumps(
                {"files": [{"path": "app.yaml", "size": 1, "kind": "k8s-manifest"}]})

            res = await main.run_discovery_extraction()

        self.assertIn("SUCCESS: Discovery extraction complete", res)
        self.assertIn("Exports: gs://test-bucket/exports.json refreshed", res)
        doc = captured["doc"]
        self.assertEqual(doc["component_seed_index"]["app.yaml"]["kinds"], ["Service"])
        self.assertEqual(doc["generations"]["discovery"], 1)
        self.assertEqual(state_container["state"]["current_state"], "STATE_ASSESSMENT")

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_refresh_exports_recomputes_and_bumps_only_changes(
            self, mock_gcs_client_class, mock_get_email):
        # The recompute tool: derivable fields publish from whatever artifacts
        # exist now; a second run with nothing new advances no generation.
        mock_get_email.return_value = "platform-user@google.com"
        state_container, mock_state_blob = self._setup_review_state(mock_gcs_client_class)
        container = self._install_exports_roundtrip()

        with tempfile.TemporaryDirectory() as tmp:
            with open(os.path.join(tmp, "app.yaml"), "w", encoding="utf-8") as f:
                f.write("apiVersion: v1\nkind: Service\nmetadata:\n"
                        "  name: s\n  namespace: web\n")
            state_container["state"]["variables"].update({
                "source_repo_url": "sso://estate", "source_branch": "main",
                "target_repo_url": "https://ssm/target.git", "target_branch": "main",
                "discovery_scope": {"root_dir": tmp, "excluded": [], "included": []},
            })
            mock_state_blob.download_as_text.return_value = json.dumps(state_container["state"])
            self.mock_manifest_blob.download_as_text.side_effect = None
            self.mock_manifest_blob.download_as_text.return_value = json.dumps(
                {"files": [{"path": "app.yaml", "size": 1, "kind": "k8s-manifest"}]})

            first = await main.refresh_exports()
            second = await main.refresh_exports()

        self.assertIn("SUCCESS: exports.json recomputed", first)
        self.assertIn("Sources changed: discovery", first)
        self.assertIn("Sources changed: none", second)
        doc = container["doc"]
        self.assertEqual(doc["target_repo"]["url"], "https://ssm/target.git",
                         "refresh_exports republishes the ship coordinates for free")
        self.assertEqual(doc["component_seed_index"]["app.yaml"]["kinds"], ["Service"])
        self.assertEqual(doc["generations"],
                         {"discovery": 1, "translation": 0, "deployment": 0,
                          "data": 0},
                         "this fixture's inventory carries no "
                         "data_dependencies section, so the data slice is "
                         "SKIPPED rather than republished as 'never scanned'")
        self.assertIn("data (the inventory carries no data_dependencies", first)
        self.assertIsNone(doc["gsa_bindings"], "translation never ran; explicit null")
        self.assertIsNone(doc["artifact_registry"], "deployment never ran; explicit null")

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_refresh_exports_skips_degraded_sources_instead_of_regressing(
            self, mock_gcs_client_class, mock_get_email):
        # The canonical refresh use case runs weeks later, possibly on another
        # machine: a source whose inputs are gone must keep its stored slice.
        mock_get_email.return_value = "platform-user@google.com"
        state_container, mock_state_blob = self._setup_review_state(mock_gcs_client_class)
        container = self._install_exports_roundtrip()

        with tempfile.TemporaryDirectory() as tmp:
            with open(os.path.join(tmp, "app.yaml"), "w", encoding="utf-8") as f:
                f.write("apiVersion: v1\nkind: Service\nmetadata:\n"
                        "  name: s\n  namespace: web\n")
            state_container["state"]["variables"].update({
                "source_repo_url": "sso://estate", "source_branch": "main",
                "discovery_scope": {"root_dir": tmp, "excluded": [], "included": []},
            })
            mock_state_blob.download_as_text.return_value = json.dumps(state_container["state"])
            self.mock_manifest_blob.reload.side_effect = None
            self.mock_manifest_blob.download_as_text.side_effect = None
            self.mock_manifest_blob.download_as_text.return_value = json.dumps(
                {"files": [{"path": "app.yaml", "size": 1, "kind": "k8s-manifest"}]})

            first = await main.refresh_exports()
            self.assertIn("Sources changed: discovery", first)

            # Same workspace, different machine: the checkout is gone.
            state_container["state"]["variables"]["discovery_scope"]["root_dir"] = (
                "/nonexistent-checkout-xyz")
            mock_state_blob.download_as_text.return_value = json.dumps(state_container["state"])

            second = await main.refresh_exports()

        self.assertIn("Sources skipped", second)
        self.assertIn("discovery seed index (the source checkout is not on "
                      "this machine", second)
        doc = container["doc"]
        self.assertEqual(doc["component_seed_index"]["app.yaml"]["kinds"], ["Service"],
                         "the hook-published seed index must survive a degraded refresh")
        self.assertEqual(doc["generations"]["discovery"], 1)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_refresh_exports_publishes_target_repo_without_the_checkout(
            self, mock_gcs_client_class, mock_get_email):
        # WB1's remedy path: configure_repositories records the coordinates,
        # refresh_exports runs later on a machine WITHOUT the source
        # checkout. The repo coordinates derive from the variables alone, so
        # they must publish; only the seed index is gated on local inputs
        # (2026-08-15 audit, family 2).
        mock_get_email.return_value = "platform-user@google.com"
        state_container, mock_state_blob = self._setup_review_state(mock_gcs_client_class)
        container = self._install_exports_roundtrip()

        with tempfile.TemporaryDirectory() as tmp:
            with open(os.path.join(tmp, "app.yaml"), "w", encoding="utf-8") as f:
                f.write("apiVersion: v1\nkind: Service\nmetadata:\n"
                        "  name: s\n  namespace: web\n")
            state_container["state"]["variables"].update({
                "discovery_scope": {"root_dir": tmp, "excluded": [], "included": []},
            })
            mock_state_blob.download_as_text.return_value = json.dumps(state_container["state"])
            self.mock_manifest_blob.reload.side_effect = None
            self.mock_manifest_blob.download_as_text.side_effect = None
            self.mock_manifest_blob.download_as_text.return_value = json.dumps(
                {"files": [{"path": "app.yaml", "size": 1, "kind": "k8s-manifest"}]})

            first = await main.refresh_exports()
            self.assertIn("SUCCESS", first)

            state_container["state"]["variables"].update({
                "target_repo_url": "sso://gitops/target", "target_branch": "main"})
            state_container["state"]["variables"]["discovery_scope"]["root_dir"] = (
                "/nonexistent-checkout-xyz")
            mock_state_blob.download_as_text.return_value = json.dumps(state_container["state"])

            second = await main.refresh_exports()

        self.assertIn("Sources changed: discovery", second)
        self.assertIn("discovery seed index (the source checkout is not on "
                      "this machine", second)
        doc = container["doc"]
        self.assertEqual(doc["target_repo"],
                         {"url": "sso://gitops/target", "branch": "main", "path": None},
                         "the ship coordinates must publish without the checkout")
        self.assertEqual(doc["component_seed_index"]["app.yaml"]["kinds"], ["Service"],
                         "the degraded refresh must not touch the stored seed index")

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_refresh_exports_skips_inventory_dependent_sources_on_read_failure(
            self, mock_gcs_client_class, mock_get_email):
        # A FAILED (not merely absent) inventory read is the fourth degraded
        # leg: image_map/node_shapes and the chart-root knowledge derive from
        # it, so discovery and deployment must skip rather than recompute
        # from an empty inventory.
        mock_get_email.return_value = "platform-user@google.com"
        self._setup_review_state(mock_gcs_client_class)
        self._install_exports_roundtrip()
        self.mock_inventory_blob.reload.side_effect = RuntimeError("transient GCS error")

        res = await main.refresh_exports()

        self.assertIn("SUCCESS", res)
        self.assertIn("discovery seed index (the inventory blob is unreadable here", res)
        self.assertIn("deployment (the inventory blob is unreadable here)", res)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_refresh_exports_is_refused_without_a_platform_session(
            self, mock_gcs_client_class, mock_get_email):
        mock_get_email.return_value = "stranger@google.com"
        self._setup_review_state(mock_gcs_client_class)

        res = await main.refresh_exports()

        self.assertIn("ERROR", res)
        self.mock_exports_blob.upload_from_string.assert_not_called()

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_get_discovery_inventory_empty_and_roundtrip(self, mock_gcs_client_class, mock_get_email):
        mock_get_email.return_value = "platform-user@google.com"
        self._setup_review_state(mock_gcs_client_class)
        self._install_inventory_roundtrip()

        res = await main.get_discovery_inventory()
        # No inventory yet: the read degrades to an empty document, never an error.
        self.assertIn("Current State: STATE_DISCOVERY_RUNNING", res)
        self.assertIn("Inventory:\n{}", res)

        await main.write_discovery_inventory({"triggers": ALL_TRIGGERS})
        res = await main.get_discovery_inventory()
        self.assertIn("Current State: STATE_ASSESSMENT", res)
        self.assertIn('"gpu_tpu": false', res)

    # --- Image discovery pipeline flows ---

    def _setup_image_flow(self, mock_gcs_client_class, approved=True):
        """Fixture for the STATE_DISCOVERY image-scan segment: authenticated
        platform user in STATE_DISCOVERY, state and inventory round-trips
        installed, elicitation answering `approved`."""
        state_container = {"state": {
            "current_state": "STATE_DISCOVERY",
            "history": [],
            "variables": {"source_repo_url": "sso://source-repo", "source_branch": "main"},
        }}
        main.state_mgr.write_local_config("gs://test-bucket", "platform", "test-workspace", "test-project")
        mock_gcs_client, _, mock_state_blob, _, _ = self.setup_mock_gcs(
            mock_gcs_client_class,
            registry_yaml="roles:\n  platform: ['platform-user@google.com']",
            state_dict=state_container["state"]
        )
        main.state_mgr.gcs_client = mock_gcs_client

        def upload_side_effect(data_str, **kwargs):
            state_container["state"] = json.loads(data_str)
            mock_state_blob.download_as_text.return_value = data_str
        mock_state_blob.upload_from_string.side_effect = upload_side_effect

        inventory_container = self._install_inventory_roundtrip()

        mock_ctx = MagicMock()
        mock_ctx.request_id = "test-request-id"
        mock_ctx.request_context.meta = None
        mock_res = MagicMock()
        mock_res.action = "accept"
        mock_res.content = {"approved": approved}
        async def mock_send_request(*args, **kwargs):
            return mock_res
        mock_ctx.request_context.session.send_request.side_effect = mock_send_request

        return state_container, inventory_container, mock_ctx

    @staticmethod
    def _write_image_fixture(root, with_render_targets):
        """A small source tree: one literal ECR ref, optionally a Helm chart
        (two values files) and a Kustomize root."""
        os.makedirs(os.path.join(root, "k8s"))
        with open(os.path.join(root, "k8s", "deploy.yaml"), "w") as f:
            f.write("image: 123456789012.dkr.ecr.us-east-1.amazonaws.com/payments/api:1.4.2\n")
        if with_render_targets:
            os.makedirs(os.path.join(root, "charts", "web"))
            with open(os.path.join(root, "charts", "web", "Chart.yaml"), "w") as f:
                f.write("name: web\nversion: 0.1.0\n")
            for vf in ("values.yaml", "values-prod.yaml"):
                with open(os.path.join(root, "charts", "web", vf), "w") as f:
                    f.write("image:\n  tag: '1.0'\n")
            os.makedirs(os.path.join(root, "overlays", "prod"))
            with open(os.path.join(root, "overlays", "prod", "kustomization.yaml"), "w") as f:
                f.write("resources: []\n")

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_image_flow_no_render_targets(self, mock_gcs_client_class, mock_get_email):
        mock_get_email.return_value = "platform-user@google.com"
        state_container, inventory_container, mock_ctx = self._setup_image_flow(mock_gcs_client_class)

        with tempfile.TemporaryDirectory() as root:
            self._write_image_fixture(root, with_render_targets=False)
            res = await main.discover_configuration_files(ctx=mock_ctx, root_dir=root)

        self.assertEqual(state_container["state"]["current_state"], "STATE_DISCOVERY_SCOPING")
        self.assertIn("STATE_DISCOVERY_IMAGE_SCAN -> STATE_DISCOVERY_SCOPING", res)
        # No render targets -> no elicitation raised.
        mock_ctx.request_context.session.send_request.assert_not_called()
        inventory = inventory_container["inventory"]
        self.assertEqual(inventory["render_targets"], [])
        self.assertEqual(len(inventory["images"]), 1)
        img = inventory["images"][0]
        self.assertEqual(img["registry"], "ecr")
        self.assertEqual(img["provenance"], [{"kind": "literal", "file": os.path.join("k8s", "deploy.yaml")}])
        self.assertIn("image inventory is complete", res)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    @patch("servers.phases.discovery.discovery_init_1.render.shutil.which")
    @patch("servers.phases.discovery.discovery_init_1.render.subprocess.run")
    async def test_image_flow_render_approved(
        self, mock_sub_run, mock_which, mock_gcs_client_class, mock_get_email
    ):
        mock_get_email.return_value = "platform-user@google.com"
        state_container, inventory_container, mock_ctx = self._setup_image_flow(mock_gcs_client_class, approved=True)
        mock_which.side_effect = lambda name: {"helm": "/usr/bin/helm", "kubectl": "/usr/bin/kubectl"}.get(name)
        mock_sub_run.return_value = MagicMock(returncode=0, stdout=(
            "apiVersion: apps/v1\nkind: Deployment\nspec:\n  template:\n    spec:\n"
            "      containers:\n      - image: 123456789012.dkr.ecr.us-east-1.amazonaws.com/web:2.0\n"
            "      initContainers:\n      - image: busybox:stable\n"
        ))

        with tempfile.TemporaryDirectory() as root:
            self._write_image_fixture(root, with_render_targets=True)
            res = await main.discover_configuration_files(ctx=mock_ctx, root_dir=root)

        self.assertEqual(state_container["state"]["current_state"], "STATE_DISCOVERY_SCOPING")
        mock_ctx.request_context.session.send_request.assert_called_once()
        inventory = inventory_container["inventory"]
        statuses = {t["id"]: t["status"] for t in inventory["render_targets"]}
        self.assertEqual(set(statuses.values()), {"rendered"})
        # helm template ran once per values file + one kustomize build.
        self.assertEqual(mock_sub_run.call_count, 3)
        refs = {img["ref"] for img in inventory["images"]}
        self.assertIn("123456789012.dkr.ecr.us-east-1.amazonaws.com/payments/api:1.4.2", refs)  # literal
        self.assertIn("123456789012.dkr.ecr.us-east-1.amazonaws.com/web:2.0", refs)  # rendered
        self.assertIn("busybox:stable", refs)
        rendered_img = next(i for i in inventory["images"] if i["ref"].endswith("web:2.0"))
        kinds = {p["kind"] for p in rendered_img["provenance"]}
        self.assertEqual(kinds, {"rendered"})
        self.assertIn("Rendered 2/2 target(s)", res)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_image_flow_render_declined_is_non_blocking(self, mock_gcs_client_class, mock_get_email):
        mock_get_email.return_value = "platform-user@google.com"
        state_container, inventory_container, mock_ctx = self._setup_image_flow(mock_gcs_client_class, approved=False)

        with tempfile.TemporaryDirectory() as root:
            self._write_image_fixture(root, with_render_targets=True)
            res = await main.discover_configuration_files(ctx=mock_ctx, root_dir=root)

        # The non-blocking guarantee: a decline still drives forward — the
        # dispatch loop drains mark_render_declined and parks at scoping, so
        # declined renders block nothing.
        self.assertEqual(state_container["state"]["current_state"], "STATE_DISCOVERY_SCOPING")
        inventory = inventory_container["inventory"]
        statuses = {t["status"] for t in inventory["render_targets"]}
        self.assertEqual(statuses, {"unrendered_declined"})
        self.assertIn("continues with literal image references only", res)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    @patch("servers.phases.discovery.discovery_init_1.render.shutil.which")
    async def test_image_flow_missing_binaries(self, mock_which, mock_gcs_client_class, mock_get_email):
        mock_get_email.return_value = "platform-user@google.com"
        state_container, inventory_container, mock_ctx = self._setup_image_flow(mock_gcs_client_class, approved=True)
        mock_which.return_value = None

        with tempfile.TemporaryDirectory() as root:
            self._write_image_fixture(root, with_render_targets=True)
            res = await main.discover_configuration_files(ctx=mock_ctx, root_dir=root)

        # Missing binaries never block: targets are marked failed with reasons
        # and the flow still parks at scoping.
        self.assertEqual(state_container["state"]["current_state"], "STATE_DISCOVERY_SCOPING")
        inventory = inventory_container["inventory"]
        statuses = {t["status"] for t in inventory["render_targets"]}
        self.assertEqual(statuses, {"render_failed"})
        reasons = {t["reason"] for t in inventory["render_targets"]}
        self.assertTrue(all("not found" in r for r in reasons))
        # Literal refs survive regardless.
        self.assertEqual(len(inventory["images"]), 1)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_extraction_carryover_preserves_scan_sections(self, mock_gcs_client_class, mock_get_email):
        # The inventory schema marks images/render_targets "Passthrough": an
        # extraction (re-)run rewrites the blob wholesale and must carry the
        # image-scan sections over rather than dropping them.
        from servers.phases.discovery import discovery_extract_3
        mock_get_email.return_value = "platform-user@google.com"
        state_container, inventory_container, mock_ctx = self._setup_image_flow(mock_gcs_client_class)

        with tempfile.TemporaryDirectory() as root:
            self._write_image_fixture(root, with_render_targets=False)
            await main.discover_configuration_files(ctx=mock_ctx, root_dir=root)

        merged = {"clusters": [], "triggers": {}}
        bucket = main.state_mgr.gcs_client.bucket("test-bucket")
        discovery_extract_3.tools.carry_scan_sections(bucket, merged)
        self.assertEqual(len(merged["images"]), 1)
        self.assertEqual(merged["render_targets"], [])
        # Provenance travels with the images: all six passthrough sections.
        self.assertTrue(merged["source"]["root_dir"])
        self.assertTrue(merged["schema_version"])
        self.assertTrue(merged["generated_at"])
        # data_dependencies is absent here on purpose: the harvest runs at
        # STATE_DISCOVERY_DATA_SCAN, after the scope gate, so the image scan
        # this flow exercises has not produced it yet.
        self.assertNotIn("data_dependencies", merged)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_scope_confirm_reaches_a_runnable_data_scan(
            self, mock_gcs_client_class, mock_get_email):
        """The whole point of the state: confirming scope must land somewhere
        the agent can act on, and the tool it names must actually populate the
        section. An earlier revision made this an INTERNAL state entered from a
        tool that does not drain the graph, which wedged the run — nothing
        could execute the state and no tool accepted it."""
        from servers.phases.discovery import discovery_scope_2, discovery_datascan_3
        mock_get_email.return_value = "platform-user@google.com"
        state_container, inventory_container, mock_ctx = self._setup_image_flow(mock_gcs_client_class)

        with tempfile.TemporaryDirectory() as root:
            self._write_image_fixture(root, with_render_targets=False)
            with open(os.path.join(root, "rds.tf"), "w") as f:
                f.write('resource "aws_db_instance" "d" {\n'
                        '  identifier        = "acme-catalog"\n'
                        '  engine            = "postgres"\n'
                        '  allocated_storage = 100\n}\n')
            await main.discover_configuration_files(ctx=mock_ctx, root_dir=root)
            self.assertEqual(state_container["state"]["current_state"],
                             "STATE_DISCOVERY_SCOPING")

            out = await discovery_scope_2.tools.confirm_discovery_scope(ctx=mock_ctx)
            self.assertNotIn("ERROR", out)
            self.assertEqual(state_container["state"]["current_state"],
                             "STATE_DISCOVERY_DATA_SCAN")

            # The state must be one the agent can act on: get_next_stage has to
            # hand back a procedure and a tool, not a dead end.
            stage = main.get_next_stage()
            self.assertIn("scan_data_dependencies", stage)

            out = await discovery_datascan_3.tools.scan_data_dependencies(ctx=mock_ctx)
            self.assertNotIn("ERROR", out)

        # The scan hands off to the human review of the mapping, not straight
        # to extraction: once extraction runs, `consumers` is carried over
        # untouched and never looked at again.
        self.assertEqual(state_container["state"]["current_state"],
                         "STATE_DISCOVERY_DATA_REVIEW")
        found = inventory_container["inventory"]["data_dependencies"]
        self.assertEqual([(d["service"], d["identifier"]) for d in found],
                         [("rds", "acme-catalog")])
        self.assertEqual(found[0]["disposition"], "migrate")

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_data_scan_records_which_workload_needs_the_database(
            self, mock_gcs_client_class, mock_get_email):
        """End to end through the tool, not just the pure join: the consumer
        has to survive the harvest, the schema validation and the ledger write,
        and be visible in what the agent is handed back."""
        from servers.phases.discovery import discovery_scope_2, discovery_datascan_3
        mock_get_email.return_value = "platform-user@google.com"
        state_container, inventory_container, mock_ctx = self._setup_image_flow(
            mock_gcs_client_class)

        with tempfile.TemporaryDirectory() as root:
            self._write_image_fixture(root, with_render_targets=False)
            with open(os.path.join(root, "rds.tf"), "w") as f:
                f.write('resource "aws_db_instance" "d" {\n'
                        '  identifier = "acme-catalog"\n}\n'
                        'resource "helm_release" "catalog" {\n'
                        '  name      = "catalog"\n'
                        '  namespace = "catalog"\n'
                        '  set { value = aws_db_instance.d.endpoint }\n}\n')
            await main.discover_configuration_files(ctx=mock_ctx, root_dir=root)
            await discovery_scope_2.tools.confirm_discovery_scope(ctx=mock_ctx)
            out = await discovery_datascan_3.tools.scan_data_dependencies(ctx=mock_ctx)

        self.assertNotIn("ERROR", out)
        found = inventory_container["inventory"]["data_dependencies"]
        self.assertEqual(len(found), 1)
        self.assertEqual(
            [(c["workload"], c["kind"], c["detection"])
             for c in found[0]["consumers"]],
            [("catalog", "helm_release", "terraform_wiring")])
        # The agent is told, not just the ledger: the step's instructions ask
        # for a table naming the workloads, which it cannot build otherwise.
        # Parsed rather than substring-matched — "catalog" is also the
        # identifier, so `assertIn` held whether or not a consumer was found.
        summary = json.loads(out[out.index("{"):out.rindex("}") + 1])
        self.assertEqual(summary["entries"][0]["consumers"],
                         ["catalog (helm_release)"])
        self.assertEqual(summary["unattributed_to_any_workload"], 0)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_data_scan_copies_the_corefile_verbatim(
            self, mock_gcs_client_class, mock_get_email):
        """The same call records the cluster DNS configuration: through the
        schema validation and the ledger write, byte for byte, and the agent
        is told where it came from without being handed the text — the text
        is the translation worker's input, not chat material. A default
        add-on beside it is a note, not a source, so the section it leaves
        behind is exactly one source."""
        from servers.phases.discovery import discovery_scope_2, discovery_datascan_3
        mock_get_email.return_value = "platform-user@google.com"
        state_container, inventory_container, mock_ctx = self._setup_image_flow(
            mock_gcs_client_class)
        corefile = (".:53 {\n    forward . 10.0.0.2\n}\n"
                    "corp.example.com:53 {\n    forward . 10.1.2.3 10.1.2.4\n}\n")

        with tempfile.TemporaryDirectory() as root:
            self._write_image_fixture(root, with_render_targets=False)
            with open(os.path.join(root, "k8s", "coredns.yaml"), "w") as f:
                f.write("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: coredns\n"
                        "  namespace: kube-system\ndata:\n  Corefile: |\n"
                        + "".join("    " + line + "\n"
                                  for line in corefile.splitlines()))
            with open(os.path.join(root, "eks.tf"), "w") as f:
                f.write('resource "aws_eks_addon" "coredns" {\n'
                        '  addon_name = "coredns"\n}\n')
            await main.discover_configuration_files(ctx=mock_ctx, root_dir=root)
            await discovery_scope_2.tools.confirm_discovery_scope(ctx=mock_ctx)
            out = await discovery_datascan_3.tools.scan_data_dependencies(ctx=mock_ctx)

        self.assertNotIn("ERROR", out)
        section = inventory_container["inventory"]["cluster_dns"]
        self.assertEqual(len(section["sources"]), 1)
        self.assertEqual(section["sources"][0]["text"], corefile)
        self.assertEqual(section["sources"][0]["path"], "k8s/coredns.yaml")
        notes = inventory_container["inventory"]["cluster_dns_scan_notes"]
        self.assertTrue(any("default configuration" in n for n in notes), notes)
        self.assertIn("1 cluster DNS configuration source(s) recorded verbatim", out)
        summary = json.loads(out[out.index("{"):out.rindex("}") + 1])
        self.assertEqual(summary["cluster_dns"]["sources_with_text"],
                         [{"kind": "configmap", "name": "coredns",
                           "where": "k8s/coredns.yaml", "form": "manifest"}])
        self.assertNotIn("10.1.2.3", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_cluster_dns_harvest_failure_does_not_block_the_data_scan(
            self, mock_gcs_client_class, mock_get_email):
        """The data section gates a pipeline; the cluster DNS section is
        advisory until its consumer exists. A crash in the new harvester
        therefore becomes a persisted note, and the data services are still
        recorded and the graph still moves."""
        from servers.phases.discovery import discovery_scope_2, discovery_datascan_3
        from servers.phases.discovery.discovery_init_1 import clusterdns
        mock_get_email.return_value = "platform-user@google.com"
        state_container, inventory_container, mock_ctx = self._setup_image_flow(
            mock_gcs_client_class)

        with tempfile.TemporaryDirectory() as root:
            self._write_image_fixture(root, with_render_targets=False)
            with open(os.path.join(root, "rds.tf"), "w") as f:
                f.write('resource "aws_db_instance" "d" {\n'
                        '  identifier = "acme-catalog"\n}\n')
            await main.discover_configuration_files(ctx=mock_ctx, root_dir=root)
            await discovery_scope_2.tools.confirm_discovery_scope(ctx=mock_ctx)
            with patch.object(clusterdns, "harvest_cluster_dns",
                              side_effect=RuntimeError("offset past end")):
                out = await discovery_datascan_3.tools.scan_data_dependencies(ctx=mock_ctx)

        self.assertNotIn("ERROR", out)
        self.assertEqual(len(inventory_container["inventory"]["data_dependencies"]), 1)
        self.assertEqual(inventory_container["inventory"]["cluster_dns"], {})
        notes = inventory_container["inventory"]["cluster_dns_scan_notes"]
        self.assertEqual(len(notes), 1)
        self.assertIn("RuntimeError: offset past end", notes[0])
        self.assertIn("nothing was recorded", notes[0])
        self.assertEqual(state_container["state"]["current_state"],
                         "STATE_DISCOVERY_DATA_REVIEW")

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_summary_does_not_reassert_a_claim_the_scan_declined(
            self, mock_gcs_client_class, mock_get_email):
        """When part of the Terraform cannot be read, the harvester refuses to
        say "nothing references this" and says "unknown" instead. The tool
        summary must not then count that entry as unattributed — the entry's
        own note and the number the agent reports would contradict each other
        in the same breath."""
        from servers.phases.discovery import discovery_scope_2, discovery_datascan_3
        mock_get_email.return_value = "platform-user@google.com"
        state_container, inventory_container, mock_ctx = self._setup_image_flow(
            mock_gcs_client_class)

        with tempfile.TemporaryDirectory() as root:
            self._write_image_fixture(root, with_render_targets=False)
            with open(os.path.join(root, "rds.tf"), "w") as f:
                f.write('resource "aws_db_instance" "d" {\n'
                        '  identifier = "acme-catalog"\n}\n'
                        'resource "aws_iam_role" "broken" {\n  name = "x"\n')
            await main.discover_configuration_files(ctx=mock_ctx, root_dir=root)
            await discovery_scope_2.tools.confirm_discovery_scope(ctx=mock_ctx)
            out = await discovery_datascan_3.tools.scan_data_dependencies(ctx=mock_ctx)

        self.assertNotIn("ERROR", out)
        summary = json.loads(out[out.index("{"):out.rindex("}") + 1])
        self.assertEqual(summary["unattributed_to_any_workload"], 0)
        self.assertEqual(summary["consumer_unknown_unreadable_terraform"], 1)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_data_scan_asks_for_a_path_when_the_checkout_is_gone(
            self, mock_gcs_client_class, mock_get_email):
        """Resuming on another workstation: the recorded path does not exist,
        so the step stays put and tells the agent to ask rather than guessing
        or re-cloning a branch that may have moved (issue 26)."""
        from servers.phases.discovery import discovery_scope_2, discovery_datascan_3
        mock_get_email.return_value = "platform-user@google.com"
        state_container, _, mock_ctx = self._setup_image_flow(mock_gcs_client_class)

        with tempfile.TemporaryDirectory() as root:
            self._write_image_fixture(root, with_render_targets=False)
            await main.discover_configuration_files(ctx=mock_ctx, root_dir=root)
            await discovery_scope_2.tools.confirm_discovery_scope(ctx=mock_ctx)
        # The checkout is gone now the tempdir is cleaned up.

        out = await discovery_datascan_3.tools.scan_data_dependencies(ctx=mock_ctx)
        self.assertIn("ERROR", out)
        self.assertIn("Ask the user for the path", out)
        # Still here, so the next session is told there is work left.
        self.assertEqual(state_container["state"]["current_state"],
                         "STATE_DISCOVERY_DATA_SCAN")

        # Supplying a path must leave the workspace usable. Extraction reads
        # the root from the scope, not from discovery_root_dir, so updating
        # only the latter advanced the graph and then wedged one state later
        # on the previous workstation's path.
        with tempfile.TemporaryDirectory() as elsewhere:
            self._write_image_fixture(elsewhere, with_render_targets=False)
            out = await discovery_datascan_3.tools.scan_data_dependencies(
                source_root=elsewhere, ctx=mock_ctx)
            self.assertNotIn("ERROR", out)
            variables = state_container["state"]["variables"]
            self.assertEqual(variables["discovery_root_dir"], elsewhere)
            self.assertEqual(variables["discovery_scope"]["root_dir"], elsewhere)
        self.assertEqual(state_container["state"]["current_state"],
                         "STATE_DISCOVERY_DATA_REVIEW")

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_data_scan_refuses_out_of_state_and_bad_paths(
            self, mock_gcs_client_class, mock_get_email):
        """The wrapper's guards, none of which had coverage. A relative
        source_root matters most: it resolves against the server's cwd and is
        then written into the scope that extraction and exports both read."""
        from servers.phases.discovery import discovery_scope_2, discovery_datascan_3
        mock_get_email.return_value = "platform-user@google.com"
        state_container, _, mock_ctx = self._setup_image_flow(mock_gcs_client_class)

        # Wrong state: the scan is only legal after the scope gate.
        out = await discovery_datascan_3.tools.scan_data_dependencies(ctx=mock_ctx)
        self.assertIn("Invalid state", out)

        with tempfile.TemporaryDirectory() as root:
            self._write_image_fixture(root, with_render_targets=False)
            await main.discover_configuration_files(ctx=mock_ctx, root_dir=root)
            await discovery_scope_2.tools.confirm_discovery_scope(ctx=mock_ctx)

            out = await discovery_datascan_3.tools.scan_data_dependencies(
                source_root=".", ctx=mock_ctx)
            self.assertIn("ERROR", out)
            self.assertEqual(state_container["state"]["current_state"],
                             "STATE_DISCOVERY_DATA_SCAN")

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_extraction_carryover_preserves_data_dependencies(
            self, mock_gcs_client_class, mock_get_email):
        # The harvest happens once, before extraction. An extraction re-run
        # (amend_scope) rewrites the blob wholesale, so without the carry the
        # workload data gate would silently lose what it reads.
        from servers.phases.discovery import discovery_extract_3
        mock_get_email.return_value = "platform-user@google.com"
        state_container, inventory_container, mock_ctx = self._setup_image_flow(mock_gcs_client_class)

        with tempfile.TemporaryDirectory() as root:
            self._write_image_fixture(root, with_render_targets=False)
            await main.discover_configuration_files(ctx=mock_ctx, root_dir=root)

        bucket = main.state_mgr.gcs_client.bucket("test-bucket")
        # Stand in for what STATE_DISCOVERY_DATA_SCAN persists.
        stored, generation = main.state_mgr.load_inventory(bucket)
        stored["data_dependencies"] = [
            {"service": "rds", "identifier": "acme-catalog", "disposition": "migrate"}]
        main.state_mgr.save_inventory(bucket, stored, generation)

        merged = {"clusters": [], "triggers": {}}
        discovery_extract_3.tools.carry_scan_sections(bucket, merged)
        self.assertEqual([d["identifier"] for d in merged["data_dependencies"]],
                         ["acme-catalog"])

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_extraction_carryover_preserves_cluster_dns(
            self, mock_gcs_client_class, mock_get_email):
        # The Corefile is copied once, at the data scan, and the translation
        # worker reads it from the inventory much later. An extraction re-run
        # must carry it — and the notes that say what was NOT recorded — or
        # the cluster-dns unit would plan against an empty section and the
        # readiness report would lose the only record of an unread Corefile.
        # An EMPTY section rides too: {} is the scan's statement that nothing
        # carried text, and it must not be confused with "never scanned".
        from servers.phases.discovery import discovery_extract_3
        mock_get_email.return_value = "platform-user@google.com"
        state_container, inventory_container, mock_ctx = self._setup_image_flow(
            mock_gcs_client_class)

        with tempfile.TemporaryDirectory() as root:
            self._write_image_fixture(root, with_render_targets=False)
            await main.discover_configuration_files(ctx=mock_ctx, root_dir=root)

        bucket = main.state_mgr.gcs_client.bucket("test-bucket")
        stored, generation = main.state_mgr.load_inventory(bucket)
        stored["cluster_dns"] = {}
        stored["cluster_dns_scan_notes"] = ["aws_eks_addon.coredns (eks.tf:1): managed "
                                            "coredns add-on with no configuration_values"]
        main.state_mgr.save_inventory(bucket, stored, generation)

        merged = {"clusters": [], "triggers": {},
                  "cluster_dns": {"sources": [{"kind": "configmap"}]}}
        discovery_extract_3.tools.carry_scan_sections(bucket, merged)
        self.assertEqual(merged["cluster_dns"], {})
        self.assertEqual(len(merged["cluster_dns_scan_notes"]), 1)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_extraction_carryover_preserves_address_space(
            self, mock_gcs_client_class, mock_get_email):
        # The address space is harvested once, at the data scan, and the
        # landing-zone design reads it two phases later. An extraction re-run
        # must carry it — with the notes that say what was NOT scanned — or
        # the design would be back to asking the user for ranges the files
        # state. An EMPTY section rides too, for the same reason as
        # cluster_dns: {} is a statement, not an absence.
        from servers.phases.discovery import discovery_extract_3
        mock_get_email.return_value = "platform-user@google.com"
        state_container, inventory_container, mock_ctx = self._setup_image_flow(
            mock_gcs_client_class)

        with tempfile.TemporaryDirectory() as root:
            self._write_image_fixture(root, with_render_targets=False)
            await main.discover_configuration_files(ctx=mock_ctx, root_dir=root)

        bucket = main.state_mgr.gcs_client.bucket("test-bucket")
        stored, generation = main.state_mgr.load_inventory(bucket)
        stored["address_space"] = {}
        stored["address_space_scan_notes"] = ["1 Terraform file(s) the confirmed scope "
                                              "excludes were not read for the network "
                                              "address space"]
        main.state_mgr.save_inventory(bucket, stored, generation)

        merged = {"clusters": [], "triggers": {},
                  "address_space": {"vpcs": [{"name": "invented", "cidr": "10.0.0.0/8"}]}}
        discovery_extract_3.tools.carry_scan_sections(bucket, merged)
        self.assertEqual(merged["address_space"], {})
        self.assertEqual(len(merged["address_space_scan_notes"]), 1)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_data_scan_records_the_source_address_space(
            self, mock_gcs_client_class, mock_get_email):
        """Through the tool, not just the pure harvest: the section has to
        survive the schema validation and the ledger write, and the agent has
        to be handed the ranges — the instructions ask for a table of them."""
        from servers.phases.discovery import discovery_scope_2, discovery_datascan_3
        mock_get_email.return_value = "platform-user@google.com"
        state_container, inventory_container, mock_ctx = self._setup_image_flow(
            mock_gcs_client_class)

        with tempfile.TemporaryDirectory() as root:
            self._write_image_fixture(root, with_render_targets=False)
            with open(os.path.join(root, "vpc.tf"), "w") as f:
                f.write('variable "cidr" {\n  default = "10.42.0.0/16"\n}\n'
                        'variable "peer_cidr" {\n  type = string\n}\n'
                        'resource "aws_vpc" "main" {\n  cidr_block = var.cidr\n}\n'
                        'resource "aws_subnet" "a" {\n'
                        '  vpc_id     = aws_vpc.main.id\n'
                        '  cidr_block = cidrsubnet(var.cidr, 3, 0)\n}\n'
                        'resource "aws_subnet" "b" {\n  count = 2\n'
                        '  vpc_id     = aws_vpc.main.id\n'
                        '  cidr_block = cidrsubnet(aws_vpc.main.cidr_block, 3, count.index + 1)\n}\n'
                        'resource "aws_eks_cluster" "c" {\n  name = "c"\n'
                        '  vpc_config {\n    subnet_ids = [aws_subnet.a.id]\n  }\n}\n'
                        'resource "aws_route" "peer" {\n'
                        '  destination_cidr_block    = var.peer_cidr\n'
                        '  vpc_peering_connection_id = "pcx-1"\n}\n')
            await main.discover_configuration_files(ctx=mock_ctx, root_dir=root)
            await discovery_scope_2.tools.confirm_discovery_scope(ctx=mock_ctx)
            out = await discovery_datascan_3.tools.scan_data_dependencies(ctx=mock_ctx)

        self.assertNotIn("ERROR", out)
        space = inventory_container["inventory"]["address_space"]
        self.assertEqual([(v["cidr"], v["cluster_vpc"]) for v in space["vpcs"]],
                         [("10.42.0.0/16", True)])
        self.assertEqual(space["subnets"][0]["cidr"], "10.42.0.0/19")
        # Two questions in the inventory: the counted subnets, whose range
        # the stated VPC covers, and the peer range, a variable with no
        # default that only the user can supply.
        self.assertEqual(space["routes"], [])
        self.assertEqual([(u["argument"], u["expression"]) for u in space["unresolved"]],
                         [("cidr_block", "cidrsubnet(aws_vpc.main.cidr_block, 3, count.index + 1) "
                                         "over for_each/count = 2"),
                          ("destination_cidr_block", "var.peer_cidr (no default)")])
        self.assertIn("address_space covers Terraform",
                      inventory_container["inventory"]["address_space_scan_notes"][-1])
        # Handed back, not only persisted — and only the peer range is put
        # to the user; the covered subnet is a count, not a question.
        self.assertIn("1 VPC range(s), 1 subnet range(s) and 1 unresolved range(s) "
                      "recorded for the address space (1 more unresolved but settled by a "
                      "stated VPC range, by the VPC's own question or not a routing range, "
                      "so no question of their own)", out)
        summary = json.loads(out.split("\n", 1)[1].rsplit("\n\nCurrent State", 1)[0])
        self.assertEqual(summary["address_space"]["vpcs"][0]["cidr"], "10.42.0.0/16")
        self.assertEqual([(u["path"], u["expression"]) for u in summary["address_space"]["unresolved"]],
                         [("vpc.tf", "var.peer_cidr (no default)")])
        self.assertEqual(summary["address_space"]["unresolved_but_covered"], 1)

    # --- the human review of the mapping (STATE_DISCOVERY_DATA_REVIEW) ------

    # One database a chain reaches, and one bucket nothing reaches. The
    # unattributed one is what the review exists for.
    REVIEW_TF = (
        'resource "aws_db_instance" "orders" {\n'
        '  identifier = "orders-db"\n}\n'
        'resource "helm_release" "orders" {\n'
        '  name      = "orders"\n'
        '  namespace = "orders"\n'
        '  set { value = aws_db_instance.orders.endpoint }\n}\n'
        'resource "aws_s3_bucket" "orders_exports" {\n'
        '  bucket = "orders-exports"\n}\n'
    )

    async def _reach_the_data_review(self, mock_gcs_client_class, tf=None,
                                     extra_files=None):
        """Drives discovery over a real tree until the mapping review.

        `extra_files` maps repository-relative paths to contents, for a
        fixture that has to put a file in a subdirectory."""
        from servers.phases.discovery import discovery_scope_2, discovery_datascan_3
        state_container, inventory_container, mock_ctx = self._setup_image_flow(
            mock_gcs_client_class)
        overrides_container = self._install_overrides_roundtrip()
        # A tempdir that outlives the call: the review, and any re-scan the
        # test drives, read the same checkout the scan did.
        root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, root, ignore_errors=True)
        self._write_image_fixture(root, with_render_targets=False)
        with open(os.path.join(root, "data.tf"), "w") as f:
            f.write(self.REVIEW_TF if tf is None else tf)
        for rel_path, content in (extra_files or {}).items():
            os.makedirs(os.path.dirname(os.path.join(root, rel_path)), exist_ok=True)
            with open(os.path.join(root, rel_path), "w") as f:
                f.write(content)
        await main.discover_configuration_files(ctx=mock_ctx, root_dir=root)
        await discovery_scope_2.tools.confirm_discovery_scope(ctx=mock_ctx)
        out = await discovery_datascan_3.tools.scan_data_dependencies(ctx=mock_ctx)
        self.assertNotIn("ERROR", out)
        return state_container, inventory_container, overrides_container, mock_ctx

    def _rewind_to_the_scan(self, state_container):
        """Puts the graph back on the scan, as amend_discovery_scope does."""
        state = dict(state_container["state"], current_state="STATE_DISCOVERY_DATA_SCAN")
        (main.state_mgr.gcs_client.bucket("test-bucket")
         .blob("platform/onboarding/state.json")
         .upload_from_string(json.dumps(state)))

    def _entry(self, inventory_container, identifier):
        return next(d for d in inventory_container["inventory"]["data_dependencies"]
                    if d["identifier"] == identifier)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_review_lists_the_mapping_and_ranks_owners_for_the_orphan(
            self, mock_gcs_client_class, mock_get_email):
        """The state has to be actionable: a procedure, a tool, and enough in
        the listing to run the conversation — including a ranked guess for the
        entry no chain reached, which is the one the review exists for."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        state_container, _, _, mock_ctx = await self._reach_the_data_review(
            mock_gcs_client_class)

        self.assertEqual(state_container["state"]["current_state"],
                         "STATE_DISCOVERY_DATA_REVIEW")
        stage = main.get_next_stage()
        self.assertIn("confirm_data_dependencies", stage)

        out = await discovery_datareview_3.tools.list_data_dependencies(ctx=mock_ctx)
        self.assertIn("used by orders (helm_release, via terraform_wiring)", out)
        # The bucket nothing references: ranked, and labeled a guess with the
        # escape hatch, never a yes/no on one name.
        self.assertIn("orders-exports", out)
        self.assertIn("GUESS", out)
        self.assertIn("none of these", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_rejected_consumer_stays_rejected_across_a_rescan(
            self, mock_gcs_client_class, mock_get_email):
        """The point of the corrections object. The chain that produced the
        wrong consumer resolves again on the next scan, so without the replay
        the reviewer's decision is silently undone."""
        from servers.phases.discovery import (
            discovery_datareview_3, discovery_datascan_3)
        mock_get_email.return_value = "platform-user@google.com"
        state_container, inventory_container, overrides_container, mock_ctx = \
            await self._reach_the_data_review(mock_gcs_client_class)

        out = await discovery_datareview_3.tools.reject_data_consumer(
            address="aws_db_instance.orders", workload="orders",
            reason="that release only reads the replica", ctx=mock_ctx)
        self.assertNotIn("ERROR", out)
        entry = self._entry(inventory_container, "orders-db")
        self.assertEqual(entry["consumers"], [])
        self.assertTrue(any("only reads the replica" in n for n in entry["notes"]))
        # Who decided, recorded with the decision.
        recorded = overrides_container["document"]["overrides"][0]
        self.assertEqual(recorded["author"], "platform-user@google.com")

        self._rewind_to_the_scan(state_container)
        out = await discovery_datascan_3.tools.scan_data_dependencies(ctx=mock_ctx)

        self.assertNotIn("ERROR", out)
        self.assertEqual(self._entry(inventory_container, "orders-db")["consumers"],
                         [])
        self.assertIn("replayed over this scan", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_an_attached_consumer_is_recorded_as_a_human_assertion(
            self, mock_gcs_client_class, mock_get_email):
        """The other half of the trade the scan makes by refusing name
        similarity: the entries it leaves unattributed can be attached by a
        human, and what they attach is marked as theirs."""
        from servers.phases.discovery import (
            discovery_datareview_3, discovery_datascan_3)
        mock_get_email.return_value = "platform-user@google.com"
        state_container, inventory_container, _, mock_ctx = \
            await self._reach_the_data_review(mock_gcs_client_class)
        from servers.phases.discovery.discovery_init_1 import consumers as consumers_lib
        self.assertIn(consumers_lib.UNATTRIBUTED_NOTE,
                      self._entry(inventory_container, "orders-exports")["notes"])

        out = await discovery_datareview_3.tools.attach_data_consumer(
            address="aws_s3_bucket.orders_exports", workload="orders",
            note="team says the nightly export writes it", ctx=mock_ctx)
        self.assertNotIn("ERROR", out)

        entry = self._entry(inventory_container, "orders-exports")
        attached = entry["consumers"][0]
        self.assertEqual(attached["detection"], "human_review")
        # Kind and namespace come from the scan's own record of that workload:
        # the reviewer should not have to retype what the Terraform states.
        self.assertEqual(attached["kind"], "helm_release")
        self.assertEqual(attached["namespace"], "orders")
        self.assertIn("platform-user@google.com", attached["evidence"])
        # The note that said nothing references it is no longer true.
        self.assertNotIn(consumers_lib.UNATTRIBUTED_NOTE, entry["notes"])

        self._rewind_to_the_scan(state_container)
        await discovery_datascan_3.tools.scan_data_dependencies(ctx=mock_ctx)
        entry = self._entry(inventory_container, "orders-exports")
        self.assertEqual([c["workload"] for c in entry["consumers"]], ["orders"])
        self.assertNotIn(consumers_lib.UNATTRIBUTED_NOTE, entry.get("notes") or [])

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_an_annotation_can_set_the_disposition_the_scan_never_sets(
            self, mock_gcs_client_class, mock_get_email):
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, inventory_container, _, mock_ctx = await self._reach_the_data_review(
            mock_gcs_client_class)

        out = await discovery_datareview_3.tools.annotate_data_dependency(
            address="aws_s3_bucket.orders_exports", disposition="keep-in-aws",
            note="the analytics team reads it from Athena", ctx=mock_ctx)
        self.assertNotIn("ERROR", out)
        entry = self._entry(inventory_container, "orders-exports")
        self.assertEqual(entry["disposition"], "keep-in-aws")
        self.assertTrue(any("Athena" in n for n in entry["notes"]))

        # A value the schema does not allow is refused HERE, before it is
        # recorded: the override is durable, so it would otherwise fail every
        # later scan's write instead of this call.
        out = await discovery_datareview_3.tools.annotate_data_dependency(
            address="aws_s3_bucket.orders_exports", disposition="leave-it",
            ctx=mock_ctx)
        self.assertIn("ERROR", out)
        self.assertIn("keep-in-aws", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_review_offers_leaving_the_service_in_aws(
            self, mock_gcs_client_class, mock_get_email):
        """A DynamoDB table has no clean Google Cloud equivalent, so the scan
        grades it `escalate` — "a human decides". Nothing in the scan can offer
        the decision that is usually right for an AWS-only service: keep it
        where it is and reach it from GKE across the boundary. If the review
        does not say so, `escalate` reads as a migration to plan and the
        engagement acquires a data project nobody asked for."""
        from servers.phases.discovery import (
            discovery_datareview_3, discovery_datascan_3)
        mock_get_email.return_value = "platform-user@google.com"
        aws_only = (
            'resource "aws_dynamodb_table" "sessions" {\n'
            '  name = "checkout-sessions"\n}\n'
            'resource "helm_release" "checkout" {\n'
            '  name = "checkout"\n'
            '  set { value = aws_dynamodb_table.sessions.name }\n}\n'
        )
        state_container, inventory_container, _, mock_ctx = \
            await self._reach_the_data_review(mock_gcs_client_class, tf=aws_only)
        entry = self._entry(inventory_container, "checkout-sessions")
        self.assertEqual(entry["disposition"], "escalate")

        out = await discovery_datareview_3.tools.list_data_dependencies(ctx=mock_ctx)

        # The option, on the entry that has no settled plan...
        self.assertIn("no clean Google Cloud equivalent", out)
        self.assertIn("leave this in AWS", out)
        self.assertIn("across the cloud boundary", out)
        # ...with what it costs, so it is offered as a trade and not a shortcut.
        self.assertIn("Interconnect", out)
        self.assertIn("egress", out)
        # ...and stated for the whole section, since "migrate the fleet but
        # keep AWS RDS" is a valid shape too — not only for the ones the
        # harvester could not place.
        self.assertIn("including one graded 'migrate'", out)

        out = await discovery_datareview_3.tools.annotate_data_dependency(
            address="aws_dynamodb_table.sessions", disposition="keep-in-aws",
            note="staying on DynamoDB; the analytics pipeline reads it directly",
            ctx=mock_ctx)
        self.assertNotIn("ERROR", out)
        entry = self._entry(inventory_container, "checkout-sessions")
        self.assertEqual(entry["disposition"], "keep-in-aws")

        out = await discovery_datareview_3.tools.confirm_data_dependencies(
            ctx=mock_ctx)

        self.assertIn("SUCCESS", out)
        self.assertIn("staying in AWS", out)
        # The commitment it creates is named at the gate, not left implied:
        # nothing in the agent provisions cross-cloud connectivity today.
        self.assertIn("cross-cloud connectivity", out)
        stamp = state_container["state"]["variables"]["data_dependency_review"]
        self.assertEqual(stamp["kept_in_aws"], 1)

        # And the decision is a correction like any other: it outlives a
        # re-scan, which would otherwise re-grade the table `escalate`.
        self._rewind_to_the_scan(state_container)
        await discovery_datascan_3.tools.scan_data_dependencies(ctx=mock_ctx)
        self.assertEqual(
            self._entry(inventory_container, "checkout-sessions")["disposition"],
            "keep-in-aws")

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_reviewed_section_is_exactly_what_the_next_scan_rebuilds(
            self, mock_gcs_client_class, mock_get_email):
        """The invariant the whole step rests on, asserted once rather than
        case by case. The review used to edit the section in place while a
        re-scan replayed the surviving records over a freshly derived one, and
        four review rounds found four sequences where the two disagreed — the
        reviewer approving a section the next scan would not produce. Both
        paths now call overrides.rebuild, so this drives a mixed sequence
        through the real tools and asserts the section is byte-identical after
        a real re-scan."""
        from servers.phases.discovery import (
            discovery_datareview_3, discovery_datascan_3)
        mock_get_email.return_value = "platform-user@google.com"
        state_container, inventory_container, _, mock_ctx = \
            await self._reach_the_data_review(mock_gcs_client_class)
        tools = discovery_datareview_3.tools

        # Every kind, including the sequences that used to diverge: a reworded
        # rejection, a re-attach, an annotation split across two calls, and a
        # decision reversed after the fact.
        for call in (
            tools.reject_data_consumer(
                address="aws_db_instance.orders", workload="orders",
                reason="it only reads the replica", ctx=mock_ctx),
            tools.reject_data_consumer(
                address="aws_db_instance.orders", workload="orders",
                reason="no: it never touches this at all", ctx=mock_ctx),
            tools.attach_data_consumer(
                address="aws_s3_bucket.orders_exports", workload="billing",
                note="the nightly export writes it", ctx=mock_ctx),
            tools.attach_data_consumer(
                address="aws_s3_bucket.orders_exports", workload="billing",
                namespace="billing", ctx=mock_ctx),
            tools.annotate_data_dependency(
                address="aws_s3_bucket.orders_exports",
                note="the analytics team reads it from Athena", ctx=mock_ctx),
            tools.annotate_data_dependency(
                address="aws_s3_bucket.orders_exports",
                disposition="keep-in-aws", ctx=mock_ctx),
        ):
            self.assertNotIn("ERROR", await call)

        reviewed = copy.deepcopy(
            inventory_container["inventory"]["data_dependencies"])

        self._rewind_to_the_scan(state_container)
        out = await discovery_datascan_3.tools.scan_data_dependencies(ctx=mock_ctx)
        self.assertNotIn("ERROR", out)
        rescanned = inventory_container["inventory"]["data_dependencies"]

        self.assertEqual(reviewed, rescanned)
        # ...and it is not trivially equal because nothing was applied.
        billing = next(e for e in rescanned if e["identifier"] == "orders-exports")
        self.assertEqual(billing["disposition"], "keep-in-aws")
        self.assertEqual([c["workload"] for c in billing["consumers"]], ["billing"])
        self.assertEqual(billing["consumers"][0]["namespace"], "billing")
        self.assertEqual(billing["consumers"][0]["note"],
                         "the nightly export writes it")
        self.assertTrue(any("Athena" in n for n in billing["notes"]))

    # Two chains reaching one database under one workload name: a helm_release
    # wiring the endpoint in, and an IRSA role whose service account is called
    # the same thing. They are two different Kubernetes objects.
    TWO_CHAINS_TF = (
        'resource "aws_db_instance" "orders" {\n'
        '  identifier = "orders-db"\n}\n'
        'resource "helm_release" "orders" {\n'
        '  name      = "orders"\n'
        '  namespace = "orders"\n'
        '  set { value = aws_db_instance.orders.endpoint }\n}\n'
        'module "orders_irsa" {\n'
        '  source = "terraform-aws-modules/iam/aws//modules/'
        'iam-role-for-service-accounts-eks"\n'
        '  role_policy_arns = { db = aws_db_instance.orders.arn }\n'
        '  oidc_providers = {\n'
        '    main = { namespace_service_accounts = ["orders:orders"] }\n'
        '  }\n}\n'
    )

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_rejection_does_not_delete_the_other_chains_attribution(
            self, mock_gcs_client_class, mock_get_email):
        """One workload name, two consumers. Told the IRSA grant is over-broad,
        the review used to delete the helm_release attribution too — the one
        that tells the workload data gate which team owns the database — durably, on
        every later scan, and reported a singular success. `_target` refuses
        this kind of spread one level up; nothing refused it here."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, inventory_container, _, mock_ctx = await self._reach_the_data_review(
            mock_gcs_client_class, tf=self.TWO_CHAINS_TF)
        kinds = {c["kind"] for c in
                 self._entry(inventory_container, "orders-db")["consumers"]}
        self.assertEqual(kinds, {"helm_release", "service_account"})

        # Unqualified, it refuses and names the choice.
        out = await discovery_datareview_3.tools.reject_data_consumer(
            address="aws_db_instance.orders", workload="orders",
            reason="the IRSA role is over-broad", ctx=mock_ctx)
        self.assertIn("ERROR", out)
        self.assertIn("helm_release", out)
        self.assertIn("service_account", out)

        out = await discovery_datareview_3.tools.reject_data_consumer(
            address="aws_db_instance.orders", workload="orders",
            kind="service_account", reason="the IRSA role is over-broad",
            ctx=mock_ctx)
        self.assertNotIn("ERROR", out)

        entry = self._entry(inventory_container, "orders-db")
        self.assertEqual([(c["workload"], c["kind"]) for c in entry["consumers"]],
                         [("orders", "helm_release")])
        # The surviving link is a real attribution, so the entry is not
        # unattributed and the rejection note names which one went.
        self.assertNotIn(consumers_lib.UNATTRIBUTED_NOTE, entry["notes"])
        self.assertTrue(any("(service_account)" in n and "over-broad" in n
                            for n in entry["notes"]))

    # An `_override.tf` renaming a resource — the ordinary reason to write
    # one — leaves two entries at one address in one directory, told apart
    # only by their identifier.
    RENAMED_BY_OVERRIDE_TF = {
        "envs/prod/main.tf":
            'resource "aws_db_instance" "orders" {\n'
            '  identifier = "orders-db"\n}\n'
            'resource "helm_release" "orders" {\n'
            '  name = "orders"\n'
            '  set { value = aws_db_instance.orders.endpoint }\n}\n',
        "envs/prod/orders_override.tf":
            'resource "aws_db_instance" "orders" {\n'
            '  identifier = "orders-db-legacy"\n}\n',
    }

    async def _reach_review_with_files(self, mock_gcs_client_class, files):
        from servers.phases.discovery import discovery_scope_2, discovery_datascan_3
        state_container, inventory_container, mock_ctx = self._setup_image_flow(
            mock_gcs_client_class)
        overrides_container = self._install_overrides_roundtrip()
        root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, root, ignore_errors=True)
        self._write_image_fixture(root, with_render_targets=False)
        for rel_path, content in files.items():
            os.makedirs(os.path.join(root, os.path.dirname(rel_path)),
                        exist_ok=True)
            with open(os.path.join(root, rel_path), "w") as f:
                f.write(content)
        await main.discover_configuration_files(ctx=mock_ctx, root_dir=root)
        await discovery_scope_2.tools.confirm_discovery_scope(ctx=mock_ctx)
        out = await discovery_datascan_3.tools.scan_data_dependencies(ctx=mock_ctx)
        self.assertNotIn("ERROR", out)
        return state_container, inventory_container, overrides_container, mock_ctx

    # One release reaching TWO datastores. `consumers.record` used to append
    # the same dict to both entries, so anything that later filled a field on
    # one wrote it onto the other — and only on the path that had not been
    # through JSON.
    SHARED_WORKLOAD_TF = (
        'resource "aws_db_instance" "orders" {\n'
        '  identifier = "orders-db"\n}\n'
        'resource "aws_s3_bucket" "orders_exports" {\n'
        '  bucket = "orders-exports"\n}\n'
        'resource "helm_release" "orders" {\n'
        '  name  = "orders"\n'
        '  chart = "bitnami/orders"\n'
        '  set { value = aws_db_instance.orders.endpoint }\n'
        '  set { value = aws_s3_bucket.orders_exports.bucket }\n}\n'
    )

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_confirmation_does_not_spread_to_the_other_service(
            self, mock_gcs_client_class, mock_get_email):
        """The invariant test's fixture had every workload reaching exactly one
        datastore, so aliasing never showed. With one release reaching two, a
        chart path stated about ONE of them appeared on both after the next
        scan — the spread-a-human-decision failure, invisible until after the
        sign-off."""
        from servers.phases.discovery import (
            discovery_datareview_3, discovery_datascan_3)
        mock_get_email.return_value = "platform-user@google.com"
        state_container, inventory_container, _, mock_ctx = \
            await self._reach_the_data_review(
                mock_gcs_client_class, tf=self.SHARED_WORKLOAD_TF)

        await discovery_datareview_3.tools.attach_data_consumer(
            address="aws_db_instance.orders", workload="orders",
            namespace="orders-prod", source_path="charts/orders", ctx=mock_ctx)

        def scopes():
            return {e["identifier"]: [(c.get("namespace"), c.get("source_path"))
                                      for c in e["consumers"]]
                    for e in inventory_container["inventory"]["data_dependencies"]}

        reviewed = scopes()
        self.assertEqual(reviewed["orders-db"], [("orders-prod", "charts/orders")])
        self.assertEqual(reviewed["orders-exports"], [(None, None)])

        self._rewind_to_the_scan(state_container)
        await discovery_datascan_3.tools.scan_data_dependencies(ctx=mock_ctx)
        self.assertEqual(scopes(), reviewed)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_rejection_note_does_not_outlive_the_link_coming_back(
            self, mock_gcs_client_class, mock_get_email):
        """A rejection and an attachment of one link can both stand in the
        store — an attachment may only retire a rejection naming exactly the
        same link. But when the attachment re-creates what the rejection
        removed, the entry listed the consumer AND carried a note saying a
        named human rejected it."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, inventory_container, _, mock_ctx = \
            await self._reach_the_data_review(mock_gcs_client_class)
        tools = discovery_datareview_3.tools

        await tools.reject_data_consumer(
            address="aws_db_instance.orders", workload="orders",
            kind="helm_release", reason="that release only reads the replica",
            ctx=mock_ctx)
        await tools.attach_data_consumer(
            address="aws_db_instance.orders", workload="orders",
            note="the orders team confirmed they own it", ctx=mock_ctx)

        entry = self._entry(inventory_container, "orders-db")
        self.assertEqual([c["kind"] for c in entry["consumers"]], ["helm_release"])
        self.assertFalse(any("was rejected at the data review" in n
                             for n in entry["notes"]))
        self.assertFalse(any("attached at the data review was withdrawn" in n
                             for n in entry["notes"]))

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_candidate_note_admits_the_chain_the_reviewer_cut(
            self, mock_gcs_client_class, mock_get_email):
        """The empty-pool branch was fixed first. With two workloads in the
        pool the ranked branch still told the reviewer no chain reached an
        entry, four lines under their own note saying one had."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        two_releases = (
            'resource "aws_db_instance" "orders" {\n'
            '  identifier = "orders-db"\n}\n'
            'resource "helm_release" "orders" {\n'
            '  name = "orders"\n'
            '  set { value = aws_db_instance.orders.endpoint }\n}\n'
            'resource "helm_release" "orders_api" {\n'
            '  name = "orders-api"\n}\n'
        )
        _, _, _, mock_ctx = await self._reach_the_data_review(
            mock_gcs_client_class, tf=two_releases)

        await discovery_datareview_3.tools.reject_data_consumer(
            address="aws_db_instance.orders", workload="orders",
            reason="wrong team", ctx=mock_ctx)
        out = await discovery_datareview_3.tools.list_data_dependencies(
            ctx=mock_ctx)

        self.assertNotIn("none reached this one", out)
        self.assertIn("the one that reached this was rejected at the review", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_transient_baseline_read_at_the_review_is_not_an_absence(
            self, mock_gcs_client_class, mock_get_email):
        """The `baseline_error` guard's only test ran at the deployment step,
        where the fallback branch re-grades the object and reproduces the
        message — so deleting the guard left the suite green. At the REVIEW
        the fallback says the object is missing and prescribes a re-scan,
        which is round 9's defect relocated."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, _, _, mock_ctx = await self._reach_the_data_review(
            mock_gcs_client_class)
        (main.state_mgr.gcs_client.bucket("test-bucket")
         .blob(discovery_datareview_3.tools.SCAN_BASELINE_BLOB)
         .download_as_text.side_effect) = exceptions.ServiceUnavailable(
             "backend error")

        out = await discovery_datareview_3.tools.annotate_data_dependency(
            address="aws_s3_bucket.orders_exports", note="checked",
            ctx=mock_ctx)

        self.assertIn("could not be read", out)
        self.assertIn("try again", out)
        self.assertNotIn("missing or unreadable", out)
        self.assertNotIn("Re-run scan_data_dependencies", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_corrections_object_of_the_wrong_shape_is_an_error(
            self, mock_gcs_client_class, mock_get_email):
        """Hand-editing this object is anticipated — the refusal tells an
        operator to repair it. A bare list is valid JSON, passed the guard, and
        raised an AttributeError out of every tool in the step."""
        from servers.phases.discovery import (
            discovery_datareview_3, discovery_datascan_3)
        mock_get_email.return_value = "platform-user@google.com"
        state_container, _, _, mock_ctx = \
            await self._reach_the_data_review(mock_gcs_client_class)
        self.mock_overrides_blob.reload.side_effect = None
        self.mock_overrides_blob.download_as_text.side_effect = None
        self.mock_overrides_blob.download_as_text.return_value = "[]"

        for call in (
            discovery_datareview_3.tools.list_data_dependencies(ctx=mock_ctx),
            discovery_datareview_3.tools.annotate_data_dependency(
                address="aws_s3_bucket.orders_exports", note="x", ctx=mock_ctx),
        ):
            self.assertIn("ERROR", await call)

        self._rewind_to_the_scan(state_container)
        out = await discovery_datascan_3.tools.scan_data_dependencies(ctx=mock_ctx)
        self.assertIn("ERROR", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_attaching_one_chain_does_not_un_reject_another(
            self, mock_gcs_client_class, mock_get_email):
        """Retiring a rejection RESTORES a consumer, so an attachment may only
        retire one that names exactly the same link. Under the looser rule,
        asserting the service account brought back the helm_release link the
        reviewer had rejected — durably, on every later scan, with the reason
        gone from the entry and from the store."""
        from servers.phases.discovery import (
            discovery_datareview_3, discovery_datascan_3)
        mock_get_email.return_value = "platform-user@google.com"
        state_container, inventory_container, overrides_container, mock_ctx = \
            await self._reach_the_data_review(mock_gcs_client_class)
        tools = discovery_datareview_3.tools

        await tools.reject_data_consumer(
            address="aws_db_instance.orders", workload="orders",
            reason="that release only reads the replica", ctx=mock_ctx)
        await tools.attach_data_consumer(
            address="aws_db_instance.orders", workload="orders",
            kind="service_account", note="the orders SA reads it directly",
            ctx=mock_ctx)

        entry = self._entry(inventory_container, "orders-db")
        self.assertEqual([(c["kind"], c["detection"]) for c in entry["consumers"]],
                         [("service_account", "human_review")])
        self.assertTrue(any("only reads the replica" in n for n in entry["notes"]))
        self.assertEqual(len(overrides_container["document"]["overrides"]), 2)

        # ...and the rejection is still in force after a re-scan derives the
        # helm_release link again.
        self._rewind_to_the_scan(state_container)
        await discovery_datascan_3.tools.scan_data_dependencies(ctx=mock_ctx)
        self.assertEqual(
            [(c["kind"], c["detection"]) for c in
             self._entry(inventory_container, "orders-db")["consumers"]],
            [("service_account", "human_review")])

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_confirming_a_derived_link_keeps_what_the_reviewer_added(
            self, mock_gcs_client_class, mock_get_email):
        """`_chart_path` is None for any registry chart, so `source_path` is
        routinely absent and routinely what the reviewer is adding — and the
        schema calls it the link to a developer's component scope. Recording
        only the note dropped it while reporting "nothing else changed"."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, inventory_container, _, mock_ctx = \
            await self._reach_the_data_review(mock_gcs_client_class)

        out = await discovery_datareview_3.tools.attach_data_consumer(
            address="aws_db_instance.orders", workload="orders",
            source_path="src/orders/chart",
            note="chart lives in the monorepo, not in the Terraform",
            ctx=mock_ctx)

        self.assertNotIn("ERROR", out)
        derived = self._entry(inventory_container, "orders-db")["consumers"][0]
        self.assertEqual(derived["detection"], "terraform_wiring")
        self.assertEqual(derived["source_path"], "src/orders/chart")
        # The namespace the Terraform states is a fact with evidence; it stands.
        self.assertEqual(derived["namespace"], "orders")

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_narrowing_reword_of_a_covering_rejection_is_refused(
            self, mock_gcs_client_class, mock_get_email):
        """The guard admitted it as a reword and `covers` — correctly — then
        declined to supersede a broad decision with a narrow one, so the call
        became a second standing rejection and the entry carried two reasons
        for one link, neither of them withdrawable."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, inventory_container, overrides_container, mock_ctx = \
            await self._reach_the_data_review(mock_gcs_client_class)
        tools = discovery_datareview_3.tools

        await tools.reject_data_consumer(
            address="aws_db_instance.orders", workload="orders",
            reason="R1 wrong team", ctx=mock_ctx)
        out = await tools.reject_data_consumer(
            address="aws_db_instance.orders", workload="orders",
            kind="helm_release", reason="R2 sharper: it only reads the replica",
            ctx=mock_ctx)

        self.assertIn("ERROR", out)
        self.assertIn("R1 wrong team", out)
        self.assertEqual(len(overrides_container["document"]["overrides"]), 1)
        reasons = [n for n in
                   self._entry(inventory_container, "orders-db")["notes"]
                   if "was rejected at the data review" in n]
        self.assertEqual(len(reasons), 1)

        # Rewording the decision that IS standing works.
        out = await tools.reject_data_consumer(
            address="aws_db_instance.orders", workload="orders",
            reason="R2 sharper: it only reads the replica", ctx=mock_ctx)
        self.assertNotIn("ERROR", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_ambiguity_refusal_offers_each_kind_once(
            self, mock_gcs_client_class, mock_get_email):
        """Two blocks of one kind and name are one answerable choice. Listing
        "helm_release, helm_release" is a refusal with no argument that
        resolves it."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        two_releases = self.TWO_CHAINS_TF + (
            'resource "helm_release" "orders_batch" {\n'
            '  name      = "orders"\n'
            '  namespace = "batch"\n'
            '  set { value = aws_db_instance.orders.endpoint }\n}\n')
        _, _, _, mock_ctx = await self._reach_the_data_review(
            mock_gcs_client_class, tf=two_releases)

        out = await discovery_datareview_3.tools.reject_data_consumer(
            address="aws_db_instance.orders", workload="orders",
            reason="x", ctx=mock_ctx)

        self.assertIn("ERROR", out)
        self.assertEqual(out.count("helm_release"), 1)
        self.assertIn("2 different kinds", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_narrowing_an_attachment_corrects_it_rather_than_adding(
            self, mock_gcs_client_class, mock_get_email):
        """"Actually it is the service account" is a correction of an earlier
        unqualified assertion, not a second one. Supersession asked whether the
        new record COVERS the old — which a narrowing never does — so the
        retracted consumer stayed in the gating section, durably, and the
        reviewer was told nothing."""
        from servers.phases.discovery import (
            discovery_datareview_3, discovery_datascan_3)
        mock_get_email.return_value = "platform-user@google.com"
        state_container, inventory_container, overrides_container, mock_ctx = \
            await self._reach_the_data_review(mock_gcs_client_class)
        tools = discovery_datareview_3.tools

        await tools.attach_data_consumer(
            address="aws_s3_bucket.orders_exports", workload="orders",
            note="team says the nightly export writes it", ctx=mock_ctx)
        out = await tools.attach_data_consumer(
            address="aws_s3_bucket.orders_exports", workload="orders",
            kind="service_account",
            note="actually it is the IRSA service account", ctx=mock_ctx)

        self.assertNotIn("ERROR", out)
        self.assertEqual(
            [(c["kind"], c["detection"]) for c in
             self._entry(inventory_container, "orders-exports")["consumers"]],
            [("service_account", "human_review")])
        self.assertEqual(len([o for o in overrides_container["document"]["overrides"]
                              if o["kind"] == "attach_consumer"]), 1)

        # ...and the correction, not the retracted assertion, is what replays.
        self._rewind_to_the_scan(state_container)
        await discovery_datascan_3.tools.scan_data_dependencies(ctx=mock_ctx)
        self.assertEqual(
            [c["kind"] for c in
             self._entry(inventory_container, "orders-exports")["consumers"]],
            ["service_account"])

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_kind_scoped_rejection_retires_the_confirmation_it_contradicts(
            self, mock_gcs_client_class, mock_get_email):
        """"Was confirmed" and "was rejected" must not stand on one link at
        once. Naming a kind on the rejection brought that back: the
        confirmation was wider, so it survived, and the section handed to the
        assessment had one named human asserting both."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, inventory_container, _, mock_ctx = \
            await self._reach_the_data_review(mock_gcs_client_class)
        tools = discovery_datareview_3.tools

        await tools.attach_data_consumer(
            address="aws_db_instance.orders", workload="orders",
            note="confirmed with the orders team", ctx=mock_ctx)
        await tools.reject_data_consumer(
            address="aws_db_instance.orders", workload="orders",
            kind="helm_release", reason="it only reads the replica",
            ctx=mock_ctx)

        notes = self._entry(inventory_container, "orders-db")["notes"]
        self.assertFalse(any("was confirmed at the data review" in n
                             for n in notes))
        self.assertTrue(any("was rejected at the data review" in n
                            and "only reads the replica" in n for n in notes))
        out = await tools.list_data_dependencies(ctx=mock_ctx)
        self.assertNotIn("attach consumer 'orders'", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_an_ambiguous_confirmation_is_refused_like_a_rejection(
            self, mock_gcs_client_class, mock_get_email):
        """Both chains reach the entry under one name. `reject_data_consumer`
        refuses; `attach_data_consumer` took whichever consumer came first,
        recorded the reviewer's reason against a Kubernetes object they had not
        named, and quoted the other one's chain back to them."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, inventory_container, _, mock_ctx = await self._reach_the_data_review(
            mock_gcs_client_class, tf=self.TWO_CHAINS_TF)
        tools = discovery_datareview_3.tools

        out = await tools.attach_data_consumer(
            address="aws_db_instance.orders", workload="orders",
            note="the IRSA role is the real user; the release just templates it",
            ctx=mock_ctx)

        self.assertIn("ERROR", out)
        self.assertIn("helm_release", out)
        self.assertIn("service_account", out)
        self.assertFalse(any("was confirmed" in n for n in
                             self._entry(inventory_container, "orders-db")["notes"]))

        out = await tools.attach_data_consumer(
            address="aws_db_instance.orders", workload="orders",
            kind="service_account",
            note="the IRSA role is the real user", ctx=mock_ctx)
        self.assertNotIn("ERROR", out)
        self.assertTrue(any("(service_account) was confirmed" in n for n in
                            self._entry(inventory_container, "orders-db")["notes"]))

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_withdrawing_your_own_attachment_does_not_claim_a_chain_found_it(
            self, mock_gcs_client_class, mock_get_email):
        """Emptying an entry by withdrawing a consumer the reviewer attached is
        not the same as cutting a link the Terraform declared. Crediting it to
        the second category states something false about the Terraform AND
        drops the entry out of every unattributed count — the section, the
        sign-off, the stamp and the scan notes the assessment reads — so the
        one service still needing an owner stops being counted as one."""
        from servers.phases.discovery import (
            discovery_datareview_3, discovery_datascan_3)
        mock_get_email.return_value = "platform-user@google.com"
        state_container, inventory_container, _, mock_ctx = \
            await self._reach_the_data_review(mock_gcs_client_class)
        tools = discovery_datareview_3.tools
        # No chain reaches orders-exports; it starts out unattributed.
        self.assertIn(consumers_lib.UNATTRIBUTED_NOTE,
                      self._entry(inventory_container, "orders-exports")["notes"])

        await tools.attach_data_consumer(
            address="aws_s3_bucket.orders_exports", workload="orders",
            note="ops said the orders release writes here", ctx=mock_ctx)
        # Kind-scoped, so it does not supersede the unqualified attach — the
        # combination that reached the miscount.
        await tools.reject_data_consumer(
            address="aws_s3_bucket.orders_exports", workload="orders",
            kind="helm_release", reason="wrong, it is the archiver",
            ctx=mock_ctx)

        entry = self._entry(inventory_container, "orders-exports")
        self.assertEqual(entry["consumers"], [])
        self.assertIn(consumers_lib.UNATTRIBUTED_NOTE, entry["notes"])
        self.assertNotIn(consumers_lib.REJECTED_NOTE, entry["notes"])

        # ...and it is still counted as unplaced at the sign-off and after a
        # re-scan, which is where the durable record ends up.
        out = await tools.confirm_data_dependencies(ctx=mock_ctx)
        self.assertNotIn("ERROR", out)
        stamp = state_container["state"]["variables"]["data_dependency_review"]
        self.assertEqual(stamp["unattributed"], 1)
        self.assertEqual(stamp["rejected"], 0)

        self._rewind_to_the_scan(state_container)
        out = await discovery_datascan_3.tools.scan_data_dependencies(ctx=mock_ctx)
        self.assertIn("1 not attributable to a workload", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_an_ambiguous_reattach_is_refused_rather_than_guessed(
            self, mock_gcs_client_class, mock_get_email):
        """Two standing assertions about one workload under different chains.
        An unqualified third call took whichever was recorded first, wrote the
        reviewer's namespace onto a consumer they had not named, and reported
        the other one. `reject_data_consumer` refuses exactly this."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, inventory_container, _, mock_ctx = \
            await self._reach_the_data_review(mock_gcs_client_class)
        tools = discovery_datareview_3.tools

        await tools.attach_data_consumer(
            address="aws_s3_bucket.orders_exports", workload="archiver",
            kind="helm_release", namespace="a", note="the release",
            ctx=mock_ctx)
        await tools.attach_data_consumer(
            address="aws_s3_bucket.orders_exports", workload="archiver",
            kind="service_account", namespace="b", note="the IRSA role",
            ctx=mock_ctx)

        out = await tools.attach_data_consumer(
            address="aws_s3_bucket.orders_exports", workload="archiver",
            namespace="corrected-ns", ctx=mock_ctx)

        self.assertIn("ERROR", out)
        self.assertIn("helm_release", out)
        self.assertIn("service_account", out)
        # Neither assertion was touched.
        by_kind = {c["kind"]: c for c in
                   self._entry(inventory_container, "orders-exports")["consumers"]}
        self.assertEqual(by_kind["helm_release"]["namespace"], "a")
        self.assertEqual(by_kind["service_account"]["namespace"], "b")

        # Named, it lands on the one they meant and says which.
        out = await tools.attach_data_consumer(
            address="aws_s3_bucket.orders_exports", workload="archiver",
            kind="helm_release", namespace="corrected-ns", ctx=mock_ctx)
        self.assertNotIn("ERROR", out)
        self.assertIn("helm_release", out)
        by_kind = {c["kind"]: c for c in
                   self._entry(inventory_container, "orders-exports")["consumers"]}
        self.assertEqual(by_kind["helm_release"]["namespace"], "corrected-ns")
        self.assertEqual(by_kind["service_account"]["namespace"], "b")

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_standing_corrections_list_tells_two_chains_apart(
            self, mock_gcs_client_class, mock_get_email):
        """The listing is the reviewer's only view of the durable store. Two
        decisions differing only by chain rendered as identical lines, so a
        reviewer could not tell which of two rejections was stale."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, _, _, mock_ctx = await self._reach_the_data_review(
            mock_gcs_client_class, tf=self.TWO_CHAINS_TF)
        tools = discovery_datareview_3.tools

        await tools.reject_data_consumer(
            address="aws_db_instance.orders", workload="orders",
            kind="helm_release", reason="wrong release", ctx=mock_ctx)
        await tools.reject_data_consumer(
            address="aws_db_instance.orders", workload="orders",
            kind="service_account", reason="over-broad grant", ctx=mock_ctx)

        out = await tools.list_data_dependencies(ctx=mock_ctx)
        self.assertIn("reject consumer 'orders' (helm_release)", out)
        self.assertIn("reject consumer 'orders' (service_account)", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_narrower_rejection_does_not_retire_a_broader_one(
            self, mock_gcs_client_class, mock_get_email):
        """`kind` is optional, so the ordinary first rejection is unqualified —
        "reject orders", meaning every chain. A later qualified one is
        NARROWER, and retiring the broad decision for it put the rejected
        consumer back on the entry silently, while writing a durable claim
        about a consumer the Terraform never declared."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, inventory_container, overrides_container, mock_ctx = \
            await self._reach_the_data_review(mock_gcs_client_class)

        await discovery_datareview_3.tools.reject_data_consumer(
            address="aws_db_instance.orders", workload="orders",
            reason="that release only reads the replica", ctx=mock_ctx)
        self.assertEqual(
            self._entry(inventory_container, "orders-db")["consumers"], [])

        out = await discovery_datareview_3.tools.reject_data_consumer(
            address="aws_db_instance.orders", workload="orders",
            kind="service_account", reason="the IRSA role, I mean",
            ctx=mock_ctx)

        # The entry never had a service_account consumer, so this is a typo —
        # and the standing broad rejection must not license it.
        self.assertIn("ERROR", out)
        entry = self._entry(inventory_container, "orders-db")
        self.assertEqual(entry["consumers"], [])
        self.assertFalse(any("(service_account)" in n for n in entry["notes"]))
        self.assertEqual(len(overrides_container["document"]["overrides"]), 1)
        self.assertTrue(any("only reads the replica" in n
                            for n in entry["notes"]))

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_an_unqualified_reattach_does_not_borrow_another_kinds_scope(
            self, mock_gcs_client_class, mock_get_email):
        """The scan pool fills in details, never identity. Letting it decide
        `consumer_kind` made a broad assertion narrow behind the reviewer's
        back and copied a chart path off a different Kubernetes object into
        their own words — and `source_path` is what points a workload gate at
        a component."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        wired = (
            'resource "aws_db_instance" "orders" {\n'
            '  identifier = "orders-db"\n}\n'
            'resource "helm_release" "orders" {\n'
            '  name      = "orders"\n'
            '  namespace = "orders-ns"\n'
            '  chart     = "./charts/orders"\n'
            '  set { value = aws_db_instance.orders.endpoint }\n}\n'
        )
        _, inventory_container, _, mock_ctx = await self._reach_the_data_review(
            mock_gcs_client_class, tf=wired)
        tools = discovery_datareview_3.tools

        await tools.attach_data_consumer(
            address="aws_db_instance.orders", workload="orders",
            kind="service_account", note="the IRSA role reaches it too",
            ctx=mock_ctx)
        # The entry now lists two consumers of that name — the derived
        # helm_release and the asserted service_account — so an unqualified
        # call is ambiguous and is refused rather than aimed by list order.
        out = await tools.attach_data_consumer(
            address="aws_db_instance.orders", workload="orders",
            note="confirmed with the payments team", ctx=mock_ctx)
        self.assertIn("ERROR", out)
        self.assertIn("different objects", out)

        out = await tools.attach_data_consumer(
            address="aws_db_instance.orders", workload="orders",
            kind="service_account", note="confirmed with the payments team",
            ctx=mock_ctx)
        self.assertNotIn("ERROR", out)
        asserted = [c for c in
                    self._entry(inventory_container, "orders-db")["consumers"]
                    if c["detection"] == "human_review"]
        self.assertEqual(len(asserted), 1)
        # Not the helm_release's namespace or chart path.
        self.assertNotEqual(asserted[0].get("source_path"), "charts/orders")
        self.assertNotEqual(asserted[0].get("namespace"), "orders-ns")

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_correction_does_not_revert_a_sibling_entrys_decision(
            self, mock_gcs_client_class, mock_get_email):
        """Two entries share an address AND a directory, so `identifier` is the
        only disambiguator — and it is the one `_target` offers the reviewer.
        Leaving it out of the override key had the second correction retire the
        first: here a customer's keep-in-aws decision, and its stated reason,
        silently reverting to `migrate`, the one disposition that gates a team."""
        from servers.phases.discovery import (
            discovery_datareview_3, discovery_datascan_3)
        mock_get_email.return_value = "platform-user@google.com"
        state_container, inventory_container, _, mock_ctx = \
            await self._reach_review_with_files(
                mock_gcs_client_class, self.RENAMED_BY_OVERRIDE_TF)
        tools = discovery_datareview_3.tools

        entries = inventory_container["inventory"]["data_dependencies"]
        self.assertEqual(sorted(e["identifier"] for e in entries),
                         ["orders-db", "orders-db-legacy"])
        self.assertEqual({e["address"] for e in entries},
                         {"aws_db_instance.orders"})

        await tools.annotate_data_dependency(
            address="aws_db_instance.orders", identifier="orders-db",
            disposition="keep-in-aws",
            note="customer keeps the live DB in AWS", ctx=mock_ctx)
        await tools.annotate_data_dependency(
            address="aws_db_instance.orders", identifier="orders-db-legacy",
            disposition="rebuild", note="legacy copy, rebuild empty",
            ctx=mock_ctx)

        by_id = {e["identifier"]: e for e in
                 inventory_container["inventory"]["data_dependencies"]}
        self.assertEqual(by_id["orders-db"]["disposition"], "keep-in-aws")
        self.assertEqual(by_id["orders-db-legacy"]["disposition"], "rebuild")
        self.assertTrue(any("keeps the live DB" in n
                            for n in by_id["orders-db"]["notes"]))

        # ...and both survive the re-scan, which is where the durable record
        # is the only thing left.
        self._rewind_to_the_scan(state_container)
        await discovery_datascan_3.tools.scan_data_dependencies(ctx=mock_ctx)
        by_id = {e["identifier"]: e for e in
                 inventory_container["inventory"]["data_dependencies"]}
        self.assertEqual(by_id["orders-db"]["disposition"], "keep-in-aws")
        self.assertEqual(by_id["orders-db-legacy"]["disposition"], "rebuild")

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_identifier_is_recorded_only_when_it_disambiguates(
            self, mock_gcs_client_class, mock_get_email):
        """Recording it when it is not needed makes the correction brittle: a
        later scan where the resource is renamed cannot place a record that
        names the old name, while an address-only record still applies. So the
        record carries the minimum that identifies its target."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, _, overrides_container, mock_ctx = \
            await self._reach_the_data_review(mock_gcs_client_class)

        # One entry at this address: the identifier is passed but not needed.
        await discovery_datareview_3.tools.annotate_data_dependency(
            address="aws_s3_bucket.orders_exports", identifier="orders-exports",
            note="ask the analytics team", ctx=mock_ctx)
        self.assertIsNone(
            overrides_container["document"]["overrides"][0].get("identifier"))

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_rejecting_one_chain_still_offers_the_other_as_a_candidate(
            self, mock_gcs_client_class, mock_get_email):
        """The pool filter matched on the workload name, so rejecting the IRSA
        link also hid the helm_release of the same name — a different
        Kubernetes object the reviewer may well want to attach — and emptied
        the pool, at which point the ranker claimed the scan had seen no
        workload at all in a listing that names one two lines above."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, _, _, mock_ctx = await self._reach_the_data_review(
            mock_gcs_client_class, tf=self.TWO_CHAINS_TF)

        await discovery_datareview_3.tools.reject_data_consumer(
            address="aws_db_instance.orders", workload="orders",
            kind="service_account", reason="the IRSA grant is over-broad",
            ctx=mock_ctx)
        out = await discovery_datareview_3.tools.list_data_dependencies(
            ctx=mock_ctx)

        self.assertNotIn("no workload at all", out)
        # The helm_release consumer is still derived, so the entry is not even
        # consumer-less — the point is that the rejection did not reach it.
        entry_kinds = {c["kind"] for c in
                       (await self._entry_after(mock_ctx))["consumers"]}
        self.assertEqual(entry_kinds, {"helm_release"})

    async def _entry_after(self, mock_ctx):
        bucket = main.state_mgr.gcs_client.bucket("test-bucket")
        inventory, _ = main.state_mgr.load_inventory(bucket)
        return next(e for e in inventory["data_dependencies"]
                    if e["identifier"] == "orders-db")

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_sign_off_does_not_call_a_rejected_link_unplaceable(
            self, mock_gcs_client_class, mock_get_email):
        """"It just cannot be placed with a team yet" is the sentence
        removed from the scan notes for an entry a reviewer emptied. This is
        the one place a human answers a question, and the stamp is the durable
        record of what they approved."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        state_container, _, _, mock_ctx = await self._reach_the_data_review(
            mock_gcs_client_class)
        asked = {}

        async def capture(request, *args, **kwargs):
            asked["prompt"] = request.root.params.message
            answer = MagicMock()
            answer.action, answer.content = "accept", {"approved": True}
            return answer

        await discovery_datareview_3.tools.reject_data_consumer(
            address="aws_db_instance.orders", workload="orders",
            reason="only reads the replica", ctx=mock_ctx)
        mock_ctx.request_context.session.send_request.side_effect = capture
        await discovery_datareview_3.tools.confirm_data_dependencies(ctx=mock_ctx)

        self.assertIn("because you rejected the one the Terraform pointed at",
                      asked["prompt"])
        stamp = state_container["state"]["variables"]["data_dependency_review"]
        self.assertEqual(stamp["rejected"], 1)
        # orders-exports has genuinely never been attributed; orders-db has.
        self.assertEqual(stamp["unattributed"], 1)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_an_unqualified_attach_is_not_promoted_to_an_arbitrary_kind(
            self, mock_gcs_client_class, mock_get_email):
        """The candidate pool holds one workload name under two kinds. Taking
        the first match turned the wildcard the reviewer meant into
        `helm_release`, so the DERIVED service_account was no longer
        recognised as derived: a second consumer was appended, carrying a
        namespace copied off the unrelated release and asserted as something a
        named human said — while the tool reported a confirmation."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        # A table reached only by IRSA, plus an unrelated release of the same
        # name that the pool also remembers.
        two_in_the_pool = (
            'resource "aws_dynamodb_table" "orders" {\n'
            '  name = "orders-table"\n}\n'
            'resource "helm_release" "orders" {\n'
            '  name      = "orders"\n'
            '  namespace = "platform"\n}\n'
            'module "orders_irsa" {\n'
            '  source = "terraform-aws-modules/iam/aws//modules/'
            'iam-role-for-service-accounts-eks"\n'
            '  role_policy_arns = { db = aws_dynamodb_table.orders.arn }\n'
            '  oidc_providers = {\n'
            '    main = { namespace_service_accounts = ["orders:orders"] }\n'
            '  }\n}\n'
        )
        _, inventory_container, _, mock_ctx = await self._reach_the_data_review(
            mock_gcs_client_class, tf=two_in_the_pool)
        before = self._entry(inventory_container, "orders-table")["consumers"]
        self.assertEqual([(c["workload"], c["kind"]) for c in before],
                         [("orders", "service_account")])

        out = await discovery_datareview_3.tools.attach_data_consumer(
            address="aws_dynamodb_table.orders", workload="orders",
            note="the team confirmed it", ctx=mock_ctx)

        self.assertNotIn("ERROR", out)
        entry = self._entry(inventory_container, "orders-table")
        # One consumer still, the derived one, and the reason on the entry.
        self.assertEqual([(c["workload"], c["kind"], c["detection"])
                          for c in entry["consumers"]],
                         [("orders", "service_account", "irsa")])
        self.assertIn("already derived", out)
        self.assertTrue(any("the team confirmed it" in n for n in entry["notes"]))

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_rejection_naming_a_kind_the_entry_never_had_is_refused(
            self, mock_gcs_client_class, mock_get_email):
        """The standing-rejection exemption is for a REWORDING. Matching on
        the workload name alone let a rejection naming a kind that never
        existed ride in on an unrelated one — reporting success, removing
        nothing, and writing a durable sentence about a consumer the Terraform
        never declared, which cannot be withdrawn."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, inventory_container, _, mock_ctx = await self._reach_the_data_review(
            mock_gcs_client_class, tf=self.TWO_CHAINS_TF)

        await discovery_datareview_3.tools.reject_data_consumer(
            address="aws_db_instance.orders", workload="orders",
            kind="helm_release", reason="wrong release", ctx=mock_ctx)
        out = await discovery_datareview_3.tools.reject_data_consumer(
            address="aws_db_instance.orders", workload="orders",
            kind="deployment", reason="typo kind", ctx=mock_ctx)

        self.assertIn("ERROR", out)
        notes = self._entry(inventory_container, "orders-db")["notes"]
        self.assertFalse(any("(deployment)" in n for n in notes))
        # ...while rewording the one that IS standing still works.
        out = await discovery_datareview_3.tools.reject_data_consumer(
            address="aws_db_instance.orders", workload="orders",
            kind="helm_release", reason="wrong release — it reads the replica",
            ctx=mock_ctx)
        self.assertNotIn("ERROR", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_attaching_a_second_kind_does_not_inherit_the_firsts_scope(
            self, mock_gcs_client_class, mock_get_email):
        """The "reviewer's own earlier record wins over the scan" carry-forward
        must not reach across kinds: the reviewer never said the IRSA service
        account lives at the release's chart path, and `source_path` is what
        points a later workload gate at a component."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, inventory_container, _, mock_ctx = \
            await self._reach_the_data_review(mock_gcs_client_class)
        tools = discovery_datareview_3.tools

        await tools.attach_data_consumer(
            address="aws_s3_bucket.orders_exports", workload="billing",
            kind="helm_release", namespace="billing",
            source_path="charts/billing", note="the release writes it",
            ctx=mock_ctx)
        await tools.attach_data_consumer(
            address="aws_s3_bucket.orders_exports", workload="billing",
            kind="service_account", note="and the IRSA role reads it",
            ctx=mock_ctx)

        by_kind = {c["kind"]: c for c in
                   self._entry(inventory_container, "orders-exports")["consumers"]}
        self.assertEqual(set(by_kind), {"helm_release", "service_account"})
        self.assertIsNone(by_kind["service_account"]["source_path"])
        self.assertIsNone(by_kind["service_account"]["namespace"])
        self.assertEqual(by_kind["helm_release"]["source_path"], "charts/billing")

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_attaching_a_second_kind_is_not_swallowed_as_a_duplicate(
            self, mock_gcs_client_class, mock_get_email):
        """The reviewer states the release itself talks to the database, not
        just the service account. That is a second Kubernetes object, and
        treating it as "already derived" discarded the assertion, the kind and
        the namespace while reporting success."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        irsa_only = self.TWO_CHAINS_TF.replace(
            'resource "helm_release" "orders" {\n'
            '  name      = "orders"\n'
            '  namespace = "orders"\n'
            '  set { value = aws_db_instance.orders.endpoint }\n}\n', "")
        _, inventory_container, _, mock_ctx = await self._reach_the_data_review(
            mock_gcs_client_class, tf=irsa_only)
        self.assertEqual(
            [c["kind"] for c in
             self._entry(inventory_container, "orders-db")["consumers"]],
            ["service_account"])

        out = await discovery_datareview_3.tools.attach_data_consumer(
            address="aws_db_instance.orders", workload="orders",
            kind="helm_release", namespace="orders",
            note="the release itself talks to it, not just the SA",
            ctx=mock_ctx)

        self.assertNotIn("ERROR", out)
        entry = self._entry(inventory_container, "orders-db")
        self.assertEqual(
            sorted((c["workload"], c["kind"], c["detection"])
                   for c in entry["consumers"]),
            [("orders", "helm_release", "human_review"),
             ("orders", "service_account", "irsa")])

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_ranker_does_not_offer_back_a_rejected_workload(
            self, mock_gcs_client_class, mock_get_email):
        """The ranking's whole justification is that name similarity is safe
        because a human decides. Re-proposing the exact link they decided
        against — as the only candidate, under an instruction to put it to the
        user — inverts that, and attaching it retires the rejection."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, _, _, mock_ctx = await self._reach_the_data_review(mock_gcs_client_class)

        await discovery_datareview_3.tools.reject_data_consumer(
            address="aws_db_instance.orders", workload="orders",
            reason="only reads the replica", ctx=mock_ctx)
        out = await discovery_datareview_3.tools.list_data_dependencies(ctx=mock_ctx)

        # The entry is now consumer-less, so it gets a candidate block — but
        # not one naming the workload just rejected.
        block = out[out.index("orders-db"):]
        block = block[:block.index("- s3") if "- s3" in block else len(block)]
        self.assertNotIn("orders (helm_release", block)
        self.assertIn("you already rejected for this entry", block)
        # ...and it must not fall back to "the scan saw no workload at all",
        # which would be false and is contradicted two lines above.
        self.assertNotIn("no workload at all", block)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_reworded_withdrawal_does_not_revert_to_calling_it_derived(
            self, mock_gcs_client_class, mock_get_email):
        """Sharpening a reason is the common case, and by the second call the
        attach it retired is gone — so the flag that carries "nothing derived
        this" had nothing to notice, and the section went back to stating that
        the Terraform declares a reference it does not."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, inventory_container, _, mock_ctx = \
            await self._reach_the_data_review(mock_gcs_client_class)
        tools = discovery_datareview_3.tools

        await tools.attach_data_consumer(
            address="aws_s3_bucket.orders_exports", workload="analytics",
            note="team said so", ctx=mock_ctx)
        await tools.reject_data_consumer(
            address="aws_s3_bucket.orders_exports", workload="analytics",
            reason="wrong team", ctx=mock_ctx)
        await tools.reject_data_consumer(
            address="aws_s3_bucket.orders_exports", workload="analytics",
            reason="wrong team — it is the billing exporter", ctx=mock_ctx)

        notes = self._entry(inventory_container, "orders-exports")["notes"]
        self.assertTrue(any("attached at the data review was withdrawn" in n
                            and "billing exporter" in n for n in notes))
        self.assertFalse(any("derived consumer 'analytics'" in n for n in notes))

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_an_amend_refuses_when_the_scans_baseline_is_gone(
            self, mock_gcs_client_class, mock_get_email):
        """The guard the round-5 rework rests on: without the scan's own
        output the section cannot be rebuilt, and the alternative — editing the
        corrected section in place — is the computation that kept disagreeing
        with the scan's. Reading and signing off still work; only the amends
        refuse, and they say how to recover."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, _, _, mock_ctx = await self._reach_the_data_review(mock_gcs_client_class)
        self.mock_scan_baseline_blob.download_as_text.side_effect = \
            exceptions.NotFound("Not Found")

        out = await discovery_datareview_3.tools.reject_data_consumer(
            address="aws_db_instance.orders", workload="orders", ctx=mock_ctx)
        self.assertIn("ERROR", out)
        self.assertIn("scan_data_dependencies", out)
        self.assertIn("are not lost", out)

        # Reading it is still fine, and so is the sign-off: neither rebuilds.
        self.assertNotIn("ERROR", await
                         discovery_datareview_3.tools.list_data_dependencies(
                             ctx=mock_ctx))
        self.assertIn("SUCCESS", await
                      discovery_datareview_3.tools.confirm_data_dependencies(
                          ctx=mock_ctx))

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_transient_error_on_the_baseline_is_an_error_string(
            self, mock_gcs_client_class, mock_get_email):
        """Not an exception. The step's contract is that a failure comes back
        as ERROR text with the state left where it is, so the next session is
        still told there is work to do."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, _, _, mock_ctx = await self._reach_the_data_review(mock_gcs_client_class)
        self.mock_scan_baseline_blob.download_as_text.side_effect = \
            exceptions.ServiceUnavailable("503")

        out = await discovery_datareview_3.tools.annotate_data_dependency(
            address="aws_s3_bucket.orders_exports", note="x", ctx=mock_ctx)
        self.assertIn("ERROR", out)
        # ...and the read-only tool still answers rather than raising.
        self.assertNotIn("ERROR", await
                         discovery_datareview_3.tools.list_data_dependencies(
                             ctx=mock_ctx))

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_second_attach_does_not_revert_the_first_ones_corrections(
            self, mock_gcs_client_class, mock_get_email):
        """The scan's defaults are a convenience, not an authority. Filling
        them in before the record was stored made the field-merge see a
        supplied value and decline to carry the reviewer's earlier one
        forward — so adding a chart path silently reverted a namespace they
        had corrected, durably, with the tool reporting success."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, inventory_container, overrides_container, mock_ctx = \
            await self._reach_the_data_review(mock_gcs_client_class)

        # `orders` is in the scan pool with namespace "orders", so the scan has
        # an opinion about this workload — that is what makes it the hard case.
        await discovery_datareview_3.tools.attach_data_consumer(
            address="aws_s3_bucket.orders_exports", workload="orders",
            namespace="analytics", note="the batch copy runs in analytics",
            ctx=mock_ctx)
        await discovery_datareview_3.tools.attach_data_consumer(
            address="aws_s3_bucket.orders_exports", workload="orders",
            source_path="src/orders", ctx=mock_ctx)

        consumer = self._entry(inventory_container, "orders-exports")["consumers"][0]
        self.assertEqual(consumer["namespace"], "analytics")
        self.assertEqual(consumer["source_path"], "src/orders")
        self.assertEqual(consumer["note"], "the batch copy runs in analytics")
        stored = overrides_container["document"]["overrides"][0]
        self.assertEqual(stored["namespace"], "analytics")

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_attaching_after_a_rejection_says_what_it_actually_did(
            self, mock_gcs_client_class, mock_get_email):
        """"I checked with the team and I was wrong" — the documented undo.
        The headline was computed against the section as it stood BEFORE the
        rebuild, where the standing rejection had stripped the derived
        consumer, so the tool said "attached by hand" while the section
        recorded a confirmation of the derived link."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, inventory_container, _, mock_ctx = \
            await self._reach_the_data_review(mock_gcs_client_class)

        await discovery_datareview_3.tools.reject_data_consumer(
            address="aws_db_instance.orders", workload="orders",
            reason="wrong team", ctx=mock_ctx)
        out = await discovery_datareview_3.tools.attach_data_consumer(
            address="aws_db_instance.orders", workload="orders",
            note="I was wrong, they do use it", ctx=mock_ctx)

        entry = self._entry(inventory_container, "orders-db")
        self.assertEqual([(c["workload"], c["detection"]) for c in entry["consumers"]],
                         [("orders", "terraform_wiring")])
        self.assertIn("already derived", out)
        self.assertIn("your reason is now on the entry", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_withdrawing_an_attachment_does_not_call_it_derived(
            self, mock_gcs_client_class, mock_get_email):
        """Reject is the documented undo for an attach, so it fires on
        consumers nothing derived. Saying "the derived consumer" there puts a
        false statement about the Terraform into the section a gate reads."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, inventory_container, _, mock_ctx = \
            await self._reach_the_data_review(mock_gcs_client_class)

        await discovery_datareview_3.tools.attach_data_consumer(
            address="aws_s3_bucket.orders_exports", workload="reports",
            note="the reports job writes here", ctx=mock_ctx)
        await discovery_datareview_3.tools.reject_data_consumer(
            address="aws_s3_bucket.orders_exports", workload="reports",
            reason="mistake, it was the archiver", ctx=mock_ctx)

        notes = self._entry(inventory_container, "orders-exports")["notes"]
        self.assertFalse(any("derived consumer 'reports'" in n for n in notes))
        self.assertTrue(any("attached at the data review was withdrawn" in n
                            and "the archiver" in n for n in notes))

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_an_unnamed_workload_cannot_be_attached(
            self, mock_gcs_client_class, mock_get_email):
        """An agent relaying an empty or unparsed answer would otherwise mark
        an unattributed service as attributed — durably, and with no way to
        withdraw it. The entry drops out of the unattributed count and loses
        the note that is the only thing keeping somebody looking for its
        owner."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, inventory_container, overrides_container, mock_ctx = \
            await self._reach_the_data_review(mock_gcs_client_class)

        out = await discovery_datareview_3.tools.attach_data_consumer(
            address="aws_s3_bucket.orders_exports", workload="   ",
            ctx=mock_ctx)

        self.assertIn("ERROR", out)
        self.assertNotIn("document", overrides_container)
        entry = self._entry(inventory_container, "orders-exports")
        self.assertEqual(entry["consumers"], [])
        self.assertIn(consumers_lib.UNATTRIBUTED_NOTE, entry["notes"])

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_rejecting_a_workload_the_entry_does_not_list_is_refused(
            self, mock_gcs_client_class, mock_get_email):
        """A typo, or the right workload named against the wrong entry, used to
        record a durable override that removed nothing and report success. The
        reviewer believes the wrong link is gone; the real consumer stays
        attached and goes on to gate that team."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, inventory_container, overrides_container, mock_ctx = \
            await self._reach_the_data_review(mock_gcs_client_class)

        out = await discovery_datareview_3.tools.reject_data_consumer(
            address="aws_db_instance.orders", workload="ordersvc",
            reason="typo in the name", ctx=mock_ctx)

        self.assertIn("ERROR", out)
        # It says what the entry does list, the way an unknown address does.
        self.assertIn("orders", out)
        # Nothing durable was recorded — the refusal happens before the write.
        self.assertNotIn("document", overrides_container)
        self.assertEqual(
            [c["workload"] for c in
             self._entry(inventory_container, "orders-db")["consumers"]],
            ["orders"])

        # An entry with no consumers at all says so, and names the right tool.
        out = await discovery_datareview_3.tools.reject_data_consumer(
            address="aws_s3_bucket.orders_exports", workload="orders",
            ctx=mock_ctx)
        self.assertIn("ERROR", out)
        self.assertIn("attach_data_consumer", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_reviewer_can_sharpen_the_reason_they_rejected_a_consumer(
            self, mock_gcs_client_class, mock_get_email):
        """The reason is "the only record of why the mapping disagrees with the
        Terraform" and it is the thing a reviewer revises after talking to the
        team. The typo guard refused every reword, because the first rejection
        had already taken the workload off the entry — so the durable reason
        was write-once, and the error pointed at attach_data_consumer, which
        would have undone the rejection."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, inventory_container, overrides_container, mock_ctx = \
            await self._reach_the_data_review(mock_gcs_client_class)

        await discovery_datareview_3.tools.reject_data_consumer(
            address="aws_db_instance.orders", workload="orders",
            reason="it only reads the replica", ctx=mock_ctx)
        out = await discovery_datareview_3.tools.reject_data_consumer(
            address="aws_db_instance.orders", workload="orders",
            reason="it never touches this at all", ctx=mock_ctx)

        self.assertNotIn("ERROR", out)
        entry = self._entry(inventory_container, "orders-db")
        self.assertTrue(any("never touches this" in n for n in entry["notes"]))
        self.assertFalse(any("reads the replica" in n for n in entry["notes"]))
        # One decision, rewritten — not two records, and no stray ATTACH from
        # the workaround the old error message pushed the agent towards.
        stored = overrides_container["document"]["overrides"]
        self.assertEqual([o["kind"] for o in stored], ["reject_consumer"])
        self.assertEqual(stored[0]["reason"], "it never touches this at all")

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_confirming_a_derived_consumer_keeps_the_reason_it_came_with(
            self, mock_gcs_client_class, mock_get_email):
        """Attaching a workload the chain already found is a confirmation, not
        an addition — but the reviewer's "here is why" was going nowhere: the
        early return skipped the consumer that would have carried it, so the
        note lived only in the corrections object and never reached the entry
        the assessment reads, while the tool reported a successful attach."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, inventory_container, _, mock_ctx = \
            await self._reach_the_data_review(mock_gcs_client_class)

        out = await discovery_datareview_3.tools.attach_data_consumer(
            address="aws_db_instance.orders", workload="orders",
            note="confirmed with the team: the nightly job writes it too",
            ctx=mock_ctx)

        self.assertNotIn("ERROR", out)
        # The tool says what it actually did rather than claiming an attach.
        self.assertIn("already derived", out)
        entry = self._entry(inventory_container, "orders-db")
        # Still one consumer, still the derived one, with its evidence file.
        self.assertEqual([(c["workload"], c["detection"]) for c in entry["consumers"]],
                         [("orders", "terraform_wiring")])
        self.assertTrue(any("nightly job writes it too" in n
                            for n in entry["notes"]))

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_scan_notes_do_not_outlive_the_correction_that_answers_them(
            self, mock_gcs_client_class, mock_get_email):
        """The scan writes "1 of 2 could not be attributed: orders-exports"
        into data_dependency_scan_notes. A human attaching that consumer does
        not rewrite the sentence, and the notes are scan-owned — extraction
        carries them to the assessment. Without a marker the review shows the
        entry with its consumer, the elicitation says "0 with no consumer", and
        the scan note still names it as unattributed."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, inventory_container, _, mock_ctx = \
            await self._reach_the_data_review(mock_gcs_client_class)
        notes = inventory_container["inventory"]["data_dependency_scan_notes"]
        self.assertTrue(any("could not be attributed" in n for n in notes))

        await discovery_datareview_3.tools.attach_data_consumer(
            address="aws_s3_bucket.orders_exports", workload="orders",
            ctx=mock_ctx)

        notes = inventory_container["inventory"]["data_dependency_scan_notes"]
        self.assertTrue(any(n.startswith(overrides_lib.CORRECTED_SINCE_SCAN)
                            for n in notes))
        # And the reviewer reads it in the same breath as the stale count.
        out = await discovery_datareview_3.tools.list_data_dependencies(ctx=mock_ctx)
        self.assertIn("describe the scan as it ran", out)

    # A bucket no chain reaches — the entry a human has to attach — beside a
    # workload the walk DOES see, wired to a different datastore. The pool row
    # is what labels the hand-attached consumer.
    ON_HELM_TF = (
        'resource "aws_s3_bucket" "exports" {\n  bucket = "exports"\n}\n'
        'resource "aws_db_instance" "carts" {\n'
        '  identifier = "carts-db"\n}\n'
        'resource "helm_release" "carts" {\n'
        '  name      = "carts"\n'
        '  namespace = "carts-ns"\n'
        '  chart     = "./charts/carts"\n'
        '  set { value = aws_db_instance.carts.endpoint }\n}\n'
    )
    MOVED_OFF_HELM_TF = (
        'resource "aws_s3_bucket" "exports" {\n  bucket = "exports"\n}\n'
        'resource "aws_db_instance" "carts" {\n'
        '  identifier = "carts-db"\n}\n'
        'resource "kubernetes_deployment" "carts" {\n'
        '  metadata {\n'
        '    name      = "carts"\n'
        '    namespace = "carts-prod"\n'
        '  }\n'
        '  set { value = aws_db_instance.carts.endpoint }\n}\n'
    )

    REGISTRY_CHART_TF = (
        'resource "aws_s3_bucket" "exports" {\n  bucket = "exports"\n}\n'
        'resource "aws_db_instance" "carts" {\n'
        '  identifier = "carts-db"\n}\n'
        'resource "helm_release" "carts" {\n'
        '  name      = "carts"\n'
        '  namespace = "carts-ns"\n'
        '  chart     = "bitnami/carts"\n'
        '  set { value = aws_db_instance.carts.endpoint }\n}\n'
    )

    BLUE_GREEN_TF = (
        'resource "aws_db_instance" "orders" {\n'
        '  identifier = "orders-db"\n}\n'
        'resource "helm_release" "orders_blue" {\n'
        '  name      = "orders"\n'
        '  namespace = "orders-blue"\n'
        '  chart     = "./charts/orders"\n'
        '  set { value = aws_db_instance.orders.endpoint }\n}\n'
        'resource "helm_release" "orders_green" {\n'
        '  name      = "orders"\n'
        '  namespace = "orders-green"\n'
        '  chart     = "./charts/orders-next"\n'
        '  set { value = aws_db_instance.orders.endpoint }\n}\n'
    )

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_blue_green_pair_survives_a_reject_then_reattach(
            self, mock_gcs_client_class, mock_get_email):
        """The same loss, from real Terraform through the real tools, and
        across the re-scan that makes it durable."""
        from servers.phases.discovery import (
            discovery_datareview_3, discovery_datascan_3)
        mock_get_email.return_value = "platform-user@google.com"
        state_container, inventory_container, _, mock_ctx = \
            await self._reach_the_data_review(
                mock_gcs_client_class, tf=self.BLUE_GREEN_TF)
        t = discovery_datareview_3.tools
        before = {c["namespace"] for c in
                  self._entry(inventory_container, "orders-db")["consumers"]}
        self.assertEqual(before, {"orders-blue", "orders-green"})

        await t.reject_data_consumer(
            address="aws_db_instance.orders", workload="orders",
            reason="the platform team said this DB is not theirs", ctx=mock_ctx)
        out = await t.attach_data_consumer(
            address="aws_db_instance.orders", workload="orders",
            kind="helm_release",
            note="I was wrong, the orders release does use it", ctx=mock_ctx)
        self.assertNotIn("ERROR", out)
        self.assertEqual(
            {c["namespace"] for c in
             self._entry(inventory_container, "orders-db")["consumers"]},
            before)

        self._rewind_to_the_scan(state_container)
        self.assertNotIn("ERROR", await
                         discovery_datascan_3.tools.scan_data_dependencies(
                             ctx=mock_ctx))
        self.assertEqual(
            {c["namespace"] for c in
             self._entry(inventory_container, "orders-db")["consumers"]},
            before)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_null_field_in_todays_pool_row_is_todays_answer(
            self, mock_gcs_client_class, mock_get_email):
        """`or` cannot tell "this scan says there is no chart" from "this scan
        saw no row", so a registry chart — `_chart_path` returns None for any
        of them — fell through to an older scan's row. The section then stated
        a chart directory the current checkout does not declare, on the field
        the workload data gate matches a component against."""
        import os
        from servers.phases.discovery import (
            discovery_datareview_3, discovery_datascan_3)
        mock_get_email.return_value = "platform-user@google.com"
        state_container, inventory_container, _, mock_ctx = \
            await self._reach_the_data_review(
                mock_gcs_client_class, tf=self.ON_HELM_TF)
        t = discovery_datareview_3.tools

        await t.attach_data_consumer(
            address="aws_s3_bucket.exports", workload="carts", ctx=mock_ctx)
        self.assertEqual(
            self._entry(inventory_container, "exports")["consumers"][0]
            ["source_path"], "charts/carts")

        # The chart moves to a registry: same row, one field now null.
        root = state_container["state"]["variables"]["discovery_root_dir"]
        with open(os.path.join(root, "data.tf"), "w") as f:
            f.write(self.REGISTRY_CHART_TF)
        self._rewind_to_the_scan(state_container)
        self.assertNotIn("ERROR", await
                         discovery_datascan_3.tools.scan_data_dependencies(
                             ctx=mock_ctx))

        out = await t.attach_data_consumer(
            address="aws_s3_bucket.exports", workload="carts", ctx=mock_ctx)

        self.assertNotIn("ERROR", out)
        consumer = next(c for c in self._entry(
            inventory_container, "exports")["consumers"]
            if c["detection"] == overrides_lib.HUMAN_DETECTION)
        self.assertIsNone(consumer["source_path"])
        self.assertEqual(consumer["namespace"], "carts-ns")

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_message_does_not_read_the_reviewers_prose_as_a_fact(
            self, mock_gcs_client_class, mock_get_email):
        """`notes[]` holds the reviewer's own words too — an annotation is
        written verbatim, so one opening by quoting the section's own sentence
        was read back as a structured fact. The tool then told them they had
        supplied a field they never passed, and that a later scan would adopt
        a value that is not in the store. What a correction did is a fact the
        rebuild records; it must not be re-derived from the sentences it
        wrote."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, _, overrides_container, mock_ctx = \
            await self._reach_the_data_review(mock_gcs_client_class)
        t = discovery_datareview_3.tools

        for prose in (
                "the namespace for 'orders' (helm_release) was supplied at "
                "the data review by a colleague, which I disagree with",
                "the source_path given for 'orders' (helm_release) at the "
                "data review (charts/old) is what the previous team recorded"):
            await t.annotate_data_dependency(
                address="aws_db_instance.orders", note=prose, ctx=mock_ctx)

        out = await t.attach_data_consumer(
            address="aws_db_instance.orders", workload="orders",
            note="confirmed with the team", ctx=mock_ctx)

        self.assertNotIn("ERROR", out)
        self.assertNotIn("you gave", out)
        self.assertNotIn("NOT applied", out)
        self.assertNotIn("is now on the derived consumer", out)
        # ...and the store agrees with the message.
        record = next(r for r in overrides_container["document"]["overrides"]
                      if r.get("kind") == overrides_lib.ATTACH)
        self.assertIsNone(record.get("namespace"))
        self.assertIsNone(record.get("source_path"))

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_rejecting_one_chain_says_which_and_what_remains(
            self, mock_gcs_client_class, mock_get_email):
        """"Consumer 'orders' rejected" reads as "orders no longer uses this
        database" — the fact the workload data gate turns on — and it is false while
        another chain of that name is still listed."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, inventory_container, _, mock_ctx = \
            await self._reach_the_data_review(
                mock_gcs_client_class, tf=self.TWO_CHAINS_TF)

        out = await discovery_datareview_3.tools.reject_data_consumer(
            address="aws_db_instance.orders", workload="orders",
            kind="service_account", reason="the IRSA grant is over-broad",
            ctx=mock_ctx)

        self.assertNotIn("ERROR", out)
        self.assertIn("(service_account) rejected", out)
        self.assertIn("Still listed under that name: orders (helm_release)",
                      out)
        self.assertEqual(
            [c["kind"] for c in
             self._entry(inventory_container, "orders-db")["consumers"]],
            ["helm_release"])

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_typo_refusal_names_the_kind_it_fired_on(
            self, mock_gcs_client_class, mock_get_email):
        """The guard fires on a wrong KIND as readily as a wrong name, and a
        list of bare names then denies and asserts the same fact in
        consecutive sentences. The reviewer's natural retry is to drop the
        kind, which rejects every chain — including the one they never
        named."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, _, _, mock_ctx = await self._reach_the_data_review(
            mock_gcs_client_class, tf=self.TWO_CHAINS_TF)

        out = await discovery_datareview_3.tools.reject_data_consumer(
            address="aws_db_instance.orders", workload="orders",
            kind="kubernetes_secret", reason="wrong team", ctx=mock_ctx)

        self.assertIn("ERROR", out)
        self.assertIn("orders (helm_release)", out)
        self.assertIn("orders (service_account)", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_relabelled_workload_is_not_composed_of_two_pool_rows(
            self, mock_gcs_client_class, mock_get_email):
        """A hint describes what the walk just saw, and the pool is derived
        fresh every scan; the stored hint exists only so a replay, which has
        no pool, can still label. Reading the stored one first made
        `consumer_kind_hint` sticky while the two newer ones were
        re-derived, so a re-attach after the estate moved off Helm produced
        the old row's kind beside the new row's namespace — a Kubernetes
        object that never existed, on one of the five identity axes."""
        import os
        from servers.phases.discovery import (
            discovery_datareview_3, discovery_datascan_3)
        mock_get_email.return_value = "platform-user@google.com"
        state_container, inventory_container, overrides_container, mock_ctx = \
            await self._reach_the_data_review(
                mock_gcs_client_class, tf=self.ON_HELM_TF)
        t = discovery_datareview_3.tools

        await t.attach_data_consumer(
            address="aws_s3_bucket.exports", workload="carts",
            note="the carts app writes the nightly export", ctx=mock_ctx)
        consumer = self._entry(inventory_container, "exports")["consumers"][0]
        self.assertEqual((consumer["kind"], consumer["namespace"]),
                         ("helm_release", "carts-ns"))

        # The estate moves off Helm. Rewind and re-scan, as amend_discovery_scope does.
        root = state_container["state"]["variables"]["discovery_root_dir"]
        with open(os.path.join(root, "data.tf"), "w") as f:
            f.write(self.MOVED_OFF_HELM_TF)
        self._rewind_to_the_scan(state_container)
        self.assertNotIn("ERROR", await
                         discovery_datascan_3.tools.scan_data_dependencies(
                             ctx=mock_ctx))

        out = await t.attach_data_consumer(
            address="aws_s3_bucket.exports", workload="carts",
            note="still true after the move off helm", ctx=mock_ctx)

        self.assertNotIn("ERROR", out)
        consumer = next(c for c in self._entry(
            inventory_container, "exports")["consumers"]
            if c["detection"] == overrides_lib.HUMAN_DETECTION)
        self.assertNotEqual(consumer["kind"], "helm_release")
        self.assertEqual(consumer["namespace"], "carts-prod")
        self.assertNotIn("helm_release", out)
        record = next(r for r in overrides_container["document"]["overrides"]
                      if r.get("kind") == overrides_lib.ATTACH)
        self.assertNotEqual(record.get("consumer_kind_hint"), "helm_release")

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_scan_pool_labels_a_consumer_without_testifying(
            self, mock_gcs_client_class, mock_get_email):
        """A reviewer who states nothing but the workload name has stated
        nothing but the workload name. `_apply_one` reads `namespace` and
        `source_path` back as things a named human asserted — filling them
        onto a derived consumer under a note saying the Terraform does not
        carry the value — so merging the scan's own output into those fields
        makes the section attribute a scan-produced value to a person.
        `consumer_kind` was split from `consumer_kind_hint` for this reason;
        these two never got the split.

        The hint still labels the hand-attached consumer, which is where the
        pool's guess is harmless: that consumer is `human_review` either way.
        """
        from servers.phases.discovery import (
            discovery_datareview_3, discovery_datascan_3)
        mock_get_email.return_value = "platform-user@google.com"
        state_container, inventory_container, overrides_container, mock_ctx = \
            await self._reach_the_data_review(mock_gcs_client_class)

        # The pool knows `orders` from the release wired to the DATABASE.
        out = await discovery_datareview_3.tools.attach_data_consumer(
            address="aws_s3_bucket.orders_exports", workload="orders",
            ctx=mock_ctx)
        self.assertNotIn("ERROR", out)

        record = next(r for r in overrides_container["document"]["overrides"]
                      if r.get("kind") == overrides_lib.ATTACH)
        self.assertIsNone(record.get("namespace"))
        self.assertIsNone(record.get("source_path"))
        # ...but the consumer it wrote is still labeled from the scan's walk.
        consumer = self._entry(
            inventory_container, "orders-exports")["consumers"][0]
        self.assertEqual(consumer["namespace"], "orders")
        self.assertEqual(consumer["detection"], overrides_lib.HUMAN_DETECTION)

        # And nothing claims the reviewer supplied it.
        entry = self._entry(inventory_container, "orders-exports")
        self.assertFalse(
            any("was supplied at the data review" in n
                or "was not applied" in n for n in entry["notes"]),
            entry["notes"])

        # The durable half: a re-scan must not quote them either.
        self._rewind_to_the_scan(state_container)
        self.assertNotIn("ERROR", await
                         discovery_datascan_3.tools.scan_data_dependencies(
                             ctx=mock_ctx))
        entry = self._entry(inventory_container, "orders-exports")
        self.assertFalse(
            any("was supplied at the data review" in n
                or "was not applied" in n for n in entry["notes"]),
            entry["notes"])

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_confirmation_reports_its_own_chain_not_the_other_ones(
            self, mock_gcs_client_class, mock_get_email):
        """Reading the outcome off the section is right; the section keys a
        consumer by (workload, kind) and the
        message was asking by name. So a call that passed no namespace at all
        was told one of theirs was held in reserve for a later scan, quoting
        the other chain's value as what the Terraform declares."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, _, _, mock_ctx = await self._reach_the_data_review(
            mock_gcs_client_class, tf=self.TWO_CHAINS_TF)
        t = discovery_datareview_3.tools

        out = await t.attach_data_consumer(
            address="aws_db_instance.orders", workload="orders",
            kind="helm_release", namespace="analytics", ctx=mock_ctx)
        self.assertNotIn("ERROR", out)
        self.assertIn("NOT applied", out)

        # A different chain, and this call gives no namespace at all.
        out = await t.attach_data_consumer(
            address="aws_db_instance.orders", workload="orders",
            kind="service_account",
            note="ops confirmed the role is bound to the same pod",
            ctx=mock_ctx)

        self.assertNotIn("ERROR", out)
        self.assertNotIn("NOT applied", out)
        self.assertNotIn("a later scan that finds none declared", out)
        self.assertIn("your reason is now on the entry", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_refused_field_is_reported_to_the_reviewer_who_gave_it(
            self, mock_gcs_client_class, mock_get_email):
        """The refusal went onto the entry while the message still said
        nothing else changed. The value is kept in the store and a later scan
        that finds none declared will adopt it, so a reviewer not told here is
        not told at all."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, inventory_container, _, mock_ctx = await self._reach_the_data_review(
            mock_gcs_client_class)

        out = await discovery_datareview_3.tools.attach_data_consumer(
            address="aws_db_instance.orders", workload="orders",
            kind="helm_release", namespace="payments", ctx=mock_ctx)

        self.assertNotIn("ERROR", out)
        self.assertNotIn("nothing else changed", out)
        self.assertIn("NOT applied", out)
        self.assertIn("namespace=orders", out)
        self.assertEqual(
            self._entry(inventory_container, "orders-db")["consumers"][0]
            ["namespace"], "orders")

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_sign_off_does_not_say_a_kept_service_needs_migrating(
            self, mock_gcs_client_class, mock_get_email):
        """The flagship shape of this step: an AWS-only service nothing
        references that the customer keeps. "Still needs migrating" then
        contradicts a disposition this very review recorded, in the one text a
        human actually answers."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, _, _, mock_ctx = await self._reach_the_data_review(
            mock_gcs_client_class)
        t = discovery_datareview_3.tools
        out = await t.annotate_data_dependency(
            address="aws_s3_bucket.orders_exports", disposition="keep-in-aws",
            note="the catalog team reads it directly; staying on S3",
            ctx=mock_ctx)
        self.assertNotIn("ERROR", out)

        with patch("servers.phases.discovery.discovery_datareview_3.tools"
                   ".run_elicitation") as elicit:
            elicit.return_value = (True, {})
            out = await t.confirm_data_dependencies(ctx=mock_ctx)
        self.assertIn("SUCCESS", out)

        asked = elicit.call_args[0][2]["prompt_template"]
        self.assertIn("stay in AWS by your decision", asked)
        self.assertNotIn("still needs migrating", asked)
        # It is still counted and named — the fact is not suppressed, only
        # the claim about what has to happen to it.
        self.assertIn("still with no consumer attached: orders-exports", asked)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_splitting_an_annotation_is_not_a_correction_since_the_scan(
            self, mock_gcs_client_class, mock_get_email):
        """The instructions ask for reason and disposition in one call, so the
        two-field annotation is the intended shape. Superseding one field
        keeps the other as a record of its own, and counting that remnant as
        new inflated the number the assessment reads verbatim."""
        from servers.phases.discovery import (
            discovery_datareview_3, discovery_datascan_3)
        mock_get_email.return_value = "platform-user@google.com"
        state_container, inventory_container, _, mock_ctx = \
            await self._reach_the_data_review(mock_gcs_client_class)
        t = discovery_datareview_3.tools
        await t.annotate_data_dependency(
            address="aws_s3_bucket.orders_exports",
            note="the customer keeps it", disposition="keep-in-aws",
            ctx=mock_ctx)
        self._rewind_to_the_scan(state_container)
        self.assertNotIn("ERROR", await
                         discovery_datascan_3.tools.scan_data_dependencies(
                             ctx=mock_ctx))

        await t.annotate_data_dependency(
            address="aws_s3_bucket.orders_exports", disposition="migrate",
            ctx=mock_ctx)

        marker = next(n for n in inventory_container["inventory"]
                      ["data_dependency_scan_notes"]
                      if n.startswith(overrides_lib.CORRECTED_SINCE_SCAN))
        self.assertIn(": 1, listed with the section", marker)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_an_unreadable_file_is_not_reported_as_nothing_uses_it(
            self, mock_gcs_client_class, mock_get_email):
        """`note_unattributed` keeps three categories apart: a chain reached
        it, no chain reached it, and part of the Terraform could not be read
        so the answer is unknown. The review layer knew only the first, so it
        turned the scan's refusal to answer into a finding — in the candidate
        note, the header, the sign-off question the human actually answers,
        and the durable stamp."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        truncated = self.REVIEW_TF + 'resource "aws_iam_role" "leftover" {\n'
        _, inventory_container, _, mock_ctx = await self._reach_the_data_review(
            mock_gcs_client_class, tf=truncated)
        entry = self._entry(inventory_container, "orders-exports")
        self.assertIn(consumers_lib.TRUNCATED_NOTE, entry["notes"])

        out = await discovery_datareview_3.tools.list_data_dependencies(
            ctx=mock_ctx)

        # The header counts it apart from a gap...
        self.assertIn("the scan could not answer for", out)
        self.assertIn("0 with no consumer the scan could find", out)
        # ...and the candidate note does not claim the chains answered.
        self.assertNotIn("the scan follows references, and none reached", out)
        self.assertNotIn("the two reference chains missed it", out)
        self.assertIn("never got to answer", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_sign_off_does_not_ask_to_approve_an_unanswered_question(
            self, mock_gcs_client_class, mock_get_email):
        """The elicitation is the one place a human answers, and the stamp is
        the durable record of what they approved. "It just cannot be placed
        with a team yet" is a statement about the estate; for these entries
        the scan declined to make one."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        truncated = self.REVIEW_TF + 'resource "aws_iam_role" "leftover" {\n'
        state_container, _, _, mock_ctx = await self._reach_the_data_review(
            mock_gcs_client_class, tf=truncated)

        with patch("servers.phases.discovery.discovery_datareview_3.tools"
                   ".run_elicitation") as elicit:
            elicit.return_value = (True, {})
            out = await discovery_datareview_3.tools.confirm_data_dependencies(
                ctx=mock_ctx)

        self.assertIn("SUCCESS", out)
        self.assertIn("the scan could not answer for", out)
        asked = elicit.call_args[0][2]["prompt_template"]
        self.assertIn("is unknown rather than answered", asked)
        self.assertNotIn("still with no consumer attached: orders-exports",
                         asked)
        self.assertNotIn("it just cannot be placed with a team yet", asked)
        stamp = state_container["state"]["variables"]["data_dependency_review"]
        self.assertEqual(stamp["unattributed"], 0)
        self.assertEqual(stamp["unknown_unreadable_terraform"], 1)

    # A bucket the estate reaches by ARN and declares nowhere: the acme shape.
    REFERENCED_TF = (
        'resource "aws_iam_role" "orders_irsa" {\n'
        '  assume_role_policy = jsonencode({ Statement = [{ Condition = { StringEquals = {\n'
        '    "oidc.eks.us-east-1.amazonaws.com:sub" = "system:serviceaccount:acme-shop:orders" } } }] })\n'
        '}\n'
        'resource "aws_iam_role_policy" "orders_s3" {\n'
        '  role = aws_iam_role.orders_irsa.id\n'
        '  policy = jsonencode({ Statement = [{ Resource = [\n'
        '    "arn:aws:s3:::acme-invoice-archive", "arn:aws:s3:::acme-invoice-archive/*"] }] })\n'
        '}\n'
    )

    # A guess: a Helm value whose key says "bucket", naming a bucket nothing
    # declares or names by ARN.
    GUESS_TF = (
        'resource "helm_release" "reports" {\n'
        '  name      = "reports"\n'
        '  namespace = "reports"\n'
        '  set {\n'
        '    name  = "archiveBucket"\n'
        '    value = "reports-archive"\n'
        '  }\n}\n'
    )

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_referenced_entry_survives_the_tool_and_is_correctable_by_its_arn(
            self, mock_gcs_client_class, mock_get_email):
        """End to end through the tool, not just the pure harvest: the entry
        has to pass the schema validation on the ledger write with its new
        `account` field, be counted in what the agent is handed back, and be
        reachable by the review tools under the ARN that is its address —
        which is the whole justification for putting the ARN there."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        state_container, inventory_container, _, mock_ctx = \
            await self._reach_the_data_review(
                mock_gcs_client_class, tf=self.REVIEW_TF + self.REFERENCED_TF)

        entry = self._entry(inventory_container, "acme-invoice-archive")
        self.assertEqual((entry["detection"], entry["address"], entry["disposition"]),
                         ("referenced", "arn:aws:s3:::acme-invoice-archive", "migrate"))
        self.assertIn("account", entry)
        self.assertEqual([(c["workload"], c["kind"], c["detection"])
                          for c in entry["consumers"]],
                         [("orders", "service_account", "irsa")])
        notes = inventory_container["inventory"]["data_dependency_scan_notes"]
        self.assertTrue(any("1 data service(s) are known only from a literal ARN or endpoint" in n
                            and "s3 acme-invoice-archive" in n for n in notes), notes)

        t = discovery_datareview_3.tools
        listing = await t.list_data_dependencies(ctx=mock_ctx)
        self.assertIn("arn:aws:s3:::acme-invoice-archive", listing)
        out = await t.annotate_data_dependency(
            address="arn:aws:s3:::acme-invoice-archive", disposition="keep-in-aws",
            note="finance owns the archive", ctx=mock_ctx)
        self.assertNotIn("ERROR", out)
        entry = self._entry(inventory_container, "acme-invoice-archive")
        self.assertEqual(entry["disposition"], "keep-in-aws")
        out = await t.attach_data_consumer(
            address="arn:aws:s3:::acme-invoice-archive", workload="orders",
            kind="helm_release", ctx=mock_ctx)
        self.assertNotIn("ERROR", out)
        entry = self._entry(inventory_container, "acme-invoice-archive")
        self.assertEqual(sorted((c["workload"], c["kind"]) for c in entry["consumers"]),
                         [("orders", "helm_release"), ("orders", "service_account")])

        # The corrections survive the ARN being sighted first in another
        # directory: a referenced entry's directory is an artifact of walk
        # order, so no correction against one is keyed on it.
        from servers.phases.discovery import discovery_datascan_3
        root = state_container["state"]["variables"]["discovery_root_dir"]
        # A root-level file sorting before data.tf: os.walk yields a
        # directory's files before its subdirectories.
        with open(os.path.join(root, "aaa-extra.tf"), "w") as f:
            f.write('resource "aws_iam_policy" "extra" {\n'
                    '  policy = jsonencode({ Resource = "arn:aws:s3:::acme-invoice-archive" })\n}\n')
        self._rewind_to_the_scan(state_container)
        out = await discovery_datascan_3.tools.scan_data_dependencies(ctx=mock_ctx)
        self.assertNotIn("ERROR", out)
        self.assertNotIn("could not be placed", out)
        entry = self._entry(inventory_container, "acme-invoice-archive")
        self.assertEqual(entry["evidence"][0], "aaa-extra.tf")
        self.assertEqual(entry["disposition"], "keep-in-aws")
        self.assertEqual(sorted((c["workload"], c["kind"]) for c in entry["consumers"]),
                         [("orders", "helm_release"), ("orders", "service_account")])

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_review_guards_hold_for_a_referenced_entry_in_a_subdirectory(
            self, mock_gcs_client_class, mock_get_email):
        """Every guard that keys a correction on the entry's directory has to
        agree that an ARN-addressed entry has none. With the policy in a
        subdirectory, a directory taken from evidence[0] would be 'iam' in
        one place and None in another, and each guard would silently stop
        firing: a rejected candidate re-offered, a narrower rejection
        admitted as a second reason, two attach assertions collapsed."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, inventory_container, overrides_container, mock_ctx = \
            await self._reach_the_data_review(
                mock_gcs_client_class,
                extra_files={"iam/policies.tf": self.REFERENCED_TF})
        t = discovery_datareview_3.tools
        arn = "arn:aws:s3:::acme-invoice-archive"
        entry = self._entry(inventory_container, "acme-invoice-archive")
        self.assertEqual(entry["evidence"], ["iam/policies.tf"])

        # A wildcard rejection, then a narrower one: refused, not doubled.
        out = await t.reject_data_consumer(address=arn, workload="orders",
                                           reason="wrong team", ctx=mock_ctx)
        self.assertNotIn("ERROR", out)
        out = await t.reject_data_consumer(address=arn, workload="orders",
                                           kind="service_account",
                                           reason="sharper", ctx=mock_ctx)
        self.assertIn("ERROR", out)
        self.assertIn("covers every chain", out)
        self.assertEqual(
            len([o for o in overrides_container["document"]["overrides"]
                 if o.get("kind") == "reject_consumer"]), 1)
        # The rejected workload is not re-offered as a candidate.
        listing = await t.list_data_dependencies(ctx=mock_ctx)
        self.assertIn("already rejected for this entry", listing)

        # Two attach assertions under two kinds, then an unqualified third:
        # refused rather than collapsed.
        for kind in ("helm_release", "service_account"):
            out = await t.attach_data_consumer(address=arn, workload="billing",
                                               kind=kind, ctx=mock_ctx)
            self.assertNotIn("ERROR", out)
        out = await t.attach_data_consumer(address=arn, workload="billing", ctx=mock_ctx)
        self.assertIn("ERROR", out)
        self.assertIn("Re-call with kind=", out)
        self.assertEqual(
            len([o for o in overrides_container["document"]["overrides"]
                 if o.get("kind") == "attach_consumer"]), 2)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_rejection_keyed_on_the_arn_still_guards_after_the_entry_folds(
            self, mock_gcs_client_class, mock_get_email):
        """The reviewer rejects a link on the referenced entry. The bucket's
        declaration then lands in the repo and the next scan folds the entry
        onto it: the rejection still applies, and the listing must not
        re-offer the workload it cut."""
        from servers.phases.discovery import discovery_datareview_3, discovery_datascan_3
        mock_get_email.return_value = "platform-user@google.com"
        state_container, inventory_container, overrides_container, mock_ctx = \
            await self._reach_the_data_review(
                mock_gcs_client_class,
                extra_files={"iam/policies.tf": self.REFERENCED_TF})
        t = discovery_datareview_3.tools
        arn = "arn:aws:s3:::acme-invoice-archive"
        out = await t.reject_data_consumer(address=arn, workload="orders",
                                           reason="wrong team", ctx=mock_ctx)
        self.assertNotIn("ERROR", out)

        root = state_container["state"]["variables"]["discovery_root_dir"]
        with open(os.path.join(root, "s3.tf"), "w") as f:
            f.write('resource "aws_s3_bucket" "archive" {\n  bucket = "acme-invoice-archive"\n}\n')
        self._rewind_to_the_scan(state_container)
        out = await discovery_datascan_3.tools.scan_data_dependencies(ctx=mock_ctx)
        self.assertNotIn("ERROR", out)
        self.assertNotIn("could not be placed", out)
        entry = self._entry(inventory_container, "acme-invoice-archive")
        self.assertEqual((entry["detection"], entry["address"], entry["arn"]),
                         ("declared", "aws_s3_bucket.archive", arn))
        self.assertEqual(entry["consumers"], [])

        listing = await t.list_data_dependencies(ctx=mock_ctx)
        section = listing.split("acme-invoice-archive", 1)[1].split("\n\n", 1)[0]
        self.assertTrue("already rejected for this entry" in section
                        or "left out of the ranking" in section, section)
        # And a narrower rejection against the ARN is still refused.
        out = await t.reject_data_consumer(address=arn, workload="orders",
                                           kind="service_account", reason="x",
                                           ctx=mock_ctx)
        self.assertIn("ERROR", out)
        self.assertIn("covers every chain", out)
        # A reword under the block address the listing now prints replaces
        # the ARN-keyed rejection rather than standing beside it.
        out = await t.reject_data_consumer(address="aws_s3_bucket.archive",
                                           workload="orders", reason="finance owns it",
                                           ctx=mock_ctx)
        self.assertNotIn("ERROR", out)
        rejections = [o for o in overrides_container["document"]["overrides"]
                      if o.get("kind") == "reject_consumer"]
        self.assertEqual([r["reason"] for r in rejections], ["finance owns it"])

        # An annotation made by ARN on the folded entry survives the
        # declaration leaving the repo again: no directory was recorded
        # beside the ARN.
        out = await t.annotate_data_dependency(address=arn, disposition="keep-in-aws",
                                               ctx=mock_ctx)
        self.assertNotIn("ERROR", out)
        os.remove(os.path.join(root, "s3.tf"))
        self._rewind_to_the_scan(state_container)
        out = await discovery_datascan_3.tools.scan_data_dependencies(ctx=mock_ctx)
        self.assertNotIn("ERROR", out)
        self.assertNotIn("could not be placed", out)
        entry = self._entry(inventory_container, "acme-invoice-archive")
        self.assertEqual((entry["detection"], entry["disposition"]),
                         ("referenced", "keep-in-aws"))

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_scan_summary_counts_referenced_entries(
            self, mock_gcs_client_class, mock_get_email):
        from servers.phases.discovery import discovery_scope_2, discovery_datascan_3
        mock_get_email.return_value = "platform-user@google.com"
        state_container, inventory_container, mock_ctx = self._setup_image_flow(
            mock_gcs_client_class)
        self._install_overrides_roundtrip()
        root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, root, ignore_errors=True)
        self._write_image_fixture(root, with_render_targets=False)
        with open(os.path.join(root, "data.tf"), "w") as f:
            f.write(self.REVIEW_TF + self.REFERENCED_TF)
        await main.discover_configuration_files(ctx=mock_ctx, root_dir=root)
        await discovery_scope_2.tools.confirm_discovery_scope(ctx=mock_ctx)
        out = await discovery_datascan_3.tools.scan_data_dependencies(ctx=mock_ctx)
        self.assertNotIn("ERROR", out)
        self.assertIn("1 known only from an ARN or endpoint and not matched to a declaration here", out)
        summary = json.loads(out.split("\n", 1)[1].rsplit("\n\nCurrent State", 1)[0])
        self.assertEqual(summary["known_only_from_an_arn_or_endpoint"], 1)
        self.assertEqual(
            [e["detection"] for e in summary["entries"] if e["identifier"] == "acme-invoice-archive"],
            ["referenced"])

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_guess_blocks_the_sign_off_until_it_is_answered(
            self, mock_gcs_client_class, mock_get_email):
        """A guess is a question. Approving over it would carry `undecided`
        into the section the assessment grades with nothing saying anyone
        looked; dismissing it is one of the two answers, and after it the
        sign-off proceeds."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, inventory_container, _, mock_ctx = await self._reach_the_data_review(
            mock_gcs_client_class, tf=self.REVIEW_TF + self.GUESS_TF)
        t = discovery_datareview_3.tools

        guess = self._entry(inventory_container, "reports-archive")
        self.assertEqual((guess["detection"], guess["disposition"], guess["address"]),
                         ("inferred", "undecided", "s3:reports-archive"))
        listing = await t.list_data_dependencies(ctx=mock_ctx)
        self.assertIn("GUESS — s3 reports-archive", listing)
        self.assertIn("used by reports (helm_release, via config_value)", listing)

        with patch("servers.phases.discovery.discovery_datareview_3.tools"
                   ".run_elicitation") as elicit:
            out = await t.confirm_data_dependencies(ctx=mock_ctx)
        self.assertIn("ERROR", out)
        self.assertIn("1 guess(es) are still unanswered", out)
        self.assertIn("s3:reports-archive", out)
        elicit.assert_not_called()

        out = await t.dismiss_data_dependency(
            address="s3:reports-archive", reason="a legacy value; the bucket is gone",
            ctx=mock_ctx)
        self.assertNotIn("ERROR", out)
        self.assertNotIn("reports-archive", [
            e["identifier"] for e in inventory_container["inventory"]["data_dependencies"]])

        with patch("servers.phases.discovery.discovery_datareview_3.tools"
                   ".run_elicitation") as elicit:
            elicit.return_value = (True, {})
            out = await t.confirm_data_dependencies(ctx=mock_ctx)
        self.assertIn("SUCCESS", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_confirmed_guess_gets_a_plan_and_survives_a_rescan(
            self, mock_gcs_client_class, mock_get_email):
        from servers.phases.discovery import (
            discovery_datareview_3, discovery_datascan_3)
        mock_get_email.return_value = "platform-user@google.com"
        state_container, inventory_container, _, mock_ctx = \
            await self._reach_the_data_review(
                mock_gcs_client_class, tf=self.REVIEW_TF + self.GUESS_TF)
        t = discovery_datareview_3.tools

        out = await t.confirm_data_dependency(
            address="s3:reports-archive", note="finance's archive; confirmed with them",
            ctx=mock_ctx)
        self.assertNotIn("ERROR", out)
        entry = self._entry(inventory_container, "reports-archive")
        self.assertEqual(entry["detection"], "human_review")
        # The table default for a bucket, since no disposition was given.
        self.assertEqual(entry["disposition"], "migrate")
        self.assertFalse(any(n.startswith("a guess: ") for n in entry["notes"]))
        self.assertTrue(any("confirmed as a real dependency" in n for n in entry["notes"]))
        self.assertEqual([c["workload"] for c in entry["consumers"]], ["reports"])

        self._rewind_to_the_scan(state_container)
        out = await discovery_datascan_3.tools.scan_data_dependencies(ctx=mock_ctx)
        self.assertNotIn("ERROR", out)
        self.assertIn("1 confirmation(s)", out)
        entry = self._entry(inventory_container, "reports-archive")
        self.assertEqual((entry["detection"], entry["disposition"]),
                         ("human_review", "migrate"))
        # Not counted as an open guess any more.
        summary = json.loads(out.split("\n", 1)[1].rsplit("\n\nCurrent State", 1)[0])
        self.assertEqual(summary["guesses_needing_a_yes_or_no"], 0)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_declared_entry_cannot_be_dismissed_as_a_guess(
            self, mock_gcs_client_class, mock_get_email):
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, inventory_container, _, mock_ctx = await self._reach_the_data_review(
            mock_gcs_client_class)
        out = await discovery_datareview_3.tools.dismiss_data_dependency(
            address="aws_s3_bucket.orders_exports", reason="not ours", ctx=mock_ctx)
        self.assertIn("ERROR", out)
        self.assertIn("is declared", out)
        self.assertIn("annotate_data_dependency", out)
        self.assertIsNotNone(self._entry(inventory_container, "orders-exports"))

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_an_added_service_is_recorded_and_lands_on_what_a_rescan_derives(
            self, mock_gcs_client_class, mock_get_email):
        """Nothing in the files proposed it; the reviewer knows it exists. It
        is recorded as theirs, and when a later scan finds the endpoint in a
        ConfigMap the note and disposition land on the derived entry."""
        from servers.phases.discovery import (
            discovery_datareview_3, discovery_datascan_3)
        mock_get_email.return_value = "platform-user@google.com"
        state_container, inventory_container, _, mock_ctx = \
            await self._reach_the_data_review(mock_gcs_client_class)
        t = discovery_datareview_3.tools

        out = await t.add_data_dependency(
            service="rds", identifier="legacy-orders", disposition="keep-in-aws",
            note="the on-prem replica everyone forgets", workload="orders",
            kind="helm_release", ctx=mock_ctx)
        self.assertNotIn("ERROR", out)
        entry = self._entry(inventory_container, "legacy-orders")
        self.assertEqual((entry["detection"], entry["disposition"], entry["address"]),
                         ("human_review", "keep-in-aws", "rds:legacy-orders"))
        self.assertEqual([(c["workload"], c["detection"]) for c in entry["consumers"]],
                         [("orders", "human_review")])
        listing = await t.list_data_dependencies(ctx=mock_ctx)
        self.assertIn("ADDED BY HAND — rds legacy-orders", listing)

        # Adding it twice is refused, naming the entry.
        out = await t.add_data_dependency(service="rds", identifier="legacy-orders",
                                          ctx=mock_ctx)
        self.assertIn("ERROR", out)
        self.assertIn("already records this resource", out)
        # And an ARN of another service is refused.
        out = await t.add_data_dependency(
            service="s3", identifier="orders", arn="arn:aws:sqs:us-east-1:123456789012:orders",
            ctx=mock_ctx)
        self.assertIn("ERROR", out)
        self.assertIn("names a sqs resource, not s3", out)

        # A later scan finds its endpoint in a ConfigMap.
        root = state_container["state"]["variables"]["discovery_root_dir"]
        with open(os.path.join(root, "config.tf"), "w") as f:
            f.write('resource "kubernetes_config_map" "orders" {\n'
                    '  metadata { name = "orders-config" }\n'
                    '  data = { DB_HOST = "legacy-orders.c9akciq32xyz.us-east-1'
                    '.rds.amazonaws.com" }\n}\n')
        self._rewind_to_the_scan(state_container)
        out = await discovery_datascan_3.tools.scan_data_dependencies(ctx=mock_ctx)
        self.assertNotIn("ERROR", out)
        entries = [e for e in inventory_container["inventory"]["data_dependencies"]
                   if e["identifier"] == "legacy-orders"]
        self.assertEqual(len(entries), 1)
        entry = entries[0]
        self.assertEqual(entry["detection"], "referenced")
        self.assertEqual(entry["disposition"], "keep-in-aws")
        self.assertTrue(any("everyone forgets" in n for n in entry["notes"]))
        self.assertEqual(sorted(c["workload"] for c in entry["consumers"]),
                         ["orders", "orders-config"])

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_an_exact_name_match_is_offered_first_and_never_attached(
            self, mock_gcs_client_class, mock_get_email):
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        tf = self.REVIEW_TF + (
            'resource "helm_release" "reports" {\n'
            '  name  = "reports"\n'
            '  chart = "./charts/reports"\n'
            '  set {\n'
            '    name  = "exportsBucket"\n'
            '    value = "orders-exports"\n'
            '  }\n}\n')
        _, inventory_container, _, mock_ctx = await self._reach_the_data_review(
            mock_gcs_client_class, tf=tf)
        self.assertEqual(self._entry(inventory_container, "orders-exports")["consumers"],
                         [])
        listing = await discovery_datareview_3.tools.list_data_dependencies(ctx=mock_ctx)
        self.assertIn("likely consumer (not attached): reports (helm_release, "
                      "charts/reports)", listing)
        self.assertIn("exportsBucket in data.tf", listing)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_an_addition_beside_a_foreign_twin_is_recorded_and_the_sign_off_counts_it(
            self, mock_gcs_client_class, mock_get_email):
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        tf = self.REVIEW_TF + (
            'provider "aws" {\n  region = "us-east-1"\n  allowed_account_ids = ["123456789012"]\n}\n'
            'resource "aws_iam_policy" "q" {\n  policy = jsonencode({ Resource = '
            '"arn:aws:sqs:us-east-1:999999999999:orders" })\n}\n')
        _, inventory_container, _, mock_ctx = await self._reach_the_data_review(
            mock_gcs_client_class, tf=tf)
        t = discovery_datareview_3.tools
        out = await t.add_data_dependency(service="sqs", identifier="orders", note="ours",
                                          ctx=mock_ctx)
        self.assertNotIn("ERROR", out)
        mine = self._entry(inventory_container, "orders")
        entries = [e for e in inventory_container["inventory"]["data_dependencies"]
                   if e["identifier"] == "orders"]
        self.assertEqual(sorted(e["address"] for e in entries),
                         ["arn:aws:sqs:us-east-1:999999999999:orders", "sqs:orders"])
        own = next(e for e in entries if e["address"] == "sqs:orders")
        self.assertTrue(any("another account, region or partition" in n for n in own["notes"]),
                        own["notes"])
        listing = await t.list_data_dependencies(ctx=mock_ctx)
        self.assertIn("1 added by hand and awaiting a consumer", listing)
        with patch("servers.phases.discovery.discovery_datareview_3.tools"
                   ".run_elicitation") as elicit:
            elicit.return_value = (False, {})
            await t.confirm_data_dependencies(ctx=mock_ctx)
        question = elicit.call_args.args[1] if elicit.call_args.args[1:] else str(elicit.call_args)
        self.assertIn("added by hand with no consumer named yet", str(elicit.call_args))

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_confirmed_guess_keeps_its_consumer_when_a_scan_states_the_fact(
            self, mock_gcs_client_class, mock_get_email):
        from servers.phases.discovery import discovery_datareview_3, discovery_datascan_3
        mock_get_email.return_value = "platform-user@google.com"
        state_container, inventory_container, _, mock_ctx = \
            await self._reach_the_data_review(
                mock_gcs_client_class, tf=self.REVIEW_TF + self.GUESS_TF)
        t = discovery_datareview_3.tools
        out = await t.confirm_data_dependency(address="s3:reports-archive", note="real",
                                              ctx=mock_ctx)
        self.assertNotIn("ERROR", out)
        root = state_container["state"]["variables"]["discovery_root_dir"]
        with open(os.path.join(root, "s3.tf"), "w") as f:
            f.write('resource "aws_s3_bucket" "reports" {\n  bucket = "reports-archive"\n}\n')
        self._rewind_to_the_scan(state_container)
        out = await discovery_datascan_3.tools.scan_data_dependencies(ctx=mock_ctx)
        self.assertNotIn("ERROR", out)
        entry = self._entry(inventory_container, "reports-archive")
        self.assertEqual(entry["detection"], "declared")
        self.assertEqual([(c["workload"], c["kind"]) for c in entry["consumers"]],
                         [("reports", "helm_release")])

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_guess_handle_that_now_names_a_foreign_spelling_is_refused(
            self, mock_gcs_client_class, mock_get_email):
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        tf = self.REVIEW_TF + (
            'provider "aws" {\n  region = "us-east-1"\n  allowed_account_ids = ["123456789012"]\n}\n'
            'resource "aws_iam_policy" "q" {\n  policy = jsonencode({ Resource = '
            '"arn:aws:sqs:us-east-1:999999999999:orders" })\n}\n')
        _, inventory_container, _, mock_ctx = await self._reach_the_data_review(
            mock_gcs_client_class, tf=tf)
        out = await discovery_datareview_3.tools.annotate_data_dependency(
            address="sqs:orders", disposition="migrate", ctx=mock_ctx)
        self.assertIn("ERROR", out)
        self.assertIn("exact address", out)
        foreign = self._entry(inventory_container, "orders")
        self.assertEqual(foreign["disposition"], "replatform")

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_two_holders_of_one_workload_and_kind_are_one_link_at_confirm_time(
            self, mock_gcs_client_class, mock_get_email):
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        manifests = {"k8s/dev.yaml": '''
apiVersion: apps/v1
kind: Deployment
metadata:
  name: orders
  namespace: dev
spec:
  template:
    spec:
      containers:
        - name: orders
          env:
            - name: ASSETS_BUCKET
              value: acme-shared-assets
''', "k8s/prod.yaml": '''
apiVersion: apps/v1
kind: Deployment
metadata:
  name: orders
  namespace: prod
spec:
  template:
    spec:
      containers:
        - name: orders
          env:
            - name: ASSETS_BUCKET
              value: acme-shared-assets
'''}
        _, inventory_container, overrides_container, mock_ctx = \
            await self._reach_the_data_review(mock_gcs_client_class, extra_files=manifests)
        out = await discovery_datareview_3.tools.confirm_data_dependency(
            address="s3:acme-shared-assets", ctx=mock_ctx)
        self.assertNotIn("ERROR", out)
        self.assertNotIn("This replaced an earlier decision", out)
        links = [o for o in overrides_container["document"]["overrides"]
                 if o["kind"] == "attach_consumer"]
        self.assertEqual([(l["workload"], l["consumer_kind"], l.get("namespace")) for l in links],
                         [("orders", "Deployment", None)])

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_adding_a_name_the_section_records_twice_is_refused(
            self, mock_gcs_client_class, mock_get_email):
        """Two same-named queues in two accounts the files cannot reconcile
        stand apart; adding the name by hand is refused, naming both, rather
        than creating a third that gates beside them."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        tf = self.REVIEW_TF + (
            'resource "aws_iam_policy" "q" {\n'
            '  policy = jsonencode({ Statement = [{ Resource = [\n'
            '    "arn:aws:sqs:us-east-1:222222222222:orders",\n'
            '    "arn:aws:sqs:us-east-1:333333333333:orders"] }] })\n}\n')
        _, inventory_container, _, mock_ctx = await self._reach_the_data_review(
            mock_gcs_client_class, tf=tf)
        queues = [e for e in inventory_container["inventory"]["data_dependencies"]
                  if e["service"] == "sqs" and e["identifier"] == "orders"]
        self.assertEqual(len(queues), 2)
        out = await discovery_datareview_3.tools.add_data_dependency(
            service="sqs", identifier="orders", note="ours", ctx=mock_ctx)
        self.assertIn("ERROR", out)
        self.assertIn("records this name 2 times", out)
        self.assertEqual(len([e for e in inventory_container["inventory"]["data_dependencies"]
                              if e["identifier"] == "orders"]), 2)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_guess_cannot_be_annotated_with_a_disposition(
            self, mock_gcs_client_class, mock_get_email):
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, inventory_container, _, mock_ctx = await self._reach_the_data_review(
            mock_gcs_client_class, tf=self.REVIEW_TF + self.GUESS_TF)
        t = discovery_datareview_3.tools
        out = await t.annotate_data_dependency(
            address="s3:reports-archive", disposition="keep-in-aws", ctx=mock_ctx)
        self.assertIn("ERROR", out)
        self.assertIn("confirm_data_dependency", out)
        guess = self._entry(inventory_container, "reports-archive")
        self.assertEqual((guess["detection"], guess["disposition"]), ("inferred", "undecided"))
        # A note alone is fine: it does not pretend to answer the question.
        out = await t.annotate_data_dependency(
            address="s3:reports-archive", note="asked finance", ctx=mock_ctx)
        self.assertNotIn("ERROR", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_reply_describes_the_rebuilt_entry_under_an_endpoint_handle(
            self, mock_gcs_client_class, mock_get_email):
        """The queue is known by URL and by ARN; a correction made under the
        URL has to be reported from the rebuilt entry, not the stale one."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        url = "https://sqs.us-east-1.amazonaws.com/123456789012/orders-events"
        tf = self.REVIEW_TF + (
            'resource "aws_iam_policy" "q" {\n  policy = jsonencode({ Resource = '
            '"arn:aws:sqs:us-east-1:123456789012:orders-events" })\n}\n'
            'resource "kubernetes_config_map" "orders" {\n  metadata { name = "orders-config" }\n'
            '  data = { QUEUE_URL = "' + url + '" }\n}\n')
        _, inventory_container, _, mock_ctx = await self._reach_the_data_review(
            mock_gcs_client_class, tf=tf)
        t = discovery_datareview_3.tools
        out = await t.attach_data_consumer(address=url, workload="reports", kind="Deployment",
                                           ctx=mock_ctx)
        self.assertNotIn("ERROR", out)
        out = await t.reject_data_consumer(address=url, workload="reports", reason="not it",
                                           ctx=mock_ctx)
        self.assertNotIn("ERROR", out)
        self.assertNotIn("Still listed", out)
        entry = self._entry(inventory_container, "orders-events")
        self.assertEqual([c["workload"] for c in entry["consumers"]], ["orders-config"])

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_confirmations_consumer_is_enriched_like_an_attachment(
            self, mock_gcs_client_class, mock_get_email):
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, inventory_container, overrides_container, mock_ctx = \
            await self._reach_the_data_review(
                mock_gcs_client_class, tf=self.REVIEW_TF + self.GUESS_TF)
        t = discovery_datareview_3.tools
        # `orders` is a helm_release the scan saw (REVIEW_TF); the confirmation
        # names it without a kind and the attach tool fills the kind in.
        out = await t.confirm_data_dependency(
            address="s3:reports-archive", workload="orders", ctx=mock_ctx)
        self.assertNotIn("ERROR", out)
        entry = self._entry(inventory_container, "reports-archive")
        by_workload = {c["workload"]: c for c in entry["consumers"]}
        self.assertEqual(by_workload["orders"]["kind"], "helm_release")
        self.assertEqual(by_workload["orders"]["detection"], "human_review")
        kinds = sorted(o["kind"] for o in overrides_container["document"]["overrides"])
        # Round 13: the guess's own holder (`reports`) is attached with the
        # confirmation too, so it follows onto the fact a later scan states.
        self.assertEqual(kinds, ["attach_consumer", "attach_consumer", "confirm_entry"])
        self.assertEqual(sorted(by_workload), ["orders", "reports"])

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_correction_typed_under_a_guess_handle_is_keyed_on_the_fact(
            self, mock_gcs_client_class, mock_get_email):
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, inventory_container, overrides_container, mock_ctx = \
            await self._reach_the_data_review(mock_gcs_client_class)
        out = await discovery_datareview_3.tools.annotate_data_dependency(
            address="s3:orders-exports", note="finance's archive", ctx=mock_ctx)
        self.assertNotIn("ERROR", out)
        self.assertNotIn("could not be placed", out)
        (record,) = overrides_container["document"]["overrides"]
        self.assertEqual((record["address"], record["directory"]), ("aws_s3_bucket.orders_exports", ""))
        self.assertIn({"address": "s3:orders-exports", "directory": None}, record["aliases"])
        entry = self._entry(inventory_container, "orders-exports")
        self.assertTrue(any("finance's archive" in n for n in entry["notes"]))

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_marker_counts_the_corrections_the_scan_has_not_seen(
            self, mock_gcs_client_class, mock_get_email):
        """"Recorded since the scan" has to mean since the scan. The store
        holds every standing correction, including the ones this scan replayed
        — and those are already inside the counts the marker declares stale, so
        counting them says the notes are wronger than they are. The sentence is
        scan-owned passthrough and reaches the assessment verbatim."""
        from servers.phases.discovery import (
            discovery_datareview_3, discovery_datascan_3)
        mock_get_email.return_value = "platform-user@google.com"
        state_container, inventory_container, _, mock_ctx = \
            await self._reach_the_data_review(mock_gcs_client_class)
        t = discovery_datareview_3.tools
        await t.attach_data_consumer(
            address="aws_s3_bucket.orders_exports", workload="orders",
            ctx=mock_ctx)

        # The scan replays that one, so it is no longer "since".
        self._rewind_to_the_scan(state_container)
        out = await discovery_datascan_3.tools.scan_data_dependencies(ctx=mock_ctx)
        self.assertNotIn("ERROR", out)
        notes = inventory_container["inventory"]["data_dependency_scan_notes"]
        self.assertFalse(any(n.startswith(overrides_lib.CORRECTED_SINCE_SCAN)
                             for n in notes))

        await t.reject_data_consumer(
            address="aws_db_instance.orders", workload="orders",
            reason="that release only reads the replica", ctx=mock_ctx)

        notes = inventory_container["inventory"]["data_dependency_scan_notes"]
        marker = next(n for n in notes
                      if n.startswith(overrides_lib.CORRECTED_SINCE_SCAN))
        self.assertIn(": 1, listed with the section", marker)
        self.assertEqual(
            2, len(self.mock_overrides_blob and
                   json.loads(self.mock_overrides_blob.upload_from_string
                              .call_args[0][0])["overrides"]))

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_marker_survives_a_store_that_shrinks_and_a_reword(
            self, mock_gcs_client_class, mock_get_email):
        """A count minus a count is not a count of anything: `record_override`
        RETIRES what a new record supersedes, so consolidating two corrections
        into one leaves fewer records than the scan replayed and the
        subtraction went negative. Rewording a reason leaves the size
        unchanged and the scan notes just as stale. Both are asked by
        identity now."""
        from servers.phases.discovery import (
            discovery_datareview_3, discovery_datascan_3)
        mock_get_email.return_value = "platform-user@google.com"
        state_container, inventory_container, overrides_container, mock_ctx = \
            await self._reach_the_data_review(mock_gcs_client_class)
        t = discovery_datareview_3.tools

        # Two records, keyed apart by consumer kind, then replayed by a scan.
        await t.reject_data_consumer(
            address="aws_db_instance.orders", workload="orders",
            kind="helm_release", reason="only reads the replica", ctx=mock_ctx)
        await t.annotate_data_dependency(
            address="aws_db_instance.orders",
            note="the orders team is deciding", ctx=mock_ctx)
        self._rewind_to_the_scan(state_container)
        self.assertNotIn("ERROR", await
                         discovery_datascan_3.tools.scan_data_dependencies(
                             ctx=mock_ctx))

        def marker():
            return next(
                (n for n in inventory_container["inventory"]
                 ["data_dependency_scan_notes"]
                 if n.startswith(overrides_lib.CORRECTED_SINCE_SCAN)), None)

        self.assertIsNone(marker())

        # A wildcard rejection `covers` the narrow one: 2 records become 2
        # (the annotation is keyed apart), but the rejection is a NEW record.
        await t.reject_data_consumer(
            address="aws_db_instance.orders", workload="orders",
            reason="nothing in orders touches it", ctx=mock_ctx)
        self.assertEqual(
            2, len(overrides_container["document"]["overrides"]))
        self.assertIn(": 1, listed with the section", marker())

        # And a reword, which changes no count at all.
        await t.annotate_data_dependency(
            address="aws_db_instance.orders",
            note="the orders team decided to keep it", ctx=mock_ctx)
        self.assertEqual(
            2, len(overrides_container["document"]["overrides"]))
        self.assertIn(": 2, listed with the section", marker())

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_baseline_from_before_the_fingerprints_reports_them_all(
            self, mock_gcs_client_class, mock_get_email):
        """A ledger written by the server version that recorded a count here
        must not crash the review or be fed to `set()`. Nothing is known to
        have been replayed, so every standing correction is reported — the
        cautious answer, and the one that shipped before fingerprints."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, inventory_container, _, mock_ctx = \
            await self._reach_the_data_review(mock_gcs_client_class)
        t = discovery_datareview_3.tools
        baseline = json.loads(
            self.mock_scan_baseline_blob.upload_from_string.call_args[0][0])
        baseline["corrections_replayed"] = 1
        (main.state_mgr.gcs_client.bucket("test-bucket")
         .blob(t.SCAN_BASELINE_BLOB).upload_from_string(json.dumps(baseline)))

        out = await t.reject_data_consumer(
            address="aws_db_instance.orders", workload="orders",
            reason="only reads the replica", ctx=mock_ctx)

        self.assertNotIn("ERROR", out)
        marker = next(n for n in inventory_container["inventory"]
                      ["data_dependency_scan_notes"]
                      if n.startswith(overrides_lib.CORRECTED_SINCE_SCAN))
        self.assertIn(": 1, listed with the section", marker)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_malformed_record_is_refused_by_every_tool_not_raised(
            self, mock_gcs_client_class, mock_get_email):
        """The shape check stopped at the container. `null` is what a partial
        hand-delete leaves behind — and hand-editing this object is the
        documented route, since a correction can be replaced but not withdrawn
        — and every consumer of the list calls `.get` on its items. Four of
        the five tools raised out of the MCP call instead of returning the
        ERROR their contract promises, and the fifth let the reviewer sign off
        on a section they could no longer read."""
        from servers.phases.discovery import (
            discovery_datareview_3, discovery_datascan_3)
        mock_get_email.return_value = "platform-user@google.com"
        state_container, _, overrides_container, mock_ctx = \
            await self._reach_the_data_review(mock_gcs_client_class)
        t = discovery_datareview_3.tools
        await t.reject_data_consumer(
            address="aws_db_instance.orders", workload="orders",
            reason="only reads the replica", ctx=mock_ctx)

        for corrupt in (None, "a note somebody typed here", 7, []):
            document = dict(overrides_container["document"])
            document["overrides"] = list(document["overrides"]) + [corrupt]
            (main.state_mgr.gcs_client.bucket("test-bucket")
             .blob(t.OVERRIDES_BLOB).upload_from_string(json.dumps(document)))

            with self.subTest(corrupt=corrupt):
                for out in (
                        await t.list_data_dependencies(ctx=mock_ctx),
                        await t.reject_data_consumer(
                            address="aws_db_instance.orders",
                            workload="orders", reason="again", ctx=mock_ctx),
                        await t.attach_data_consumer(
                            address="aws_db_instance.orders",
                            workload="billing", ctx=mock_ctx),
                        await t.annotate_data_dependency(
                            address="aws_db_instance.orders",
                            note="staying put", ctx=mock_ctx)):
                    self.assertIn("ERROR", out)
                # And the scan says how to repair it rather than reporting a
                # bare Python message.
                self._rewind_to_the_scan(state_container)
                out = await discovery_datascan_3.tools.scan_data_dependencies(
                    ctx=mock_ctx)
                self.assertIn("ERROR", out)
                self.assertIn("repair or remove the object", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_malformed_baseline_entry_is_refused_not_raised(
            self, mock_gcs_client_class, mock_get_email):
        """The same one-line gap in the second loader's guard."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, _, _, mock_ctx = await self._reach_the_data_review(
            mock_gcs_client_class)
        t = discovery_datareview_3.tools
        baseline = json.loads(
            self.mock_scan_baseline_blob.upload_from_string.call_args[0][0])
        baseline["data_dependencies"] = list(
            baseline["data_dependencies"]) + ["not an entry"]
        (main.state_mgr.gcs_client.bucket("test-bucket")
         .blob(t.SCAN_BASELINE_BLOB).upload_from_string(json.dumps(baseline)))

        out = await t.annotate_data_dependency(
            address="aws_db_instance.orders", note="staying put", ctx=mock_ctx)

        self.assertIn("ERROR", out)
        self.assertIn("Re-run scan_data_dependencies", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_message_describes_this_call_not_an_earlier_one(
            self, mock_gcs_client_class, mock_get_email):
        """A rebuild re-derives the fill from the merged standing record every
        time, so the handle is present on every later call about that link.
        The sentence then reported a field the reviewer had not passed — the
        comment above it already says an earlier call's value "is not news"."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, _, _, mock_ctx = await self._reach_the_data_review(
            mock_gcs_client_class)
        t = discovery_datareview_3.tools

        out = await t.attach_data_consumer(
            address="aws_db_instance.orders", workload="orders",
            source_path="charts/orders", note="confirmed with the team",
            ctx=mock_ctx)
        self.assertIn("source_path", out)

        out = await t.attach_data_consumer(
            address="aws_db_instance.orders", workload="orders",
            note="second thought", ctx=mock_ctx)

        self.assertNotIn("ERROR", out)
        self.assertNotIn("you gave", out)
        self.assertIn("your reason is now on the entry", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_scan_baseline_of_the_wrong_shape_is_refused_not_raised(
            self, mock_gcs_client_class, mock_get_email):
        """Valid JSON that is not a baseline reached `.get` on a list. The
        step's contract is that a failure comes back as an ERROR string the
        agent can act on, not an exception out of the tool."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        _, _, _, mock_ctx = await self._reach_the_data_review(mock_gcs_client_class)
        (main.state_mgr.gcs_client.bucket("test-bucket")
         .blob(discovery_datareview_3.tools.SCAN_BASELINE_BLOB)
         .upload_from_string(json.dumps(
             [{"service": "rds", "identifier": "orders-db"}])))

        t = discovery_datareview_3.tools
        for call in (
                t.reject_data_consumer(
                    address="aws_db_instance.orders", workload="orders",
                    reason="only reads the replica", ctx=mock_ctx),
                t.attach_data_consumer(
                    address="aws_db_instance.orders", workload="billing",
                    ctx=mock_ctx),
                t.annotate_data_dependency(
                    address="aws_db_instance.orders",
                    note="staying in AWS", ctx=mock_ctx)):
            out = await call
            self.assertIn("ERROR", out)
            self.assertIn("Re-run scan_data_dependencies", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_reason_for_keeping_a_service_is_not_lost_to_the_decision(
            self, mock_gcs_client_class, mock_get_email):
        """Two annotations on one entry — the customer's reason, then the
        disposition that acts on it — are two decisions. Keying them together
        made the second delete the first from the durable store, so the reason
        survived the reviewer's session and vanished at the next scan. That is
        a lost human decision, which is the whole thing this step exists to
        prevent."""
        from servers.phases.discovery import (
            discovery_datareview_3, discovery_datascan_3)
        mock_get_email.return_value = "platform-user@google.com"
        state_container, inventory_container, overrides_container, mock_ctx = \
            await self._reach_the_data_review(mock_gcs_client_class)

        await discovery_datareview_3.tools.annotate_data_dependency(
            address="aws_s3_bucket.orders_exports",
            note="customer keeps it: the analytics pipeline reads it directly",
            ctx=mock_ctx)
        out = await discovery_datareview_3.tools.annotate_data_dependency(
            address="aws_s3_bucket.orders_exports", disposition="keep-in-aws",
            ctx=mock_ctx)
        self.assertNotIn("ERROR", out)

        self.assertEqual(len(overrides_container["document"]["overrides"]), 2)
        live = self._entry(inventory_container, "orders-exports")

        self._rewind_to_the_scan(state_container)
        await discovery_datascan_3.tools.scan_data_dependencies(ctx=mock_ctx)
        replayed = self._entry(inventory_container, "orders-exports")

        self.assertEqual(replayed["disposition"], "keep-in-aws")
        self.assertTrue(any("analytics pipeline" in n for n in replayed["notes"]))
        # And the two paths agree: what was approved is what the scan rebuilds.
        self.assertEqual(sorted(live["notes"]), sorted(replayed["notes"]))

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_an_approval_will_not_cover_a_section_that_moved_under_it(
            self, mock_gcs_client_class, mock_get_email):
        """The stamp asserts counts read from the inventory but is written to
        state.json, so state.json's generation guards nothing about the
        section. The elicitation is human-length; a second platform session
        correcting the mapping during it would otherwise have the sign-off
        commit against entries the reviewer never saw."""
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        state_container, inventory_container, _, mock_ctx = \
            await self._reach_the_data_review(mock_gcs_client_class)

        # Another session writes the inventory while the question is open.
        async def amend_then_answer(*args, **kwargs):
            bucket = main.state_mgr.gcs_client.bucket("test-bucket")
            stored, generation = main.state_mgr.load_inventory(bucket)
            stored["data_dependencies"][0]["disposition"] = "keep-in-aws"
            main.state_mgr.save_inventory(bucket, stored, generation)
            answer = MagicMock()
            answer.action, answer.content = "accept", {"approved": True}
            return answer
        mock_ctx.request_context.session.send_request.side_effect = amend_then_answer

        out = await discovery_datareview_3.tools.confirm_data_dependencies(
            ctx=mock_ctx)

        self.assertIn("ERROR", out)
        self.assertIn("changed while the approval was being asked", out)
        self.assertNotIn("data_dependency_review",
                         state_container["state"]["variables"])
        self.assertEqual(state_container["state"]["current_state"],
                         "STATE_DISCOVERY_DATA_REVIEW")

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_correction_it_cannot_aim_is_refused_not_guessed(
            self, mock_gcs_client_class, mock_get_email):
        """Two root modules can declare the same block address, and they are
        different resources in different accounts. Rejecting a consumer on the
        wrong one is the wrong-team failure this step exists to prevent."""
        from servers.phases.discovery import (
            discovery_datareview_3, discovery_datascan_3, discovery_scope_2)
        mock_get_email.return_value = "platform-user@google.com"
        two_environments = {
            "envs/dev/main.tf": 'resource "aws_sqs_queue" "orders" {\n'
                                '  name = "orders-dev"\n}\n',
            "envs/prod/main.tf": 'resource "aws_sqs_queue" "orders" {\n'
                                 '  name = "orders-prod"\n}\n',
        }
        state_container, inventory_container, mock_ctx = self._setup_image_flow(
            mock_gcs_client_class)
        self._install_overrides_roundtrip()
        with tempfile.TemporaryDirectory() as root:
            self._write_image_fixture(root, with_render_targets=False)
            for rel_path, content in two_environments.items():
                os.makedirs(os.path.join(root, os.path.dirname(rel_path)),
                            exist_ok=True)
                with open(os.path.join(root, rel_path), "w") as f:
                    f.write(content)
            await main.discover_configuration_files(ctx=mock_ctx, root_dir=root)
            await discovery_scope_2.tools.confirm_discovery_scope(ctx=mock_ctx)
            await discovery_datascan_3.tools.scan_data_dependencies(ctx=mock_ctx)

            out = await discovery_datareview_3.tools.annotate_data_dependency(
                address="aws_sqs_queue.orders", note="ask the owner",
                ctx=mock_ctx)
            self.assertIn("ERROR", out)
            self.assertIn("orders-dev", out)
            self.assertIn("orders-prod", out)

            # Named, it lands on exactly one of them.
            out = await discovery_datareview_3.tools.annotate_data_dependency(
                address="aws_sqs_queue.orders", identifier="orders-prod",
                note="ask the owner", ctx=mock_ctx)
            self.assertNotIn("ERROR", out)
            self.assertFalse(any("ask the owner" in n for n in
                                 self._entry(inventory_container, "orders-dev")["notes"]))
            self.assertTrue(any("ask the owner" in n for n in
                                self._entry(inventory_container, "orders-prod")["notes"]))

            # An address nothing declares is refused with what IS declared.
            out = await discovery_datareview_3.tools.reject_data_consumer(
                address="module.nope", workload="orders", ctx=mock_ctx)
            self.assertIn("ERROR", out)
            self.assertIn("aws_sqs_queue.orders", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_two_environments_whose_names_are_expressions_are_still_told_apart(
            self, mock_gcs_client_class, mock_get_email):
        """The dangerous shape of the ambiguity above. `name = "${var.env}-orders"`
        makes the harvester fall back to the block address for the identifier —
        its own documented common case — so both entries carry the same address
        AND the same identifier, and `identifier` cannot separate them. Offering
        it as the disambiguator produced an error listing the same string twice,
        and passing it applied a dev-only decision to prod as well, silently and
        on every later scan."""
        from servers.phases.discovery import (
            discovery_datareview_3, discovery_datascan_3, discovery_scope_2)
        mock_get_email.return_value = "platform-user@google.com"
        by_expression = 'resource "aws_sqs_queue" "orders" {\n'\
                        '  name = "${var.env}-orders"\n}\n'\
                        'resource "helm_release" "orders" {\n'\
                        '  name = "orders"\n'\
                        '  set { value = aws_sqs_queue.orders.url }\n}\n'
        state_container, inventory_container, mock_ctx = self._setup_image_flow(
            mock_gcs_client_class)
        self._install_overrides_roundtrip()
        with tempfile.TemporaryDirectory() as root:
            self._write_image_fixture(root, with_render_targets=False)
            for env in ("dev", "prod"):
                os.makedirs(os.path.join(root, "envs", env), exist_ok=True)
                with open(os.path.join(root, "envs", env, "main.tf"), "w") as f:
                    f.write(by_expression)
            await main.discover_configuration_files(ctx=mock_ctx, root_dir=root)
            await discovery_scope_2.tools.confirm_discovery_scope(ctx=mock_ctx)
            await discovery_datascan_3.tools.scan_data_dependencies(ctx=mock_ctx)

            entries = inventory_container["inventory"]["data_dependencies"]
            self.assertEqual(len(entries), 2)
            # The premise: neither address nor identifier separates them.
            self.assertEqual({e["identifier"] for e in entries},
                             {"aws_sqs_queue.orders"})

            # Refused, and the refusal offers something that actually differs.
            out = await discovery_datareview_3.tools.reject_data_consumer(
                address="aws_sqs_queue.orders", workload="orders",
                reason="dev only", ctx=mock_ctx)
            self.assertIn("ERROR", out)
            self.assertIn("directory='envs/dev'", out)
            self.assertIn("directory='envs/prod'", out)
            # Passing the identifier it prints does NOT make it proceed.
            out = await discovery_datareview_3.tools.reject_data_consumer(
                address="aws_sqs_queue.orders", identifier="aws_sqs_queue.orders",
                workload="orders", reason="dev only", ctx=mock_ctx)
            self.assertIn("ERROR", out)

            out = await discovery_datareview_3.tools.reject_data_consumer(
                address="aws_sqs_queue.orders", directory="envs/dev",
                workload="orders", reason="dev only", ctx=mock_ctx)
            self.assertNotIn("ERROR", out)

            by_dir = {(e.get("evidence") or [""])[0]: e for e in
                      inventory_container["inventory"]["data_dependencies"]}
            self.assertEqual(by_dir["envs/dev/main.tf"]["consumers"], [])
            # The one nobody corrected keeps its attribution.
            self.assertEqual(
                [c["workload"] for c in by_dir["envs/prod/main.tf"]["consumers"]],
                ["orders"])

            # And it stays that way: the record carries the directory, so the
            # replay aims at the same one entry.
            self._rewind_to_the_scan(state_container)
            await discovery_datascan_3.tools.scan_data_dependencies(ctx=mock_ctx)
            by_dir = {(e.get("evidence") or [""])[0]: e for e in
                      inventory_container["inventory"]["data_dependencies"]}
            self.assertEqual(by_dir["envs/dev/main.tf"]["consumers"], [])
            self.assertEqual(
                [c["workload"] for c in by_dir["envs/prod/main.tf"]["consumers"]],
                ["orders"])

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_approving_the_mapping_starts_extraction_and_records_who(
            self, mock_gcs_client_class, mock_get_email):
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        state_container, _, _, mock_ctx = await self._reach_the_data_review(
            mock_gcs_client_class)

        out = await discovery_datareview_3.tools.confirm_data_dependencies(
            ctx=mock_ctx)

        self.assertIn("SUCCESS", out)
        self.assertEqual(state_container["state"]["current_state"],
                         "STATE_DISCOVERY_RUNNING")
        stamp = state_container["state"]["variables"]["data_dependency_review"]
        self.assertEqual(stamp["approved_by"], "platform-user@google.com")
        self.assertEqual(stamp["entries"], 2)
        self.assertEqual(stamp["unattributed"], 1)

        # And the step is closed: the amend tools refuse outside the review.
        out = await discovery_datareview_3.tools.reject_data_consumer(
            address="aws_db_instance.orders", workload="orders", ctx=mock_ctx)
        self.assertIn("Invalid state", out)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_declining_leaves_the_review_open_with_its_corrections(
            self, mock_gcs_client_class, mock_get_email):
        from servers.phases.discovery import discovery_datareview_3
        mock_get_email.return_value = "platform-user@google.com"
        state_container, _, overrides_container, mock_ctx = \
            await self._reach_the_data_review(mock_gcs_client_class)
        await discovery_datareview_3.tools.attach_data_consumer(
            address="aws_s3_bucket.orders_exports", workload="orders",
            ctx=mock_ctx)
        # The elicitation comes back declined.
        declined = MagicMock()
        declined.action = "accept"
        declined.content = {"approved": False}

        async def decline(*args, **kwargs):
            return declined
        mock_ctx.request_context.session.send_request.side_effect = decline

        out = await discovery_datareview_3.tools.confirm_data_dependencies(
            ctx=mock_ctx)

        self.assertIn("not approved", out)
        self.assertEqual(state_container["state"]["current_state"],
                         "STATE_DISCOVERY_DATA_REVIEW")
        self.assertNotIn("data_dependency_review",
                         state_container["state"]["variables"])
        # A decline is not a reset: what the reviewer already corrected stands.
        self.assertEqual(len(overrides_container["document"]["overrides"]), 1)
        # And it is the only trace a declined review leaves.
        self.assertTrue(any("declined" in h
                            for h in state_container["state"]["history"]))

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_scan_refuses_to_run_without_the_corrections_it_replays(
            self, mock_gcs_client_class, mock_get_email):
        """A scan that cannot read the corrections object would rebuild the
        section without them and report success — every human decision gone,
        with nothing saying so. It stops instead."""
        from servers.phases.discovery import discovery_datascan_3
        mock_get_email.return_value = "platform-user@google.com"
        state_container, _, _, mock_ctx = await self._reach_the_data_review(
            mock_gcs_client_class)
        self.mock_overrides_blob.reload.side_effect = None
        self.mock_overrides_blob.download_as_text.side_effect = None
        self.mock_overrides_blob.download_as_text.return_value = "{ truncated"

        self._rewind_to_the_scan(state_container)
        out = await discovery_datascan_3.tools.scan_data_dependencies(ctx=mock_ctx)

        self.assertIn("ERROR", out)
        self.assertIn("data_consumer_overrides.json", out)
        # Still on the scan, so the run is resumable once the object is fixed.
        self.assertEqual(state_container["state"]["current_state"],
                         "STATE_DISCOVERY_DATA_SCAN")

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_rescan_asks_for_the_sign_off_again(
            self, mock_gcs_client_class, mock_get_email):
        """The corrections are durable and the approval is not. A re-scan
        rebuilds the section from the checkout, so a sign-off carried across it
        would cover entries the reviewer never saw."""
        from servers.phases.discovery import (
            discovery_datareview_3, discovery_datascan_3)
        mock_get_email.return_value = "platform-user@google.com"
        state_container, _, _, mock_ctx = await self._reach_the_data_review(
            mock_gcs_client_class)
        await discovery_datareview_3.tools.confirm_data_dependencies(ctx=mock_ctx)
        self.assertIn("data_dependency_review",
                      state_container["state"]["variables"])

        self._rewind_to_the_scan(state_container)
        await discovery_datascan_3.tools.scan_data_dependencies(ctx=mock_ctx)

        self.assertEqual(state_container["state"]["current_state"],
                         "STATE_DISCOVERY_DATA_REVIEW")
        self.assertNotIn("data_dependency_review",
                         state_container["state"]["variables"])


class StagePayloadTest(unittest.TestCase):
    """Assembly of the get_next_stage response: state line, knowledge, instructions."""

    # The assessment phase: fully converted, and its knowledge doc exists.
    # (Discovery's eks-discovery.md was deleted with the map-reduce pipeline.)
    PHASE = "assessment"
    DOC = "migration-assessment.md"
    STEP = "servers/phases/assessment/assessment_review_1"
    INSTRUCTIONS = "servers/phases/assessment/assessment_review_1/instructions.md"

    def setUp(self):
        # Delivery bookkeeping lives in module state for the life of the
        # process, so each test starts from an empty session.
        main.knowledge_served.clear()

    def tearDown(self):
        main.knowledge_served.clear()

    def state(self, **overrides):
        state_def = {
            "type": "AGENT_TASK",
            "phase": self.PHASE,
            "step": self.STEP,
            "instructions": self.INSTRUCTIONS,
            "knowledge": [self.DOC],
            "transitions": {"on_tool_call_received": "STATE_NEXT"},
        }
        state_def.update(overrides)
        return state_def

    def repo_text(self, rel_path):
        with open(os.path.join(main.REPO_ROOT, rel_path), "r", encoding="utf-8") as f:
            return f.read()

    def test_declared_knowledge_is_served_in_full(self):
        payload = main.build_stage_payload("STATE_ASSESSMENT", self.state())

        self.assertIn("Current state: STATE_ASSESSMENT. Type: AGENT_TASK.", payload)
        self.assertIn("--- Phase knowledge: migration-assessment ---", payload)
        self.assertIn(
            self.repo_text(f"servers/phases/{self.PHASE}/knowledge/{self.DOC}"), payload)
        self.assertIn(self.repo_text(self.INSTRUCTIONS), payload)

    def test_knowledge_precedes_instructions(self):
        payload = main.build_stage_payload("STATE_ASSESSMENT", self.state())
        # Background before the procedure that assumes it; the agent reads the
        # payload top to bottom.
        self.assertLess(
            payload.index("--- Phase knowledge:"), payload.index("--- Step instructions ---"))

    def test_state_declaring_no_knowledge_gets_none(self):
        payload = main.build_stage_payload("STATE_X", self.state(knowledge=None))
        self.assertNotIn("Phase knowledge", payload)
        self.assertIn("--- Step instructions ---", payload)

    def test_knowledge_ignored_without_a_phase(self):
        # The phase supplies the directory; a document name alone is not
        # resolvable, so it is dropped rather than guessed at.
        payload = main.build_stage_payload("STATE_X", self.state(phase=None))
        self.assertNotIn("Phase knowledge", payload)

    def test_bare_state_line_when_nothing_is_declared(self):
        payload = main.build_stage_payload(
            "STATE_COMPLETED", {"type": "TERMINAL", "status": "SUCCESS"})
        self.assertEqual(payload, "Current state: STATE_COMPLETED. Type: TERMINAL.")

    def test_served_once_per_session(self):
        first = main.build_stage_payload("STATE_ASSESSMENT", self.state())
        second = main.build_stage_payload("STATE_ASSESSMENT", self.state())

        self.assertIn("--- Phase knowledge: migration-assessment ---", first)
        self.assertNotIn("Phase knowledge", second)
        # The step's own procedure is not deduped: it is what the agent is
        # being told to do right now, every time it lands on the state.
        self.assertIn("--- Step instructions ---", second)

    def test_dedup_is_scoped_per_phase_and_document(self):
        main.build_stage_payload("STATE_ASSESSMENT", self.state())
        # A different phase's document, resolved inside that phase's own
        # knowledge/ directory. Having served one document must not suppress it.
        other = main.build_stage_payload(
            "STATE_LZ_DESIGN",
            self.state(
                phase="landingzone",
                knowledge=["gke-landing-zone.md"],
                step="servers/phases/landingzone/landingzone_design_2",
                instructions="servers/phases/landingzone/landingzone_design_2/instructions.md",
            ),
        )
        self.assertIn("--- Phase knowledge: gke-landing-zone ---", other)

    def test_bootstrap_migration_clears_the_session(self):
        main.build_stage_payload("STATE_ASSESSMENT", self.state())
        self.assertIn(f"{self.PHASE}/{self.DOC}", main.knowledge_served)

        main.bootstrap_migration()

        # Rewinding replays steps the agent has already seen, so what they
        # declare has to be deliverable again.
        self.assertEqual(main.knowledge_served, set())
        replayed = main.build_stage_payload("STATE_ASSESSMENT", self.state())
        self.assertIn("--- Phase knowledge: migration-assessment ---", replayed)

    def test_malformed_phase_name_is_refused(self):
        for phase in ("../../etc", "..", ".", "discovery/../../etc"):
            with self.subTest(phase=phase):
                main.knowledge_served.clear()
                payload = main.build_stage_payload(
                    "STATE_X", self.state(phase=phase, step=None, instructions=None))
                self.assertNotIn("Phase knowledge", payload)
                self.assertEqual(main.knowledge_served, set())

    def test_knowledge_name_with_a_separator_is_refused(self):
        payload = main.build_stage_payload(
            "STATE_X", self.state(knowledge=["../../../../etc/passwd"]))
        self.assertNotIn("Phase knowledge", payload)
        self.assertEqual(main.knowledge_served, set())

    def test_missing_knowledge_file_does_not_break_the_payload(self):
        # A graph can reference a document that is not on disk. The step still
        # needs its instructions, so the gap is logged and skipped.
        payload = main.build_stage_payload(
            "STATE_X", self.state(knowledge=["no-such-doc.md", self.DOC]))

        self.assertNotIn("no-such-doc", payload)
        self.assertIn("--- Phase knowledge: migration-assessment ---", payload)
        self.assertIn("--- Step instructions ---", payload)
        # Nothing was delivered for the missing name, so a later step declaring
        # it is not told it has already been sent.
        self.assertNotIn(f"{self.PHASE}/no-such-doc.md", main.knowledge_served)

    def test_missing_instructions_file_does_not_break_the_payload(self):
        payload = main.build_stage_payload(
            "STATE_X",
            self.state(
                knowledge=None,
                step="servers/phases/discovery/discovery_absent_9",
                instructions="servers/phases/discovery/discovery_absent_9/instructions.md",
            ),
        )
        self.assertEqual(payload, "Current state: STATE_X. Type: AGENT_TASK.")

    def test_read_repo_file_refuses_paths_outside_the_repository(self):
        self.assertIsNone(main.read_repo_file("../../../../etc/passwd", "test"))
        self.assertIsNone(main.read_repo_file("/etc/passwd", "test"))


class GraphTraversalPayloadTest(unittest.TestCase):
    """Payload assembly across the real platform graph, not a synthetic state."""

    @classmethod
    def setUpClass(cls):
        dag_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "platform_dag.json")
        with open(dag_path, "r") as f:
            cls.dag = json.load(f)

    def setUp(self):
        main.knowledge_served.clear()

    def tearDown(self):
        main.knowledge_served.clear()

    def test_every_node_resolves_to_real_content(self):
        for name, state in self.dag["states"].items():
            with self.subTest(state=name):
                main.knowledge_served.clear()
                payload = main.build_stage_payload(name, state)

                self.assertTrue(payload.startswith(f"Current state: {name}. Type: {state['type']}."))

                if state.get("instructions"):
                    self.assertIn("--- Step instructions ---", payload)
                    # An unresolvable path degrades to an empty section rather
                    # than raising, which is the failure this is here to catch.
                    body = payload.split("--- Step instructions ---", 1)[1].strip()
                    self.assertTrue(body, f"{name}: instructions resolved to nothing")

                for doc in state.get("knowledge") or []:
                    self.assertIn(f"--- Phase knowledge: {doc[:-3]} ---", payload)

                if state["type"] != "AGENT_TASK":
                    # Only AGENT_TASK nodes carry content; everything else is a
                    # state line the executor acts on directly.
                    self.assertEqual(payload, f"Current state: {name}. Type: {state['type']}.")

    def test_each_document_served_once_across_a_rediscover_loop(self):
        # Declining the STATE_ASSESSMENT review loops back to STATE_DISCOVERY,
        # STATE_BLOCKER_RESOLUTION loops back to itself while a blocker is
        # unowned, and rejecting the translation plan returns to
        # STATE_LZ_DESIGN. Re-entering a state is exactly where a missing
        # dedup update shows up, so the walk covers all three loops.
        walk = [
            "STATE_DISCOVERY",
            "STATE_ASSESSMENT",
            "STATE_DISCOVERY",
            "STATE_ASSESSMENT",
            "STATE_BLOCKER_RESOLUTION",
            "STATE_BLOCKER_RESOLUTION",
            "STATE_LZ_DESIGN",
            "STATE_LZ_DESIGN",
        ]
        self.assertEqual(
            self.dag["states"]["STATE_ASSESSMENT"]["transitions"].get("on_reject"),
            "STATE_DISCOVERY",
            "the graph no longer loops back; update this walk",
        )
        self.assertEqual(
            self.dag["states"]["STATE_BLOCKER_RESOLUTION"]["transitions"].get(
                "on_blockers_outstanding"),
            "STATE_BLOCKER_RESOLUTION",
            "the blocker gate no longer self-loops; update this walk",
        )
        self.assertEqual(
            self.dag["states"]["STATE_LZ_TRANSLATION_PLAN_REVIEW"]["transitions"].get(
                "on_reject"),
            "STATE_LZ_DESIGN",
            "the plan review no longer returns to design; update this walk",
        )

        payloads = [main.build_stage_payload(n, self.dag["states"][n]) for n in walk]
        combined = "\n".join(payloads)

        for doc in ("migration-assessment", "gke-landing-zone"):
            with self.subTest(doc=doc):
                self.assertEqual(combined.count(f"--- Phase knowledge: {doc} ---"), 1)

        # The loop still tells the agent what to do each time round.
        self.assertEqual(combined.count("--- Step instructions ---"), len(walk))


class LedgerDagValidationTest(unittest.TestCase):
    """The graph read back from the ledger is untrusted and validated on read."""

    def blob_returning(self, text):
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_blob.download_as_text.return_value = text
        mock_bucket.blob.return_value = mock_blob
        return mock_bucket

    def test_valid_ledger_dag_is_loaded(self):
        dag_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "platform_dag.json")
        with open(dag_path, "r") as f:
            text = f.read()

        dag = main.load_dag(self.blob_returning(text), "platform_dag.json")
        self.assertIn("STATE_DISCOVERY", dag["states"])

    def test_tampered_ledger_dag_is_refused(self):
        # Anyone with bucket access can rewrite this object. Passing validation
        # at start-up says nothing about what the ledger holds now, and an
        # instructions path is a file the server would read back to the agent.
        tampered = {
            "name": "platform",
            "version": "1.5",
            "start_state": "STATE_DISCOVERY",
            "states": {
                "STATE_DISCOVERY": {
                    "type": "AGENT_TASK",
                    "phase": "discovery",
                    "step": "servers/phases/discovery/discovery_init_1",
                    "instructions": "servers/phases/discovery/discovery_init_1/instructions.md",
                    "knowledge": ["../../../../etc/passwd"],
                    "transitions": {"on_tool_call_received": "STATE_DISCOVERY"},
                },
            },
        }

        with self.assertRaises(DagValidationError):
            main.load_dag(self.blob_returning(json.dumps(tampered)), "platform_dag.json")


class BucketCreationTest(unittest.TestCase):
    """The ledger bucket has to be able to hold managed folders."""

    def setUp(self):
        self.saved_client = main.state_mgr.gcs_client
        self.client = MagicMock()
        self.bucket = MagicMock()
        self.client.bucket.return_value = self.bucket
        main.state_mgr.gcs_client = self.client

    def tearDown(self):
        main.state_mgr.gcs_client = self.saved_client

    def test_bucket_is_created_with_uniform_access(self):
        main.provision_gcs_bucket("proj", "gs://b", "ws")
        self.bucket.create.assert_called_once_with(project="proj")
        self.assertTrue(self.bucket.iam_configuration.uniform_bucket_level_access_enabled,
                        "managed folders require uniform bucket-level access")

    def test_bucket_is_created_with_public_access_prevented(self):
        main.provision_gcs_bucket("proj", "gs://b", "ws")
        self.assertEqual(self.bucket.iam_configuration.public_access_prevention, "enforced")

    def test_an_existing_bucket_without_uniform_access_is_upgraded(self):
        # Re-running bootstrap over a bucket that predates this code: it cannot
        # hold managed folders until uniform access is turned on.
        self.bucket.create.side_effect = Exception("409 You already own this bucket")

        def reload_as_legacy(*args, **kwargs):
            self.bucket.iam_configuration.uniform_bucket_level_access_enabled = False

        self.bucket.reload.side_effect = reload_as_legacy

        main.provision_gcs_bucket("proj", "gs://b", "ws")

        self.bucket.patch.assert_called_once()
        self.assertTrue(self.bucket.iam_configuration.uniform_bucket_level_access_enabled)

    def test_an_existing_bucket_already_uniform_is_left_alone(self):
        self.bucket.create.side_effect = Exception("409 You already own this bucket")
        main.provision_gcs_bucket("proj", "gs://b", "ws")
        self.bucket.patch.assert_not_called()

    def test_a_real_creation_failure_still_raises(self):
        self.bucket.create.side_effect = Exception("403 forbidden")
        with self.assertRaises(RuntimeError):
            main.provision_gcs_bucket("proj", "gs://b", "ws")


class BootstrapMutationTest(unittest.TestCase):
    """execute_internal_mutations walks the bootstrap graph, one state per mutation."""

    ROLES = {"admins": ["a@x.com"], "platform_engineers": ["p@x.com"], "developers": []}

    def setUp(self):
        self.saved_state = main.current_state
        self.saved_client = main.state_mgr.gcs_client
        main.current_state = "STATE_GCS_PROVISIONING"
        main.state_mgr.gcs_client = MagicMock()

    def tearDown(self):
        main.current_state = self.saved_state
        main.state_mgr.gcs_client = self.saved_client

    def run_mutations(self):
        return main.execute_internal_mutations("ws", "proj", "gs://b", self.ROLES)

    def test_the_graph_routes_bucket_creation_into_iam_provisioning(self):
        transitions = main.bootstrap_dag["states"]["STATE_GCS_PROVISIONING"]["transitions"]
        self.assertEqual(transitions["on_success"], "STATE_IAM_PROVISIONING",
                         "the IAM grants are consented to; they must not be skippable")

    @patch("main.write_platform_state_machine")
    @patch("main.write_workspace_registry")
    @patch("main.provision_ledger_iam")
    @patch("main.provision_gcs_bucket")
    def test_all_four_mutations_reach_completion(self, bucket, iam, registry, platform):
        self.run_mutations()
        # Not a terminal: the admin parks where the role lists can still be
        # corrected and the migration reset (the friction log's "no chance
        # to fix a mistake" once the emails are in).
        self.assertEqual(main.current_state, "STATE_WORKSPACE_ADMIN")
        for mutation in (bucket, iam, registry, platform):
            mutation.assert_called_once()

    @patch("main.write_platform_state_machine")
    @patch("main.write_workspace_registry")
    @patch("main.provision_ledger_iam")
    @patch("main.provision_gcs_bucket")
    def test_iam_is_granted_the_roles_the_user_approved(self, bucket, iam, registry, platform):
        self.run_mutations()
        iam.assert_called_once_with("gs://b", self.ROLES)

    @patch("main.write_platform_state_machine")
    @patch("main.write_workspace_registry")
    @patch("main.provision_ledger_iam")
    @patch("main.provision_gcs_bucket")
    def test_a_failed_iam_grant_aborts_the_bootstrap(self, bucket, iam, registry, platform):
        # The user approved the bucket *and* the grants on it. A ledger with no
        # cloud-side boundary is not the thing that was consented to, so the run
        # stops rather than reporting success.
        iam.side_effect = RuntimeError("caller lacks storage.buckets.setIamPolicy")

        with self.assertRaises(RuntimeError):
            self.run_mutations()

        self.assertEqual(main.current_state, "STATE_ABORTED")
        registry.assert_not_called()
        platform.assert_not_called()


class FragmentContractTest(unittest.TestCase):
    """Persisted fragments are reusable only under the contract that produced them."""

    def test_contract_changes_with_the_schema_and_the_rules(self):
        from servers.phases.discovery.discovery_extract_3 import tools as extract_tools

        schema = {"properties": {"workloads": {"type": "object"}}}
        base = extract_tools.extraction_contract(schema)
        # Deterministic for an identical contract — this is what makes reuse valid.
        self.assertEqual(base, extract_tools.extraction_contract(schema))

        grown = {"properties": {"workloads": {"type": "object"}, "new_field": {}}}
        self.assertNotEqual(base, extract_tools.extraction_contract(grown))

        with patch.object(extract_tools.extractor, "EXTRACT_SYSTEM_RULES",
                          extract_tools.extractor.EXTRACT_SYSTEM_RULES + "\n- a new rule"):
            self.assertNotEqual(base, extract_tools.extraction_contract(schema))


class BlockerCriteriaTest(unittest.TestCase):
    """The blocker taxonomy is parsed out of the assessment knowledge document."""

    def test_the_step_4_table_is_the_taxonomy(self):
        categories = blocker_criteria.load_blocker_categories(main.REPO_ROOT, refresh=True)

        # Fifteen rows today. The count is asserted because the parser silently
        # returning a subset is the failure that would leave the gate accepting
        # categories the document does not list.
        self.assertEqual(len(categories), 15)
        self.assertIn("Custom CNI (non-VPC-CNI) in production use", categories)
        # The five conditions the Step 2/Step 3 rubric names at blocker level
        # must stay registrable; each is pinned verbatim.
        self.assertIn("Local `hostPath` volumes in workloads", categories)
        self.assertIn(
            "VPC CNI-specific features in use (ENI per pod, security groups for pods)",
            categories)
        self.assertIn(
            "On-prem-pinned dependency or cross-account RDS without a replica path",
            categories)
        self.assertIn(
            "Cluster authentication via legacy `aws-auth` ConfigMap (CONFIG_MAP mode)",
            categories)
        self.assertIn(
            "Secrets-at-rest encryption parity (EKS KMS `encryption_config`)",
            categories)
        for category, resolution in categories.items():
            self.assertTrue(resolution.strip(), f"{category}: no resolution path")

    def test_a_missing_section_is_fatal(self):
        # Start-up enforcement is the point: an empty taxonomy would disable the
        # category check rather than announce that it is broken.
        with self.assertRaises(blocker_criteria.BlockerCriteriaError):
            blocker_criteria.parse_blocker_categories(
                "# Assessment\n\n## Step 9 — Something else\n", "test.md")

    def test_a_duplicate_category_is_fatal(self):
        text = (
            "### Step 4 — Identify blockers\n\n"
            "| Category | Resolution |\n|---|---|\n"
            "| Custom CNI | Migrate to Dataplane V2 |\n"
            "| Custom CNI | Something else |\n"
        )
        with self.assertRaises(blocker_criteria.BlockerCriteriaError):
            blocker_criteria.parse_blocker_categories(text, "test.md")


class ReadinessReportTest(unittest.TestCase):
    """write_readiness_report's blocker validation, against the parsed taxonomy."""

    def known_category(self):
        return sorted(blocker_criteria.load_blocker_categories(main.REPO_ROOT))[0]

    def blocker(self, **overrides):
        entry = {
            "id": "B-001",
            "title": "Cilium CNI in production",
            "category": self.known_category(),
            "rationale": "Dataplane V2 replaces it, and the policy set has to be ported.",
            "resolution_path": "Port NetworkPolicies to Dataplane V2 and re-test.",
        }
        entry.update(overrides)
        return entry

    def test_a_well_formed_blocker_is_accepted(self):
        self.assertEqual(review_tools.validate_blockers([self.blocker()]), [])

    def test_a_category_outside_the_table_is_rejected(self):
        errors = review_tools.validate_blockers(
            [self.blocker(category="Vibes were off")])
        self.assertTrue(any("not in the Step 4 blocker table" in e for e in errors))

    def test_a_category_differing_only_in_markdown_is_accepted(self):
        # The Step 4 table wraps some categories in markdown backticks; an agent
        # reading the rendered document naturally submits the words without them,
        # and in a different case. That is the same category — enforce the words,
        # not the markup.
        known = blocker_criteria.load_blocker_categories(main.REPO_ROOT)
        with_backticks = next((c for c in known if "`" in c), None)
        self.assertIsNotNone(
            with_backticks, "expected a backtick-wrapped category in the taxonomy")
        stripped = with_backticks.replace("`", "")

        self.assertEqual(
            review_tools.validate_blockers([self.blocker(category=stripped)]), [])
        self.assertEqual(
            review_tools.validate_blockers([self.blocker(category=stripped.upper())]), [])

    def test_the_error_lists_categories_without_markup(self):
        # The "use one of" hint should read as the vocabulary, not its markdown,
        # so a rejected agent retries with words rather than copying backticks.
        errors = review_tools.validate_blockers(
            [self.blocker(category="Vibes were off")])
        self.assertTrue(errors)
        self.assertFalse(any("`" in e for e in errors))

    def test_duplicate_ids_are_rejected(self):
        errors = review_tools.validate_blockers([self.blocker(), self.blocker()])
        self.assertTrue(any("duplicate blocker id" in e for e in errors))

    def test_a_tbd_resolution_path_is_rejected(self):
        # The knowledge document's own Validation section: "Every blocker has a
        # resolution path. None is left as 'TBD'."
        errors = review_tools.validate_blockers(
            [self.blocker(resolution_path="TBD — need to ask the network team")])
        self.assertTrue(any("resolution_path is 'TBD'" in e for e in errors))

    def test_a_missing_required_field_is_rejected(self):
        errors = review_tools.validate_blockers([self.blocker(rationale="  ")])
        self.assertTrue(any("'rationale' is required" in e for e in errors))

    def test_stored_blockers_start_unowned(self):
        stored = review_tools.normalize_blockers([self.blocker()])
        self.assertIsNone(stored[0]["owner"])
        self.assertIsNone(stored[0]["target_close_date"])


class BlockerGateTest(unittest.TestCase):
    """Landing zone design is unreachable until every blocker is owned and dated."""

    def setUp(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        with open(os.path.join(dir_path, "platform_dag.json"), "r") as f:
            self.dag = json.load(f)

    def state(self, blockers):
        return {
            "current_state": "STATE_BLOCKER_RESOLUTION",
            "history": [],
            "variables": {"blockers": blockers},
        }

    def owned(self, blocker_id):
        return {"id": blocker_id, "owner": "p@x.com", "target_close_date": "2099-01-01"}

    def test_an_unowned_blocker_holds_the_gate(self):
        state = self.state([self.owned("B-001"), {"id": "B-002"}])
        self.assertEqual(
            blocker_tools.apply_gate(state, self.dag), "STATE_BLOCKER_RESOLUTION")
        self.assertEqual(state["current_state"], "STATE_BLOCKER_RESOLUTION")

    def test_an_owner_without_a_date_holds_the_gate(self):
        # Half an assignment is not an assignment: an owner with no date is a
        # blocker nobody has committed to closing.
        state = self.state([{"id": "B-001", "owner": "p@x.com"}])
        self.assertEqual(
            blocker_tools.apply_gate(state, self.dag), "STATE_BLOCKER_RESOLUTION")

    def test_owning_the_last_blocker_unlocks_landing_zone(self):
        state = self.state([self.owned("B-001"), self.owned("B-002")])
        self.assertEqual(blocker_tools.apply_gate(state, self.dag), "STATE_LZ_DESIGN")
        self.assertEqual(state["current_state"], "STATE_LZ_DESIGN")

    def test_a_report_with_no_blockers_skips_the_gate_entirely(self):
        transitions = self.dag["states"]["STATE_ASSESSMENT"]["transitions"]
        self.assertEqual(transitions["on_no_blockers"], "STATE_LZ_DESIGN")
        self.assertEqual(transitions["on_blockers_found"], "STATE_BLOCKER_RESOLUTION")


class CloseDateTest(unittest.TestCase):
    """Dates are the half of an assignment the agent is most likely to invent."""

    def test_an_iso_date_in_the_future_is_accepted(self):
        parsed, error = blocker_tools.parse_close_date("2099-03-14")
        self.assertEqual(parsed, "2099-03-14")
        self.assertEqual(error, "")

    def test_a_malformed_date_is_rejected(self):
        for value in ("next Friday", "14/03/2099", "2099-13-01", "", None):
            with self.subTest(value=value):
                parsed, error = blocker_tools.parse_close_date(value)
                self.assertEqual(parsed, "")
                self.assertTrue(error)

    def test_a_past_date_is_rejected(self):
        # Usually a relative date resolved against the wrong day, which produces
        # a blocker that reads as scheduled but is not.
        _, error = blocker_tools.parse_close_date("2020-01-01")
        self.assertIn("in the past", error)


class RegisterLedgerMemberTest(unittest.TestCase):
    """Registering a blocker owner who is not yet in the workspace."""

    REGISTRY = {"roles": {"admins": ["a@x.com"], "platform_engineers": [], "developers": []}}

    def setUp(self):
        self.saved_client = main.state_mgr.gcs_client
        main.state_mgr.gcs_client = MagicMock()

    def tearDown(self):
        main.state_mgr.gcs_client = self.saved_client

    def variables(self, team="platform"):
        return {
            "blockers": [{"id": "B-001", "owner": None, "target_close_date": None}],
            "pending_owner": {
                "blocker_id": "B-001",
                "email": "jane.doe@x.com",
                "target_close_date": "2099-03-14",
            },
            "elicitation_responses": {"STATE_CONFIRM_NEW_MEMBER": {"team": team}},
        }

    def run_action(self, variables):
        blob = main.state_mgr.gcs_client.bucket.return_value.blob.return_value
        blob.download_as_text.return_value = json.dumps(self.REGISTRY)
        return main.action_register_ledger_member(variables, {"ledger_uri": "gs://b"})

    @patch("main.provision_ledger_iam")
    @patch("main.add_registry_member", return_value=True)
    def test_platform_maps_to_platform_engineers(self, add_member, iam):
        variables = self.variables(team="platform")
        key, _ = self.run_action(variables)

        self.assertEqual(key, "on_success")
        add_member.assert_called_once_with("gs://b", "jane.doe@x.com", "platform_engineers")

    @patch("main.provision_ledger_iam")
    @patch("main.add_registry_member", return_value=True)
    def test_application_maps_to_developers(self, add_member, iam):
        self.run_action(self.variables(team="application"))
        add_member.assert_called_once_with("gs://b", "jane.doe@x.com", "developers")

    @patch("main.provision_ledger_iam")
    @patch("main.add_registry_member", return_value=True)
    def test_the_iam_grant_is_re_run(self, add_member, iam):
        # Not an optimisation. The conditional objectViewer on the registry is
        # granted to the union of the role lists, and authorize_and_rehydrate
        # reads that file as the caller — so a member added without this is
        # registered and locked out with a 403.
        self.run_action(self.variables())
        iam.assert_called_once()

    @patch("main.provision_ledger_iam")
    @patch("main.add_registry_member", return_value=True)
    def test_the_parked_assignment_is_applied_and_cleared(self, add_member, iam):
        variables = self.variables()
        self.run_action(variables)

        blocker = variables["blockers"][0]
        self.assertEqual(blocker["owner"], "jane.doe@x.com")
        self.assertEqual(blocker["target_close_date"], "2099-03-14")
        self.assertNotIn("pending_owner", variables)

    @patch("main.provision_ledger_iam")
    @patch("main.add_registry_member", return_value=True)
    def test_a_failed_iam_grant_leaves_the_blocker_unowned(self, add_member, iam):
        iam.side_effect = RuntimeError("caller lacks storage.buckets.setIamPolicy")
        variables = self.variables()

        key, message = self.run_action(variables)

        self.assertEqual(key, "on_failure")
        self.assertIn("cannot read the ledger", message)
        self.assertIsNone(variables["blockers"][0]["owner"])

    @patch("main.provision_ledger_iam")
    @patch("main.add_registry_member")
    def test_no_team_answer_registers_nobody(self, add_member, iam):
        variables = self.variables()
        variables["elicitation_responses"] = {}

        key, _ = self.run_action(variables)

        self.assertEqual(key, "on_failure")
        add_member.assert_not_called()
        iam.assert_not_called()


class AssignBlockerOwnerTest(unittest.IsolatedAsyncioTestCase):
    """The whole assignment path, through the ledger and the elicitation."""

    REGISTRY = """
workspace_name: "test-workspace"
gcp_project: "test-project"
roles:
  platform_engineers:
    - "platform-user@google.com"
"""

    def setUp(self):
        main.state_mgr.write_local_config(
            ledger_uri="gs://test-ledger-bucket",
            resolved_role="platform",
            workspace_name="test-workspace",
            gcp_project="test-project",
        )
        self.container = {
            "state": {
                "current_state": "STATE_BLOCKER_RESOLUTION",
                "history": [],
                "variables": {
                    "blockers": [
                        {"id": "B-001", "title": "Cilium CNI", "category": "c",
                         "affected_workloads": [], "rationale": "r",
                         "resolution_path": "p", "owner": None, "target_close_date": None},
                        {"id": "B-002", "title": "App Mesh", "category": "c",
                         "affected_workloads": [], "rationale": "r",
                         "resolution_path": "p", "owner": None, "target_close_date": None},
                    ],
                },
            }
        }

    def tearDown(self):
        shutil.rmtree(main.state_mgr.LEDGER_CONFIG_DIR, ignore_errors=True)
        if os.path.exists(main.LEDGER_CONFIG_PATH):
            os.remove(main.LEDGER_CONFIG_PATH)
        main.gcs_client = None
        main.state_mgr.gcs_client = None

    def wire_gcs(self, mock_gcs_client_class):
        mock_client = MagicMock()
        mock_gcs_client_class.return_value = mock_client
        bucket = MagicMock()
        mock_client.bucket.return_value = bucket

        registry_blob, state_blob, dag_blob = MagicMock(), MagicMock(), MagicMock()
        dir_path = os.path.dirname(os.path.realpath(__file__))
        with open(os.path.join(dir_path, "platform_dag.json"), "r") as f:
            dag_blob.download_as_text.return_value = f.read()

        registry_blob.download_as_text.return_value = self.REGISTRY
        state_blob.download_as_text.return_value = json.dumps(self.container["state"])
        state_blob.generation = 123

        def save(data_str, **kwargs):
            self.container["state"] = json.loads(data_str)
            state_blob.download_as_text.return_value = data_str
        state_blob.upload_from_string.side_effect = save

        def blob_for(path):
            if path == "platform/onboarding/state.json":
                return state_blob
            if path == "platform_dag.json":
                return dag_blob
            return registry_blob

        bucket.blob.side_effect = blob_for
        return bucket

    def accepting_ctx(self, content):
        ctx = MagicMock()
        ctx.request_id = "test-request-id"
        ctx.request_context.meta = None
        res = MagicMock()
        res.action = "accept"
        res.content = content

        async def send_request(*args, **kwargs):
            return res
        ctx.request_context.session.send_request.side_effect = send_request
        return ctx

    def declining_ctx(self):
        ctx = MagicMock()
        ctx.request_id = "test-request-id"
        ctx.request_context.meta = None
        res = MagicMock()
        res.action = "decline"
        res.content = None

        async def send_request(*args, **kwargs):
            return res
        ctx.request_context.session.send_request.side_effect = send_request
        return ctx

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_registered_owner_is_assigned_directly(self, gcs, email):
        email.return_value = "platform-user@google.com"
        self.wire_gcs(gcs)

        res = await main.assign_blocker_owner(
            "B-001", "platform-user@google.com", "2099-03-14")

        blockers = self.container["state"]["variables"]["blockers"]
        self.assertEqual(blockers[0]["owner"], "platform-user@google.com")
        self.assertEqual(blockers[0]["target_close_date"], "2099-03-14")
        # B-002 is still unowned, so the gate holds.
        self.assertEqual(self.container["state"]["current_state"], "STATE_BLOCKER_RESOLUTION")
        self.assertIn("B-002", res)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_a_past_date_is_refused_before_anything_is_written(self, gcs, email):
        email.return_value = "platform-user@google.com"
        self.wire_gcs(gcs)

        res = await main.assign_blocker_owner(
            "B-001", "platform-user@google.com", "2020-01-01")

        self.assertIn("ERROR", res)
        self.assertIsNone(self.container["state"]["variables"]["blockers"][0]["owner"])

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_an_unknown_blocker_id_is_refused(self, gcs, email):
        email.return_value = "platform-user@google.com"
        self.wire_gcs(gcs)

        res = await main.assign_blocker_owner("B-404", "platform-user@google.com", "2099-03-14")

        self.assertIn("ERROR", res)
        self.assertIn("B-001", res)

    @patch("main.provision_ledger_iam")
    @patch("main.add_registry_member", return_value=True)
    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_an_unregistered_owner_is_registered_then_assigned(
        self, gcs, email, add_member, iam
    ):
        email.return_value = "platform-user@google.com"
        self.wire_gcs(gcs)
        ctx = self.accepting_ctx({"team": "application"})

        await main.assign_blocker_owner("B-001", "jane.doe@x.com", "2099-03-14", ctx=ctx)

        # elicit -> register -> back, with the grant re-run on the way through.
        add_member.assert_called_once_with(
            "gs://test-ledger-bucket", "jane.doe@x.com", "developers")
        iam.assert_called_once()

        state = self.container["state"]
        self.assertEqual(state["variables"]["blockers"][0]["owner"], "jane.doe@x.com")
        self.assertEqual(state["current_state"], "STATE_BLOCKER_RESOLUTION")
        self.assertNotIn("pending_owner", state["variables"])
        self.assertNotIn("pending_owner_email", state["variables"])

    @patch("main.provision_ledger_iam")
    @patch("main.add_registry_member", return_value=True)
    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_declining_the_team_question_registers_nobody(
        self, gcs, email, add_member, iam
    ):
        email.return_value = "platform-user@google.com"
        self.wire_gcs(gcs)

        res = await main.assign_blocker_owner(
            "B-001", "jane.doe@x.com", "2099-03-14", ctx=self.declining_ctx())

        add_member.assert_not_called()
        iam.assert_not_called()
        state = self.container["state"]
        self.assertIsNone(state["variables"]["blockers"][0]["owner"])
        self.assertEqual(state["current_state"], "STATE_BLOCKER_RESOLUTION")
        self.assertIn("declined", res)

    @patch("main.provision_ledger_iam")
    @patch("main.add_registry_member", return_value=True)
    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_owning_the_last_blocker_unlocks_landing_zone(
        self, gcs, email, add_member, iam
    ):
        email.return_value = "platform-user@google.com"
        self.container["state"]["variables"]["blockers"][1].update(
            {"owner": "platform-user@google.com", "target_close_date": "2099-01-01"})
        self.wire_gcs(gcs)

        await main.assign_blocker_owner("B-001", "platform-user@google.com", "2099-03-14")

        self.assertEqual(self.container["state"]["current_state"], "STATE_LZ_DESIGN")

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    async def test_the_tool_refuses_to_run_outside_its_state(self, gcs, email):
        email.return_value = "platform-user@google.com"
        self.container["state"]["current_state"] = "STATE_LZ_DESIGN"
        self.wire_gcs(gcs)

        res = await main.assign_blocker_owner("B-001", "platform-user@google.com", "2099-03-14")
        self.assertIn("ERROR: Invalid state", res)


class VerifyRepositoriesIdentityTest(unittest.TestCase):
    """The write-permission probe commits as the session's user, like the
    PR commits, and refuses to probe without an identity."""

    VARIABLES = {
        "source_repo_url": "sso://src", "source_branch": "main", "source_path": "infra",
        "target_repo_url": "sso://dst", "target_branch": "main", "target_path": "gke",
    }

    def _run(self, config):
        variables = dict(self.VARIABLES)
        with patch.object(main.git_client, "verify_git_path_exists", return_value=True), \
             patch.object(main.git_client, "verify_git_write_permission",
                          return_value=True) as probe:
            key, msg = main.action_verify_repositories(variables, config)
        return key, msg, variables, probe

    def test_write_check_probes_as_the_session_user(self):
        key, _, variables, probe = self._run({"user_email": "pe@example.com"})
        self.assertEqual(key, "on_success")
        self.assertTrue(variables["target_write_permission_verified"])
        probe.assert_called_once_with("sso://dst", "main", user_email="pe@example.com")

    def test_write_check_without_a_session_identity_fails_before_probing(self):
        key, msg, _, probe = self._run({})
        self.assertEqual(key, "on_failure")
        self.assertIn("caller identity", msg)
        probe.assert_not_called()


class SubmitLzPrActionTest(unittest.TestCase):
    """action_submit_lz_pr: commit, push, and the PR (or its fallback link)."""

    VARIABLES = {
        "target_repo_url": "sso://target-repo",
        "target_branch": "main",
        "lz_branch_name": "migration/gke-landing-zone-u-1",
    }

    # What _resolve_session puts on the session config: the caller's
    # resolved identity, which the commits are authored as.
    CONFIG = {"user_email": "platform-user@google.com"}

    def _run(self, clone_dir, git_client, ssm_client, config=None, **overrides):
        variables = {**self.VARIABLES, "target_clone_path": clone_dir, **overrides}
        with patch.object(lz_actions, "git_client", git_client), \
             patch.object(lz_actions, "ssm_client", ssm_client):
            key, msg = lz_actions.action_submit_lz_pr(
                variables, self.CONFIG if config is None else config)
        return key, msg, variables

    def test_missing_clone_fails_without_touching_git(self):
        git, ssm = MagicMock(), MagicMock()
        key, msg, _ = self._run("/no/such/dir", git, ssm)
        self.assertEqual(key, "on_failure")
        self.assertIn("does not exist", msg)
        git.stage_and_commit.assert_not_called()

    def test_missing_identity_fails_before_touching_git(self):
        git, ssm = MagicMock(), MagicMock()
        with tempfile.TemporaryDirectory() as clone:
            key, msg, _ = self._run(clone, git, ssm, config={})
        self.assertEqual(key, "on_failure")
        self.assertIn("caller identity", msg)
        git.stage_and_commit.assert_not_called()
        git.rebase_and_push.assert_not_called()

    def test_commit_failure_is_reported(self):
        git, ssm = MagicMock(), MagicMock()
        git.stage_and_commit.side_effect = RuntimeError("index locked")
        with tempfile.TemporaryDirectory() as clone:
            key, msg, _ = self._run(clone, git, ssm)
        self.assertEqual(key, "on_failure")
        self.assertIn("Git commit failed", msg)
        git.rebase_and_push.assert_not_called()

    def test_ssm_target_gets_a_real_pull_request(self):
        git, ssm = MagicMock(), MagicMock()
        ssm.SSMClient.return_value.check_repository_exists.return_value = True
        ssm.SSMClient.return_value.create_pull_request.return_value = "https://ssm/pr/99"
        with tempfile.TemporaryDirectory() as clone:
            key, msg, variables = self._run(clone, git, ssm)
        self.assertEqual(key, "on_success")
        self.assertEqual(variables["pull_request_url"], "https://ssm/pr/99")
        git.stage_and_commit.assert_called_once()
        git.rebase_and_push.assert_called_once()
        # Both git steps run as the session's user, not as the product.
        self.assertEqual(git.stage_and_commit.call_args.kwargs["user_email"],
                         "platform-user@google.com")
        self.assertEqual(git.rebase_and_push.call_args.kwargs["user_email"],
                         "platform-user@google.com")

    def test_github_target_gets_a_compare_link(self):
        git, ssm = MagicMock(), MagicMock()
        ssm.SSMClient.return_value.check_repository_exists.return_value = False
        with tempfile.TemporaryDirectory() as clone:
            key, msg, variables = self._run(
                clone, git, ssm, target_repo_url="git@github.com:acme/infra.git")
        self.assertEqual(key, "on_success")
        self.assertEqual(
            variables["pull_request_url"],
            "https://github.com/acme/infra/compare/main...migration/gke-landing-zone-u-1?expand=1")

    def test_other_hosts_report_the_pushed_branch(self):
        git, ssm = MagicMock(), MagicMock()
        ssm.SSMClient.return_value.check_repository_exists.return_value = False
        with tempfile.TemporaryDirectory() as clone:
            key, msg, variables = self._run(
                clone, git, ssm, target_repo_url="https://gitlab.example.com/acme/infra")
        self.assertEqual(key, "on_success")
        self.assertIn("Branch pushed", msg)
        self.assertEqual(variables["pull_request_url"], "https://gitlab.example.com/acme/infra")


class TranslationShipTest(unittest.IsolatedAsyncioTestCase):
    """run_generated_validation's tail: ship approval, the PR, and where the
    graph parks. The end state is read from the graph rather than named, so
    states appended after the PR submission do not break these tests."""

    REGISTRY = """
workspace_name: "test-workspace"
gcp_project: "test-project"
roles:
  platform_engineers:
    - "platform-user@google.com"
"""

    UNIT_ENTRY = {
        "unit": {"unit_id": "u-1", "kind": "workload", "title": "payments"},
        "result": {"files": [{"path": "main.tf", "content": "# generated\n"}],
                   "tradeoffs": "t", "assumptions": [], "open_questions": []},
    }

    CLEAN_REPORT = {"all_valid": True, "clean": [".", "translation-units/u-1"],
                    "fixed": [], "remaining": []}

    def setUp(self):
        main.state_mgr.write_local_config(
            ledger_uri="gs://test-ledger-bucket",
            resolved_role="platform",
            workspace_name="test-workspace",
            gcp_project="test-project",
        )
        dir_path = os.path.dirname(os.path.realpath(__file__))
        with open(os.path.join(dir_path, "platform_dag.json"), "r") as f:
            self.dag = json.load(f)
        self.shipped_state = (
            self.dag["states"]["STATE_TRANSLATION_SUBMIT_PR"]["transitions"]["on_success"])

    def tearDown(self):
        shutil.rmtree(main.state_mgr.LEDGER_CONFIG_DIR, ignore_errors=True)
        if os.path.exists(main.LEDGER_CONFIG_PATH):
            os.remove(main.LEDGER_CONFIG_PATH)
        main.gcs_client = None
        main.state_mgr.gcs_client = None

    def wire(self, mock_gcs_client_class, clone_dir, unit_entry=None):
        self.container = {"state": {
            "current_state": "STATE_TRANSLATION_VALIDATE",
            "history": [],
            "variables": {
                "target_repo_url": "sso://target-repo",
                "target_branch": "main",
                "lz_branch_name": "migration/gke-landing-zone-u-1",
                "target_clone_path": clone_dir,
                "translation_plan": {"units": [{"unit_id": "u-1", "status": "done"}]},
            },
        }}
        mock_client = MagicMock()
        mock_gcs_client_class.return_value = mock_client
        bucket = MagicMock()
        mock_client.bucket.return_value = bucket

        registry_blob, state_blob, dag_blob, unit_blob = (
            MagicMock(), MagicMock(), MagicMock(), MagicMock())
        dir_path = os.path.dirname(os.path.realpath(__file__))
        with open(os.path.join(dir_path, "platform_dag.json"), "r") as f:
            dag_blob.download_as_text.return_value = f.read()
        registry_blob.download_as_text.return_value = self.REGISTRY
        state_blob.download_as_text.return_value = json.dumps(self.container["state"])
        state_blob.generation = 123
        unit_blob.download_as_text.return_value = json.dumps(unit_entry or self.UNIT_ENTRY)
        self.unit_blob = unit_blob

        def save(data_str, **kwargs):
            self.container["state"] = json.loads(data_str)
            state_blob.download_as_text.return_value = data_str
        state_blob.upload_from_string.side_effect = save

        exports_blob = MagicMock()
        exports_blob.reload.side_effect = exceptions.NotFound("Not Found")
        self.exports_captured = {}

        def capture_exports(data_str, **kwargs):
            self.exports_captured["doc"] = json.loads(data_str)
        exports_blob.upload_from_string.side_effect = capture_exports

        def blob_for(path):
            if path == "platform/onboarding/state.json":
                return state_blob
            if path == "platform_dag.json":
                return dag_blob
            if path == f"{UNIT_BLOB_PREFIX}/u-1.json":
                return unit_blob
            if path == exports_lib.EXPORTS_BLOB:
                return exports_blob
            return registry_blob

        bucket.blob.side_effect = blob_for
        main.state_mgr.gcs_client = mock_client

    def ctx_answering(self, approved):
        ctx = MagicMock()
        ctx.request_id = "test-request-id"
        ctx.request_context.meta = None
        res = MagicMock()
        res.action = "accept"
        res.content = {"approved": approved}

        async def send_request(*args, **kwargs):
            return res
        ctx.request_context.session.send_request.side_effect = send_request
        return ctx

    def ssm_with_pr(self, ssm, url="https://ssm/pr/99"):
        ssm.SSMClient.return_value.check_repository_exists.return_value = True
        ssm.SSMClient.return_value.create_pull_request.return_value = url

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    @patch("servers.phases.translation.translation_validate_3.tools.shutil.which")
    @patch("servers.phases.translation.translation_validate_3.tools.validation.run_validation",
           new_callable=AsyncMock)
    @patch("servers.phases.landingzone.actions.git_client")
    @patch("servers.phases.landingzone.actions.ssm_client")
    async def test_approved_ship_opens_the_pr_and_parks_past_it(
            self, mock_ssm, mock_git, mock_validation, mock_which,
            mock_gcs_client_class, mock_get_email):
        mock_get_email.return_value = "platform-user@google.com"
        mock_which.return_value = "/usr/bin/terraform"
        mock_validation.return_value = dict(self.CLEAN_REPORT)
        self.ssm_with_pr(mock_ssm)

        with tempfile.TemporaryDirectory() as clone:
            self.wire(mock_gcs_client_class, clone)
            res = await main.run_generated_validation(ctx=self.ctx_answering(True))

        self.assertIn("pull request was opened", res)
        self.assertEqual(self.container["state"]["current_state"], self.shipped_state)
        self.assertEqual(
            self.container["state"]["variables"]["pull_request_url"], "https://ssm/pr/99")
        mock_git.stage_and_commit.assert_called_once()
        mock_git.rebase_and_push.assert_called_once()

    @staticmethod
    def _elicitation_message(ctx):
        request = ctx.request_context.session.send_request.call_args[0][0]
        params = getattr(request, "root", request).params
        return params.message

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    @patch("servers.phases.translation.translation_validate_3.tools.shutil.which")
    @patch("servers.phases.translation.translation_validate_3.tools.validation.run_validation",
           new_callable=AsyncMock)
    @patch("servers.phases.landingzone.actions.git_client")
    @patch("servers.phases.landingzone.actions.ssm_client")
    async def test_an_unverified_cluster_dns_value_rides_the_ship_elicitation(
            self, mock_ssm, mock_git, mock_validation, mock_which,
            mock_gcs_client_class, mock_get_email):
        # A cluster whose cluster_dns comes through a variable passes the
        # gate with a note; the note must be in front of the user BEFORE the
        # approve/decline answer, not in the reply that follows the PR.
        mock_get_email.return_value = "platform-user@google.com"
        mock_which.return_value = "/usr/bin/terraform"
        mock_validation.return_value = dict(self.CLEAN_REPORT)
        self.ssm_with_pr(mock_ssm)

        with tempfile.TemporaryDirectory() as clone:
            with open(os.path.join(clone, "main.tf"), "w") as f:
                f.write('resource "google_container_cluster" "this" {\n'
                        '  dns_config { cluster_dns = var.cluster_dns }\n}\n')
            self.wire(mock_gcs_client_class, clone)
            ctx = self.ctx_answering(True)
            res = await main.run_generated_validation(ctx=ctx)

        self.assertIn("pull request was opened", res)
        self.assertIn("1 cluster_dns value(s) behind an expression", res)
        message = self._elicitation_message(ctx)
        self.assertIn("Before you approve", message)
        self.assertIn("google_container_cluster.this sets cluster_dns = var.cluster_dns", message)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    @patch("servers.phases.translation.translation_validate_3.tools.shutil.which")
    @patch("servers.phases.translation.translation_validate_3.tools.validation.run_validation",
           new_callable=AsyncMock)
    @patch("servers.phases.landingzone.actions.git_client")
    @patch("servers.phases.landingzone.actions.ssm_client")
    async def test_a_literal_cluster_dns_raises_no_ship_notice(
            self, mock_ssm, mock_git, mock_validation, mock_which,
            mock_gcs_client_class, mock_get_email):
        # Positive control: the notice is absent when there is nothing to say.
        mock_get_email.return_value = "platform-user@google.com"
        mock_which.return_value = "/usr/bin/terraform"
        mock_validation.return_value = dict(self.CLEAN_REPORT)
        self.ssm_with_pr(mock_ssm)

        with tempfile.TemporaryDirectory() as clone:
            with open(os.path.join(clone, "main.tf"), "w") as f:
                f.write('resource "google_container_cluster" "this" {\n'
                        '  dns_config { cluster_dns = "CLOUD_DNS" }\n}\n')
            self.wire(mock_gcs_client_class, clone)
            ctx = self.ctx_answering(True)
            res = await main.run_generated_validation(ctx=ctx)

        self.assertIn("pull request was opened", res)
        self.assertNotIn("Before you approve", self._elicitation_message(ctx))

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    @patch("servers.phases.translation.translation_validate_3.tools.shutil.which")
    @patch("servers.phases.translation.translation_validate_3.tools.validation.run_validation",
           new_callable=AsyncMock)
    @patch("servers.phases.landingzone.actions.git_client")
    @patch("servers.phases.landingzone.actions.ssm_client")
    async def test_a_cluster_without_dns_config_returns_to_review(
            self, mock_ssm, mock_git, mock_validation, mock_which,
            mock_gcs_client_class, mock_get_email):
        mock_get_email.return_value = "platform-user@google.com"
        mock_which.return_value = "/usr/bin/terraform"
        mock_validation.return_value = dict(self.CLEAN_REPORT)

        with tempfile.TemporaryDirectory() as clone:
            with open(os.path.join(clone, "main.tf"), "w") as f:
                f.write('resource "google_container_cluster" "this" {\n  name = "x"\n}\n')
            self.wire(mock_gcs_client_class, clone)
            res = await main.run_generated_validation(ctx=self.ctx_answering(True))

        self.assertIn("FAILED", res)
        self.assertIn("add dns_config { cluster_dns = \"CLOUD_DNS\" }", res)
        self.assertEqual(self.container["state"]["current_state"], "STATE_TRANSLATION_REVIEW")
        mock_ssm.SSMClient.return_value.create_pull_request.assert_not_called()

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    @patch("servers.phases.translation.translation_validate_3.tools.shutil.which")
    @patch("servers.phases.translation.translation_validate_3.tools.validation.run_validation",
           new_callable=AsyncMock)
    @patch("servers.phases.landingzone.actions.git_client")
    @patch("servers.phases.landingzone.actions.ssm_client")
    async def test_declined_ship_returns_to_the_unit_review(
            self, mock_ssm, mock_git, mock_validation, mock_which,
            mock_gcs_client_class, mock_get_email):
        mock_get_email.return_value = "platform-user@google.com"
        mock_which.return_value = "/usr/bin/terraform"
        mock_validation.return_value = dict(self.CLEAN_REPORT)

        with tempfile.TemporaryDirectory() as clone:
            self.wire(mock_gcs_client_class, clone)
            res = await main.run_generated_validation(ctx=self.ctx_answering(False))

        self.assertEqual(self.container["state"]["current_state"], "STATE_TRANSLATION_REVIEW")
        self.assertIn("Review UI", res)
        mock_git.stage_and_commit.assert_not_called()

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    @patch("servers.phases.translation.translation_validate_3.tools.shutil.which")
    @patch("servers.phases.translation.translation_validate_3.tools.validation.run_validation",
           new_callable=AsyncMock)
    @patch("servers.phases.landingzone.actions.git_client")
    @patch("servers.phases.landingzone.actions.ssm_client")
    async def test_failed_pr_submission_returns_here_to_retry(
            self, mock_ssm, mock_git, mock_validation, mock_which,
            mock_gcs_client_class, mock_get_email):
        mock_get_email.return_value = "platform-user@google.com"
        mock_which.return_value = "/usr/bin/terraform"
        mock_validation.return_value = dict(self.CLEAN_REPORT)
        self.ssm_with_pr(mock_ssm)
        mock_git.rebase_and_push.side_effect = RuntimeError("remote rejected")

        with tempfile.TemporaryDirectory() as clone:
            self.wire(mock_gcs_client_class, clone)
            res = await main.run_generated_validation(ctx=self.ctx_answering(True))

        self.assertEqual(self.container["state"]["current_state"], "STATE_TRANSLATION_VALIDATE")
        self.assertIn("did not complete", res)
        self.assertIn("re-run run_generated_validation", res)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    @patch("servers.phases.translation.translation_validate_3.tools.shutil.which")
    @patch("servers.phases.translation.translation_validate_3.tools.validation.run_validation",
           new_callable=AsyncMock)
    @patch("servers.phases.landingzone.actions.git_client")
    @patch("servers.phases.landingzone.actions.ssm_client")
    async def test_invalid_manifest_returns_to_review_without_shipping(
            self, mock_ssm, mock_git, mock_validation, mock_which,
            mock_gcs_client_class, mock_get_email):
        mock_get_email.return_value = "platform-user@google.com"
        mock_which.return_value = "/usr/bin/terraform"
        mock_validation.return_value = dict(self.CLEAN_REPORT)
        unit_entry = {
            "unit": {"unit_id": "u-1", "kind": "storage", "title": "storage classes"},
            "result": {"files": [
                {"path": "main.tf", "content": "# generated\n"},
                {"path": "storageclass.yaml", "content": "kind: StorageClass\n"},
            ], "tradeoffs": "t", "assumptions": [], "open_questions": []},
        }

        with tempfile.TemporaryDirectory() as clone:
            self.wire(mock_gcs_client_class, clone, unit_entry=unit_entry)
            res = await main.run_generated_validation(ctx=self.ctx_answering(True))

        self.assertIn("Validation FAILED", res)
        self.assertIn("storageclass.yaml", res)
        self.assertIn("missing 'apiVersion'", res)
        self.assertEqual(self.container["state"]["current_state"], "STATE_TRANSLATION_REVIEW")
        mock_git.stage_and_commit.assert_not_called()

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    @patch("servers.phases.translation.translation_validate_3.tools.shutil.which")
    @patch("servers.phases.translation.translation_validate_3.tools.validation.run_validation",
           new_callable=AsyncMock)
    @patch("servers.phases.landingzone.actions.git_client")
    @patch("servers.phases.landingzone.actions.ssm_client")
    async def test_valid_manifests_are_counted_and_ship(
            self, mock_ssm, mock_git, mock_validation, mock_which,
            mock_gcs_client_class, mock_get_email):
        mock_get_email.return_value = "platform-user@google.com"
        mock_which.return_value = "/usr/bin/terraform"
        mock_validation.return_value = dict(self.CLEAN_REPORT)
        self.ssm_with_pr(mock_ssm)
        unit_entry = {
            "unit": {"unit_id": "u-1", "kind": "storage", "title": "storage classes"},
            "result": {"files": [
                {"path": "main.tf", "content": "# generated\n"},
                {"path": "storageclass.yaml", "content":
                    "apiVersion: storage.k8s.io/v1\nkind: StorageClass\n"
                    "metadata:\n  name: fast-ssd\n"},
            ], "tradeoffs": "t", "assumptions": [], "open_questions": []},
        }

        with tempfile.TemporaryDirectory() as clone:
            self.wire(mock_gcs_client_class, clone, unit_entry=unit_entry)
            res = await main.run_generated_validation(ctx=self.ctx_answering(True))

        self.assertIn("1/1 manifests structurally valid", res)
        self.assertIn("pull request was opened", res)
        self.assertEqual(self.container["state"]["current_state"], self.shipped_state)

    # A post-straddle workload-identity unit: the Google half (service account
    # + workloadIdentityUser binding) and the handover output, with no
    # Kubernetes ServiceAccount of any kind — that half is the workload
    # pipeline's wkld-identity unit. gsa_bindings must still derive from
    # exactly this shape, with the parser reading past the resource blocks.
    WI_OUTPUT_TF = (
        'resource "google_service_account" "orders" {\n'
        '  account_id = "orders"\n'
        '  project    = "test-project"\n}\n\n'
        'resource "google_service_account_iam_member" "orders_wi" {\n'
        "  service_account_id = google_service_account.orders.name\n"
        '  role               = "roles/iam.workloadIdentityUser"\n'
        "  member             = "
        '"serviceAccount:test-project.svc.id.goog[acme-shop/orders]"\n}\n\n'
        'output "ksa_annotations" {\n  value = {\n'
        '    "acme-shop/orders" = "orders@test-project.iam.gserviceaccount.com"\n'
        "  }\n}\n"
    )

    def wi_unit_entry(self, tf_content):
        return {
            "unit": {"unit_id": "u-1", "kind": "workload-identity", "title": "IRSA to WI"},
            "result": {"files": [{"path": "main.tf", "content": tf_content}],
                       "tradeoffs": "t", "assumptions": [], "open_questions": []},
        }

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    @patch("servers.phases.translation.translation_validate_3.tools.shutil.which")
    @patch("servers.phases.translation.translation_validate_3.tools.validation.run_validation",
           new_callable=AsyncMock)
    @patch("servers.phases.landingzone.actions.git_client")
    @patch("servers.phases.landingzone.actions.ssm_client")
    async def test_missing_ksa_contract_returns_to_review_without_shipping(
            self, mock_ssm, mock_git, mock_validation, mock_which,
            mock_gcs_client_class, mock_get_email):
        # The workload-identity unit must ship the ksa_annotations output the
        # exports derivation parses; a unit without it is a finding, not a pass.
        mock_get_email.return_value = "platform-user@google.com"
        mock_which.return_value = "/usr/bin/terraform"
        mock_validation.return_value = dict(self.CLEAN_REPORT)

        with tempfile.TemporaryDirectory() as clone:
            self.wire(mock_gcs_client_class, clone,
                      unit_entry=self.wi_unit_entry("# generated, no outputs\n"))
            res = await main.run_generated_validation(ctx=self.ctx_answering(True))

        self.assertIn("Validation FAILED", res)
        self.assertIn("ksa_annotations", res)
        self.assertEqual(self.container["state"]["current_state"], "STATE_TRANSLATION_REVIEW")
        mock_git.stage_and_commit.assert_not_called()

    def cluster_dns_unit_entry(self, configmap_yaml):
        # A stub domain to on-prem resolvers: the customization that does
        # carry over verbatim (a root `forward . 10.0.0.2` would be the AWS
        # VPC resolver, which the knowledge document maps to nothing).
        corefile = "corp.example.com:53 {\n    forward . 10.1.2.3\n}\n"
        return {
            # u-1, because the mocked terraform report (CLEAN_REPORT) names
            # that directory clean; the kind is what the contract keys on.
            "unit": {"unit_id": "u-1", "kind": "cluster-dns",
                     "title": "Cluster DNS: CoreDNS customizations on Cloud DNS",
                     "inputs": {"decision": "GKE_STANDARD_NAP",
                                "cluster_dns": {"sources": [
                                    {"kind": "configmap", "name": "coredns",
                                     "text": corefile}]}}},
            "result": {"files": [{"path": "kube-dns.yaml", "content": configmap_yaml}],
                       "tradeoffs": "t", "assumptions": [], "open_questions": []},
        }

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    @patch("servers.phases.translation.translation_validate_3.tools.shutil.which")
    @patch("servers.phases.translation.translation_validate_3.tools.validation.run_validation",
           new_callable=AsyncMock)
    @patch("servers.phases.landingzone.actions.git_client")
    @patch("servers.phases.landingzone.actions.ssm_client")
    async def test_an_invented_resolver_in_the_cluster_dns_unit_returns_to_review(
            self, mock_ssm, mock_git, mock_validation, mock_which,
            mock_gcs_client_class, mock_get_email):
        # The cluster-dns worker translated a Corefile it was handed verbatim;
        # an upstream nameserver that is in its ConfigMap but not in that
        # text is an invention, and the gate names it. The mirror image — a
        # faithful ConfigMap — ships. Same fixture, one address changed.
        mock_get_email.return_value = "platform-user@google.com"
        mock_which.return_value = "/usr/bin/terraform"
        mock_validation.return_value = dict(self.CLEAN_REPORT)
        configmap = ("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: kube-dns\n"
                     "  namespace: kube-system\ndata:\n"
                     '  stubDomains: \'{"corp.example.com": ["%s"]}\'\n')

        with tempfile.TemporaryDirectory() as clone:
            self.wire(mock_gcs_client_class, clone,
                      unit_entry=self.cluster_dns_unit_entry(configmap % "8.8.8.8"))
            res = await main.run_generated_validation(ctx=self.ctx_answering(True))
        self.assertIn("Validation FAILED", res)
        self.assertIn("8.8.8.8", res)
        self.assertIn("does not come from the source", res)
        self.assertEqual(self.container["state"]["current_state"], "STATE_TRANSLATION_REVIEW")
        mock_git.stage_and_commit.assert_not_called()

        # A fresh report object: the step marks the one it is handed.
        mock_validation.return_value = dict(self.CLEAN_REPORT)
        self.ssm_with_pr(mock_ssm)
        with tempfile.TemporaryDirectory() as clone:
            self.wire(mock_gcs_client_class, clone,
                      unit_entry=self.cluster_dns_unit_entry(configmap % "10.1.2.3"))
            res = await main.run_generated_validation(ctx=self.ctx_answering(True))
        self.assertIn("pull request was opened", res)
        self.assertEqual(self.container["state"]["current_state"], self.shipped_state)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    @patch("servers.phases.translation.translation_validate_3.tools.shutil.which")
    @patch("servers.phases.translation.translation_validate_3.tools.validation.run_validation",
           new_callable=AsyncMock)
    @patch("servers.phases.landingzone.actions.git_client")
    @patch("servers.phases.landingzone.actions.ssm_client")
    async def test_an_empty_ksa_map_over_recorded_bindings_returns_to_review(
            self, mock_ssm, mock_git, mock_validation, mock_which,
            mock_gcs_client_class, mock_get_email):
        # The undecided-project escape is loud: an empty map while the unit's
        # inputs record IRSA bindings is a finding, never a silent pass.
        mock_get_email.return_value = "platform-user@google.com"
        mock_which.return_value = "/usr/bin/terraform"
        mock_validation.return_value = dict(self.CLEAN_REPORT)

        entry = self.wi_unit_entry('output "ksa_annotations" {\n  value = {}\n}\n')
        entry["unit"]["inputs"] = {"irsa_bindings": ["acme-shop/orders"]}
        with tempfile.TemporaryDirectory() as clone:
            self.wire(mock_gcs_client_class, clone, unit_entry=entry)
            res = await main.run_generated_validation(ctx=self.ctx_answering(True))

        self.assertIn("Validation FAILED", res)
        self.assertIn("empty map", res)
        self.assertEqual(self.container["state"]["current_state"], "STATE_TRANSLATION_REVIEW")
        mock_git.stage_and_commit.assert_not_called()

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    @patch("servers.phases.translation.translation_validate_3.tools.shutil.which")
    @patch("servers.phases.translation.translation_validate_3.tools.validation.run_validation",
           new_callable=AsyncMock)
    @patch("servers.phases.landingzone.actions.git_client")
    @patch("servers.phases.landingzone.actions.ssm_client")
    async def test_a_conforming_ksa_contract_ships(
            self, mock_ssm, mock_git, mock_validation, mock_which,
            mock_gcs_client_class, mock_get_email):
        mock_get_email.return_value = "platform-user@google.com"
        mock_which.return_value = "/usr/bin/terraform"
        mock_validation.return_value = dict(self.CLEAN_REPORT)
        self.ssm_with_pr(mock_ssm)

        with tempfile.TemporaryDirectory() as clone:
            self.wire(mock_gcs_client_class, clone,
                      unit_entry=self.wi_unit_entry(self.WI_OUTPUT_TF))
            res = await main.run_generated_validation(ctx=self.ctx_answering(True))

        self.assertIn("pull request was opened", res)
        self.assertEqual(self.container["state"]["current_state"], self.shipped_state)
        # The translation-completion hook published the exports slice: the
        # contract's bindings land verbatim, and the untouched sources stay null.
        doc = self.exports_captured["doc"]
        self.assertEqual(doc["gsa_bindings"],
                         {"acme-shop/orders": "orders@test-project.iam.gserviceaccount.com"})
        self.assertIsNone(doc["storage_class_menu"],
                          "no done storage unit in this fixture; menu must stay null")
        self.assertIsNone(doc["gateway"])
        self.assertEqual(doc["generations"],
                         {"discovery": 0, "translation": 1, "deployment": 0,
                          "data": 0})
        self.assertIn("exports.json refreshed (translation fields)", res)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    @patch("servers.phases.translation.translation_validate_3.tools.shutil.which")
    @patch("servers.phases.translation.translation_validate_3.tools.validation.run_validation",
           new_callable=AsyncMock)
    @patch("servers.phases.landingzone.actions.git_client")
    @patch("servers.phases.landingzone.actions.ssm_client")
    async def test_translation_hook_publishes_the_storage_class_menu(
            self, mock_ssm, mock_git, mock_validation, mock_which,
            mock_gcs_client_class, mock_get_email):
        mock_get_email.return_value = "platform-user@google.com"
        mock_which.return_value = "/usr/bin/terraform"
        mock_validation.return_value = dict(self.CLEAN_REPORT)
        self.ssm_with_pr(mock_ssm)
        unit_entry = {
            "unit": {"unit_id": "u-1", "kind": "storage", "title": "storage classes"},
            "result": {"files": [
                {"path": "classes.yaml", "content":
                    "apiVersion: storage.k8s.io/v1\nkind: StorageClass\n"
                    "metadata:\n  name: gp3-encrypted\n"},
            ], "tradeoffs": "t", "assumptions": [], "open_questions": []},
        }

        with tempfile.TemporaryDirectory() as clone:
            self.wire(mock_gcs_client_class, clone, unit_entry=unit_entry)
            res = await main.run_generated_validation(ctx=self.ctx_answering(True))

        doc = self.exports_captured["doc"]
        self.assertEqual(doc["storage_class_menu"], ["gp3-encrypted"])
        self.assertIsNone(doc["gsa_bindings"])
        self.assertTrue(any("no done workload-identity unit" in n
                            for n in doc["derivation_notes"]))

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    @patch("servers.phases.translation.translation_validate_3.tools.shutil.which")
    @patch("servers.phases.translation.translation_validate_3.tools.validation.run_validation",
           new_callable=AsyncMock)
    @patch("servers.phases.landingzone.actions.git_client")
    @patch("servers.phases.landingzone.actions.ssm_client")
    async def test_autofix_foldback_keeps_the_units_manifests(
            self, mock_ssm, mock_git, mock_validation, mock_which,
            mock_gcs_client_class, mock_get_email):
        # The fix loop only rewrites .tf files; folding its output back into
        # the unit blob must MERGE over the existing file list. Replacing the
        # list dropped a mixed unit's manifests from the ledger, and the next
        # materialize would then drop them from the clone and the PR.
        mock_get_email.return_value = "platform-user@google.com"
        mock_which.return_value = "/usr/bin/terraform"
        mock_validation.return_value = {
            "all_valid": True, "clean": ["."], "remaining": [],
            "fixed": [{"dir": "translation-units/u-1", "attempts": 1,
                       "original_error": "boom"}]}
        self.ssm_with_pr(mock_ssm)
        unit_entry = {
            "unit": {"unit_id": "u-1", "kind": "storage", "title": "storage classes"},
            "result": {"files": [
                {"path": "main.tf", "content": "# generated\n"},
                {"path": "ns.yaml", "content":
                    "apiVersion: v1\nkind: Namespace\nmetadata:\n  name: payments\n"},
            ], "tradeoffs": "t", "assumptions": [], "open_questions": []},
        }

        with tempfile.TemporaryDirectory() as clone:
            self.wire(mock_gcs_client_class, clone, unit_entry=unit_entry)
            res = await main.run_generated_validation(ctx=self.ctx_answering(True))

        self.assertIn("pull request was opened", res)
        saved = json.loads(self.unit_blob.upload_from_string.call_args[0][0])
        self.assertEqual([f["path"] for f in saved["result"]["files"]],
                         ["main.tf", "ns.yaml"])
        self.assertTrue(saved["unit"]["autofixed"])

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    @patch("servers.phases.translation.translation_validate_3.tools.shutil.which")
    @patch("servers.phases.translation.translation_validate_3.tools.validation.run_validation",
           new_callable=AsyncMock)
    @patch("servers.phases.landingzone.actions.git_client")
    @patch("servers.phases.landingzone.actions.ssm_client")
    async def test_units_are_wired_into_a_root_module_before_validation_runs(
            self, mock_ssm, mock_git, mock_validation, mock_which,
            mock_gcs_client_class, mock_get_email):
        # Terraform compiles a module no root module references, so the wiring
        # must be on disk BEFORE the root pass — otherwise that pass reports
        # success over code it never read (DESIGN §14 issue 14).
        mock_get_email.return_value = "platform-user@google.com"
        mock_which.return_value = "/usr/bin/terraform"
        self.ssm_with_pr(mock_ssm)
        seen = {}

        def validate(clone_dir, tf_dirs, terraform_bin, *args, **kwargs):
            with open(os.path.join(clone_dir, "translation-units.tf"),
                      "r", encoding="utf-8") as f:
                seen["wiring"] = f.read()
            seen["dirs"] = list(tf_dirs)
            return dict(self.CLEAN_REPORT)
        mock_validation.side_effect = validate

        with tempfile.TemporaryDirectory() as clone:
            self.wire(mock_gcs_client_class, clone)
            res = await main.run_generated_validation(ctx=self.ctx_answering(True))

        self.assertIn('module "unit-u-1" {', seen["wiring"])
        self.assertIn('source = "./translation-units/u-1"', seen["wiring"])
        # Unit directories first, the root last: a unit-level error must be
        # repaired by the unit's own pass before the root compiles the units.
        self.assertEqual(seen["dirs"], ["translation-units/u-1", "."])
        self.assertIn("1/1 unit directories wired into the generated "
                      "translation-units.tf", res)
        self.assertIn("pull request was opened", res)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    @patch("servers.phases.translation.translation_validate_3.tools.shutil.which")
    @patch("servers.phases.translation.translation_validate_3.tools.validation.run_validation",
           new_callable=AsyncMock)
    @patch("servers.phases.landingzone.actions.git_client")
    @patch("servers.phases.landingzone.actions.ssm_client")
    async def test_a_yaml_only_unit_leaves_no_root_wiring_file(
            self, mock_ssm, mock_git, mock_validation, mock_which,
            mock_gcs_client_class, mock_get_email):
        # The manifest gate covers YAML-only units; terraform refuses a module
        # directory with no configuration files, so wiring one would break the
        # root pass rather than widen it.
        mock_get_email.return_value = "platform-user@google.com"
        mock_which.return_value = "/usr/bin/terraform"
        mock_validation.return_value = dict(self.CLEAN_REPORT)
        self.ssm_with_pr(mock_ssm)
        unit_entry = {
            "unit": {"unit_id": "u-1", "kind": "tenancy", "title": "namespaces"},
            "result": {"files": [{"path": "namespace.yaml", "content":
                                  "apiVersion: v1\nkind: Namespace\n"
                                  "metadata:\n  name: acme-shop\n"}],
                       "tradeoffs": "t", "assumptions": [], "open_questions": []},
        }

        with tempfile.TemporaryDirectory() as clone:
            self.wire(mock_gcs_client_class, clone, unit_entry=unit_entry)
            res = await main.run_generated_validation(ctx=self.ctx_answering(True))
            wiring_exists = os.path.exists(os.path.join(clone, "translation-units.tf"))

        self.assertFalse(wiring_exists)
        self.assertNotIn("wired into", res)
        self.assertIn("pull request was opened", res)

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    @patch("servers.phases.translation.translation_validate_3.tools.shutil.which")
    @patch("servers.phases.translation.translation_validate_3.tools.validation.run_validation",
           new_callable=AsyncMock)
    @patch("servers.phases.landingzone.actions.git_client")
    @patch("servers.phases.landingzone.actions.ssm_client")
    async def test_a_module_block_the_fix_loop_removed_fails_validation(
            self, mock_ssm, mock_git, mock_validation, mock_which,
            mock_gcs_client_class, mock_get_email):
        # The wiring file is one of the root directory's .tf files. A repair
        # worker cannot reach unit code from the root, so deleting the module
        # block "fixes" the root — and restores the vacuous pass. That is a
        # finding, not a ship.
        mock_get_email.return_value = "platform-user@google.com"
        mock_which.return_value = "/usr/bin/terraform"
        self.ssm_with_pr(mock_ssm)

        def validate(clone_dir, tf_dirs, terraform_bin, *args, **kwargs):
            with open(os.path.join(clone_dir, "translation-units.tf"),
                      "w", encoding="utf-8") as f:
                f.write("# the module block the fix worker deleted\n")
            return dict(self.CLEAN_REPORT,
                        fixed=[{"dir": ".", "attempts": 1, "original_error": "boom"}])
        mock_validation.side_effect = validate

        with tempfile.TemporaryDirectory() as clone:
            self.wire(mock_gcs_client_class, clone)
            res = await main.run_generated_validation(ctx=self.ctx_answering(True))

        self.assertIn("Validation FAILED", res)
        self.assertIn("unit-u-1", res)
        self.assertEqual(self.container["state"]["current_state"], "STATE_TRANSLATION_REVIEW")
        mock_git.stage_and_commit.assert_not_called()

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    @patch("servers.phases.translation.translation_validate_3.tools.shutil.which")
    @patch("servers.phases.translation.translation_validate_3.tools.validation.run_validation",
           new_callable=AsyncMock)
    @patch("servers.phases.landingzone.actions.git_client")
    @patch("servers.phases.landingzone.actions.ssm_client")
    async def test_a_customer_owned_wiring_filename_is_a_finding_not_a_casualty(
            self, mock_ssm, mock_git, mock_validation, mock_which,
            mock_gcs_client_class, mock_get_email):
        # The clone is the customer's target repository. A pre-existing
        # translation-units.tf the agent did not generate is never
        # overwritten or deleted; the wiring is not written, so the run must
        # fail (the root pass reached no unit) and route to the reviewer.
        mock_get_email.return_value = "platform-user@google.com"
        mock_which.return_value = "/usr/bin/terraform"
        mock_validation.return_value = dict(self.CLEAN_REPORT)
        self.ssm_with_pr(mock_ssm)
        foreign = 'module "theirs" {\n  source = "./their-module"\n}\n'

        with tempfile.TemporaryDirectory() as clone:
            with open(os.path.join(clone, "translation-units.tf"),
                      "w", encoding="utf-8") as f:
                f.write(foreign)
            self.wire(mock_gcs_client_class, clone)
            res = await main.run_generated_validation(ctx=self.ctx_answering(True))
            with open(os.path.join(clone, "translation-units.tf"),
                      "r", encoding="utf-8") as f:
                on_disk = f.read()

        self.assertEqual(on_disk, foreign)
        self.assertIn("Validation FAILED", res)
        self.assertIn("did not generate", res)
        self.assertEqual(self.container["state"]["current_state"],
                         "STATE_TRANSLATION_REVIEW")
        mock_git.stage_and_commit.assert_not_called()

    @patch("servers.dag.state_management.get_authenticated_user_email")
    @patch("main.storage.Client")
    @patch("servers.phases.translation.translation_validate_3.tools.shutil.which")
    async def test_missing_terraform_is_a_clean_error(
            self, mock_which, mock_gcs_client_class, mock_get_email):
        mock_get_email.return_value = "platform-user@google.com"
        mock_which.return_value = None

        with tempfile.TemporaryDirectory() as clone:
            self.wire(mock_gcs_client_class, clone)
            res = await main.run_generated_validation(ctx=self.ctx_answering(True))

        self.assertIn("ERROR: terraform is not on PATH", res)
        self.assertEqual(self.container["state"]["current_state"], "STATE_TRANSLATION_VALIDATE")


class MaterializeUnitsPruneTest(unittest.TestCase):
    """The clone persists across runs: a unit directory a previous run
    placed and this run does not (skipped after a failed validation) must
    not ship unreferenced and unvalidated in the customer's PR."""

    def test_a_stale_unit_directory_from_a_previous_run_is_pruned(self):
        from servers.phases.translation.translation_validate_3 import tools
        done = [{"unit": {"unit_id": "u-1"},
                 "result": {"files": [{"path": "main.tf", "content": "# new\n"}]}}]
        with tempfile.TemporaryDirectory() as clone:
            stale = os.path.join(clone, "translation-units", "old-unit")
            os.makedirs(stale)
            with open(os.path.join(stale, "main.tf"), "w", encoding="utf-8") as f:
                f.write("# stale, skipped this run\n")
            placed = tools.materialize_units(clone, done)
            self.assertEqual(placed, {"u-1": "translation-units/u-1"})
            self.assertFalse(os.path.isdir(stale))
            self.assertTrue(os.path.isfile(
                os.path.join(clone, "translation-units", "u-1", "main.tf")))


class AddRegistryMemberTest(unittest.TestCase):
    """The read-modify-write counterpart to the create-only registry writer."""

    def setUp(self):
        self.saved_client = main.state_mgr.gcs_client
        main.state_mgr.gcs_client = MagicMock()
        self.blob = main.state_mgr.gcs_client.bucket.return_value.blob.return_value
        self.blob.generation = 7
        self.blob.download_as_text.return_value = json.dumps(
            {"roles": {"admins": ["a@x.com"], "platform_engineers": [], "developers": []}})

    def tearDown(self):
        main.state_mgr.gcs_client = self.saved_client

    def written(self):
        return json.loads(self.blob.upload_from_string.call_args[0][0])

    def test_the_member_is_appended_to_the_named_role(self):
        self.assertTrue(main.add_registry_member("gs://b", "jane@x.com", "developers"))

        roles = self.written()["roles"]
        self.assertEqual(roles["developers"], ["jane@x.com"])
        # Nobody else is disturbed.
        self.assertEqual(roles["admins"], ["a@x.com"])

    def test_the_write_is_generation_matched(self):
        # write_workspace_registry writes with if_generation_match=0 and can only
        # create; this one has to not clobber a concurrent join.
        main.add_registry_member("gs://b", "jane@x.com", "developers")
        self.assertEqual(
            self.blob.upload_from_string.call_args.kwargs["if_generation_match"], 7)

    def test_an_existing_member_is_a_no_op(self):
        main.add_registry_member("gs://b", "a@x.com", "admins")
        self.blob.upload_from_string.assert_not_called()

    def test_a_concurrent_write_is_reported_rather_than_retried(self):
        self.blob.upload_from_string.side_effect = exceptions.PreconditionFailed("412")
        with self.assertRaises(RuntimeError):
            main.add_registry_member("gs://b", "jane@x.com", "developers")


class ArtifactRegistryScanTest(unittest.TestCase):
    """scan_artifact_registry_destinations: the replication destination record
    is only as good as this scan."""

    def _write(self, root, rel_path, content):
        full = os.path.join(root, rel_path)
        os.makedirs(os.path.dirname(full), exist_ok=True)
        with open(full, "w") as f:
            f.write(content)

    def test_literal_resource_composes_url(self):
        with tempfile.TemporaryDirectory() as root:
            self._write(root, "modules/ar/main.tf", AR_TF)
            found = deployment_actions.scan_artifact_registry_destinations(root)
            self.assertEqual(found, [{
                "project": "shared-artifacts",
                "location": "us-central1",
                "repository": "test-workspace",
                "url": "us-central1-docker.pkg.dev/shared-artifacts/test-workspace",
            }])

    def test_computed_attributes_record_null_and_no_url(self):
        with tempfile.TemporaryDirectory() as root:
            self._write(root, "main.tf", (
                'resource "google_artifact_registry_repository" "this" {\n'
                '  project       = var.project_id\n'
                '  location      = var.location\n'
                '  repository_id = "containers"\n'
                '  format        = "DOCKER"\n'
                '}\n'
            ))
            found = deployment_actions.scan_artifact_registry_destinations(root)
            self.assertEqual(found, [{
                "project": None, "location": None,
                "repository": "containers", "url": None,
            }])

    def test_interpolated_string_is_not_a_literal(self):
        with tempfile.TemporaryDirectory() as root:
            self._write(root, "main.tf", (
                'resource "google_artifact_registry_repository" "this" {\n'
                '  project       = "prefix-${var.env}"\n'
                '  location      = "us-central1"\n'
                '  repository_id = "containers"\n'
                '}\n'
            ))
            found = deployment_actions.scan_artifact_registry_destinations(root)
            self.assertIsNone(found[0]["project"])
            self.assertIsNone(found[0]["url"])

    def test_multiple_resources_across_files_are_all_found(self):
        with tempfile.TemporaryDirectory() as root:
            self._write(root, "a/main.tf", AR_TF)
            self._write(root, "b/main.tf", AR_TF.replace('"test-workspace"', '"nonprod"'))
            found = deployment_actions.scan_artifact_registry_destinations(root)
            self.assertEqual({d["repository"] for d in found}, {"test-workspace", "nonprod"})

    def test_no_registry_returns_empty(self):
        with tempfile.TemporaryDirectory() as root:
            self._write(root, "main.tf", 'resource "google_container_cluster" "x" {\n}\n')
            self.assertEqual(deployment_actions.scan_artifact_registry_destinations(root), [])

    def test_dot_terraform_is_skipped(self):
        with tempfile.TemporaryDirectory() as root:
            self._write(root, ".terraform/modules/cached/main.tf", AR_TF)
            self.assertEqual(deployment_actions.scan_artifact_registry_destinations(root), [])


class ContainerClusterScanTest(unittest.TestCase):
    """scan_container_clusters: literal-only, like the registry scan."""

    LITERAL_TF = (
        'resource "google_container_cluster" "prod" {\n'
        '  name     = "acme-gke"\n'
        '  location = "us-central1"\n'
        '  project  = "acme-prod"\n'
        "}\n"
    )
    COMPUTED_TF = (
        'resource "google_container_cluster" "prod" {\n'
        "  name     = var.cluster_name\n"
        "  location = local.region\n"
        '  project  = "${var.project_id}"\n'
        "}\n"
    )

    @staticmethod
    def _write(clone, rel, text):
        full = os.path.join(clone, rel)
        os.makedirs(os.path.dirname(full), exist_ok=True)
        with open(full, "w", encoding="utf-8") as f:
            f.write(text)

    def test_literal_attributes_are_scanned(self):
        with tempfile.TemporaryDirectory() as clone:
            self._write(clone, "cluster.tf", self.LITERAL_TF)
            self.assertEqual(
                deployment_actions.scan_container_clusters(clone),
                [{"name": "acme-gke", "location": "us-central1",
                  "project": "acme-prod"}])

    def test_computed_attributes_record_none_never_a_guess(self):
        with tempfile.TemporaryDirectory() as clone:
            self._write(clone, "cluster.tf", self.COMPUTED_TF)
            self.assertEqual(
                deployment_actions.scan_container_clusters(clone),
                [{"name": None, "location": None, "project": None}])

    def test_every_declared_cluster_is_reported_in_walk_order(self):
        with tempfile.TemporaryDirectory() as clone:
            self._write(clone, "a.tf", self.LITERAL_TF)
            self._write(clone, "b.tf", self.COMPUTED_TF)
            scanned = deployment_actions.scan_container_clusters(clone)
            self.assertEqual([c["name"] for c in scanned], ["acme-gke", None])

    def test_git_and_terraform_dirs_are_skipped(self):
        with tempfile.TemporaryDirectory() as clone:
            self._write(clone, ".terraform/modules/x/cluster.tf", self.LITERAL_TF)
            self._write(clone, ".git/cluster.tf", self.LITERAL_TF)
            self.assertEqual(deployment_actions.scan_container_clusters(clone), [])

    def test_deployment_export_inputs_runs_the_scan_leg(self):
        with tempfile.TemporaryDirectory() as clone:
            self._write(clone, "cluster.tf", self.LITERAL_TF)
            planned, scanned = main.deployment_export_inputs(
                {"target_clone_path": clone}, {})
            self.assertEqual(planned, {})
            self.assertEqual(scanned[0]["name"], "acme-gke")

    def test_deployment_export_inputs_without_a_clone_returns_none_scan(self):
        planned, scanned = main.deployment_export_inputs(
            {"target_clone_path": "/nonexistent-clone-xyz"}, {})
        self.assertIsNone(scanned, "a missing clone is 'unknown', not 'no clusters'")


class ProvisionArtifactRegistryTest(unittest.TestCase):
    """provision_artifact_registry: destination resolution and the
    check-then-create against the Artifact Registry API."""

    CONFIG = {"gcp_project": "test-project", "workspace_name": "Test Workspace"}
    DEST = {"project": "shared-artifacts", "location": "us-central1",
            "repository": "containers",
            "url": "us-central1-docker.pkg.dev/shared-artifacts/containers"}

    def _session(self, get_status=200, post_status=200):
        session = MagicMock()
        session.get.return_value = MagicMock(status_code=get_status, text="")
        session.post.return_value = MagicMock(status_code=post_status, text="")
        return session

    def test_existing_repository_is_left_alone(self):
        variables = {"artifact_registry_destinations": [dict(self.DEST)]}
        session = self._session(get_status=200)
        key, msg = deployment_actions.provision_artifact_registry(
            variables, self.CONFIG, session=session)
        self.assertEqual(key, "on_success")
        self.assertIn("exists", msg)
        session.post.assert_not_called()

    def test_missing_repository_is_created(self):
        variables = {"artifact_registry_destinations": [dict(self.DEST)]}
        session = self._session(get_status=404, post_status=200)
        key, msg = deployment_actions.provision_artifact_registry(
            variables, self.CONFIG, session=session)
        self.assertEqual(key, "on_success")
        self.assertIn("created", msg)
        _, kwargs = session.post.call_args
        self.assertEqual(kwargs["params"], {"repositoryId": "containers"})
        self.assertEqual(kwargs["json"]["format"], "DOCKER")

    def test_api_error_is_a_non_blocking_failure(self):
        variables = {"artifact_registry_destinations": [dict(self.DEST)]}
        key, msg = deployment_actions.provision_artifact_registry(
            variables, self.CONFIG, session=self._session(get_status=500))
        self.assertEqual(key, "on_failure")
        self.assertIn("HTTP 500", msg)
        self.assertIn("before image replication", msg)

    def test_default_destination_when_nothing_recorded_or_declared(self):
        variables = {}
        key, msg = deployment_actions.provision_artifact_registry(
            variables, self.CONFIG, session=self._session(get_status=404))
        self.assertEqual(key, "on_success")
        self.assertEqual(
            variables["artifact_registry_destinations"][0]["url"],
            "us-central1-docker.pkg.dev/test-project/test-workspace")
        self.assertIn("default", msg)

    def test_design_declared_registry_wins_over_default(self):
        with tempfile.TemporaryDirectory() as clone:
            with open(os.path.join(clone, "main.tf"), "w") as f:
                f.write(AR_TF)
            variables = {"target_clone_path": clone}
            key, msg = deployment_actions.provision_artifact_registry(
                variables, self.CONFIG, session=self._session(get_status=200))
        self.assertEqual(key, "on_success")
        self.assertEqual(
            variables["artifact_registry_destinations"][0]["url"],
            "us-central1-docker.pkg.dev/shared-artifacts/test-workspace")
        self.assertIn("declared in the landing-zone design", msg)

    def test_computed_values_are_reported_not_created(self):
        variables = {"artifact_registry_destinations": [
            {"project": None, "location": None, "repository": "containers", "url": None}]}
        session = self._session()
        key, msg = deployment_actions.provision_artifact_registry(
            variables, self.CONFIG, session=session)
        self.assertEqual(key, "on_success")
        self.assertIn("terraform apply", msg)
        session.get.assert_not_called()
        session.post.assert_not_called()

    def test_location_env_overrides_the_default(self):
        variables = {}
        with patch.dict(os.environ, {"GKE_AGENTIC_MIGRATION_AR_LOCATION": "europe-west1"}):
            deployment_actions.provision_artifact_registry(
                variables, self.CONFIG, session=self._session(get_status=404))
        self.assertEqual(
            variables["artifact_registry_destinations"][0]["url"],
            "europe-west1-docker.pkg.dev/test-project/test-workspace")


class PromptRenderingTest(unittest.TestCase):
    """prompt_template is the prompt; it used to be decorative."""

    CONFIG = {"gcp_project": "test-project", "workspace_name": "test-workspace"}

    def render(self, template, variables=None):
        return main.render_prompt(
            "STATE_X", {"prompt_template": template},
            {"variables": variables or {}}, self.CONFIG)

    def test_variables_and_config_are_both_in_scope(self):
        rendered = self.render(
            "Create '{ssm_repository}' in '{project_id}' for '{workspace_name}'.",
            {"ssm_repository": "my-repo"})
        self.assertEqual(
            rendered, "Create 'my-repo' in 'test-project' for 'test-workspace'.")

    def test_an_unknown_placeholder_degrades_to_the_raw_template(self):
        # A placeholder typo should produce an odd-looking prompt, not abort a
        # migration mid-run.
        template = "Owner {no_such_field} is not registered."
        self.assertEqual(self.render(template), template)

    def test_every_hitl_state_renders_against_its_own_variables(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        with open(os.path.join(dir_path, "platform_dag.json"), "r") as f:
            dag = json.load(f)

        # Every placeholder a HITL prompt uses must be a name the server
        # actually sets, or the user is shown raw braces at an approval gate.
        supplied = {
            "STATE_ELICIT_SSM_CREATION_APPROVAL": {
                "ssm_repository": "r", "ssm_instance": "i"},
            "STATE_ASSESSMENT": {},
            "STATE_CONFIRM_NEW_MEMBER": {"pending_owner_email": "jane.doe@x.com"},
            "STATE_LZ_TRANSLATION_PLAN_REVIEW": {},
            "STATE_TRANSLATION_APPROVED": {},
            "STATE_DISCOVERY_RENDER_APPROVAL": {
                "helm_chart_count": 1, "kustomize_root_count": 1},
            "STATE_DISCOVERY_DATA_REVIEW": {},
            "STATE_DEPLOYMENT_IMAGE_REPLICATION": {},
        }
        for name, state in dag["states"].items():
            # Keyed on prompt ownership, not node type: an AGENT_TASK whose
            # tool raises the elicitation in-call carries a prompt too.
            if "prompt_template" not in state:
                continue
            with self.subTest(state=name):
                self.assertIn(name, supplied, "new HITL state: add its variables here")
                rendered = main.render_prompt(
                    name, state, {"variables": supplied[name]}, self.CONFIG)
                self.assertNotIn("{", rendered)

    def test_every_hitl_state_has_a_response_schema(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        with open(os.path.join(dir_path, "platform_dag.json"), "r") as f:
            dag = json.load(f)

        # The schema cannot live on the state definition, so nothing structural
        # catches a state that was added without one — it would silently fall
        # back to a yes/no form and discard whatever the state actually asked.
        for name, state in dag["states"].items():
            if state["type"] == "HITL_ELICITATION":
                self.assertIn(name, dispatch.ELICITATIONS)


class ConfigurationScanTest(unittest.TestCase):
    """find_configuration_files: what reaches the model decides what the inventory can say."""

    # The budget the scan used to enforce. Nothing may reintroduce it.
    FORMER_CAP = 100000

    def _write(self, root, rel_path, content):
        full = os.path.join(root, rel_path)
        os.makedirs(os.path.dirname(full), exist_ok=True)
        with open(full, "w", encoding="utf-8") as f:
            f.write(content)

    def test_a_tree_larger_than_the_former_cap_is_returned_whole(self):
        # Twelve files of 10 KB each: the old 100 000-char budget would have cut
        # this off partway through, and the inventory would have silently
        # described only the files that happened to come first in the walk.
        with tempfile.TemporaryDirectory() as root:
            for i in range(12):
                self._write(root, f"stack{i}/main.tf", f'# marker-{i}\n' + "x" * 10000)

            result = find_configuration_files(root)

        for i in range(12):
            self.assertIn(f"marker-{i}", result)
        self.assertNotIn("truncated", result.lower())
        self.assertGreater(len(result), self.FORMER_CAP)

    def test_a_single_file_larger_than_the_former_cap_is_returned_in_full(self):
        with tempfile.TemporaryDirectory() as root:
            self._write(root, "big.yaml", "# marker-big\n" + "y" * (self.FORMER_CAP + 1))

            result = find_configuration_files(root)

        self.assertIn("marker-big", result)
        self.assertNotIn("SKIPPED", result)

    def test_vendored_and_generated_directories_are_skipped(self):
        # A vendored terraform-aws-modules copy declares its own aws_eks_cluster.
        # Scanning it puts a cluster in the inventory that the user does not run.
        with tempfile.TemporaryDirectory() as root:
            self._write(root, "main.tf", 'resource "aws_eks_cluster" "real" {}')
            self._write(root, ".terraform/modules/eks/main.tf",
                        'resource "aws_eks_cluster" "vendored" {}')
            self._write(root, "node_modules/chart/values.yaml", "vendored: true")
            self._write(root, "vendor/mod/main.tf", 'resource "aws_eks_cluster" "vendored" {}')
            self._write(root, ".git/config.yaml", "vendored: true")

            result = find_configuration_files(root)

        self.assertIn("real", result)
        self.assertNotIn("vendored", result)

    def test_an_unreadable_file_is_reported_without_losing_the_rest(self):
        with tempfile.TemporaryDirectory() as root:
            self._write(root, "good.tf", "# marker-good")
            self._write(root, "bad.tf", "\n")
            os.chmod(os.path.join(root, "bad.tf"), 0o000)

            try:
                result = find_configuration_files(root)
            finally:
                os.chmod(os.path.join(root, "bad.tf"), 0o600)

        self.assertIn("marker-good", result)
        self.assertIn("ERROR READING FILE", result)

    def test_a_missing_directory_is_an_error_not_an_empty_scan(self):
        result = find_configuration_files("/nonexistent/source/checkout")
        self.assertIn("ERROR", result)


class InventorySchemaTest(unittest.TestCase):
    """The schema is the only thing enforcing the eks-discovery Validation Checkpoint."""

    def _document(self, **analysis):
        doc = {
            "schema_version": "1.0",
            "generated_at": "2026-07-29T00:00:00Z",
            "source": {"root_dir": "/src"},
            "images": [],
            "render_targets": [],
        }
        doc.update(analysis)
        return doc

    def _assert_rejected(self, **analysis):
        with self.assertRaises(ValueError):
            main.state_mgr.validate_inventory(self._document(**analysis))

    def test_a_fully_populated_analysis_validates(self):
        main.state_mgr.validate_inventory(self._document(
            triggers=ALL_TRIGGERS,
            clusters=[{
                "name": "eks-1",
                "version": "1.30",
                "workloads": {
                    "namespaces": [{"name": "default", "deployments": 0}],
                    "irsa_bindings": [{
                        "namespace": "default", "sa": "app-sa",
                        "role_arn": "arn:aws:iam::123456789012:role/app",
                    }],
                },
            }],
            storage=[{"kind": "storage_class", "name": "gp3", "provisioner": "ebs.csi.aws.com"}],
            data_dependencies=[{"service": "rds", "identifier": "payments-prod"}],
            observability={"log_groups": [{"name": "/aws/eks/prod"}]},
            escalations=[{"category": "Custom CNI"}],
        ))

    def test_analysis_objects_stay_open_to_fields_the_schema_does_not_name(self):
        main.state_mgr.validate_inventory(self._document(
            triggers=ALL_TRIGGERS,
            clusters=[{"name": "eks-1", "authentication_mode": "API", "vpc": {"id": "vpc-1"}}],
        ))

    # test_a_partially_populated_trigger_set_is_rejected was removed: the
    # extraction pipeline (f255019) dropped the all-four-triggers requirement
    # from the schema — worker fragments legitimately carry partial trigger
    # sets and the merger completes them.

    def test_a_non_boolean_trigger_is_rejected(self):
        self._assert_rejected(triggers={**ALL_TRIGGERS, "karpenter": "yes"})

    def test_a_cluster_without_a_name_is_rejected(self):
        self._assert_rejected(clusters=[{"version": "1.30"}])

    def test_a_namespace_without_a_workload_count_is_rejected(self):
        self._assert_rejected(
            clusters=[{"name": "eks-1", "workloads": {"namespaces": [{"name": "default"}]}}])

    def test_an_irsa_binding_without_a_role_arn_is_rejected(self):
        self._assert_rejected(clusters=[{
            "name": "eks-1",
            "workloads": {"irsa_bindings": [{"namespace": "default", "sa": "app-sa"}]},
        }])

    def test_an_unknown_data_dependency_service_is_rejected(self):
        # "aurora" used to stand in for an unknown service; the harvester emits
        # it for aws_rds_cluster and the rds-aurora module, so it is a known
        # one now. The enum still has to reject what it does not model —
        # an unrecognized resource type is skipped by the harvester, so a
        # service name it never emits must not validate either.
        self._assert_rejected(data_dependencies=[{"service": "quantumledger", "identifier": "db"}])

    def test_the_harvester_s_own_service_names_validate(self):
        """Every service datastores.py can emit must satisfy the schema, or a
        real estate produces an inventory the server then refuses to save."""
        from servers.phases.discovery.discovery_init_1 import datastores
        emitted = set(datastores.RESOURCE_SERVICES.values()) | {
            service for _, service in datastores.MODULE_PATTERNS}
        for service in sorted(emitted):
            with self.subTest(service=service):
                main.state_mgr.validate_inventory(self._document(
                    data_dependencies=[{"service": service, "identifier": "x"}]))


class ReplicationRefMappingTest(unittest.TestCase):
    """The pure ref arithmetic replicate_images rests on: what gets pulled,
    what it is called at the destination."""

    HOST = "123456789012.dkr.ecr.eu-west-1.amazonaws.com"

    def image(self, **overrides):
        image = {
            "ref": f"{self.HOST}/payments/api:1.4.2",
            "repository": f"{self.HOST}/payments/api",
            "tag": "1.4.2", "digest": None,
        }
        image.update(overrides)
        return image

    def test_source_prefers_the_digest_when_discovery_recorded_one(self):
        digest = "sha256:" + "b" * 64
        self.assertEqual(
            replication._src_ref(self.image(digest=digest)),
            f"{self.HOST}/payments/api@{digest}")

    def test_source_falls_back_to_the_ref_as_found(self):
        self.assertEqual(
            replication._src_ref(self.image()), f"{self.HOST}/payments/api:1.4.2")

    def test_destination_keeps_the_source_path_under_the_registry(self):
        self.assertEqual(
            replication._dest_ref("us-central1-docker.pkg.dev/p/repo", self.image()),
            "us-central1-docker.pkg.dev/p/repo/payments/api:1.4.2")

    def test_digest_only_pins_get_a_digest_derived_tag(self):
        # Registries refuse pushes to @digest references; the synthetic tag is
        # just an addressable name — content equality rides on
        # --preserve-digests, not the tag.
        digest = "sha256:" + "c" * 64
        image = self.image(tag=None, digest=digest,
                           ref=f"{self.HOST}/payments/api@{digest}")
        self.assertEqual(replication._dest_tag(image), "sha-" + "c" * 12)

    def test_an_untagged_ref_lands_as_latest(self):
        self.assertEqual(
            replication._dest_tag(self.image(tag=None,
                                             ref=f"{self.HOST}/payments/api")),
            "latest")

    def test_login_command_names_the_region_of_the_host(self):
        cmd = replication._login_command(self.HOST)
        self.assertIn("--region eu-west-1", cmd)
        self.assertIn(f"skopeo login --username AWS --password-stdin {self.HOST}", cmd)

    def test_public_ecr_needs_no_aws_cli(self):
        self.assertNotIn("aws ecr get-login-password",
                         replication._login_command("public.ecr.aws"))


import fake_gcs


class _ForbiddenWorkloadsClient(fake_gcs.FakeStorageClient):
    """FakeGCS whose workloads/* prefix answers 403 — the missing
    managed-folder binding, as GCS itself would report it."""

    def bucket(self, name):
        real = super().bucket(name)

        class _Bucket:
            name = real.name

            def blob(self, path):
                if path.startswith("workloads/"):
                    blob = MagicMock()
                    forbidden = exceptions.Forbidden("403: no managed-folder binding")
                    blob.reload.side_effect = forbidden
                    blob.download_as_text.side_effect = forbidden
                    blob.upload_from_string.side_effect = forbidden
                    return blob
                return real.blob(path)

        return _Bucket()


class DeveloperJoinTest(unittest.IsolatedAsyncioTestCase):
    """join_ledger developer path + get_next_stage routing over FakeGCS.

    The platform/admin joins are covered by MainTest and must stay untouched;
    everything here goes through the new developer branch.
    """

    LEDGER = "gs://dev-ledger"
    REGISTRY = {
        "workspace_name": "ws-dev",
        "gcp_project": "proj",
        "roles": {"admins": ["adm@x.com"], "platform_engineers": ["pe@x.com"],
                  "developers": ["dev-a@x.com", "dev-b@x.com"]},
    }

    def setUp(self):
        main.state_mgr.LEDGER_CONFIG_PATH = testing_env.LEDGER_CONFIG_PATH
        main.state_mgr.LEDGER_CONFIG_DIR = testing_env.LEDGER_CONFIG_DIR
        shutil.rmtree(main.state_mgr.LEDGER_CONFIG_DIR, ignore_errors=True)
        if os.path.exists(main.state_mgr.LEDGER_CONFIG_PATH):
            os.remove(main.state_mgr.LEDGER_CONFIG_PATH)
        self.fake = fake_gcs.FakeStorageClient()
        self.bucket = self.fake.bucket("dev-ledger")
        self.bucket.blob("workspace_registry.yaml").upload_from_string(
            json.dumps(self.REGISTRY))
        main.state_mgr.gcs_client = None
        self._patches = [
            patch("main.storage.Client", return_value=self.fake),
            patch("servers.dag.state_management.storage.Client",
                  return_value=self.fake),
            patch("main.frontend_launcher.announcement", return_value=""),
            patch("servers.dag.state_management.get_authenticated_user_email",
                  return_value="dev-a@x.com"),
        ]
        self.mocks = [p.start() for p in self._patches]
        self.mock_email = self.mocks[3]

    def tearDown(self):
        for p in self._patches:
            p.stop()
        shutil.rmtree(main.state_mgr.LEDGER_CONFIG_DIR, ignore_errors=True)
        if os.path.exists(main.state_mgr.LEDGER_CONFIG_PATH):
            os.remove(main.state_mgr.LEDGER_CONFIG_PATH)
        main.state_mgr.gcs_client = None
        main.gcs_client = None

    # -- helpers -----------------------------------------------------------

    def read_json(self, path):
        return json.loads(self.bucket.blob(path).download_as_text())

    def seed_component(self, component="orders-component", version=None,
                       current_state="STATE_WKLD_SCOPE", claimant="dev-a@x.com"):
        """Pre-seeds a component's dag copy + state, like an earlier join did."""
        dag, raw = main._load_bundled_developer_dag()
        if version is not None:
            dag = json.loads(raw)
            dag["version"] = version
            raw = json.dumps(dag)
        self.bucket.blob(f"workloads/{component}/dag.json").upload_from_string(raw)
        state = {
            "current_state": current_state,
            "history": ["seeded"],
            "variables": {"component": component,
                          "claim": {"claimant": claimant, "claimed_at": "T0"}},
        }
        self.bucket.blob(f"workloads/{component}/state.json").upload_from_string(
            json.dumps(state))

    # -- fresh claim -------------------------------------------------------

    async def test_fresh_claim_initializes_dag_state_and_cache(self):
        res = await main.join_ledger(self.LEDGER, component="orders-component")
        self.assertIn("Successfully joined", res)
        self.assertIn("Assumed Role: developer", res)
        self.assertIn("Component: orders-component", res)
        self.assertIn("Claim: new claim", res)
        self.assertIn("Current Component State: STATE_WKLD_SCOPE", res)
        # The admin binding step is reported on every new claim (D11).
        self.assertIn("gcloud storage managed-folders create", res)
        dag = self.read_json("workloads/orders-component/dag.json")
        self.assertEqual(dag["version"], "0.4")
        state = self.read_json("workloads/orders-component/state.json")
        self.assertEqual(state["current_state"], "STATE_WKLD_SCOPE")
        self.assertEqual(state["variables"]["claim"]["claimant"], "dev-a@x.com")
        config = main.state_mgr.read_local_config()
        self.assertEqual(config["resolved_role"], "developers")
        self.assertEqual(config["component"], "orders-component")

    async def test_missing_component_errors_and_writes_no_cache(self):
        res = await main.join_ledger(self.LEDGER)
        self.assertIn("ERROR", res)
        self.assertIn("component=<id>", res)
        self.assertIn("RFC-1123", res)
        with self.assertRaises(ValueError):
            main.state_mgr.read_local_config()

    async def test_bad_slug_errors(self):
        res = await main.join_ledger(self.LEDGER, component="Orders.Component")
        self.assertIn("ERROR", res)
        self.assertIn("RFC-1123", res)

    async def test_reconfigure_is_refused_for_developers(self):
        res = await main.join_ledger(self.LEDGER, component="orders-component",
                                     reconfigure=True)
        self.assertIn("ERROR", res)
        self.assertIn("admin-only", res)

    async def test_platform_join_with_component_errors(self):
        self.mock_email.return_value = "pe@x.com"
        res = await main.join_ledger(self.LEDGER, component="orders-component")
        self.assertIn("ERROR", res)
        self.assertIn("developer joins only", res)

    # -- resume / upgrade --------------------------------------------------

    async def test_self_rejoin_resumes_without_readvertising_the_binding(self):
        await main.join_ledger(self.LEDGER, component="orders-component")
        res = await main.join_ledger(self.LEDGER, component="orders-component")
        self.assertIn("Claim: resumed", res)
        self.assertNotIn("gcloud storage managed-folders create", res)
        state = self.read_json("workloads/orders-component/state.json")
        self.assertEqual(state["variables"]["claim"]["claimant"], "dev-a@x.com")

    async def test_upgrade_survives_a_state_write_conflict(self):
        # Write-ordering regression: dag.json must not be overwritten before
        # the MAPPED state.json commits — with the reverse order, one 412 on
        # the state write leaves the copy at the new version, the retry sees
        # copy == bundled, and the mapping is silently dropped forever.
        # Exercises the REAL v0.1 -> v0.2 bump: a component parked on v0.1
        # moves into the planning pipeline (AWAIT_PIPELINE -> PLAN).
        self.seed_component(version="0.1",
                            current_state="STATE_WKLD_AWAIT_PIPELINE")
        fail_once = {"armed": True}
        real_blob = self.bucket.blob

        def flaky_blob(name):
            blob = real_blob(name)
            if name.endswith("state.json"):
                real_upload = blob.upload_from_string

                def upload(*args, **kwargs):
                    if fail_once["armed"]:
                        fail_once["armed"] = False
                        raise exceptions.PreconditionFailed("injected conflict")
                    return real_upload(*args, **kwargs)
                blob.upload_from_string = upload
            return blob

        with patch.object(self.bucket, "blob", side_effect=flaky_blob):
            res = await main.join_ledger(self.LEDGER, component="orders-component")
        self.assertNotIn("ERROR", res)
        self.assertIn("developer DAG upgraded v0.1 -> v0.4", res)
        self.assertFalse(fail_once["armed"])  # the conflict really fired
        dag = self.read_json("workloads/orders-component/dag.json")
        self.assertEqual(dag["version"], "0.4")
        state = self.read_json("workloads/orders-component/state.json")
        self.assertEqual(state["current_state"], "STATE_WKLD_PLAN")
        self.assertTrue(any("state STATE_WKLD_AWAIT_PIPELINE -> STATE_WKLD_PLAN"
                            in h for h in state["history"]))

    def test_init_refuses_a_developer_version_bump_without_mapping(self):
        # The start-up gate: a bump whose version has no STATE_MAPPINGS entry
        # must refuse to start, not explode on the first re-join post-release.
        with patch.dict(main.workload_join_lib.STATE_MAPPINGS, clear=True):
            with self.assertRaisesRegex(DagValidationError, "STATE_MAPPINGS"):
                main.init()

    async def test_developer_path_never_announces_the_review_frontend(self):
        # The review UI renders the platform pipeline only: developer IAM
        # cannot read it, and a de-escalated admin must not have platform
        # state rendered into a developer session either.
        announcement = self.mocks[2]
        await main.join_ledger(self.LEDGER, component="orders-component")
        main.get_next_stage()
        announcement.assert_not_called()

    async def test_rejoin_upgrades_an_older_dag_copy_in_place(self):
        self.seed_component(version="0.0",
                            current_state="STATE_WKLD_AWAIT_PIPELINE")
        res = await main.join_ledger(self.LEDGER, component="orders-component")
        self.assertIn("Claim: resumed", res)
        self.assertIn("developer DAG upgraded v0.0 -> v0.4", res)
        dag = self.read_json("workloads/orders-component/dag.json")
        self.assertEqual(dag["version"], "0.4")
        state = self.read_json("workloads/orders-component/state.json")
        # The composed v0.2 hop moves the parked state into the planning
        # pipeline; the v0.3 hop leaves PLAN where it is.
        self.assertEqual(state["current_state"], "STATE_WKLD_PLAN")
        self.assertTrue(any("Upgraded developer DAG copy v0.0 -> v0.4" in h
                            for h in state["history"]))

    # -- the v0.4 guarded DONE -> TRANSLATE re-entry (M4) ------------------

    def seed_plan(self, statuses, component="orders-component"):
        units = [{"unit_id": f"unit-{i}",
                  "family": "wkld-routing" if s == "parked"
                  else "wkld-manifests", "status": s}
                 for i, s in enumerate(statuses)]
        self.bucket.blob(f"workloads/{component}/plan.json") \
            .upload_from_string(json.dumps({"component": component,
                                            "units": units}))

    def seed_gateway(self, gateway={"name": "shared-gw",
                                    "namespace": "gateway-infra"}):
        """Publishes the exports half of the guard: a live Gateway."""
        self.bucket.blob("exports.json").upload_from_string(
            json.dumps({"gateway": gateway,
                        "generations": {"discovery": 1, "translation": 2,
                                        "deployment": 0}}))

    async def test_v04_rejoin_reenters_done_component_with_parked_units(self):
        self.seed_component(version="0.3", current_state="STATE_WKLD_DONE")
        self.seed_plan(["done", "parked"])
        self.seed_gateway()
        res = await main.join_ledger(self.LEDGER, component="orders-component")
        self.assertIn("developer DAG upgraded v0.3 -> v0.4", res)
        self.assertIn("Current Component State: STATE_WKLD_TRANSLATE", res)
        state = self.read_json("workloads/orders-component/state.json")
        self.assertEqual(state["current_state"], "STATE_WKLD_TRANSLATE")
        self.assertEqual(
            self.read_json("workloads/orders-component/dag.json")["version"],
            "0.4")

    async def test_v04_rejoin_keeps_done_when_nothing_is_parked(self):
        self.seed_component(version="0.3", current_state="STATE_WKLD_DONE")
        self.seed_plan(["done", "skipped"])
        res = await main.join_ledger(self.LEDGER, component="orders-component")
        self.assertIn("Current Component State: STATE_WKLD_DONE", res)
        self.assertEqual(
            self.read_json("workloads/orders-component/state.json")
            ["current_state"], "STATE_WKLD_DONE")
        self.assertEqual(
            self.read_json("workloads/orders-component/dag.json")["version"],
            "0.4")

    async def test_v04_rejoin_with_no_plan_warns_and_stays_done(self):
        self.seed_component(version="0.3", current_state="STATE_WKLD_DONE")
        res = await main.join_ledger(self.LEDGER, component="orders-component")
        self.assertIn("could not be evaluated", res)
        self.assertIn("Current Component State: STATE_WKLD_DONE", res)
        self.assertEqual(
            self.read_json("workloads/orders-component/state.json")
            ["current_state"], "STATE_WKLD_DONE")

    async def test_v04_rejoin_waits_for_the_gateway_then_reenters(self):
        """The guard is STANDING: the upgrade join is not the only chance.

        A component shipped before the platform Gateway published upgrades to
        v0.4 and stays DONE (nothing to unpark yet). The gateway publishes
        later; the very next join re-enters it, even though the version
        mapping has already been consumed.
        """
        self.seed_component(version="0.3", current_state="STATE_WKLD_DONE")
        self.seed_plan(["done", "parked"])
        first = await main.join_ledger(self.LEDGER,
                                       component="orders-component")
        self.assertIn("Current Component State: STATE_WKLD_DONE", first)
        self.assertEqual(
            self.read_json("workloads/orders-component/dag.json")["version"],
            "0.4")
        self.seed_gateway()
        second = await main.join_ledger(self.LEDGER,
                                        component="orders-component")
        self.assertIn("Current Component State: STATE_WKLD_TRANSLATE", second)
        state = self.read_json("workloads/orders-component/state.json")
        self.assertEqual(state["current_state"], "STATE_WKLD_TRANSLATE")
        self.assertTrue(any("Re-entry: guarded mapping" in h
                            for h in state["history"]))

    async def test_v04_reentry_of_a_shipped_component_is_a_handover(self):
        """Claim class is read from the state as STORED, not as re-entered.

        A DONE component belongs to whoever picks it up; classifying it after
        the re-entry would make it look midflight and refuse the second
        developer for no reason.
        """
        self.seed_component(version="0.3", current_state="STATE_WKLD_DONE")
        self.seed_plan(["done", "parked"])
        self.seed_gateway()
        self.mock_email.return_value = "dev-b@x.com"
        res = await main.join_ledger(self.LEDGER, component="orders-component")
        self.assertNotIn("ERROR", res)
        self.assertIn("Current Component State: STATE_WKLD_TRANSLATE", res)
        state = self.read_json("workloads/orders-component/state.json")
        self.assertEqual(state["variables"]["claim"]["claimant"],
                         "dev-b@x.com")

    # -- claims across developers -----------------------------------------

    async def test_midflight_refusal_names_the_claimant(self):
        self.seed_component(current_state="STATE_WKLD_SCOPE_CONFIRM")
        self.mock_email.return_value = "dev-b@x.com"
        res = await main.join_ledger(self.LEDGER, component="orders-component")
        self.assertIn("ERROR", res)
        self.assertIn("dev-a@x.com", res)
        self.assertIn("reclaim_component=True", res)
        # No cache: this session holds no claim.
        with self.assertRaises(ValueError):
            main.state_mgr.read_local_config()
        state = self.read_json("workloads/orders-component/state.json")
        self.assertEqual(state["variables"]["claim"]["claimant"], "dev-a@x.com")

    async def test_reclaim_takes_over_midflight_with_history(self):
        self.seed_component(current_state="STATE_WKLD_SCOPE_CONFIRM")
        self.mock_email.return_value = "dev-b@x.com"
        res = await main.join_ledger(self.LEDGER, component="orders-component",
                                     reclaim_component=True)
        self.assertIn("Claim: takeover from dev-a@x.com", res)
        # A takeover is a new claim for dev-b: the binding step is reported.
        self.assertIn("--member=user:dev-b@x.com", res)
        state = self.read_json("workloads/orders-component/state.json")
        self.assertEqual(state["variables"]["claim"]["claimant"], "dev-b@x.com")
        self.assertTrue(any("Claim takeover: dev-a@x.com -> dev-b@x.com" in h
                            for h in state["history"]))
        config = main.state_mgr.read_local_config()
        self.assertEqual(config["component"], "orders-component")

    async def test_parked_component_hands_over_without_reclaim(self):
        self.seed_component(current_state="STATE_WKLD_AWAIT_PIPELINE")
        self.mock_email.return_value = "dev-b@x.com"
        res = await main.join_ledger(self.LEDGER, component="orders-component")
        self.assertIn("handed over from dev-a@x.com", res)
        state = self.read_json("workloads/orders-component/state.json")
        self.assertEqual(state["variables"]["claim"]["claimant"], "dev-b@x.com")

    async def test_second_dev_is_refused_while_a_draft_exists_at_start_state(self):
        # The whole scope draft is iterated INSIDE the start state, so with
        # work present a same-id join is a refusal naming the claimant — not
        # a silent last-writer-wins that inherits the draft.
        self.seed_component(current_state="STATE_WKLD_SCOPE")
        state = self.read_json("workloads/orders-component/state.json")
        state["variables"]["workload_scope"] = {
            "root_dir": "", "included": ["charts/orders"], "excluded": []}
        self.bucket.blob("workloads/orders-component/state.json") \
            .upload_from_string(json.dumps(state))
        self.mock_email.return_value = "dev-b@x.com"
        res = await main.join_ledger(self.LEDGER, component="orders-component")
        self.assertIn("ERROR", res)
        self.assertIn("dev-a@x.com", res)
        self.assertIn("reclaim_component=True", res)
        unchanged = self.read_json("workloads/orders-component/state.json")
        self.assertEqual(unchanged["variables"]["claim"]["claimant"],
                         "dev-a@x.com")

    async def test_undrafted_start_state_is_still_last_writer_wins(self):
        # Without a draft nothing is lost: the true join race stays open.
        self.seed_component(current_state="STATE_WKLD_SCOPE")
        self.mock_email.return_value = "dev-b@x.com"
        res = await main.join_ledger(self.LEDGER, component="orders-component")
        self.assertIn("raced initial claim, last-writer-wins", res)
        state = self.read_json("workloads/orders-component/state.json")
        self.assertEqual(state["variables"]["claim"]["claimant"], "dev-b@x.com")

    async def test_admin_deescalation_to_developer_works(self):
        self.mock_email.return_value = "adm@x.com"
        res = await main.join_ledger(self.LEDGER, role="developers",
                                     component="orders-component")
        self.assertIn("Assumed Role: developer", res)
        self.assertIn("Claim: new claim", res)
        state = self.read_json("workloads/orders-component/state.json")
        self.assertEqual(state["variables"]["claim"]["claimant"], "adm@x.com")

    # -- blocked on the managed-folder binding ------------------------------

    async def test_403_reports_binding_step_and_still_writes_the_cache(self):
        blocked = _ForbiddenWorkloadsClient()
        blocked.bucket("dev-ledger").blob("workspace_registry.yaml") \
            .upload_from_string(json.dumps(self.REGISTRY))
        self.mocks[0].return_value = blocked
        self.mocks[1].return_value = blocked
        res = await main.join_ledger(self.LEDGER, component="orders-component")
        self.assertNotIn("ERROR", res)
        self.assertIn("blocked on managed-folder binding", res)
        self.assertIn("gcloud storage managed-folders create", res)
        self.assertIn("Re-run join_ledger", res)
        config = main.state_mgr.read_local_config()
        self.assertEqual(config["component"], "orders-component")

    # -- routing -------------------------------------------------------------

    async def test_get_next_stage_routes_developer_to_the_scope_step(self):
        await main.join_ledger(self.LEDGER, component="orders-component")
        with patch("main.frontend_launcher.announcement", return_value=""):
            payload = main.get_next_stage()
        self.assertIn("Current state: STATE_WKLD_SCOPE", payload)
        self.assertIn("Type: AGENT_TASK", payload)
        # The scope step's instructions ride in the payload.
        self.assertIn("submit_workload_scope", payload)
        self.assertIn("browse_component_seed", payload)

    async def test_get_next_stage_enforces_the_claimant_on_routing(self):
        await main.join_ledger(self.LEDGER, component="orders-component")
        # Another developer stole the claim after this session joined.
        state = self.read_json("workloads/orders-component/state.json")
        state["variables"]["claim"]["claimant"] = "dev-b@x.com"
        self.bucket.blob("workloads/orders-component/state.json") \
            .upload_from_string(json.dumps(state))
        with patch("main.frontend_launcher.announcement", return_value=""):
            payload = main.get_next_stage()
        self.assertIn("ERROR", payload)
        self.assertIn("dev-b@x.com", payload)


if __name__ == "__main__":
    unittest.main()
