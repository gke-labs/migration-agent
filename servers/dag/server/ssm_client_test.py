import unittest
from unittest.mock import patch, MagicMock
from google.api_core import exceptions
from server import ssm_client

class SSMClientTest(unittest.TestCase):

    def _assert_components(self, res, repository="my-target-repo"):
        self.assertEqual(res["project"], "191207960751")
        self.assertEqual(res["project_id"], "my-project")
        self.assertEqual(res["location"], "us-central1")
        self.assertEqual(res["instance"], "ssm-instance-us-1")
        self.assertEqual(res["repository"], repository)

    def test_parse_ssm_url_git_https(self):
        url = "https://ssm-instance-us-1-191207960751-git.us-central1.sourcemanager.dev/my-project/my-target-repo.git"
        res = ssm_client.parse_ssm_url(url)
        self._assert_components(res)
        self.assertEqual(res["endpoint"], "git")
        self.assertFalse(res["private_service_connect"])

    def test_parse_ssm_url_web_ui(self):
        url = "https://ssm-instance-us-1-191207960751.us-central1.sourcemanager.dev/my-project/my-target-repo"
        res = ssm_client.parse_ssm_url(url)
        self._assert_components(res)
        self.assertEqual(res["endpoint"], "html")

    def test_parse_ssm_url_ssh_forms(self):
        for url in (
            "ssh://git@ssm-instance-us-1-191207960751-ssh.us-central1.sourcemanager.dev/my-project/my-target-repo.git",
            "git@ssm-instance-us-1-191207960751-ssh.us-central1.sourcemanager.dev:my-project/my-target-repo.git",
            # an explicit port on the ssh:// form is tolerated
            "ssh://git@ssm-instance-us-1-191207960751-ssh.us-central1.sourcemanager.dev:22/my-project/my-target-repo.git",
        ):
            with self.subTest(url=url):
                res = ssm_client.parse_ssm_url(url)
                self._assert_components(res)
                self.assertEqual(res["endpoint"], "ssh")

    def test_parse_ssm_url_private_service_connect(self):
        url = "https://ssm-instance-us-1-191207960751-git.us-central1.p.sourcemanager.dev/my-project/my-target-repo.git"
        res = ssm_client.parse_ssm_url(url)
        self._assert_components(res)
        self.assertTrue(res["private_service_connect"])

    def test_parse_ssm_url_repo_name_with_dots(self):
        url = "https://ssm-instance-us-1-191207960751-git.us-central1.sourcemanager.dev/my-project/platform.gke.git"
        self._assert_components(ssm_client.parse_ssm_url(url), repository="platform.gke")

    def test_parse_ssm_url_invalid(self):
        for url in (
            "https://github.com/my-org/my-repo",
            # missing the PROJECT_ID path segment
            "https://ssm-instance-us-1-191207960751.us-central1.sourcemanager.dev/my-target-repo.git",
            # project ID where the project number belongs
            "https://instance-project.us-central1.sourcemanager.dev/repo.git",
            # wrong domain
            "https://ssm-instance-us-1-191207960751-git.us-central1.sourcemanager.example/my-project/repo.git",
        ):
            with self.subTest(url=url), self.assertRaises(ValueError):
                ssm_client.parse_ssm_url(url)

    @patch("server.ssm_client.securesourcemanager_v1.SecureSourceManagerClient")
    def test_check_repository_exists_true(self, mock_client_class):
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        
        url = "https://ssm-instance-us-1-191207960751-git.us-central1.sourcemanager.dev/my-project/my-repo.git"
        client = ssm_client.SSMClient()
        
        res = client.check_repository_exists(url)
        self.assertTrue(res)
        mock_client.get_repository.assert_called_with(
            name="projects/191207960751/locations/us-central1/repositories/my-repo"
        )
        mock_client_class.assert_called_with(
            client_options={"api_endpoint": "securesourcemanager.us-central1.rep.googleapis.com"}
        )

    @patch("server.ssm_client.securesourcemanager_v1.SecureSourceManagerClient")
    def test_check_repository_exists_false(self, mock_client_class):
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.get_repository.side_effect = exceptions.NotFound("Not Found")
        
        url = "https://ssm-instance-us-1-191207960751-git.us-central1.sourcemanager.dev/my-project/my-repo.git"
        client = ssm_client.SSMClient()
        
        res = client.check_repository_exists(url)
        self.assertFalse(res)

    @patch("server.ssm_client.securesourcemanager_v1.SecureSourceManagerClient")
    def test_create_repository_instance_exists(self, mock_client_class):
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        
        # Mock instance to exist and be active
        mock_instance = MagicMock()
        mock_instance.state = ssm_client.securesourcemanager_v1.Instance.State.ACTIVE
        mock_client.get_instance.return_value = mock_instance
        
        # Mock repository LRO operation
        mock_repo_op = MagicMock()
        mock_repo_op.done.return_value = True
        mock_client.create_repository.return_value = mock_repo_op
        
        url = "https://ssm-instance-us-1-191207960751-git.us-central1.sourcemanager.dev/my-project/my-repo.git"
        client = ssm_client.SSMClient()
        
        res = client.create_repository(url)
        self.assertTrue(res)
        
        # Verify get_instance is called, but create_instance is NOT called
        mock_client.get_instance.assert_called_with(name="projects/191207960751/locations/us-central1/instances/ssm-instance-us-1")
        mock_client.create_instance.assert_not_called()
        mock_client.create_repository.assert_called_once()

    @patch("server.ssm_client.securesourcemanager_v1.SecureSourceManagerClient")
    def test_create_repository_instance_missing_auto_creates(self, mock_client_class):
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        
        # get_instance returns NotFound first (triggering creation), then returns active instance
        mock_instance = MagicMock()
        mock_instance.state = ssm_client.securesourcemanager_v1.Instance.State.ACTIVE
        mock_client.get_instance.side_effect = [exceptions.NotFound("not found"), mock_instance]
        
        # Mock create_instance LRO operation
        mock_inst_op = MagicMock()
        mock_inst_op.done.return_value = True
        mock_client.create_instance.return_value = mock_inst_op
        
        # Mock create_repository LRO operation
        mock_repo_op = MagicMock()
        mock_repo_op.done.return_value = True
        mock_client.create_repository.return_value = mock_repo_op
        
        url = "https://ssm-instance-us-1-191207960751-git.us-central1.sourcemanager.dev/my-project/my-repo.git"
        client = ssm_client.SSMClient()
        
        res = client.create_repository(url)
        self.assertTrue(res)
        
        # Verify it tries to create the instance first
        mock_client.create_instance.assert_called_once()
        called_args, called_kwargs = mock_client.create_instance.call_args
        self.assertEqual(called_kwargs["parent"], "projects/191207960751/locations/us-central1")
        self.assertEqual(called_kwargs["instance_id"], "ssm-instance-us-1")
        
        # Verify repository creation proceeds
        mock_client.create_repository.assert_called_once()

    @patch("server.ssm_client.securesourcemanager_v1.SecureSourceManagerClient")
    def test_trigger_create_instance(self, mock_client_class):
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        
        mock_op = MagicMock()
        mock_op.operation.name = "projects/my-project/locations/us-central1/operations/inst-op-123"
        mock_client.create_instance.return_value = mock_op
        
        client = ssm_client.SSMClient()
        lro_name = client.trigger_create_instance_by_details("my-project", "us-central1", "my-instance")
        
        self.assertEqual(lro_name, "projects/my-project/locations/us-central1/operations/inst-op-123")
        mock_client.create_instance.assert_called_once()

    @patch("server.ssm_client.securesourcemanager_v1.SecureSourceManagerClient")
    def test_trigger_create_repository(self, mock_client_class):
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        
        mock_op = MagicMock()
        mock_op.operation.name = "projects/my-project/locations/us-central1/operations/repo-op-123"
        mock_client.create_repository.return_value = mock_op
        
        client = ssm_client.SSMClient()
        lro_name = client.trigger_create_repository_by_details("my-project", "us-central1", "my-instance", "my-repo")
        
        self.assertEqual(lro_name, "projects/my-project/locations/us-central1/operations/repo-op-123")
        mock_client.create_repository.assert_called_once()

    @patch("server.ssm_client.securesourcemanager_v1.SecureSourceManagerClient")
    def test_trigger_create_instance_relative_name(self, mock_client_class):
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        
        mock_op = MagicMock()
        mock_op.operation.name = "operations/inst-op-123"
        mock_client.create_instance.return_value = mock_op
        
        client = ssm_client.SSMClient()
        lro_name = client.trigger_create_instance_by_details("my-project", "us-central1", "my-instance")
        
        self.assertEqual(lro_name, "projects/my-project/locations/us-central1/operations/inst-op-123")

    @patch("server.ssm_client.securesourcemanager_v1.SecureSourceManagerClient")
    def test_trigger_create_repository_relative_name(self, mock_client_class):
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        
        mock_op = MagicMock()
        mock_op.operation.name = "operations/repo-op-123"
        mock_client.create_repository.return_value = mock_op
        
        client = ssm_client.SSMClient()
        lro_name = client.trigger_create_repository_by_details("my-project", "us-central1", "my-instance", "my-repo")
        
        self.assertEqual(lro_name, "projects/my-project/locations/us-central1/operations/repo-op-123")

    @patch("server.ssm_client.securesourcemanager_v1.SecureSourceManagerClient")
    def test_get_operation_status_pending(self, mock_client_class):
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        
        mock_op_proto = MagicMock()
        mock_op_proto.done = False
        mock_client.transport.operations_client.get_operation.return_value = mock_op_proto
        
        client = ssm_client.SSMClient()
        done, err, response = client.get_operation_status("us-central1", "projects/my-project/locations/us-central1/operations/repo-op-123")
        
        self.assertFalse(done)
        self.assertIsNone(err)
        self.assertIsNone(response)

    @patch("server.ssm_client.securesourcemanager_v1.SecureSourceManagerClient")
    def test_get_operation_status_done_success(self, mock_client_class):
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        
        mock_op_proto = MagicMock()
        mock_op_proto.done = True
        mock_op_proto.HasField.return_value = False  # No error field
        mock_response = MagicMock()
        mock_op_proto.response = mock_response
        mock_client.transport.operations_client.get_operation.return_value = mock_op_proto
        
        client = ssm_client.SSMClient()
        done, err, response = client.get_operation_status("us-central1", "projects/my-project/locations/us-central1/operations/repo-op-123")
        
        self.assertTrue(done)
        self.assertIsNone(err)
        self.assertEqual(response, mock_response)

    @patch("server.ssm_client.securesourcemanager_v1.SecureSourceManagerClient")
    def test_get_operation_status_not_found_treated_as_pending(self, mock_client_class):
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.transport.operations_client.get_operation.side_effect = exceptions.NotFound("Operation not found yet")
        
        client = ssm_client.SSMClient()
        done, err, response = client.get_operation_status("us-central1", "projects/my-project/locations/us-central1/operations/repo-op-123")
        
        self.assertFalse(done)
        self.assertIsNone(err)
        self.assertIsNone(response)

    @patch("server.ssm_client.securesourcemanager_v1.SecureSourceManagerClient")
    def test_create_pull_request_success(self, mock_client_class):
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        
        # Mock Pull Request creation LRO operation
        mock_op = MagicMock()
        mock_op.done.return_value = True
        
        mock_pr = MagicMock()
        mock_pr.name = "projects/191207960751/locations/us-central1/repositories/my-repo/pullRequests/12"
        mock_op.result.return_value = mock_pr
        
        mock_client.create_pull_request.return_value = mock_op
        
        url = "https://ssm-instance-us-1-191207960751-git.us-central1.sourcemanager.dev/my-project/my-repo.git"
        client = ssm_client.SSMClient()
        
        pr_url = client.create_pull_request(
            repo_url=url,
            title="My PR Title",
            body="My PR Body",
            source_branch="my-feature",
            target_branch="main"
        )
        
        expected_console_url = "https://console.cloud.google.com/secure-source-manager/locations/us-central1/instances/ssm-instance-us-1/repositories/my-repo/pull-requests/12?project=191207960751"
        self.assertEqual(pr_url, expected_console_url)
        
        mock_client.create_pull_request.assert_called_once()
        called_args, called_kwargs = mock_client.create_pull_request.call_args
        self.assertEqual(called_kwargs["parent"], "projects/191207960751/locations/us-central1/repositories/my-repo")
        self.assertEqual(called_kwargs["pull_request"]["title"], "My PR Title")
        self.assertEqual(called_kwargs["pull_request"]["body"], "My PR Body")
        self.assertEqual(called_kwargs["pull_request"]["base"]["ref"], "refs/heads/main")
        self.assertEqual(called_kwargs["pull_request"]["head"]["ref"], "refs/heads/my-feature")

if __name__ == "__main__":
    unittest.main()
