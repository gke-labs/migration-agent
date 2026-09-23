import unittest
from unittest.mock import patch, MagicMock
import os
import git

REAL_EXISTS = os.path.exists

from server import git_client

class GitClientTest(unittest.TestCase):

    @patch("server.git_client.git.Repo.init")
    @patch("server.git_client.tempfile.mkdtemp")
    @patch("server.git_client.shutil.rmtree")
    @patch("server.git_client.open", create=True)
    def test_verify_git_write_permission_success(self, mock_open, mock_rmtree, mock_mkdtemp, mock_repo_init):
        mock_mkdtemp.return_value = "/tmp/fake-git-repo"
        mock_repo = self._repo_with_git_user_name("Ada Lovelace")
        mock_repo_init.return_value = mock_repo

        mock_origin = MagicMock()
        mock_repo.create_remote.return_value = mock_origin

        res = git_client.verify_git_write_permission("sso://fake-repo", "main",
                                                     user_email="ada@example.com")
        self.assertTrue(res)

        mock_repo.create_remote.assert_called_with("origin", "sso://fake-repo")
        mock_origin.push.assert_called_once()
        self.assertTrue(mock_origin.push.call_args.kwargs["dry_run"])
        mock_repo.git.custom_environment.assert_called()
        # The probe commit is the user's, like every other commit; the
        # identity goes to index.commit explicitly, since GitPython's native
        # commit does not read the subprocess environment.
        kwargs = mock_repo.index.commit.call_args.kwargs
        for who in ("author", "committer"):
            self.assertEqual((kwargs[who].name, kwargs[who].email),
                             ("Ada Lovelace", "ada@example.com"), who)

    @patch("server.git_client.git.Repo.init")
    @patch("server.git_client.tempfile.mkdtemp")
    @patch("server.git_client.shutil.rmtree")
    @patch("server.git_client.open", create=True)
    def test_verify_git_write_permission_failure(self, mock_open, mock_rmtree, mock_mkdtemp, mock_repo_init):
        mock_mkdtemp.return_value = "/tmp/fake-git-repo"
        mock_repo = self._repo_with_git_user_name("Ada Lovelace")
        mock_repo_init.return_value = mock_repo

        mock_origin = MagicMock()
        mock_repo.create_remote.return_value = mock_origin
        mock_origin.push.side_effect = git.exc.GitCommandError("push", "Permission Denied")

        res = git_client.verify_git_write_permission("sso://fake-repo", "main",
                                                     user_email="ada@example.com")
        self.assertFalse(res)

    @patch("server.git_client.tempfile.mkdtemp")
    def test_verify_git_write_permission_without_an_email_raises(self, mock_mkdtemp):
        # A caller error, not a denied write: nothing is even initialised.
        with self.assertRaises(ValueError):
            git_client.verify_git_write_permission("sso://fake-repo", "main", user_email="")
        mock_mkdtemp.assert_not_called()

    @patch("server.git_client.git.cmd.Git")
    def test_verify_git_path_exists_root(self, mock_git_class):
        mock_git = MagicMock()
        mock_git_class.return_value = mock_git
        mock_git.ls_remote.return_value = "fake_ref_hash\trefs/heads/main"
        
        res = git_client.verify_git_path_exists("sso://fake-repo", "main", "/")
        self.assertTrue(res)
        mock_git.ls_remote.assert_called_with("sso://fake-repo", "main")

    @patch("server.git_client.git.Repo.init")
    @patch("server.git_client.tempfile.mkdtemp")
    @patch("server.git_client.shutil.rmtree")
    @patch("server.git_client.os.path.exists")
    @patch("server.git_client.open", create=True)
    def test_verify_git_path_exists_subpath_exists(self, mock_open, mock_exists, mock_rmtree, mock_mkdtemp, mock_repo_init):
        mock_mkdtemp.return_value = "/tmp/fake-git-repo"
        mock_repo = MagicMock()
        mock_repo_init.return_value = mock_repo
        mock_origin = MagicMock()
        mock_repo.create_remote.return_value = mock_origin
        
        def exists_side_effect(path):
            if path == "/tmp/fake-git-repo/foo/bar":
                return True
            return REAL_EXISTS(path)
        mock_exists.side_effect = exists_side_effect
        
        res = git_client.verify_git_path_exists("sso://fake-repo", "main", "foo/bar")
        self.assertTrue(res)
        
        mock_origin.fetch.assert_called_with("main", depth=1)
        mock_repo.git.checkout.assert_called_with("origin/main")

    @patch("server.git_client.git.Repo.init")
    @patch("server.git_client.tempfile.mkdtemp")
    @patch("server.git_client.shutil.rmtree")
    @patch("server.git_client.os.path.exists")
    @patch("server.git_client.open", create=True)
    def test_verify_git_path_exists_subpath_missing(self, mock_open, mock_exists, mock_rmtree, mock_mkdtemp, mock_repo_init):
        mock_mkdtemp.return_value = "/tmp/fake-git-repo"
        mock_repo = MagicMock()
        mock_repo_init.return_value = mock_repo
        
        def exists_side_effect(path):
            if path == "/tmp/fake-git-repo/foo/bar":
                return False
            return REAL_EXISTS(path)
        mock_exists.side_effect = exists_side_effect
        
        res = git_client.verify_git_path_exists("sso://fake-repo", "main", "foo/bar")
        self.assertFalse(res)

    @patch("server.git_client.git.Repo.clone_from")
    @patch("server.git_client.os.path.exists")
    @patch("server.git_client.os.makedirs")
    @patch("server.git_client.shutil.rmtree")
    def test_clone_repository(self, mock_rmtree, mock_makedirs, mock_exists, mock_clone_from):
        mock_exists.return_value = True
        
        git_client.clone_repository("sso://my-repo", "my-branch", "/tmp/clone-dir")
        
        mock_rmtree.assert_called_once_with("/tmp/clone-dir", ignore_errors=True)
        mock_makedirs.assert_called_once_with("/tmp/clone-dir", exist_ok=True)
        mock_clone_from.assert_called_once_with(
            "sso://my-repo",
            "/tmp/clone-dir",
            branch="my-branch",
            depth=1,
            env={"GIT_TERMINAL_PROMPT": "0"}
        )

    @patch("server.git_client.git.Repo")
    def test_create_and_checkout_branch(self, mock_repo_class):
        mock_repo = MagicMock()
        mock_repo_class.return_value = mock_repo
        
        git_client.create_and_checkout_branch("/tmp/repo-dir", "new-feature-branch")
        
        mock_repo_class.assert_called_once_with("/tmp/repo-dir")
        mock_repo.git.checkout.assert_called_once_with(b="new-feature-branch")

    @staticmethod
    def _repo_with_git_user_name(name):
        """A repo whose `git config user.name` answers `name`; None means
        unset, which git reports with a non-zero exit."""
        repo = MagicMock()
        if name is None:
            repo.git.config.side_effect = git.exc.GitCommandError("config", 1)
        else:
            repo.git.config.return_value = name
        return repo

    def test_commit_identity_is_the_user_with_their_git_name(self):
        repo = self._repo_with_git_user_name("Ada Lovelace")
        env = git_client.commit_identity(repo, "ada@example.com")
        self.assertEqual(env["GIT_AUTHOR_NAME"], "Ada Lovelace")
        self.assertEqual(env["GIT_COMMITTER_NAME"], "Ada Lovelace")
        self.assertEqual(env["GIT_AUTHOR_EMAIL"], "ada@example.com")
        self.assertEqual(env["GIT_COMMITTER_EMAIL"], "ada@example.com")
        self.assertEqual(env["GIT_TERMINAL_PROMPT"], "0")
        # Asked of git itself, not of GitPython's config parser.
        repo.git.config.assert_called_once_with("user.name")

    def test_commit_identity_without_a_git_name_uses_the_email_as_name(self):
        # Blank, whitespace, or unset (git exits non-zero): the e-mail stands in.
        for missing in ("", "   ", None):
            env = git_client.commit_identity(
                self._repo_with_git_user_name(missing), "ada@example.com")
            self.assertEqual(env["GIT_AUTHOR_NAME"], "ada@example.com", missing)
            self.assertEqual(env["GIT_COMMITTER_NAME"], "ada@example.com", missing)

    def test_commit_identity_survives_an_unreadable_git_config(self):
        repo = MagicMock()
        repo.git.config.side_effect = OSError("no config")
        env = git_client.commit_identity(repo, "ada@example.com")
        self.assertEqual(env["GIT_AUTHOR_NAME"], "ada@example.com")

    def test_commit_identity_refuses_a_missing_email(self):
        # No fall-through to the machine's git config: a commit with no
        # resolved user is an error, not the product's or someone else's.
        for missing in ("", None):
            with self.assertRaises(ValueError):
                git_client.commit_identity(self._repo_with_git_user_name("x"), missing)

    @patch("server.git_client.git.Repo")
    def test_stage_and_commit(self, mock_repo_class):
        mock_repo = self._repo_with_git_user_name("Ada Lovelace")
        mock_repo_class.return_value = mock_repo

        git_client.stage_and_commit("/tmp/repo-dir", "test commit message",
                                    user_email="ada@example.com")

        mock_repo_class.assert_called_once_with("/tmp/repo-dir")
        mock_repo.git.add.assert_called_once_with(A=True)
        mock_repo.git.commit.assert_called_once_with(m="test commit message")
        # The commit ran under the user's identity, not a product one.
        mock_repo.git.custom_environment.assert_called_once_with(
            GIT_AUTHOR_NAME="Ada Lovelace", GIT_AUTHOR_EMAIL="ada@example.com",
            GIT_COMMITTER_NAME="Ada Lovelace", GIT_COMMITTER_EMAIL="ada@example.com",
            GIT_TERMINAL_PROMPT="0")

    @patch("server.git_client.git.Repo")
    def test_stage_and_commit_without_an_email_stages_and_commits_nothing(self, mock_repo_class):
        mock_repo = self._repo_with_git_user_name("Ada Lovelace")
        mock_repo_class.return_value = mock_repo

        with self.assertRaises(ValueError):
            git_client.stage_and_commit("/tmp/repo-dir", "test commit message",
                                        user_email="")

        mock_repo.git.add.assert_not_called()
        mock_repo.git.commit.assert_not_called()

    @patch("server.git_client.git.Repo")
    def test_rebase_and_push_branch_exists(self, mock_repo_class):
        mock_repo = self._repo_with_git_user_name("Ada Lovelace")
        mock_repo_class.return_value = mock_repo
        mock_origin = MagicMock()
        mock_repo.remote.return_value = mock_origin

        mock_repo.git.rev_parse.return_value = "some-commit-hash"

        git_client.rebase_and_push("/tmp/repo-dir", "main-branch",
                                   user_email="ada@example.com")

        mock_repo_class.assert_called_once_with("/tmp/repo-dir")
        mock_origin.fetch.assert_called_once()
        mock_repo.git.rev_parse.assert_called_with("origin/main-branch")
        mock_repo.git.pull.assert_called_once_with("origin", "main-branch", rebase=True)
        mock_origin.push.assert_called_once_with(refspec="HEAD:refs/heads/main-branch")
        # The rebase re-creates the commits, so it runs as the user too.
        for call in mock_repo.git.custom_environment.call_args_list:
            self.assertEqual(call.kwargs["GIT_COMMITTER_EMAIL"], "ada@example.com")
            self.assertEqual(call.kwargs["GIT_COMMITTER_NAME"], "Ada Lovelace")

    @patch("server.git_client.git.Repo")
    def test_rebase_and_push_branch_missing(self, mock_repo_class):
        mock_repo = self._repo_with_git_user_name("Ada Lovelace")
        mock_repo_class.return_value = mock_repo
        mock_origin = MagicMock()
        mock_repo.remote.return_value = mock_origin
        
        mock_repo.git.rev_parse.side_effect = git.exc.GitCommandError("rev-parse", "not found")

        git_client.rebase_and_push("/tmp/repo-dir", "new-branch",
                                   user_email="ada@example.com")
        
        mock_repo_class.assert_called_once_with("/tmp/repo-dir")
        mock_origin.fetch.assert_called_once()
        mock_repo.git.rev_parse.assert_called_with("origin/new-branch")
        mock_repo.git.pull.assert_not_called()
        mock_origin.push.assert_called_once_with(refspec="HEAD:refs/heads/new-branch")

if __name__ == "__main__":
    unittest.main()
