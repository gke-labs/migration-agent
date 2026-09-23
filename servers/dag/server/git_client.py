import tempfile
import uuid
import shutil
import logging
import os
import git

logger = logging.getLogger("migration-dag")

# Disable interactive terminal prompts for all Git operations to prevent hanging
os.environ["GIT_TERMINAL_PROMPT"] = "0"

def verify_git_write_permission(repo_url: str, branch: str, *, user_email: str) -> bool:
    """Verifies write permissions to a remote Git repository using a dry-run push.

    The probe commit is made as the user (commit_identity) like every other
    commit the product makes, although a dry run sends no object, and so no
    identity, to the remote: one rule, with no exception to remember. A
    missing e-mail raises rather than reading as a denied write.
    """
    if not user_email:
        raise ValueError(
            "No caller identity for the write check: the session resolved no e-mail.")
    temp_dir = tempfile.mkdtemp()
    logger.debug(f"Initializing ephemeral Git repository at {temp_dir} for write verification.")
    try:
        repo = git.Repo.init(temp_dir)
        env = commit_identity(repo, user_email)
        actor = git.Actor(env["GIT_AUTHOR_NAME"], env["GIT_AUTHOR_EMAIL"])

        # Write dummy file
        dummy_file = f"{temp_dir}/git-write-check"
        with open(dummy_file, "w") as f:
            f.write("GKE Agentic Migration repository configuration validation.")

        repo.index.add([dummy_file])

        # index.commit is GitPython-native and does not read the git
        # subprocess environment, so the identity is passed explicitly.
        repo.index.commit("Validation commit from GKE Agentic Migration",
                          author=actor, committer=actor)

        git_cmd = repo.git
        # Add remote
        origin = repo.create_remote("origin", repo_url)
        
        # Perform dry-run push to a temporary validation branch
        check_branch = f"refs/heads/migration-agent/auth-check-{uuid.uuid4()}"
        logger.debug(f"Executing dry-run push to branch {check_branch} on remote {repo_url}")
        
        with git_cmd.custom_environment(**env):
            origin.push(refspec=f"HEAD:{check_branch}", dry_run=True)
            
        logger.debug("Git dry-run push verification succeeded.")
        return True
    except git.exc.GitCommandError as e:
        logger.error(f"Git write verification failed with error: {e}")
        return False
    except Exception as e:
        logger.exception(f"Unexpected error during Git write verification: {e}")
        return False
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)

def verify_git_path_exists(repo_url: str, branch: str, path: str) -> bool:
    """Verifies that a directory path exists inside a remote Git branch using sparse checkout."""
    logger.debug(f"Verifying path '{path}' exists in remote repo '{repo_url}' on branch '{branch}'")
    
    if not path or path == "/":
        # Root path is always considered to exist if the branch exists
        g = git.cmd.Git()
        try:
            ls_remote_out = g.ls_remote(repo_url, branch)
            return bool(ls_remote_out)
        except git.exc.GitCommandError as e:
            logger.error(f"Failed to verify remote branch '{branch}': {e}")
            return False
            
    temp_dir = tempfile.mkdtemp()
    try:
        repo = git.Repo.init(temp_dir)
        origin = repo.create_remote("origin", repo_url)
        
        # Configure sparse checkout
        repo.git.config("core.sparseCheckout", "true")
        
        cleaned_path = path.strip("/")
        sparse_file = os.path.join(temp_dir, ".git", "info", "sparse-checkout")
        os.makedirs(os.path.dirname(sparse_file), exist_ok=True)
        with open(sparse_file, "w") as f:
            f.write(f"{cleaned_path}\n")
            
        logger.debug(f"Fetching remote metadata from {repo_url} branch {branch}")
        origin.fetch(branch, depth=1)
        
        repo.git.checkout(f"origin/{branch}")
        
        local_path = os.path.join(temp_dir, cleaned_path)
        exists = os.path.exists(local_path)
        logger.debug(f"Local sparse path check for {local_path} resulted in exists={exists}")
        return exists
    except git.exc.GitCommandError as e:
        logger.error(f"Sparse checkout check failed: {e}")
        return False
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)

def clone_repository(repo_url: str, branch: str, target_dir: str) -> None:
    """Clones a remote repository branch to a local directory."""
    logger.info(f"Cloning {repo_url} branch {branch} to {target_dir}")
    if os.path.exists(target_dir):
        shutil.rmtree(target_dir, ignore_errors=True)
    os.makedirs(target_dir, exist_ok=True)
    try:
        env = {"GIT_TERMINAL_PROMPT": "0"}
        git.Repo.clone_from(repo_url, target_dir, branch=branch, depth=1, env=env)
        logger.info("Clone completed successfully.")
    except Exception as e:
        logger.error(f"Failed to clone repository: {e}")
        raise

def is_git_work_tree(path: str) -> bool:
    """True when `path` is inside a git work tree.

    The precondition stage_and_commit actually has: an ordinary directory
    reaches git.Repo() and raises InvalidGitRepositoryError, which callers
    then report as an opaque commit failure. Checking it up front lets them
    name the real cause instead.
    """
    if not path or not os.path.isdir(path):
        return False
    try:
        git.Repo(path)
        return True
    except Exception:
        return False

def remote_origin_url(path: str):
    """The work tree's `origin` remote URL, or None (no repo / no origin).

    The workload validate step's clone self-heal compares this against the
    CURRENT target coordinates before reusing an existing clone: a clone of
    coordinates that have since changed must be re-cloned, not silently
    reused with a stale origin.
    """
    try:
        return git.Repo(path).remotes.origin.url
    except Exception:
        return None

def create_and_checkout_branch(repo_dir: str, branch_name: str) -> None:
    """Creates a new branch locally and checks it out."""
    logger.info(f"Creating and checking out branch {branch_name} in {repo_dir}")
    try:
        repo = git.Repo(repo_dir)
        repo.git.checkout(b=branch_name)
    except Exception as e:
        logger.error(f"Failed to create and checkout branch: {e}")
        raise

def commit_identity(repo: "git.Repo", user_email: str) -> dict:
    """Author and committer environment for a commit made on the user's behalf.

    The e-mail is the caller's ledger identity — the account every ledger
    write is authorized as — so the commits in the pull request name the
    person who shipped them, not the product. The display name is the
    user's own git ``user.name``, asked of git itself so every config level
    and override git honours applies and the value comes back verbatim;
    without one the e-mail stands in rather than a guessed name. Raises
    ValueError when no e-mail was resolved: a commit must not fall back to
    whatever identity the machine's git config happens to carry.
    """
    if not user_email:
        raise ValueError(
            "No caller identity for the commit: the session resolved no e-mail.")
    try:
        name = repo.git.config("user.name")  # exits non-zero when unset
    except Exception as e:  # unset or unreadable: not a reason to fail the ship
        logger.debug(f"No git user.name for the commit identity: {e}")
        name = ""
    name = str(name or "").strip() or user_email
    return {
        "GIT_AUTHOR_NAME": name,
        "GIT_AUTHOR_EMAIL": user_email,
        "GIT_COMMITTER_NAME": name,
        "GIT_COMMITTER_EMAIL": user_email,
        "GIT_TERMINAL_PROMPT": "0",
    }


def stage_and_commit(repo_dir: str, message: str, paths: list = None, *,
                     user_email: str) -> None:
    """Stages changes and commits them as the user (see commit_identity).

    paths=None stages everything (the landing-zone ship). A non-empty paths
    list stages ONLY those repo-relative paths — the workload PR commits a
    single component's directory and must never sweep in the rest of the
    clone.
    """
    logger.info(f"Staging changes and committing to {repo_dir} "
                f"(paths={paths or 'ALL'})")
    try:
        repo = git.Repo(repo_dir)
        env = commit_identity(repo, user_email)
        if paths:
            repo.git.add("--", *paths)
        else:
            repo.git.add(A=True)

        # Nothing staged is not an error: a re-run after the commit already
        # landed (e.g. retrying the PR submission) leaves the working tree
        # matching HEAD. Committing anyway would fail with "nothing to commit"
        # and wedge the ship step, so treat a clean index as a no-op.
        if not repo.git.diff("--cached", "--name-only").strip():
            logger.info("Nothing to commit; the branch already carries the generated code.")
            return

        with repo.git.custom_environment(**env):
            repo.git.commit(m=message)
        logger.info("Commit completed successfully.")
    except Exception as e:
        logger.error(f"Failed to commit changes: {e}")
        raise

def rebase_and_push(repo_dir: str, branch: str, *, user_email: str) -> None:
    """Performs pre-push pull rebase and pushes to remote.

    The rebase re-creates the commits, so it runs with the same user
    identity as the commit (see commit_identity): git keeps each commit's
    original author and takes the committer from the environment. The
    push itself only needs the prompt switched off.
    """
    logger.info(f"Rebasing and pushing branch {branch} in {repo_dir}")
    try:
        repo = git.Repo(repo_dir)
        env = commit_identity(repo, user_email)
        
        origin = repo.remote("origin")
        logger.debug("Fetching from remote origin...")
        with repo.git.custom_environment(**env):
            origin.fetch()
            
        remote_branch_exists = False
        try:
            repo.git.rev_parse(f"origin/{branch}")
            remote_branch_exists = True
        except git.exc.GitCommandError:
            logger.info(f"Branch origin/{branch} does not exist yet on remote. Skipping pre-push rebase.")
            
        if remote_branch_exists:
            logger.debug(f"Pulling and rebasing from origin/{branch}")
            with repo.git.custom_environment(**env):
                repo.git.pull("origin", branch, rebase=True)
            
        logger.debug(f"Pushing refs to origin/{branch}")
        with repo.git.custom_environment(**env):
            origin.push(refspec=f"HEAD:refs/heads/{branch}")
        logger.info("Push completed successfully.")
    except Exception as e:
        logger.error(f"Failed to rebase and push: {e}")
        raise
