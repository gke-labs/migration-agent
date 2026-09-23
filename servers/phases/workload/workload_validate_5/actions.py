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

"""Server-side action for the workload PR submission state.

(variables, config) -> (transition_key, message): the
INTERNAL_TASK_SERVER_MUTATION contract, dispatched by
main.run_internal_mutation through the ACTIONS table below — the
landingzone actions.py arrangement, reused.

The commit stages ONLY the component's path (workloads/<component>/): a
per-component PR must never sweep in another component's files or anything
else sitting in the clone. On failure the graph returns to
STATE_WKLD_APPROVED (the deliberate divergence from the platform graph): a PR-submission failure invalidates nothing the
validate step checked, so the retry re-raises the ship approval directly.
"""

import logging
import os
import re

from servers.dag.server import git_client, ssm_client

logger = logging.getLogger("migration-dag")


def action_submit_workload_pr(variables: dict, config: dict) -> tuple:
    component = variables.get("component") or config.get("component")
    branch = variables.get("workload_branch_name")
    clone_dir = variables.get("workload_clone_path")
    target_url = variables.get("target_repo_url")
    target_branch = variables.get("target_branch")
    if not component or not branch or not clone_dir:
        return "on_failure", (
            "Workload PR coordinates are incomplete (component/branch/clone "
            "path) — run_workload_validation records them before this state.")
    if not os.path.isdir(clone_dir):
        return "on_failure", f"Target clone directory '{clone_dir}' does not exist."
    # isdir() alone is not the precondition git_client.stage_and_commit has:
    # run_workload_validation falls back to a SCRATCH directory when the
    # workspace records no target repository, and a plain directory reaches
    # git.Repo() and dies with an opaque InvalidGitRepositoryError. Name the
    # real cause instead.
    if not git_client.is_git_work_tree(clone_dir):
        return "on_failure", (
            f"Target clone directory '{clone_dir}' is not a git work tree — "
            "the component was validated over a scratch directory (no "
            "target repository was resolvable). Re-run "
            "run_workload_validation: it re-reads the coordinates "
            "(exports.target_repo, then the cached variables) and re-clones "
            "each run. If exports.target_repo is null, the platform side "
            "runs configure_repositories and then refresh_exports — "
            "developer sessions can neither read platform/ nor run those "
            "tools.")
    if not target_url:
        return "on_failure", (
            "No target_repo_url is resolved for this workspace, so there "
            "is nowhere to open the component Pull Request. Re-run "
            "run_workload_validation (it resolves the coordinates from "
            "exports.target_repo); if that field is null, the platform "
            "side runs configure_repositories, then refresh_exports.")

    # The commits are authored as the developer who ships them: the
    # session's resolved identity, which _resolve_session puts on the config.
    user_email = config.get("user_email")
    if not user_email:
        return "on_failure", (
            "Internal error: no caller identity on the session config (the "
            "session resolver always sets one), so the pull request's commits "
            "cannot be authored as you. Nothing was committed or pushed.")

    component_path = f"workloads/{component}"
    try:
        git_client.stage_and_commit(
            clone_dir,
            f"chore: translated workload component {component} (validated)",
            paths=[component_path], user_email=user_email)
    except Exception as e:
        return "on_failure", f"Git commit failed: {e}"
    try:
        git_client.rebase_and_push(clone_dir, branch, user_email=user_email)
    except Exception as e:
        return "on_failure", f"Git push failed: {e}"

    pr_title = f"Migrate workload component {component}"
    pr_body = variables.get("workload_pr_body") or (
        f"Automated Pull Request: translated Kubernetes manifests for "
        f"component {component}.")
    try:
        is_ssm = ssm_client.SSMClient().check_repository_exists(target_url)
    except Exception:
        is_ssm = False
    if is_ssm:
        try:
            pr_url = ssm_client.SSMClient().create_pull_request(
                repo_url=target_url, title=pr_title, body=pr_body,
                source_branch=branch, target_branch=target_branch)
            variables["workload_pull_request_url"] = pr_url
            return "on_success", (
                f"Pull Request created on Secure Source Manager: {pr_url}")
        except Exception as e:
            return "on_failure", f"Failed to create SSM Pull Request: {e}"
    try:
        match = re.search(
            r"github\.com[:/](?P<owner>[a-zA-Z0-9\-]+)/(?P<repo>[a-zA-Z0-9\-\_]+)",
            target_url or "")
        if match:
            repo = match.group("repo")
            repo = repo[:-4] if repo.endswith(".git") else repo
            pr_url = (f"https://github.com/{match.group('owner')}/{repo}/"
                      f"compare/{target_branch}...{branch}?expand=1")
            variables["workload_pull_request_url"] = pr_url
            return "on_success", f"Pull Request compare link created: {pr_url}"
        variables["workload_pull_request_url"] = target_url
        return "on_success", f"Branch pushed to target host. Branch: {branch}"
    except Exception as e:
        logger.warning(f"Failed to construct compare link: {e}")
        variables["workload_pull_request_url"] = target_url
        return "on_success", f"Branch pushed to target host. Branch: {branch}"


# Dispatch table read by main.run_internal_mutation, keyed by the "action"
# field of the developer graph's INTERNAL_TASK_SERVER_MUTATION states.
ACTIONS = {
    "submit_workload_pr": action_submit_workload_pr,
}
