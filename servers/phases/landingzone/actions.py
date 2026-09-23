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

"""Server-side action for the landing zone's PR submission state.

It is (variables, config) -> (transition_key, message), the
INTERNAL_TASK_SERVER_MUTATION contract the engine dispatches on:

  submit_lz_pr   (reached from the translation tail — opening the PR is the
                  last step of translation, after the generated code has been
                  validated and the human has approved the before/after)

It lives with the phase rather than in main.py because it is landing zone
behaviour, not engine lifecycle; main.run_internal_mutation reaches it
through the ACTIONS table at the bottom of this module.

Terraform validation of the generated code happens earlier, in the
translation validate step (run_generated_validation), not here.
"""

import logging
import os
import re

from servers.dag.server import git_client, ssm_client

from .workspace import target_clone_path

logger = logging.getLogger("migration-dag")


def action_submit_lz_pr(variables: dict, config: dict) -> tuple[str, str]:
    target_url = variables.get("target_repo_url")
    target_branch = variables.get("target_branch")
    lz_branch_name = variables.get("lz_branch_name")

    # The clone the validator materialized into and validated is the single
    # source of truth. Fall back to recomputing it from the branch uuid for
    # ledgers written before the path was recorded as a variable.
    target_dir = variables.get("target_clone_path") or target_clone_path(variables.get("lz_branch_uuid"))

    if not os.path.exists(target_dir):
        return "on_failure", f"Target clone directory '{target_dir}' does not exist."

    # The commits are authored as the user who ships them: the session's
    # resolved identity, which _resolve_session puts on the config.
    user_email = config.get("user_email")
    if not user_email:
        return "on_failure", (
            "Internal error: no caller identity on the session config (the "
            "session resolver always sets one), so the pull request's commits "
            "cannot be authored as you. Nothing was committed or pushed.")

    try:
        git_client.stage_and_commit(
            target_dir,
            "chore: generated GKE code — landing zone + translated units (validated)",
            user_email=user_email)
    except Exception as e:
        return "on_failure", f"Git commit failed: {e}"

    try:
        git_client.rebase_and_push(target_dir, lz_branch_name, user_email=user_email)
    except Exception as e:
        return "on_failure", f"Git push failed: {e}"

    is_ssm = ssm_client.SSMClient().check_repository_exists(target_url)

    if is_ssm:
        try:
            client = ssm_client.SSMClient()
            pr_title = "Generate GKE Landing Zone"
            pr_body = "Automated Pull Request containing generated GKE landing zone Terraform HCL modules."
            pr_url = client.create_pull_request(
                repo_url=target_url,
                title=pr_title,
                body=pr_body,
                source_branch=lz_branch_name,
                target_branch=target_branch
            )
            variables["pull_request_url"] = pr_url
            return "on_success", f"Pull Request successfully created on Secure Source Manager: {pr_url}"
        except Exception as e:
            return "on_failure", f"Failed to create SSM Pull Request: {e}"
    else:
        try:
            match = re.search(r"github\.com[:/](?P<owner>[a-zA-Z0-9\-]+)/(?P<repo>[a-zA-Z0-9\-\_]+)", target_url)
            if match:
                owner = match.group("owner")
                repo = match.group("repo")
                if repo.endswith(".git"):
                    repo = repo[:-4]
                pr_url = f"https://github.com/{owner}/{repo}/compare/{target_branch}...{lz_branch_name}?expand=1"
                variables["pull_request_url"] = pr_url
                return "on_success", f"Pull Request compare link created: {pr_url}"
            else:
                variables["pull_request_url"] = target_url
                return "on_success", f"Branch pushed to target host. Branch: {lz_branch_name}"
        except Exception as e:
            logger.warning(f"Failed to construct compare link: {e}")
            variables["pull_request_url"] = target_url
            return "on_success", f"Branch pushed to target host. Branch: {lz_branch_name}"


# Dispatch table read by main.run_internal_mutation. Keyed by the "action"
# field of the INTERNAL_TASK_SERVER_MUTATION states in platform_dag.json.
ACTIONS = {
    "submit_lz_pr": action_submit_lz_pr,
}
