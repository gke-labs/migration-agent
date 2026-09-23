---
name: "gke-migration-join"
description: "Join the GKE migration ledger workspace and initialize local repository configuration."
commands:
  - name: "/gke-migration-join"
    description: "Join local workstation context to remote ledger workspace"
    args:
      - name: "ledger_uri"
        description: "URI of the target migration state ledger (e.g., gs://bucket-name)"
        required: false
      - name: "--role"
        description: "Request a specific role to assume during onboarding (e.g. platform, admins)"
        required: false
      - name: "--reconfigure"
        description: "Force reset of the local cached workspace configuration (restricted to admins)"
        required: false
      - name: "--component"
        description: "Developer joins only: the workload component id to claim (lowercase slug, e.g. orders-component)"
        required: false
      - name: "--reclaim-component"
        description: "Developer joins only: take over a component mid-flight from its current claimant (recorded in the component history)"
        required: false
---

# GKE Migration Join Skill

## Intent
Connect the local workstation context to a remote migration ledger, resolve the user's role, and execute the repository onboarding playbook.

## General Rules
* NEVER inspect the MCP server source code for any reason.
* NEVER attempt to run cloud CLI commands (such as `gcloud`, `gsutil`, `aws`, or similar CLIs) directly. The agent must strictly rely on the tools provided by the MCP server for all cloud and resource access.
* If the DAG enters a `TERMINAL` state (such as `STATE_ABORTED`), you must not make any attempts to troubleshoot the process or fix it; simply report the outcome to the user and terminate immediately.
* NEVER suggest pressing Enter (or submitting an empty response) to accept default values when prompting the user. Always instruct the user to explicitly write the default value or their preferred choice.

## Workflows

### `/gke-migration-join [ledger_uri] [--role <role>] [--reconfigure] [--component <id>] [--reclaim-component]`

1. If `ledger_uri` is not specified, prompt the user to enter the target GCS migration state ledger URI (e.g. `gs://my-migration-bucket`). Await their response.

2. Call the `join_ledger` tool on the `migration-dag` MCP server. Pass:
   - `ledger_uri`: the provided or gathered `<ledger_uri>`
   - `role`: `<role>` (if provided)
   - `reconfigure`: `True` (if `--reconfigure` flag is present, otherwise `False`)
   - `component`: `<component id>` (developer joins only; see the DEVELOPER section below)
   - `reclaim_component`: `True` only if `--reclaim-component` is present

3. Parse the string returned by the tool:
   - If the return starts with `ERROR:`, display the error message to the user and stop execution.
   - If the return value contains `SUCCESS: Joined ledger`, read the resolved role.
     - Note: The tool also caches the config to `~/.ledger_config.d/<sha256(cwd)[:16]>.yaml`,
       scoped to the server's working directory (the legacy `~/.ledger_config.yaml` is only a
       read fallback for sessions joined before the scoping, consumed on first read).
   
4. Execute the onboarding state machine loop based on the resolved role:
   - Call the `gke-migration-dag-executor` skill.
   - Refer to the corresponding **Role-Specific Stage Instructions** section below to handle each state.
   - Continue polling and executing state transitions until a terminal state is reached.

---

## Role-Specific Stage Instructions

### PLATFORM (or ADMINS acting as Platform)
Use these instructions when the resolved role is `platform` or `admins`:

* **`STATE_CONFIGURE_REPOSITORIES`**:
  Gather the repository configuration details by prompting the user for each input one by one. Do NOT ask for all inputs at once in a single message, as it is confusing to provide multiple inputs without a navigable interface:
  1. Ask the user for the **Source Repository URL** (e.g., `https://github.com/<org>/<repo>.git`).
  2. Once provided, ask for the **Source Branch** (e.g., `main`).
  3. Once provided, ask for the **Source Path** (e.g., `/src` or `/` - the subpath containing the source manifests).
  4. Once provided, prompt the user for the **Target Repository**.
     - Give a short pitch on Secure Source Manager (SSM): SSM is Google Cloud's highly secure, scalable, and fully managed Git repository service that integrates natively with Google Cloud security and IAM controls.
     - Ask the user if they want to create a new repository in SSM or use an existing standard Git repository:
       - If they want to use a NEW SSM repository, ask them for the following details one by one:
         a. **SSM Instance Name** (default: `migration-instance`)
         b. **SSM Location** (default: `us-central1`)
         c. **SSM Repository Name** (default: the workspace name)
       - If they want to use an EXISTING standard Git repository (or an existing SSM repository via direct URL), ask them to provide its Git URL (e.g., `https://github.com/<org>/<repo>.git`, or for SSM `https://<instance>-<project-number>-git.<location>.sourcemanager.dev/<project-id>/<repo>.git`).
  5. Once provided, ask for the **Target Branch** (e.g., `main`).
  6. Once provided, ask for the **Target Path** (default: `/` - the subpath where translated manifests will be written).
  
  Once you have gathered all these values, call the `configure_repositories` tool on the `migration-dag` MCP server, passing:
  - `source_repo_url`: `<source_repo_url>`
  - `source_branch`: `<source_branch>`
  - `source_path`: `<source_path>`
  - `target_branch`: `<target_branch>`
  - `target_path`: `<target_path>`
  - `target_repo_url`: `<target_repo_url>` (if existing/non-SSM, otherwise omit/pass null)
  - `ssm_instance`: `<ssm_instance>` (if new SSM, otherwise omit/pass null)
  - `ssm_location`: `<ssm_location>` (if new SSM, otherwise omit/pass null)
  - `ssm_repository`: `<ssm_repository>` (if new SSM, otherwise omit/pass null)

  When the `configure_repositories` tool call returns:
  - Evaluate the tool output. If the tool call succeeded, return control to the DAG execution loop.
  - If the tool call failed:
    - Present the failure message to the user.
    - Repeat the configuration prompts to correct the inputs and retry.

* **`STATE_CREATE_SSM_REPOSITORY`**:
  The Secure Source Manager repository or instance is currently provisioning in GCP.
  1. Call the `configure_repositories` tool on the `migration-dag` MCP server. You MUST pass the exact same repository configuration parameters that you gathered and passed in `STATE_CONFIGURE_REPOSITORIES` (i.e., `source_repo_url`, `source_branch`, `source_path`, `target_branch`, `target_path`, and either `target_repo_url` or `ssm_instance`, `ssm_location`, `ssm_repository`).
  2. Evaluate the tool output:
     - If the tool call succeeded (provisioning completed), return control to the DAG execution loop.
     - If the tool call failed:
       - Present the error details.
       - Return control to the DAG execution loop.
     - If GSSM creation is still pending:
       - Present the progress message and the GCP Console URL to the user.
       - Immediately loop back to step 1 to call the tool again. Do not wait or use a timer.

* **`STATE_ABORTED`**:
  This is a `TERMINAL` state.
  - Instruction: Display the message:
    "ERROR: The onboarding process was aborted. Please check details or consult with an administrator."
  - Terminate execution.

---

### DEVELOPER (registered developers, or admins and platform engineers de-escalating with `--role developers`)

A developer join claims one **component**: the slice of the estate one application team owns.
Each component has its own graph and its own folder in the ledger (`workloads/<component>/`).

1. If no `--component` was given, ask the user for the component id before calling `join_ledger`:
   a lowercase id that starts with a letter and contains only letters, digits and hyphens,
   2–63 characters (team convention: `<name>-component`, e.g. `orders-component`). If they
   want to take over a component someone else is working on, they must say so explicitly; only
   then pass `reclaim_component=True`.
2. Call `join_ledger` with `ledger_uri`, `role` (if given), `component`, and `reclaim_component`.
3. Read the result:
   - `ERROR:` — show it and stop. A naming-rule error means the id was invalid; ask again.
   - A message saying ledger writes are **blocked** because access to `workloads/<component>/`
     has not been granted yet — show the user the commands it contains **verbatim** and tell them
     an administrator must run them. Do not run them yourself. Once the admin confirms, the user
     joins again from the same folder.
   - `SUCCESS` — continue.
4. Call the `gke-migration-dag-executor` skill. The component graph runs the workload journey:
   agree the component's files (scope), plan them into unit families, translate, review,
   validate and push a pull-request branch to the target repository. Each state's instructions
   arrive from `get_next_stage`; follow them and stop at every approval form.
5. Re-joining later from the same folder with the same component resumes where the component
   left off. A component that shipped with a routing unit parked (the shared platform Gateway
   was not published yet) re-enters at the translate step on the first re-join after the
   Gateway is published, and produces a new pull request.
