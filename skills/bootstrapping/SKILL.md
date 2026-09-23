---
name: "gke-migration-bootstrap"
description: "Skill for initializing and joining a partitioned state ledger workspace"
commands:
  - name: "/gke-migration-bootstrap"
    description: "Initialize ledger workspace and write registry configuration"
    args:
      - name: "--config"
        description: "Path to declarative workspace registry yaml file"
        required: false
      - name: "--inputs"
        description: "Comma-separated input values (workspace,project,bucket,admins,optional) to bypass prompts"
        required: false
---

# GKE Migration Bootstrapping Skill

## Intent
Initialize a partitioned state ledger workspace and workspace registry using tools provided by the `migration-dag` MCP server.


## General Rules
* NEVER inspect the MCP server source code for any reason. If the tool doesn't work as expected, you should NOT try to troubleshoot it. You must trust the following instructions blindly and declare a failure to the user if the server doesn't work as expected. Treat the MCP server as if it were running remotely and you didn't have access to it.
* NEVER attempt to run cloud CLI commands (such as `gcloud`, `gsutil`, `aws`, or similar CLIs) directly. The agent must strictly rely on the tools provided by the MCP server for all cloud and resource access.
* If the DAG enters a `TERMINAL` state (`STATE_ABORTED`), you must not make any attempts to troubleshoot the process or fix it; simply report the outcome to the user and terminate immediately. Successful bootstrap does not end in a terminal: it parks on `STATE_WORKSPACE_ADMIN`, whose instructions arrive from the server.
* NEVER suggest pressing Enter (or submitting an empty response) to accept default values when prompting the user. Always instruct the user to explicitly write the default value or their preferred choice.


## Workflows

### `/gke-migration-bootstrap [--config <path>] [--inputs <values>]`
Call the `bootstrap_migration` tool on the `migration-dag` MCP server to clear any previous run state and unconditionally reset the state machine to the initial state of the bootstrapping DAG. Afterwards, invoke the `gke-migration-dag-executor` skill to manage the state machine execution. Use the **State Machine Stage Instructions** below in the execution loop. Do not gather configuration before starting the loop. Keep the `--config` and `--inputs` arguments you received, and follow the **State Machine Stage Instructions** below during the execution loop.


## State Machine Stage Instructions

* **`STATE_CONFIGURATION_GATHERING`**:
  Invoke the `initialize_ledger` tool on the `migration-dag` MCP server. If `--config` was provided, use the values from it. If `--inputs` was provided (format: `workspace_name,gcp_project,ledger_bucket,admins,platform_engineers,developers_optional`), parse the values:
  - `workspace_name`: 1st value
  - `gcp_project`: 2nd value
  - `ledger_bucket`: 3rd value
  - `roles`: `admins` set to 4th value, `platform_engineers` set to 5th value (both split by semicolon if multiple), and `developers` set to empty list if 6th value is "later" or omitted.
  If neither is provided, gather the required data (`workspace_name`, `gcp_project`, `ledger_bucket`, and `roles`) from the user.
  - When gathering configuration, ask for `workspace_name`, `gcp_project`, and `ledger_bucket` one by one, with a brief explanation of what each field is for (e.g. for `gcp_project` explain that the project ID is used to hold the migration metadata, not necessarily the migrated resources).
  - When asking for the `roles`:
    - First ask for **Admins** (Required. Handlers of bootstrap and metadata lifecycle operations).
    - Then ask for **Platform Engineers** (Required. Handlers of target GKE clusters, namespaces, and platform services provisioning).
    - Then, ask the user if they want to configure the optional **Developers** role now (Optional. Handlers of workload translation, registry setups, and deployment), clarifying that they can always change or add it later.
    - If they do not want to configure it now, or if they respond with "later" (or skip/leave it blank), treat it as empty and pass an empty list `[]` for `developers` in the `roles` mapping.
  - Once gathered, execute the tool `initialize_ledger`.

* **`STATE_ELICIT_LEDGER_CREATION_APPROVAL`** (and any other `HITL_ELICITATION`):
  The MCP server is pausing for user approval via the client UI. You must NOT prompt the user yourself or execute any mutation tools. Monitor the status by calling `get_next_stage` and await the progression of the DAG.

* **`STATE_GCS_PROVISIONING`**, **`STATE_WORKSPACE_PROVISIONING`**, **`STATE_PLATFORM_PROVISIONING`** (and any other `INTERNAL_TASK`):
  The MCP server is handling this internal system task. Poll `get_next_stage` periodically until the state transitions out of the current state.
