# Step: Workspace administration

**DAG state:** `STATE_WORKSPACE_ADMIN` · **Expected tool call:** `describe_workspace` on arrival, then whichever the admin's request names — `describe_workspace`, `add_workspace_member`, `remove_workspace_member`, `reset_migration`, `upgrade_ledger_dags` or `reset_dag_state`

The workspace exists. This step keeps it right: the admin can change who is on which team, start the migration over, bring the ledger's graphs up to the version this server ships, or rewind one graph to its start. Nothing happens here unless the admin asks for it.

## On entry

1. Call `describe_workspace` and present what it returns: the workspace, project and bucket, the three role lists, the platform graph's version and current state, and each component with its version, state and claimant.
2. Remind the admin that the bucket address (`gs://…`) is the invitation to give the platform engineers and developers.
3. Tell the admin, in one sentence, that if they need any changes they can just ask — and name what can be changed:
   - add, remove, or move platform engineers and developers,
   - reset the entire migration (the workspace, its members and the bucket stay),
   - update the ledger to the latest graph versions,
   - reset the platform team's graph, or one component's graph, to its start.
4. **End your turn.** Hand control back to the admin and wait for their next request. Do not call `get_next_stage` again — it returns these same instructions.

## Handling a request

Map what the admin asks for to exactly one tool, then report what it returned and end your turn again. Stay in this state.

| The admin asks to… | Call |
|---|---|
| add someone as a platform engineer / developer, or move them between the two | `add_workspace_member(email, team)` with `team` = `platform` or `application` |
| remove someone | `remove_workspace_member(email)` |
| change or remove an admin | nothing — say the admin list is fixed at bootstrap and is not changed from here |
| start the migration over, wipe the ledger, begin again | `reset_migration()` |
| update / upgrade the ledger, pick up the new graph version | `upgrade_ledger_dags()` |
| reset the platform team, rewind onboarding, start the platform journey again | `reset_dag_state("platform")` |
| reset a developer's / a component's progress | `reset_dag_state("<component id>")` — take the id from `describe_workspace` if the admin names a person instead |
| see the current state of the workspace | `describe_workspace()` |

Rules that apply to every request:

- Use the exact email the admin typed. Do not correct spelling, add a domain, or offer an example address as a value.
- `reset_migration`, `reset_dag_state` and `upgrade_ledger_dags` raise an approval form themselves before touching anything. Do **not** ask the admin to confirm first; the form is the confirmation.
- A tool response starting with `ERROR:` is the outcome. Report it verbatim and stop; do not retry with different arguments, and never suggest editing the bucket by hand.
- A request that maps to none of these — deleting the bucket, changing the project, renaming the workspace — is not available from here. Say so and offer the nearest thing that is.
