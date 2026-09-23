# Workload Step 2 — Confirm the Component Scope

**DAG state:** `STATE_WKLD_SCOPE_CONFIRM` · **Expected tool call:** `confirm_workload_scope`

A scope proposal for this component is recorded from
[step 1](../workload_scope_1/instructions.md). This step captures the human
sign-off.

## What to do

1. Summarize the proposal for the user: the include/exclude patterns, the
   resolved in-scope file count (or the degraded-mode statement), a few sample
   paths, and any cluster-scoped-kind warnings.
2. Call `confirm_workload_scope()`. The server raises the approval elicitation
   directly — the user's answer there IS the decision. Do not ask for approval
   in chat first, and do not call the tool again to act on the answer.

## What happens next

- **Approve** → the scope persists to `workloads/<component>/scope.json` and
  the graph advances to `STATE_WKLD_PLAN`
  (see [the plan step](../workload_plan_2/instructions.md)).
- **Reject** → back to `STATE_WKLD_SCOPE` for another pass. Nothing is
  persisted. Do not re-submit an unchanged proposal — change the scope first.

## The data-dependency advisory

The elicitation may open with a note listing AWS data services this
component uses that have not moved to GCP yet. It is a WARNING and nothing
more: approving over it is a normal, expected answer, and everything between
here and the pull request stays correct while a database is still moving.
Relay it, and do not treat it as a reason to narrow the scope or to wait.

The refusal comes later, at the [validate step](../workload_validate_5/instructions.md),
and only for the pull request itself. Nothing here is a developer's to fix:
the services are reported as they land by the platform team.

## Rules

- If the tool returns an `ERROR`, report it verbatim and stop.
