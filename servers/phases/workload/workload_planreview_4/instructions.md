# Workload Step — Review and Approve the Plan

**DAG state:** `STATE_WKLD_PLAN_REVIEW` · **Expected tool call:** `confirm_workload_plan`

A translation plan for this component is recorded from the
[plan step](../workload_plan_2/instructions.md). This step captures the
human sign-off before any translation work runs.

## What to do

1. Summarize the plan for the user: every unit with its family, status
   (`planned` / `parked` / `skipped`), whether it is a placeholder, the
   fact-derived brief highlights, the chart carriers, and the plan-level
   notes. Parked and placeholder units are part of the summary — coverage,
   not silence.
2. If the user wants units in or out, call
   `update_workload_plan(skip=[...], unskip=[...])` — repeatable, no
   transition. Do not unskip placeholder units; they carry no facts.
3. Call `confirm_workload_plan()`. The server raises the approval
   elicitation directly — the user's answer there IS the decision. Do not
   ask for approval in chat first, and do not call the tool again to act
   on the answer.

## What happens next

- **Approve** → the component parks at `STATE_WKLD_AWAIT_PIPELINE`
  (see [await step](../workload_await_3/instructions.md)) until the
  translation milestone ships; the plan stays persisted at
  `workloads/<component>/plan.json`.
- **Reject** → back to `STATE_WKLD_PLAN` for a re-plan. The planner is
  deterministic: change the scope or the checkout before re-planning.

## Rules

- If a tool returns an `ERROR`, report it verbatim and stop.
