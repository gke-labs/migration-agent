# Landing Zone Step 4 — Confirm the Translation Plan

**DAG state:** `STATE_LZ_TRANSLATION_PLAN_REVIEW` · **Expected tool call:** `confirm_translation_plan`

The inventory is decomposed into translation units. Before subagents spend LLM
calls generating code, the client decides which units are actually in scope.

## What to do

1. Walk the user through the plan, unit by unit: what each unit will translate and
   which landing-zone decision it honors. If the plan's `coverage` section lists
   findings (omitted rows, overlapping rows, uncited units, citations of unknown
   rows, map rows naming unknown inventory sections), surface them: overlap and
   traceability are warnings for the sign-off, but an OMITTED row is the
   condition the validate step later enforces — it fails the run naming the
   row, unless the gate's `UNENFORCED_ROWS` pin excuses that row — so it
   deserves a decision here (rediscovery/re-plan, or an explicit skip), not a
   shrug. The sign-off prompt will repeat the finding names.
2. Apply their decisions with `update_translation_plan`:
   - `skip`: unit_ids they don't want translated (e.g. a nodegroup being retired).
   - `unskip`: restore previously skipped units.
   The tool returns the updated plan and can be called repeatedly.
   Units already `skipped` by the planner name an inventory section that held
   no facts — they are coverage lines, not choices to reverse here. If the
   user says the estate does have those facts (or a placeholder carries a
   WARNING about contradicting evidence), the fix is rediscovery, not
   `unskip`: unskipping without new facts hands the translator empty inputs.
3. When they are satisfied, call `confirm_translation_plan()`. The server then
   asks the user directly (an elicitation) to approve the landing zone plan —
   the target-shape decisions, the Terraform draft, and the unit list. That
   answer is the sign-off; never approve on the user's behalf, and do not call
   the tool again to act on their answer.

## What happens next

- **Approved** → the landing zone phase closes and translation picks the plan
  up: one subagent per unit generates Terraform and/or Kubernetes manifests
  plus a tradeoffs write-up, then the generated code is validated (terraform
  validate with an auto-fix pass; manifest structure checks), reviewed with a
  before/after view, and only then submitted as a PR.
- **Declined** → back to `STATE_LZ_DESIGN` to redo the design.

## Rules

- Never confirm on the user's behalf; this gate exists to capture their sign-off
  before translation spend.
- If a tool returns an `ERROR`, report it to the user verbatim and stop.
