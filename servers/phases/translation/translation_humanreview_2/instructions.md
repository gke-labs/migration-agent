# Translation Step 2 — Review the Generated Code and Tradeoffs

**DAG state:** `STATE_TRANSLATION_REVIEW` · **Expected tool call:** `approve_translation`

Every unit has its generated code — Terraform and/or Kubernetes manifests — and
a tradeoffs write-up. The client reviews them unit by unit and decides what
ships. **The code and tradeoffs are for the human to read from the unit blobs
in the ledger, not to be reprinted into this conversation** — point the user
there rather than pasting HCL or YAML back to them.

## What to do

1. Call `get_translation_results()` for the summary, then
   `get_translation_results(unit_id=...)` for each unit the user wants to discuss.
   This returns a lean view — status, the file list, assumptions, and open
   questions — deliberately **not** the file bodies or the full tradeoffs prose
   (those live in the ledger). Direct the user to the unit blobs to read the
   actual code and tradeoffs for a unit.
2. Work through the open questions and assumptions with the user — these are the
   items only they can answer.
3. Record their decision:
   - **Approve** — `approve_translation(action="approve")`. Requires every
     non-skipped unit to be done.
   - **Revise specific units** — `request_unit_revision(unit_ids=[...],
     feedback="...")` with their exact feedback; only those units are
     retranslated, and the worker must address the feedback in its tradeoffs.
   - **Skip units** — `skip_translation_units(unit_ids=[...], reason="...")`
     for units the client decides not to migrate (or that persistently fail);
     skipped units stop blocking approval and the reason is recorded.
   - **Retranslate everything** — `approve_translation(action="retranslate",
     feedback="...")` if the whole approach needs to change.

## What happens next

- `approve` advances to `STATE_TRANSLATION_VALIDATE`
  ([translation step 3](../translation_validate_3/instructions.md)), where the
  generated code is validated and, when clean, the ship approval and the Pull
  Request follow; the approved code lives in the ledger under
  `platform/translation/units/`.
- `request_unit_revision` / `retranslate` return to `STATE_TRANSLATION_RUNNING`
  ([translation step 1](../translation_translate_1/instructions.md)); only pending
  or revised units are re-run.

## Rules

- Never approve on the user's behalf; this step exists to capture their sign-off.
- Relay reviewer feedback verbatim into `request_unit_revision` — the worker sees
  exactly what you pass.
- If a tool returns an `ERROR`, report it to the user verbatim and stop.
