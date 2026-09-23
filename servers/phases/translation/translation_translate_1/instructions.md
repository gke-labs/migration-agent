# Translation Step 1 — Translate Each Unit (Subagent per Unit)

**DAG state:** `STATE_TRANSLATION_RUNNING` · **Expected tool call:** `run_translation`

The plan is confirmed. Each unit now gets its own small-context translation
subagent: the unit's inventory slice and landing-zone decisions in, Terraform for
the google provider and/or Kubernetes manifests (for in-cluster objects such as
StorageClasses, Namespaces, or Gateways) plus an explicit tradeoffs write-up out.

## What to do

1. Call `run_translation()` — no arguments. The server fans the pending units out
   to workers (bounded concurrency), validates each result structurally, and
   persists per-unit results to the ledger. Units already done are not redone;
   units a reviewer sent back carry their feedback into the worker prompt.
   This can take several minutes — each unit's result is persisted to the ledger
   (platform/translation/units/<unit_id>.json) the moment its worker finishes,
   so the user can watch the units land there.
2. Read the returned summary: done / failed / skipped counts per unit. The
   generated code is **not** in the summary and should not be reprinted into
   this conversation — the human reads it from the unit blobs in the ledger.
3. If SOME units failed but at least one succeeded, the DAG has already moved to
   review — retry the failures from there (`request_unit_revision` on the failed
   units, or `approve_translation(action="retranslate")`). Only when NO unit
   succeeded does the DAG stay here, where re-running `run_translation()` retries
   them; if a unit keeps failing every run, exclude it with
   `skip_translation_units` and surface the error to the user.
4. If the tool reports missing LLM credentials, relay the message — the
   workstation needs `ANTHROPIC_API_KEY` or Vertex configuration.

## What happens next

The DAG advances to `STATE_TRANSLATION_REVIEW`
(see [translation step 2](../translation_humanreview_2/instructions.md)), where the
client reviews each unit's code and tradeoffs.

## Rules

- Never write the Terraform or manifests yourself in this conversation; every
  unit's code must come from its translation worker so results stay reviewable
  per unit.
- If the tool returns an `ERROR`, report it to the user verbatim and stop.
