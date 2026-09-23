# Discovery Step 3b — Extract the Inventory (Map-Reduce)

**DAG state:** `STATE_DISCOVERY_RUNNING` · **Expected tool call:** `run_discovery_extraction`

The scope is confirmed. Your goal is to produce the ground-truth inventory
without loading the estate into your own context: the server fans the in-scope
files out to small-context LLM workers (one bounded chunk each), validates their
fragments against the inventory schema, merges them deterministically, and writes
both the inventory and a readiness report to the ledger.

## What to do

1. Call `run_discovery_extraction()` — no arguments needed; the server uses the
   scope the client confirmed in step 2 (root directory, exclusions, additions).
   It handles chunking, worker fan-out, schema validation, merge, report
   generation, and persistence. This can take a few minutes on large estates.
2. Read the returned summary. It includes chunk counts (total / reused / failed)
   and the decision triggers (`karpenter`, `privileged_daemonsets`, `gpu_tpu`,
   `vpc_peering`).
3. If some chunks failed, re-run `run_discovery_extraction` once — completed
   fragments are cached in the ledger, so only failed chunks are retried.
4. If the tool reports missing LLM credentials, relay the message to the user —
   the workstation running the server needs credentials for one of the worker
   backends: `GEMINI_API_KEY` (or `GOOGLE_GENAI_USE_VERTEXAI=True` + gcloud ADC)
   for the default backend, or `ANTHROPIC_API_KEY` / `CLAUDE_CODE_USE_VERTEX=1`
   + gcloud ADC for the Claude backup.

## Rules

- NEVER author the inventory yourself. `write_discovery_inventory` exists only
  as a manual fallback for when the **user explicitly directs** you to enter an
  inventory they supply; producing it from your own reading of the files defeats
  the schema-validated extraction pipeline this step exists for. If
  `run_discovery_extraction` fails, relay its error to the user and stop.

## Manual fallback

For very small estates you may instead analyze the files yourself and persist the
result with
`write_discovery_inventory(inventory_json=...)`. The JSON must follow
`servers/dag/server/schema/inventory.json`. Prefer `run_discovery_extraction` —
it keeps this conversation's context small.

## What happens next

Either tool advances the DAG to `STATE_ASSESSMENT`
(see [assessment step 1](../../assessment/assessment_review_1/instructions.md)), where the
user reviews the inventory and readiness report — and can still amend the scope,
which loops back here for a cheap delta extraction.
