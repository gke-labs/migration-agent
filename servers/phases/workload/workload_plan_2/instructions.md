# Workload Step — Build the Translation Plan

**DAG state:** `STATE_WKLD_PLAN` · **Expected tool call:** `plan_workload_translation`

The component's file scope is confirmed
([scope step](../workload_scope_1/instructions.md)). This step turns the
scoped files of the developer's LOCAL clone into the four-unit translation
plan (wkld-manifests / wkld-identity / wkld-storage / wkld-routing).

## What to do

1. Ask the user for the path of their local source checkout (the clone the
   confirmed scope's globs select from) if you do not already know it.
2. Call `plan_workload_translation(source_root=<that path>)`. The planner
   is deterministic pure code: it enumerates the scoped files, renders Helm
   charts (`helm template`, in-repo values only) and kustomize directories
   (`kubectl kustomize`, local bases only) and classifies every document
   via the closed kind→family table. Nothing is sent to an LLM.
3. Present the returned unit summary to the user in full: planned units,
   **parked** units (routing facts without a complete Gateway attach point:
   `exports.gateway` is null, or is published but missing a name or a
   namespace — they unpark at translate entry once BOTH are present, no
   re-plan needed), and **placeholder** units (families with no documents) — the
   plan is a coverage claim, so the empty cells matter as much as the full
   ones. Mention any plan-level notes (render failures, remote-base
   refusals, parse failures): each is honest degradation to review, not an
   error to fix silently. When the manifests unit's brief lists pod DNS
   facts (`dnsPolicy`, `dnsConfig`, `hostAliases`), say so: those pods
   name resolvers or search domains that may not exist on GKE, and the
   translation worker is given the mapping document for them.

## What happens next

`plan_workload_translation` advances to `STATE_WKLD_PLAN_REVIEW`
(see [plan review](../workload_planreview_4/instructions.md)), where the
user signs the plan off via an elicitation.

## Rules

- The planner reads only the local clone and `exports.json` — never
  `platform/*`, and never ask the user for platform artifacts.
- An all-placeholder plan is refused: the fix is the scope or the
  source_root, never forcing an empty plan through.
- If the tool returns an `ERROR`, report it verbatim and stop.
