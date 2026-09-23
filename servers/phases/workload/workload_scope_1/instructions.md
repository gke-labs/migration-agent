# Workload Step 1 — Agree the Component's File Scope

**DAG state:** `STATE_WKLD_SCOPE` · **Expected tool call:** `submit_workload_scope`

You are working one component (`variables.component`) of the workload pipeline.
The component id is an opaque handle the developer chose at join — it is not a
namespace, not a team name, and it proves nothing about which files belong to it.

## What to do

1. Call `browse_component_seed()` with no filters first. It reads ONLY the
   exports seed index (`exports.json` at the ledger root — the one platform
   object developers can read; never ask for or read `platform/*`).
2. Present the **terrain note** from that output to the user BEFORE any filter
   suggestion: it states which ownership signals actually discriminate in THIS
   estate (namespace count, team-label coverage, path prefixes). Namespace is
   one signal among several, never a component boundary — multiple teams can
   share one namespace (a real estate pattern), so do not equate the two.
3. The output also offers a **token-match guess** derived from the component id.
   It is labeled a guess; treat it as one candidate filter, nothing more. If it
   matched nothing, say so — that does not mean no candidates exist.
4. Iterate with the user: filter the seed (`path_glob`, `namespace`,
   `team_label`, `token`) and record decisions with
   `update_workload_scope(include=..., exclude=...)` — repeatable, no
   transition. The tool reports the resolved in-scope count after each change.
5. If the seed lists cluster-scoped kinds (StorageClass, Namespace, CRDs,
   node-pool CRs), advise excluding those files: they are platform-owned per
   the coverage map. This is advice, not enforcement.
6. **Degraded mode**: when the tool reports that exports or the seed index is
   absent, state that honestly. Includes/excludes are then recorded against the
   developer's own clone unverified — still agree them with the user; never
   guess paths and never present the gap as an error.
7. When the user is satisfied, call `submit_workload_scope()`. An empty
   resolution is refused — adjust the scope instead of forcing it through.

## What happens next

`submit_workload_scope` advances to `STATE_WKLD_SCOPE_CONFIRM`
(see [step 2](../workload_scopeconfirm_2/instructions.md)), where the user
signs the proposal off via an elicitation.

## Rules

- Read facts only from `browse_component_seed`; do not read estate files into
  this conversation.
- If a tool returns an `ERROR`, report it verbatim and stop.
