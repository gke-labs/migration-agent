# Workload Step — Review the Translated Units

**DAG state:** `STATE_WKLD_REVIEW` · **Expected tool call:** `approve_workload_translation`

Workers have produced each unit's Kubernetes YAML plus tradeoffs,
assumptions and open questions
([translate step](../workload_translate_3/instructions.md)). The developer
now reviews per unit and decides what moves forward.

## What to do

1. Call `get_workload_results()` for the plan-wide summary, then
   `get_workload_results(unit_id=...)` per unit the user wants to discuss.
   Walk the user through every assumption and open question — the code and
   tradeoffs prose live in the ledger unit blobs
   (`workloads/<component>/units/`), not in chat.
2. Act on the user's decisions:
   - `request_workload_unit_revision(unit_ids, feedback)` — sends the named
     units back to `STATE_WKLD_TRANSLATE`; ONLY those units re-run.
   - `skip_workload_units(unit_ids, reason)` — excludes units (recorded,
     visible, reversible at plan review); no transition.
   - `approve_workload_translation(action='approve')` — Gate D analog:
     every unit that is not skipped or parked must be done; advances to
     `STATE_WKLD_VALIDATE`
     (see [validate step](../workload_validate_5/instructions.md)).
   - `approve_workload_translation(action='retranslate')` — redoes every
     active unit.
   - `approve_workload_translation(action='replan')` — returns to
     `STATE_WKLD_PLAN` without touching unit statuses. This is the only
     action that clears a validate **staleness** finding: every unit blob is
     stamped with the plan's frozen exports stamp, so retranslating writes
     the same stale stamp back and the finding returns. Re-planning rebuilds
     the briefs and the stamp from the current `exports.json`.

## Rules

- Parked units (wkld-routing awaiting `exports.gateway`) cannot be revised
  — no worker ran — and never block approval; they ship as a named parked
  list, not silently.
- The validate step can send the component BACK here with findings
  (manifest gate, re-render gate, staleness, ledger blobs that carry no
  output, carrier-strategy disagreements, two-unit edits): treat those
  findings as the review agenda, not as an error to work around. Match the
  remedy to the finding — revision for content, `replan` for staleness.
- If a tool returns an `ERROR`, report it verbatim and stop.
