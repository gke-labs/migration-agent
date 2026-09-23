# Workload Step — Translate the Units

**DAG state:** `STATE_WKLD_TRANSLATE` · **Expected tool call:** `run_workload_translation`

The plan is approved ([plan review](../workload_planreview_4/instructions.md)).
This step fans the plan's active units out to translation workers — one
worker per unit, Kubernetes YAML only — and runs the server-side
deterministic pass (image map, IRSA→Workload Identity swap, storage-class
menu check) over the output before it persists. A unit whose plan recorded
pod DNS facts, or a render source it could not read, gets the pod DNS
mapping document (`pod-dns-translation.md`) appended to its worker prompt;
the validate step checks the shipped pod specs against those facts.

## What to do

1. Call `run_workload_translation()`. It runs for minutes: each unit's
   result lands incrementally under `workloads/<component>/units/` in the
   ledger.
2. Report the returned summary to the user faithfully, including:
   - **reused** units (a previous run's persisted blob passed the reuse
     predicate — they were NOT re-translated and that is correct, not a
     shortcut);
   - **parked** units (wkld-routing while `exports.gateway` is null, or
     published with a blank name/namespace — no worker runs for them; they
     are coverage awaiting a complete attach point);
   - **unparked** units: when the current exports has since published the
     shared Gateway, the tool refreshes parked routing units to `planned`
     at entry — the summary names the unparked units, any units it
     **re-queued**, and the old → new exports stamp. Report all of that
     verbatim: the re-stamp deliberately invalidates blobs translated
     against the old exports, and units that were already `done` are
     demoted back to `planned` so they genuinely re-run (staleness posture,
     not a bug — that is the cost of a stamp that covers the whole plan).
     A component that re-entered from DONE via a re-join lands
     here and unparks the same way;
   - an **Unpark DECLINED** line, when exports moved in fields beyond
     `gateway`. Nothing was unparked and nothing was written. Do not retry
     the translation hoping for a different answer and do not describe the
     parked unit as broken: the remedy the line names is
     `approve_workload_translation(action='replan')`, which rebuilds every
     unit's brief from the current exports. Relay the refusal and that
     remedy to the user.
   - **failed** units and their errors, verbatim.

## What happens next

Once at least one active unit is done the DAG advances to
`STATE_WKLD_REVIEW` (see [unit review](../workload_review_4/instructions.md)).
Failed units stay marked `error` for the reviewer to revise, skip, or re-run.

## Rules

- Never re-run the tool while it reports a run in flight (the lease error);
  wait or let the lease expire — a second fan-out would clobber artifacts.
- Never paste generated YAML into chat; the human reads the unit blobs.
- Do not hand-edit image references, WI annotations, or storage class
  names in follow-up conversation: on plain manifests the deterministic
  pass owns them, and inside chart/kustomize sources the translation
  worker transcribes the exports literals its brief lists — either way
  the findings are already routed to the unit's open questions, and the
  validate step re-checks the rendered output.
- If the tool returns an `ERROR`, report it verbatim and stop.
