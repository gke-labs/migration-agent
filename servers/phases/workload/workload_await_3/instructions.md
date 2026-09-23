# Workload Step 3 — Await the Pipeline

**DAG state:** `STATE_WKLD_AWAIT_PIPELINE` · **Expected tool call:** none (report and stop)

The workload pipeline beyond this point is not yet shipped in this graph
version. This is a parking state, not a failure and not a TERMINAL.

## What to do

Report to the user that the component's scope is confirmed and the component
is parked awaiting the next pipeline milestone, then STOP — do not
troubleshoot, do not call tools, do not attempt to plan or translate anything.

When a newer milestone lands, re-join with `join_ledger` (same ledger, same
component) and this graph upgrades in place, mapping this state forward into
the new pipeline.
