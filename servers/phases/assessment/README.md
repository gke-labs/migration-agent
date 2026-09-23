# Assessment Phase

Converts the discovery inventory into a decision-ready readiness report, then holds the
migration at a gate until every blocker the report names has an owner and a target close
date. Discovery answers *what is there*; assessment answers *can it move, what stops it,
and who owns each thing that stops it*.

## Layout

```
servers/phases/assessment/
├── __init__.py                # register(mcp) — wires every step's tools into the server
├── assessment_review_1/       # STATE_ASSESSMENT: one review from extraction to blockers
│   ├── instructions.md        # agent instructions returned by get_next_stage
│   └── tools.py               # get_discovery_inventory, amend_discovery_scope, submit_assessment
├── assessment_blockers_2/     # STATE_BLOCKER_RESOLUTION: assign every blocker
│   ├── instructions.md
│   └── tools.py               # list_blockers, assign_blocker_owner
└── knowledge/
    └── migration-assessment.md
```

## The gate

`STATE_BLOCKER_RESOLUTION` will not release to `STATE_LZ_DESIGN` while any blocker lacks an
`owner` or a `target_close_date`. The check runs on the server, in
`assign_blocker_owner`, against the blocker list held in the ledger — the agent chooses
neither the outcome nor the moment. With no blockers at all, `submit_assessment`
returns `on_no_blockers` and the blocker step is skipped entirely.

`STATE_ASSESSMENT` itself is the single human review between discovery and the
landing zone: extraction already produced the inventory and readiness report,
so this step presents them, agrees the blocker list, and raises one approval
elicitation — what used to be a separate discovery review step plus an
assessment gate. Declining it is the rescan request (back to
`STATE_DISCOVERY`); a scope correction (`amend_discovery_scope`) loops back to
extraction without a decision.

## Where the blocker criteria come from

`knowledge/migration-assessment.md` is delivered to the agent at `STATE_ASSESSMENT`, and
its `### Step 4 — Identify blockers` table is *also* parsed by the server at start-up
(`servers/dag/server/blocker_criteria.py`). Every blocker submitted to
`submit_assessment` must carry a `category` drawn from that table.

This makes the markdown the single source: adding a blocker category is an edit to the
knowledge doc, not to the server. It also means breaking the table's shape stops the
server from starting, which is deliberate — the alternative is category validation
silently degrading to a no-op.

## Registering an owner who is not in the ledger

A blocker can be assigned to someone who has never joined the workspace. When that
happens `assign_blocker_owner` does not guess: it routes through
`STATE_CONFIRM_NEW_MEMBER`, a HITL elicitation that asks whether the person is on the
platform team (registered as a platform engineer) or an application team (registered as a
developer). Only after that answer does `STATE_REGISTER_MEMBER` write the registry and
re-run the ledger IAM provisioning.

Both halves are required. `provision_ledger_iam` grants the conditional read on
`workspace_registry.yaml` to the union of the three role lists, and the registry is read
with the caller's own credentials — so a member added to the registry without the IAM
re-run is registered and locked out.

## Adding a step

1. Create `servers/phases/assessment/assessment_<purpose>_N/` with `instructions.md` and
   (if the step needs server-side tools) a `tools.py` exposing `register(mcp)`.
2. Call the new step's `register` from `servers/phases/assessment/__init__.py`.
3. Add or update the state in `servers/dag/platform_dag.json` with matching `phase`,
   `step`, and `instructions` fields.
