# Bootstrap Phase

The admin's phase. Bootstrap creates the workspace — the ledger bucket, its
IAM boundary, the registry, the platform graph — and then, rather than
ending, parks on a step where the admin can put right what the first pass
got wrong. Before this step existed the graph reached a terminal state as
soon as the ledger was seeded, and a mistyped email or a wrong team
assignment had no in-product fix.

## Layout

```
servers/phases/bootstrap/
├── __init__.py                # register(mcp) — wires every step's tools into the server
├── README.md
└── bootstrap_admin_2/         # STATE_WORKSPACE_ADMIN: the admin's standing step
    ├── instructions.md        # agent instructions returned by get_next_stage
    ├── tools.py               # describe_workspace, add/remove_workspace_member,
    │                          #   reset_migration, upgrade_ledger_dags, reset_dag_state
    └── tools_test.py
```

There is no `bootstrap_configure_1/`: `STATE_CONFIGURATION_GATHERING` is
still served by `skills/bootstrapping` and `initialize_ledger` in
`servers/dag/main.py`, the last unconverted admin step (DESIGN.md §9.5).

## The standing step

`STATE_WORKSPACE_ADMIN` is an `AGENT_TASK` with one self-transition and no
exit. The agent presents the workspace once, tells the admin that changes
are a request away, and ends its turn; each later request maps to one tool,
and the graph stays where it is. The bootstrap graph lives in server memory,
so the step lasts as long as the session — an admin who comes back later
reaches the same tools through `join_ledger` (they authorize on the registry,
not on the in-memory graph).

## What the tools change

| Tool | Registry | IAM | Ledger objects |
|---|---|---|---|
| `add_workspace_member(email, team)` | adds or moves the email between `platform_engineers` and `developers` | grants first, records second — a principal IAM refuses never reaches the registry, so it cannot poison later grants | — |
| `remove_workspace_member(email)` | removes the email | revokes the bucket and `platform/` bindings after the registry write | — |
| `reset_migration()` | kept | kept | deletes everything else, then re-seeds `platform_dag.json` and `platform/onboarding/state.json` from the bundle |
| `upgrade_ledger_dags()` | — | — | replaces `platform_dag.json` when the bundled version differs; upgrades every `workloads/<component>/dag.json` copy through the join's state mapping |
| `reset_dag_state(target)` | — | — | rewinds `platform/onboarding/state.json` or one component's `state.json` to the graph's start; component claims survive |

The admin list is not editable here. It is what holds `roles/storage.admin`
on the bucket, and an admin removed by mistake could not undo it.

`reset_migration`, `reset_dag_state` and `upgrade_ledger_dags` each raise an
approval elicitation before touching anything; the agent must not ask a second
time.

## Adding a step

1. Create `servers/phases/bootstrap/bootstrap_<purpose>_N/` with `instructions.md` and
   (if the step needs server-side tools) a `tools.py` exposing `register(mcp)`.
2. Call the new step's `register` from `servers/phases/bootstrap/__init__.py`.
3. Add or update the state in `servers/dag/bootstrap_dag.json` with matching `phase`,
   `step`, and `instructions` fields.
