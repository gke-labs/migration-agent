# Landing Zone Phase

Designs the GCP target foundation the EKS estate lands on — project hierarchy, Shared VPC,
GKE clusters, org policies, baseline IAM, observability and budgets — as modular Terraform
in the target GitOps repository, then generates the per-workload translation units, validates
the whole build and opens the Pull Request. This one phase now spans the entire target build:
the LLM writes the landing-zone HCL and the client signs off on the translation plan, then the
translation steps generate the units, a compiled tool proves the whole thing compiles, and the
PR covers the landing-zone draft plus the generated units together. The phase applies nothing.

The translation steps (run, review, validate, ship, PR) keep their code under
`servers/phases/translation/`, but their DAG states carry `phase: "landingzone"`, so the whole
target build reads as one band in the review UI.

Reachable only once every assessment blocker has an owner and a target close date.

## Layout

```
servers/phases/landingzone/
├── __init__.py                 # register(mcp) — wires every step's tools into the server
├── actions.py                  # submit_lz_pr + the ACTIONS table
├── workspace.py                # where a landing zone's target clone lives
├── landingzone_design_2/       # STATE_LZ_DESIGN: resolve decisions, prepare the clone, write HCL
│   ├── instructions.md         # agent instructions returned by get_next_stage
│   ├── tools.py                # resolve_lz_decision (allocates the clone), finalize_landing_zone_design
│   └── ranges.py               # target ranges proposed clear of inventory.address_space (pure logic)
├── landingzone_translationplan_3/  # STATE_LZ_TRANSLATION_PLAN: decompose inventory into units
│   ├── instructions.md
│   ├── tools.py                # plan_translation
│   ├── planner.py              # pure decomposition (inventory + LZ decisions -> units)
│   └── coverage.py             # pure coverage-map instantiation + omission/overlap/traceability checks
├── landingzone_planreview_4/   # STATE_LZ_TRANSLATION_PLAN_REVIEW: client plan sign-off
│   ├── instructions.md
│   └── tools.py                # update_translation_plan, confirm_translation_plan
└── knowledge/
    ├── gke-landing-zone.md     # the design authority: decisions, defaults, modules, controls
    └── coverage-map.md         # artifact kind -> owner/column table, machine-read (see below)
```

The plan and plan-review steps are the design half's closing agreement: with the target shape
decided, the client signs off on the translation plan — the bounded units the translation
steps will generate Terraform for. The design half agrees WHAT needs to be done; the
translation steps (under `servers/phases/translation/`) do it, then validate and ship.

`knowledge/gke-landing-zone.md` is declared by `STATE_LZ_DESIGN`, so it arrives with the first
step of the phase (§7.1 of DESIGN.md — knowledge delivery is deduplicated per server session).

`knowledge/coverage-map.md` is different: no state declares it for delivery. It is a
machine-read authority — `servers/dag/server/coverage_map.py` parses its ownership table at
start-up (a broken table is a refusal to start), `plan_translation` instantiates it against
the approved inventory (`landingzone_translationplan_3/coverage.py`) to check the plan's unit
citations as WARNING-grade review material, and the translation validate step enforces the
omission half (`translation_validate_3/coverage_gate.py`): a facts-present row with no
artifact and no explicit skip fails validation naming the row. Editing it is editing what the
server enforces.

## The gate chain

The design half is design and agreement only; generation, validation and the PR run in the
translation steps at the end, where they cover the landing-zone draft plus the generated units
together:

```
STATE_LZ_DESIGN
  └─ finalize_landing_zone_design    records the design (nothing validated or pushed)
       └─ STATE_LZ_TRANSLATION_PLAN         plan_translation (deterministic decomposition)
            └─ STATE_LZ_TRANSLATION_PLAN_REVIEW
                 └─ confirm_translation_plan   raises THE plan-approval elicitation
                      ├─ on_reject  → STATE_LZ_DESIGN   (redo the design)
                      └─ on_approve → STATE_TRANSLATION_RUNNING (generate → review →
                                       validate+autofix → ship approval → PR)
```

`STATE_LZ_DESIGN` is the phase entry: `resolve_lz_decision` records each of the four
target-shape decisions (and, on the first call, allocates the target clone workspace), and
`finalize_landing_zone_design` refuses to advance until all four are on record.

Two consequences the step instructions spell out for the agent: it must read the state the
call reports back rather than assuming approval carried, and it must not ask for approval
itself — the elicitation does that, and its answer is the one the graph reads.

The knowledge document still requires a root module: when validation runs (in the translation
steps) it compiles from the **root** of the clone, and a module no root module references is
never compiled.

## Tools, actions, and the shared path

The phase owns all of its landing-zone entry points. The five tools are registered per step by
`__init__.py`; the `submit_lz_pr` action is reached by `main.run_internal_mutation` through
`actions.ACTIONS`, keyed by the `action` field of the internal states. `main.py` keeps only
re-export lines so existing callers and tests can still say `main.resolve_lz_decision`.

`workspace.py` exists because two entry points need the same directory: the tool that
allocates it and the `submit_lz_pr` action that reads it. They each used to derive it from
their own `__file__`, which is what kept the tools in `main.py` — move one half and the
shipper looks somewhere the generator never wrote. The clone stays under `servers/dag/scratch/`
regardless of which package asks, since that is what `.gitignore` and DESIGN.md §3.1 name
and where clones from earlier runs already are.

## Who creates the clone

Nothing on the server does. The first `resolve_lz_decision` call allocates `target_clone_path`
but does not create it, and `git_client.clone_repository` / `create_and_checkout_branch` exist
with no callers. The agent clones the target repository itself, per
`landingzone_design_2/instructions.md`; `submit_lz_pr` (at the end of the phase) fails with
*"Target clone directory does not exist"* if it did not. Tracked in DESIGN.md §14.

## Adding a step

1. Create `servers/phases/landingzone/landingzone_<purpose>_N/` with `instructions.md` and
   (if the step needs server-side tools) a `tools.py` exposing `register(mcp)`.
2. Call the new step's `register` from `servers/phases/landingzone/__init__.py`.
3. Add or update the state in `servers/dag/platform_dag.json` with matching `phase`,
   `step`, and `instructions` fields.
