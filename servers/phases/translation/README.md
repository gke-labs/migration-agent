# Translation Steps

Everything related to translating the EKS → GKE workloads lives in this folder:
per-step agent instructions and the MCP tools each step exposes. It mirrors the
[discovery phase](../discovery/README.md).

These steps are no longer a separate DAG phase: their states
(`STATE_TRANSLATION_*`) carry `phase:"landingzone"`, so the whole target build —
design, plan, generate, validate, ship — reads as one band in the review UI. The
code stays here because it is a coherent unit (workers, validation, shipping);
only the DAG label moved. See the [landing zone phase](../landingzone/README.md).

Translation **generates the code**: Terraform for resources provisioned through
GCP APIs, Kubernetes manifests for in-cluster objects. Agreeing *what* to generate happens at
the end of the [landing zone phase](../landingzone/README.md): its closing
steps decompose the approved discovery inventory into bounded translation
units (`landingzone_translationplan_3`) and get the client's sign-off on the
unit list (`landingzone_planreview_4`). This phase then runs one subagent per
unit — Terraform and/or Kubernetes manifests plus an explicit tradeoffs
write-up each — and the client
reviews every unit: approving, or sending specific units back with feedback.

## Layout

```
servers/phases/translation/
├── __init__.py                 # register(mcp) — wires every step's tools into the server
├── translation_translate_1/    # STATE_TRANSLATION_RUNNING: subagent per unit
│   ├── instructions.md         # agent instructions returned by get_next_stage
│   ├── tools.py                # run_translation
│   └── translator.py           # worker prompt, validation, fan-out
├── translation_humanreview_2/  # STATE_TRANSLATION_REVIEW: review generated code + tradeoffs
│   ├── instructions.md
│   └── tools.py                # get_translation_results, request_unit_revision,
│                               #   skip_translation_units, approve_translation
└── translation_validate_3/     # STATE_TRANSLATION_VALIDATE: terraform validate + auto-fix,
                                #   manifest structure check
    ├── instructions.md
    ├── tools.py                # run_generated_validation (then ship approval + PR)
    └── validation.py           # validate/fix loop (pure core + terraform + worker glue)
```

Worker primitives (Agent SDK call, JSON parsing, credential preflight) are shared
with discovery via `servers/phases/agent_workers.py`. The translation model is
configurable via `GKE_AGENTIC_MIGRATION_TRANSLATE_MODEL` (default `claude-opus-5` — translation
correctness is worth the bigger model), concurrency via
`GKE_AGENTIC_MIGRATION_TRANSLATE_CONCURRENCY`.

## How it connects to the DAG

The flow (platform DAG v2.6): the landing zone closes by approving the plan,
and this phase executes it end to end — generation, proof, and shipping:
`STATE_TRANSLATION_RUNNING → STATE_TRANSLATION_REVIEW →
STATE_TRANSLATION_VALIDATE (terraform validate + LLM auto-fix over the
landing-zone draft and every unit directory) → STATE_TRANSLATION_APPROVED
(the ship decision: before/after + tradeoffs in the Review UI) →
STATE_TRANSLATION_SUBMIT_PR (the Pull Request into the customer's target
repository) → STATE_DEPLOYMENT_COMPLETED`. From review,
`request_unit_revision` and `approve_translation(action="retranslate")` loop
back to `STATE_TRANSLATION_RUNNING`; completed units are cached, so only
revised units are re-run — with the reviewer's feedback in the worker prompt.
Validation failures that survive the auto-fix pass return to review with the
report.

The units consume the discovery inventory plus the landing-zone target-shape
decisions (`variables.lz_decisions`); the landing zone owns the base VPC and
cluster Terraform, so units only layer workload-specific resources on top.

Per-unit results (generated files, tradeoffs, assumptions, open questions) live in
the ledger at `platform/translation/units/<unit_id>.json`; the plan lives at
`platform/translation/plan.json` and in the state variables. The generated
code is reviewed from those ledger blobs, not reprinted into the
conversation — the read-only Review UI that renders them lands in the next
change of this stack.

## Adding a step

1. Create `servers/phases/translation/translation_<purpose>_N/` with
   `instructions.md` and (if the step needs server-side tools) a `tools.py`
   exposing a `register(mcp)` function.
2. Call the new step's `register` from `servers/phases/translation/__init__.py`.
3. Add or update the state in `servers/dag/platform_dag.json` with matching
   `phase`, `step`, and `instructions` fields.
