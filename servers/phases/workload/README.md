# Workload phase package

The developer-persona phase: one instance of the developer DAG
(`servers/dag/developer_dag.json`, copied to `workloads/<component>/dag.json`
at join) runs per claimed component. Each `workload_<purpose>_N` subfolder
owns one step of the phase:

- `instructions.md` — agent instructions returned by `get_next_stage` for the
  DAG state that references this step
- `tools.py` — the MCP tools the agent calls at this step (absent for
  report-and-stop states like `workload_await_3`)
- other `.py` files — pure helpers with `_test.py` files beside them

Steps (numeric suffixes are reading-order convention only; nothing parses
them, and suffixes may repeat):

| Step | DAG state | Purpose |
|---|---|---|
| `workload_scope_1` | `STATE_WKLD_SCOPE` | Browse the exports seed index, agree include/exclude patterns |
| `workload_scopeconfirm_2` | `STATE_WKLD_SCOPE_CONFIRM` | Human sign-off (in-call elicitation), persist `scope.json`, data-dependency advisory |
| `workload_plan_2` | `STATE_WKLD_PLAN` | Pure deterministic planner: scoped files → four unit families (the manifests unit persists `pod_dns_facts`), persist `plan.json` |
| `workload_planreview_4` | `STATE_WKLD_PLAN_REVIEW` | Plan sign-off (in-call elicitation); reject returns to PLAN |
| `workload_translate_3` | `STATE_WKLD_TRANSLATE` | Worker fan-out (YAML-only contract, chart re-render gate, per-fact knowledge via `FACT_KNOWLEDGE`), lease+CAS, four-conjunct blob reuse, deterministic transforms post-pass |
| `workload_review_4` | `STATE_WKLD_REVIEW` | Per-unit review: results summary, revise/skip/approve/retranslate |
| `workload_validate_5` | `STATE_WKLD_VALIDATE` | Carrier-first materialization, manifest gates ONLY (structural, re-render, deterministic re-check, routing and pod DNS contracts), staleness cross-check, the data-gate park, ship elicitation walk, `submit_workload_pr` action |
| `workload_await_3` | — (retired) | parking step from earlier graph versions; kept on disk for not-yet-upgraded ledger graph copies |

`datagate.py` is the phase-level pure module both of those steps use: whether
this component may ship yet, given the AWS data services it depends on
(DESIGN.md §6.4). Warn early at scope confirm, block late at validate. It
reads two exports fields and nothing else — `data_gate` for what is owed, and
`component_seed_index` for who owes it, the latter being the whole
name-matching half of the attribution — and it restates the gating
disposition rather than importing it from the deployment phase, since the two
run in different sessions under different credentials.

`knowledge/acme-workload-translation-manual.md` is the English copy of the
2026-08-13 acme workload translation manual: the source material the family
briefs in `workload_plan_2/planner.py` derive from. It is not machine-parsed
and no DAG state declares it as `knowledge`. `knowledge/pod-dns-translation.md`
is different: it is machine-delivered — appended to a translation worker's
prompt whenever its unit carries non-empty `inputs.pod_dns_facts` or
`inputs.pod_dns_unread` (a render source the plan could not read)
(`workload_translate_3/translator.py` `FACT_KNOWLEDGE`, validated at start-up)
— and its output contract is what `workload_validate_5/poddns_contract.py`
enforces. A new pod DNS case is an edit to that document, not to code.

Facts come ONLY from `exports.json` (the single platform→developer channel);
this package never reads `platform/*`. Every state-mutating tool goes through
`state_management.authorize_and_rehydrate_workload`, which enforces
caller == recorded claimant in one place.

## Adding a step

Create `workload_<purpose>_<N>/` with `instructions.md` (+ `tools.py` with a
`register(mcp)` if the step has tools), reference it from the developer DAG
state, add the tools module to `__init__.py`'s `register`, and declare the
version bump's state mapping in `servers/dag/server/workload_join.py`
(`STATE_MAPPINGS`).
