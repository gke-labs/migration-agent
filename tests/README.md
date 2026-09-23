# Local E2E tests

Run this suite after changing anything that affects the plugin surface:
`servers/`, `skills/`, `.mcp.json`, or `.claude-plugin/`.

```bash
./tests/run_e2e.sh        # interactive (asks before creating buckets)
./tests/run_e2e.sh -y     # non-interactive (agents, scripts)
```

## What it tests

| Tier | What | Needs |
|---|---|---|
| `smoke` | Plugin manifest validates (`claude plugin validate` + static checks), MCP server boots over stdio, required tools (`bootstrap_migration`, `initialize_ledger`, `get_next_stage`, `join_ledger`) are exposed | python3 |
| `smoke/startup-validation` | A malformed bundled DAG makes the server **refuse to start** rather than serve states with no procedure. Copies `servers/` and `reference/` to a temp dir, breaks a transition in each of `bootstrap_dag.json` / `platform_dag.json`, and asserts a non-zero exit naming the bad state. The real tree is never touched | python3 |
| `e2e/mcp-direct` | Full bootstrap flow via the official python MCP client: elicitation approved programmatically, DAG parks on `STATE_WORKSPACE_ADMIN` (the admin's standing step; success has no terminal), and the GCS bucket + 3 ledger objects (`workspace_registry.yaml`, `platform_dag.json`, `platform/onboarding/state.json`) really exist. Also checks the cloud-side access boundary: uniform bucket-level access is on, the `platform/` and `workloads/` managed folders exist, the admin holds `storage.admin`, the platform engineer holds `objectAdmin` on `platform/`, and registry read is granted **only** under an IAM condition naming that one object. Deterministic — no LLM involved. | gcloud + ADC |
| `e2e/knowledge-session` | Phase knowledge is served **once per server process** and **again after a restart**. Provisions a ledger, parks it on `STATE_ASSESSMENT` (which declares `migration-assessment.md`), then calls `get_next_stage` twice in one process and once in a fresh one. Pins the session-scoped dedup semantics — a unit test clearing the module-level set would pass just as happily if delivery were persisted to the ledger | gcloud + ADC |
| `e2e/assessment-flow` | Demo steps 2 and 3 against the real graph: the ledger parks on `STATE_ASSESSMENT` (which serves `migration-assessment.md`), one `submit_assessment` call raises the pause-and-explain **elicitation**, a submitted two-blocker list routes to `STATE_BLOCKER_RESOLUTION`, and landing zone design unlocks **only** once every blocker has an owner and a target close date. With `--new-owner`, also drives the unregistered-owner path: the team question is asked, the answer registers the person in `workspace_registry.yaml`, and the IAM re-grant runs. Without it that leg is skipped and reported, because the grant names a real principal | gcloud + ADC |
| `e2e/cc-plugin` | Plugin loads under the real Claude Code harness (`--plugin-dir`) and the `bootstrapping` skill is exposed | claude CLI |
| `e2e/cc-approve` | Bootstrap flow driven by the Claude Code harness with the server's MCP elicitation answered **accept** by an `Elicitation` hook; DAG parks on `STATE_WORKSPACE_ADMIN`, bucket verified in GCS | claude CLI, gcloud + ADC |
| `e2e/cc-reject` | Same harness, but the `Elicitation` hook **declines**. Asserts the flow stops (`STATE_ABORTED` / rejection reported) and that **no bucket is created** — guards against a decline still provisioning resources | claude CLI, gcloud + ADC |
| `e2e/agy-mcp-import` | The stdio MCP server is importable and its tools are callable through the **Antigravity SDK** harness: registers the server, calls `bootstrap_migration` + `get_next_stage` (neither triggers an elicitation), asserts the bootstrap start state. Scoped to MCP import only — the SDK cannot answer elicitations, so it does not run the bucket flow. Skip with `--skip-agy`. | `google-antigravity` (auto-installed to `tests/.venv-agy`), Vertex ADC |

The suite exits non-zero if any tier fails and prints a per-tier summary.

## Configuration

Everything defaults to your current gcloud environment. Override with flags or env vars:

| Setting | Flag | Env var | Default |
|---|---|---|---|
| GCP project | `--project` | `E2E_GCP_PROJECT` | `gcloud config get-value project` |
| Bucket base name | `--bucket-base` | `E2E_BUCKET_BASE` | `claude-local-test-bucket-<rand>` (suffixed `-mcp` / `-knowledge` / `-assessment` / `-cc`) |
| Admin email | `--admin` | `E2E_ADMIN_EMAIL` | `gcloud config get-value account` |
| Platform engineers | `--platform-engineers` | `E2E_PLATFORM_ENGINEERS` | admin email (semicolon-separated list) |
| Unregistered blocker owner | `--new-owner` | `E2E_NEW_OWNER` | none — the `assessment-flow` tier then skips member registration. Must be an address the project can bind in IAM, since registering them grants a real conditional binding |

Other flags: `--skip-smoke`, `--skip-cc`, `--skip-agy`, `--keep-bucket` (keep buckets for inspection), `-y`.

All tiers (smoke, mcp-direct, cc, agy) run by default. Use the `--skip-*` flags to narrow.

## Prerequisites

- `gcloud` with Application Default Credentials (`gcloud auth application-default login`)
  and permission to create/delete GCS buckets in the target project. Bootstrap also
  sets IAM on the bucket it creates, so the account needs `roles/storage.admin`
  (specifically `storage.buckets.setIamPolicy` and `storage.managedFolders.create`)
  — without it the bootstrap tiers fail by design rather than provisioning a ledger
  with no access boundary.
- `python3` (the suite manages its own venvs at `tests/.venv` and `tests/.venv-agy`).
- `claude` CLI, logged in, for the `cc` tier (or pass `--skip-cc`).
- Vertex AI access via the same ADC for the `agy` tier; the tier installs
  `google-antigravity` from PyPI into `tests/.venv-agy` (or pass `--skip-agy`).
- First run is slower: the MCP server launcher builds `servers/dag/.venv`, and the
  agy tier downloads the SDK.

## Safety notes

- Test buckets are created fresh (the run aborts if the target bucket already exists)
  and deleted on exit unless `--keep-bucket` is passed.
- Both session caches — the legacy `~/.ledger_config.yaml` and the cwd-scoped
  `~/.ledger_config.d/<sha256(cwd)[:16]>.yaml` for the invocation directory — are
  backed up before the run and restored afterwards (`bootstrap_migration` deletes
  both as part of its reset; `join_ledger` rewrites the scoped one). The
  `knowledge-session` and `assessment-flow` tiers additionally snapshot and
  restore both files themselves (`e2e/mcp_direct/session_cache.py`), so the
  runner's backup is not the only thing standing between a test run and a
  developer's real session.
- `--new-owner` grants a real IAM binding to the address you name, on the test
  bucket only, and it goes away with the bucket. It is opt-in for that reason:
  the tier otherwise never touches a principal you did not already list.
- On `cc` tier failure, the working directory (`/tmp/gke-migration-e2e-cc.*`) is kept
  with `result.json`, `stderr.log`, and the elicitation hook input log for debugging.
  The MCP server also logs to `/tmp/mcp_server.log`.

## Known harness limitations (as of 2026-07)

- Claude Code headless auto-rejects MCP elicitations unless an `Elicitation` hook is
  configured — the `cc` tier sets one up automatically in its temp workdir.
- The Antigravity SDK (`google-antigravity` 0.1.8) cannot answer MCP elicitations at all
  (no elicitation path between its Go MCP client and the SDK surface), so there is no
  Antigravity tier yet; the bootstrap flow hangs until its 3-minute MCP timeout there.

## e2e-qa — agent-level runs

`tests/e2e-qa/` drives the whole platform journey with Claude Code sessions as
the personas (model in the loop, real server, real ledger, real Git remotes),
either presented live or fully autonomous. The tiers above call tools
directly and can never see model behaviour — rule violations, phrasing drift,
judgement failures; this harness exists for exactly those. Start at
`tests/e2e-qa/README.md`.
