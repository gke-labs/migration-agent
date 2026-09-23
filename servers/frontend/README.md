# GKE Agentic Migration review frontend

A **read-only** web UI for humans reviewing a migration. It shows:

- **The DAG** from `platform_dag.json`, grouped into collapsible phase
  bands (setup → discovery → assessment → landingzone → translation): the phase with
  the current step is expanded, the rest are minimized to a one-line
  header (name, step count, done/in-progress status) and expand on
  click, so the whole migration fits the panel. Agent steps, server
  mutations, human-approval gates, and back-edges (rejections,
  re-discovery) are drawn distinctly.
- **Where the workspace is**, as a progress traffic light: completed
  steps are green, the current step is blue ("YOU ARE HERE"), and steps
  not yet reached are grey (a legend under the graph spells this out).
- **The artifacts each step produced**, fetched live from the ledger:
  file manifest, discovery scope, extraction fragments, inventory,
  the data mapping corrections a reviewer recorded, the
  readiness report, the landing zone plan, the translation plan, every
  translated unit (generated files + tradeoffs + assumptions + open
  questions), the validation report (terraform + manifests), the before/after
  (AWS → GCP) comparison the reviewer signs off, and the pull request
  opened in the target repository, rendered for review. While extraction runs, a live
  progress artifact (`/api/progress/extraction`) shows chunks completed
  and the resources merged from fragments so far, refreshed on every
  poll. While translation runs, a matching artifact
  (`/api/progress/translation`) shows a live bar over the units — filling
  in as each worker finishes — and the "Translated units" tab populates
  unit by unit, so a multi-minute run is visibly working rather than
  appearing stuck.
- **An Audit tab** listing the recorded actions — transitions, tool
  calls, server mutations, human elicitation responses — from the
  workspace history (most recent 100 entries), kept off the main view
  (the agent conversation runs side by side).
- **The role you are under**: the authenticated user, the role the local
  session is acting as, and the role registered in the workspace registry.

The UI is the review surface, the conversation is the control surface:
approvals, scope changes and revision requests happen through the MCP
tools in the agent conversation. This server never writes to the ledger
and cannot advance the DAG.

## Running against the live ledger

**Started automatically:** the DAG server launches this UI when a workspace
session becomes active (`join_ledger`, or the first `get_next_stage` after a
server restart) and puts the URL in the tool output. The announcement tells the
driving agent to open that URL in its integrated/embedded browser (so the
reviewer can watch inside the editor rather than alt-tabbing to Chrome) and to
also show the plain link to the human — for viewing only, not to fetch or poll.
The instance is detached so it survives MCP server restarts, and it re-reads
the session cache per request (cwd-scoped `~/.ledger_config.d/<hash>.yaml`,
inherited from the spawning server via `GKMA_SESSION_CWD` — a variable only
this frontend interprets; the DAG server always scopes by its own cwd — with
the legacy `~/.ledger_config.yaml` as a one-shot read fallback), so re-joins
are picked up without a restart. The launcher reuses a running instance only
when the ledger it serves matches the joining session's; an instance pinned
to another session's scope is left alone and a new one is spawned on the next
port. When the UI first comes up, the launcher also opens it in the
reviewer's browser. Set `GKE_AGENTIC_MIGRATION_FRONTEND=0` to disable auto-start,
`GKE_AGENTIC_MIGRATION_FRONTEND_PORT` to move it off 8642 (the next 9 ports are tried when
the port is held by something else), or `GKE_AGENTIC_MIGRATION_FRONTEND_OPEN=0` to keep the
browser from being opened automatically. The one exception to "on unless set to 0": inside
a unit-test process (`unittest` or `pytest` imported) both switches default to off, logged
once at INFO, and setting the variable to `1` restores the normal behaviour.

To run it by hand instead — uses the same session cache and Application
Default Credentials as the MCP server (run `join_ledger` first; set
`GKMA_SESSION_CWD` to the MCP server's cwd when running from elsewhere):

```bash
cd <repo root>
PYTHONPATH=. python -m servers.frontend.server --port 8642
# open http://127.0.0.1:8642
```

No extra dependencies: Starlette and uvicorn already ship with the `mcp`
package used by the DAG server.

## Security notes

- Binds `127.0.0.1` by default and has **no authentication of its own**.
  The host allow-list admits only the bind address and localhost names, so
  merely changing `--host` does not expose it — but if you widen both, anyone
  who can reach the port sees the ledger the way your credentials do.
- Strictly read-only — the app registers no write routes at all.
- `/api/blob` serves only artifact paths from the explicit allow-list in
  `ledger_view.py` — never `state.json`, the registry, or the DAG blob.
- Host-header validation (`TrustedHostMiddleware`) blocks DNS rebinding.
- The page inserts all ledger/LLM content as text (`textContent`), so
  report markdown cannot inject script into the review page.
