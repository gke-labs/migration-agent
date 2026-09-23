# Agent rules — GKE Agentic Migration

## Presenting migration output

These rules apply whenever you drive the migration state machine (the `dag_executor` loop / the
`migration-dag` MCP tools) and present its results to the user. They are rendering rules only — they
never change what you do, only how you report it. Design: [docs/ux/adapter.md](docs/ux/adapter.md).

### Callout taxonomy

Wrap significant announcements in exactly one GitHub-flavored alert, chosen by event class:

| Event class | Alert |
| --- | --- |
| Background discovery finished, inventory synced, residual work listed | `[!NOTE]` |
| Cost / right-sizing proposal (reclaimable capacity, savings available) | `[!TIP]` |
| Gate unlocked, milestone reached, phase completed | `[!IMPORTANT]` |
| Threshold approaching: quota caps, credential/TTL expiry, canary warnings | `[!WARNING]` |
| Destructive action awaiting confirmation, SLO breach, rollback under way | `[!CAUTION]` |

Routine conversation and intermediate progress get no alert.

### Callout anatomy

Every alert body follows this 4-part shape:

1. Tagged headline, 60 characters max: `🔔 **[DOMAIN TAG]**: what happened`
2. The decision-driving numbers, **bolded** (e.g. sync lag **0.08s**, savings **$420.00/mo**)
3. Which persona owns the decision (Migration Admin / Platform Engineer / App Developer)
4. `👉` exactly one explicit next action: a command to type, a link to open, or a value to provide

### Structure and copy

- Never dump raw JSON, HCL, or logs into chat. Summarize into a table and link the artifact file.
- Bold the numbers that drive decisions; keep everything else plain.
- Never wrap link text in backticks — it breaks link rendering in some harnesses.
- Failures and rollbacks get calm, blameless copy: the metric observed, the action taken, the next
  step. No alarm words, no exclamation marks.

### Worked example — milestone

> [!IMPORTANT]
> 🔔 **[LANDING ZONE READY]**: Terraform validated and PR opened for `gke-landing-zone`.
> Projected cost **$1,420.00/mo** vs legacy **$1,850.00/mo** (**−$430.00/mo**). Owner: Platform Engineer.
> 👉 Review and approve the PR to proceed to workload translation.

### Worked example — destructive confirmation

> [!CAUTION]
> 🛑 **[AWS TEARDOWN CONFIRMATION]**: About to plan destruction of 3 stateful resources in `migratorium-eks`.
> Includes **1 PostgreSQL RDS** and **2 EBS volumes** — not recoverable after apply. Owner: Platform Engineer.
> 👉 Type the source cluster name to confirm, or `abort` to stop here.
