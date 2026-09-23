# Workload Step — Validate, Ship Approval, and the Component PR

**DAG state:** `STATE_WKLD_VALIDATE` · **Expected tool call:** `run_workload_validation`

The unit review is approved
([review step](../workload_review_4/instructions.md)). This step
materializes the done units into the target clone under
`workloads/<component>/` (chart carrier first, then each unit's owned-file
edits), gates the materialized output, and — when clean — raises the ship
elicitation and, on approve, opens the per-component Pull Request
(`migration/workload-<component>-<uuid>`) in the same call.

## What to do

1. Call `run_workload_validation()`.
2. If it reports findings, the DAG is back at `STATE_WKLD_REVIEW`: relay
   every finding verbatim (file, unit, both generation values for a
   staleness finding) and continue per the review instructions.
3. If validation passes, the ship elicitation appears — the elicitation IS
   the decision; do not pre-ask in chat. Summarize for the user first:
   units shipping, tradeoffs highlights, the parked list, open questions.
4. If the response says the component is HELD, the data gate holds it: the
   manifests are fine and the DAG has not moved. Relay the named services,
   who owns the decision and both exits verbatim — do NOT offer to fix
   anything, and do not suggest revising or re-planning. There is nothing
   for the developer to do but wait; re-run `run_workload_validation()`
   once the platform team reports the service settled.
5. Report the end state faithfully: PR opened (component DONE — terminal),
   ship declined (back at review), or PR submission failed (the ship
   approval is re-raised on the next run — a PR failure invalidates
   nothing validation checked).

## Gates (and non-gates)

- ONLY manifest gates run here: the structural check over every
  materialized manifest and the deterministic re-render gate over every
  materialized chart/kustomize source. No `terraform validate`, no LLM
  auto-fix loop — a failing unit goes back to review with its finding.
- Exports cross-check: every unit blob's stamped exports generations must
  equal the CURRENT `exports.json` generations, EXCEPT the `data` source,
  which is excluded on both sides — a database landing moves that counter
  and makes no brief stale; a mismatch is a finding
  naming the unit and both values, never a silent pass. Its remedy is
  `approve_workload_translation(action='replan')` — retranslating re-writes
  the same stale stamp. The ONE exception is the gateway unpark, which
  refreshes the stamp itself and re-queues the units that were already done,
  so they re-run against it; it refuses whenever exports moved in any field
  beyond `gateway`, and then this replan remedy is the only path.
- Ledger cross-check: a unit the plan marks done whose blob carries no
  successful result is a finding, not a silent zero-file materialization.
- Ship-completeness: every unit that is not a placeholder, not skipped and
  not parked must be done. Parked units are listed, never blocking — a
  component shipped with a parked routing unit re-enters at translate on
  the first re-join AFTER the platform Gateway publishes (the
  re-entry rule is standing, so it does not matter how many joins happened
  in between).
- The target clone: this step resolves the target repository (component
  variables, then `exports.target_repo` — the developer-readable channel)
  and clones it onto the PR branch before materializing, self-healing a
  stale or mismatched clone from the current coordinates. If nothing
  resolves, the gates still run over a SCRATCH directory but that is a
  BLOCKING finding — the ship approval will not rise. Relay the finding
  verbatim: the fix is platform-side (`configure_repositories`, then
  `refresh_exports`), after which re-running this tool clones and ships.
- The data gate: a component may not ship while an AWS data service it
  uses is graded `migrate` and has not been reported as landed. This is a
  PARK, not a finding — validation still PASSES, the report and comparisons
  are persisted, and the component stays at `STATE_WKLD_VALIDATE`. Both
  exits are platform-side (`mark_data_service_migrated`, or
  `annotate_data_dependency(disposition='keep-in-aws')` for a service that
  turns out not to be movable), so never present either as something the
  developer can call. A service the mapping attributes to nobody is
  reported but holds no component; if the user recognizes one as theirs,
  the place to attach the consumer is the platform team's data review.
- The deterministic re-check: rendered chart/kustomize documents and
  materialized plain files are re-run through the deterministic transforms
  detect-only; a value the pass would still rewrite (a source-registry
  image with a replicated mapping, an IRSA role-arn with a published GSA
  email) is a BLOCKING finding — return the unit to review, do not
  hand-patch files.
- The pod DNS contract: every shipped pod spec (plain files and re-rendered
  chart/kustomize output) is checked for the four Kubernetes DNS policies,
  the nameserver/search limits, and a closed list of addresses that never
  ship (the AWS VPC resolver, the EKS default cluster DNS addresses, the
  GKE node resolver addresses, the EC2 search suffixes). The manifests
  unit's shipped pod DNS fields are also held to the facts its plan
  recorded: every source nameserver, search domain, option and host alias
  is kept or named in the unit's tradeoffs/open_questions/assumptions,
  nothing the source did not carry appears, and `dnsPolicy` is unchanged
  except `None` -> `ClusterFirst` (`ClusterFirstWithHostNet` on a
  host-network pod; required when a `None` pod's source nameservers
  included the cluster DNS address). A finding BLOCKS and
  names the pod and the remedy; a unit whose blob predates the facts is
  reported as not conservation-checked — a re-plan followed by
  `approve_workload_translation(action='retranslate')` gates it (a re-plan
  alone reuses the old blob). A blob whose facts differ from the current
  plan's is a finding with the same retranslate remedy; a unit whose plan
  could not render a source is reported as "facts incomplete" and its
  shipped values are not called invented. The hostNetwork-under-ClusterFirst
  note is advisory, never blocking.

## Rules

- Never run the PR path outside this tool; the branch and commit scope
  (only `workloads/<component>/`) are fixed by the server.
- If the tool returns an `ERROR`, report it verbatim and stop.
