# Translation Step 3 — Validate the Generated Code

**DAG state:** `STATE_TRANSLATION_VALIDATE` · **Expected tool call:** `run_generated_validation`

The unit review is approved. Before anything ships, the server proves the
generated code is sound: every done unit is materialized into the target
clone beside the landing-zone draft, a generated `translation-units.tf` at
the clone root references each unit directory that carries Terraform (a
module nothing references is never compiled, so without it the root pass
would prove nothing about the units), `terraform validate` runs over the
root and each unit directory with a bounded LLM auto-fix pass, the
recorded values behind the root's variables are printed into
`migration.auto.tfvars` (after the fix pass; a no-default root variable
with NO recorded source fails the run pointing at the declaring
file:line — the server never invents a value or a default, so unprinted
means `terraform plan` would halt on a prompt), every
generated Kubernetes manifest is structurally checked (offline — parseable
YAML, one apiVersion/kind/metadata.name object per document; no fix pass,
a failing manifest goes back to review), and the coverage-omission gate is
enforced: a coverage-map row owned by landing-zone/platform-translation
whose inventory sections hold facts must have an artifact behind it — a
done unit citing it, an explicitly skipped citation, or (for the GKE
cluster row) a `google_container_cluster` declared somewhere in the clone
— or the run fails naming the row. The cluster row is also checked one
field deep: every declared cluster must set `dns_config { cluster_dns =
"CLOUD_DNS" }` unless it is Autopilot, because the standing default and
the `cluster-dns` unit assume Cloud DNS and `terraform validate` accepts
the block's absence. Compiling greenly and containing the migration's
artifacts are different properties; this gate checks the second.

## What to do

1. Call `run_generated_validation()` — no arguments. This can take a few
   minutes when fixes are needed; the validation report and the before/after
   comparison land in the Review UI as it finishes.
2. Read the state the call reports back:
   - **All valid** → the server raises the ship-approval elicitation (the
     user's answer is the decision — never approve on their behalf). On
     approval it opens the Pull Request in the target repository and the
     phase completes. Report the PR link from the message.
   - **Failures remain** → the DAG returns to `STATE_TRANSLATION_REVIEW`
     with the failing directories, manifests, contract findings, and
     coverage-omission rows in the message. For a failing directory,
     manifest or contract: discuss with the user, then send the unit back
     with `request_unit_revision` (or, if the user decides not to migrate
     it at all, `skip_translation_units`) and approve again.
   - A **root variable with no recorded source** names a value the ledger
     does not hold. The remedy is never to invent one: revise the unit so
     the value is not required (own the resource, derive it from the
     unit's own inputs, or drop it with an open question), or fix the
     landing-zone draft so the fact is recorded as a literal the printer
     can read. When the value is genuinely unknowable in-band, say so
     plainly to the user — recording the fact is their decision.
   - A **coverage-omission** row is not one of those. It says the migration
     would ship without an artifact the inventory calls for (e.g. no
     `google_container_cluster` anywhere in the clone), or that the
     artifact is there with the wrong value (a cluster field finding: no
     `dns_config { cluster_dns = "CLOUD_DNS" }` on a cluster the scan cannot
     prove is Autopilot; the finding names the file, the resource and the
     fix), so revising a unit
     does not address it, and skipping the citing unit must never be used
     to clear it — that turns the gate green while shipping exactly the
     hole it named. Fix the landing-zone draft or the unit family that owes
     the row, and say plainly to the user when neither is possible: an
     omission the team accepts is a decision for them to state, not a call
     for the agent to make on the way to a green run.

## What the reviewer sees

The Review UI shows the validation report (clean / auto-fixed / failing, plus
the manifest check) and a per-unit **before/after**: the AWS-side inputs
discovery found on one side, the generated (and possibly auto-fixed) GCP
Terraform and Kubernetes manifests plus their tradeoffs on the other. Point
the user there for the final read.

## Rules

- Never re-print the generated code into this conversation — the ledger
  and the Review UI are the review surface.
- If the tool reports missing terraform or LLM credentials, relay the message
  verbatim and stop.
