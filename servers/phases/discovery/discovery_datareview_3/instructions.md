# Discovery Step 3b — Review Who Uses Each Data Service

**DAG state:** `STATE_DISCOVERY_DATA_REVIEW` · **Expected tool call:** `confirm_data_dependencies`

The data services are recorded and the scan has attributed the ones it could. This step
puts that mapping in front of the platform engineer before extraction runs.

It is the only place the mapping gets a human look. `run_discovery_extraction` carries
`data_dependencies` over untouched, so whatever is attached when it starts is what the
assessment grades and what a later workload gate would hold an application team on.

## What to do

1. Call `list_data_dependencies()`. It prints the section, the corrections already
   standing, and the scan notes.
2. Put it to the user as a table: the service, what it is called, which workloads use it,
   and what the migration will do with it. Lead with the count that needs migrating, and name
   the two decisions that are theirs to make — who owns an unattributed service, and whether any
   of them should stay in AWS rather than move.
3. Work through the entries with no consumer. For each one the tool prints a ranked list
   of candidate workloads from name similarity. **Offer the whole list plus "none of
   these", and say it is a guess.** Never ask the user to confirm the top candidate on its
   own — a wrong yes there stops the wrong team's release, and the user cannot tell how
   thin the evidence was.
4. Record what the user says, one call per decision. All three are repeatable and none of
   them advance the graph, so the review can take as many rounds as it needs:
   - `attach_data_consumer(address, workload, ...)` — a workload the user says uses it.
   - `reject_data_consumer(address, workload, reason)` — a derived consumer they say is
     wrong. Ask for the reason; it is the only record of why the mapping disagrees with
     the Terraform.
   - `annotate_data_dependency(address, note=..., disposition=...)` — a note, or a
     decision like `keep-in-aws` that the scan will never make on its own (see below).
5. Call `confirm_data_dependencies()`. The server raises the sign-off elicitation directly
   — the user's answer there IS the decision. Do not ask for approval in chat first, and
   do not call the tool again to act on the answer.

## Guesses: entries the scan is asking about, not asserting

An entry marked `detection: inferred` is a **guess**. A configuration key's name typed its
value — `INVOICE_BUCKET=acme-invoice-archive` in a Deployment's env, `invoiceBucket:` in a
chart's values, `SQS_QUEUE` in a ConfigMap — and nothing in the scanned files declares such a
resource or names it by ARN or endpoint. The workload holding the key is listed as its
consumer, because that half is not a guess; what is uncertain is whether the value really
names a data service. A guess has disposition `undecided` and gates nothing.

**The sign-off refuses while any guess is unanswered.** Put each one to the user as a yes/no
question, with the key and file that produced it, and record the answer:

- `confirm_data_dependency(address, disposition=..., note=...)` — yes, it is real. Omit the
  disposition to take the service's default (a bucket migrates, a cache is rebuilt). Add
  `workload=` if the user names a further consumer.
- `dismiss_data_dependency(address, reason)` — no. A stale value, a bucket that no longer
  exists, a name that only looks like one. The reason is required; the entry is removed and
  stays removed across re-scans.

The address of a guess is `<service>:<identifier>` (`s3:acme-invoice-archive`), as the
listing prints it.

## Services nothing in the files proposed

Ask the user, once, whether the list is complete: a database declared in CloudFormation, a
bucket a library reaches by default, a service they know about that the files never name.
The scan notes may also list ARNs it could not record — wildcards like `arn:aws:s3:::acme-*`
and names built from variables — and the user may know what those stand for. Record each with
`add_data_dependency(service, identifier, disposition?, note?, workload?)`. The entry is
theirs (`detection: human_review`); if a later scan finds the same resource in the files, the
note and disposition land on that entry.

## Likely consumers offered from an exact name match

When a workload's configuration states a recorded entry's name exactly — the chart's values
hold `invoiceBucket: acme-invoice-archive` and the bucket is recorded from a policy ARN — the
listing prints it as a **likely consumer (not attached)** with the key and file. It ranks above
the name-similarity guesses because the match is exact, but it is still a match on a name, so
it is offered, never attached. If the user confirms, `attach_data_consumer` it.

## Entries known only from an ARN or endpoint

An entry with `detection: referenced` was found by a literal ARN or endpoint — in an IAM
policy, an IRSA module's arguments, a Helm value, a ConfigMap, a manifest's env — and is
declared nowhere in the scanned repository (one exception: a note beginning "the replica in
AWS region …" marks the estate's own replica of a declared secret or table; the declared
entry is the primary, the replica is answered and non-gating, and its consumers already sit
on the primary). Its `address` is that ARN or the canonical endpoint, and the correction
tools take it as such:

```
annotate_data_dependency(address="arn:aws:s3:::acme-invoice-archive",
                         note="owned by the finance team's Terraform; provisioned 2023")
```

Tell the user what "referenced" means: the service is real and the workload reaches it,
but no declaration in this repository states its name. Two things can be true: another
team or the console provisioned it, or this repository declares it under a name built
from variables the scan cannot resolve (the note says so). Ask which. If it is a bucket
or table another team keeps, `keep-in-aws` is the likely answer and the cross-cloud costs
below apply; if it is this estate's own resource under another name, `attach_data_consumer`
and a note on the declared entry are the right record.

## Leaving a data service in AWS is one of the answers

Not every data service has to move. The customer can keep one where it is and have the GKE
workload reach it across the cloud boundary — a DynamoDB table the application talks to over the
AWS SDK, an S3 bucket a pipeline already writes to, an RDS instance nobody wants to touch this
quarter. For the AWS-only services this is frequently the *sensible* answer, not a concession:
there is no equivalent to move to, and re-platforming is a project of its own.

**Say this out loud when you present the table.** The scan cannot propose it — it grades what the
Terraform declares, and marks a service with no clean Google Cloud equivalent `escalate`, which
means exactly "a human decides". This step is where that human is. A reviewer who is never told
the option exists reads `escalate` as a problem to solve rather than a choice to make, and the
migration acquires a data project nobody asked for.

Record the decision, with the customer's reason:

```
annotate_data_dependency(address="module.orders_ddb", disposition="keep-in-aws",
                         note="staying on DynamoDB; the analytics pipeline reads it directly")
```

Be straight about what it commits them to, and put these in the same breath as the option:

- **Cross-cloud connectivity.** The workload on GKE has to reach the service — a VPN or
  Interconnect, and network rules to match. That is landing-zone work, and nothing in the agent
  provisions it today.
- **Credentials across the boundary.** The pod needs AWS credentials it used to get from IRSA.
  The GKE equivalent is not automatic.
- **Egress cost and latency on every call.** The assessment's risk register already carries
  "cross-cloud egress cost during co-existence"; a service kept in AWS is precisely that risk,
  and it wants the same guardrail — a budget alert and a capped co-existence period.

What it changes downstream: only `migrate` should hold up an application team, so a `keep-in-aws`
entry does not gate anybody. It stays in the inventory, it is still a dependency, and it is now a
recorded decision rather than an open question.

Do not assume it. An entry the customer has not ruled on stays `escalate` or `undecided`, and
that is an honest state to approve — see the rules below.

| Disposition | Meaning |
|---|---|
| `migrate` | Real data to move. Holds up the application team until it is done. |
| `rebuild` | Starts empty on the target; nothing to copy (a cache). |
| `replatform` | The wiring is rebuilt; no data carried across (queues, streams). |
| `escalate` | No clean Google Cloud equivalent. The customer decides — and `keep-in-aws` is one of the things they may decide. |
| `keep-in-aws` | Stays where it is, reached across the cloud boundary. Only a human ever sets it — here, or later at the deployment data migration step, where it is the exit for a service that turns out not to be movable. |
| `undecided` | Recognised, no plan yet. |

## What happens next

- **Approve** → the graph advances to `STATE_DISCOVERY_RUNNING` and extraction starts.
- **Decline** → the review stays open. The corrections already recorded are kept. Change
  something before submitting again.

## What survives, and what does not

Corrections are durable. They live in `platform/discovery/data_consumer_overrides.json`
and are replayed over every later scan, so an amended scope (which re-runs the scan) does
not lose them. Say so when the user hesitates over a correction they think they may have
to redo.

The approval is not durable, deliberately. A re-scan rebuilds the section from the
checkout, so the sign-off is asked again rather than carried onto entries nobody saw.

## Rules

- Only attach what the **user** states. The ranking is a prompt for them, never a fact.
- An entry with no consumer is not a problem to be cleared. Leaving it unattributed is a
  legitimate outcome: it still needs migrating, it just cannot be placed with a team yet,
  and saying so is more useful than a guess. Two exceptions, and the tools already withhold
  the sentence for both — do not put it back. A service the customer decided to keep in AWS
  is not being migrated, so there is no team to place it with. And an entry the listing
  marks as one the scan could not answer for (part of the Terraform was unreadable) has no
  finding behind it at all: "nobody uses it" is a claim the scan declined to make.
- An empty section still gets the sign-off, and it is the case where the scan notes matter
  most: an estate declared in CloudFormation, Crossplane or CDK produces an empty result
  too, and the notes are the only thing separating that from an estate with no data
  services. Relay them before asking.
- If a tool returns an `ERROR`, report it verbatim and stop. The state stays here, so the
  step is retryable.
