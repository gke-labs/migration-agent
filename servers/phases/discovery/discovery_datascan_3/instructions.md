# Discovery Step 3a — Record the Managed Data Services

**DAG state:** `STATE_DISCOVERY_DATA_SCAN` · **Expected tool call:** `scan_data_dependencies`

The scope is confirmed. Before extraction spends anything on the LLM workers, record
the managed data services the workloads depend on — databases, object storage, caches,
queues, streams, secret and parameter stores — the cluster DNS configuration: the
CoreDNS Corefile and add-on settings, copied verbatim — and the source network address
space: the VPC and subnet CIDRs, each cluster's service range and hybrid remote ranges,
and the ranges the VPC routes to, whatever the target (peering, a transit gateway, a VPN, an
appliance instance, a gateway). This is deterministic:
the server reads Terraform, eksctl and CloudFormation declarations and plain manifests
from the in-scope files. Nothing goes to a model, and nothing is interpreted.

## What to do

1. Call `scan_data_dependencies()`. It takes no arguments in the normal case — the
   server reuses the checkout discovery already scanned.
2. If it returns an error saying there is no local checkout, the migration is being
   resumed on a different machine from the one that ran discovery. Ask the user for
   the path of their clone of the source repository, then call
   `scan_data_dependencies(source_root=<that path>)`. Do not clone it yourself, and do
   not guess a path.
3. Present the result to the user as a short table: the service, what it is called,
   which workloads use it, and what the migration will do with it. Lead with the count
   that needs migrating, since those are the ones that will hold up an application team
   later.
4. Relay every `scan_notes` line verbatim. They record what was **not** scanned —
   files the confirmed scope excludes, and the fact that only Terraform is parsed. An
   estate declared in CloudFormation, Crossplane or CDK produces an empty result, and
   the note is the only thing distinguishing that from an estate with no databases.
   One of them says how many corrections from an earlier review were replayed over this
   scan, and how many named a block this scan no longer finds.
5. Report the `cluster_dns` block in one or two sentences: how many sources carry a
   Corefile or add-on configuration and where they are, then every `scan_notes` line under
   it verbatim. Those notes are the whole story for anything the scan saw but could not
   record — a managed add-on at its defaults, a Corefile behind a variable or a `file()`
   the scan does not follow, a Helm chart whose values were not read. An unread Corefile
   is reported as unread, never guessed at; if the user can paste it, say that the
   translation later needs the text and where it would come from.
6. Report the `address_space` block as a short table: each VPC with its CIDR and whether
   the cluster sits in it, each cluster's service range, public-endpoint CIDRs and remote
   node/pod ranges, and the routed ranges (`10.20.0.0/16 via vpc_peering_connection`).
   These are the ranges the landing zone must not overlap, and the design step later
   proposes target ranges from them. Then list every entry under `unresolved` in the
   summary with its expression: each is a range the files build from something the scan
   does not follow (a variable with no default, a `for` expression over a data source, an
   `${ENV}` placeholder) and only the user can supply it — ask for the value, and do not
   fill it in yourself. The summary's `unresolved_but_covered` counts the entries the
   inventory keeps for the record that need no answer of their own — a subnet's range inside
   a VPC whose range is stated or whose range is itself among the questions, or a
   public-endpoint allow list; say the count and ask nothing for them. A VPC marked
   `defaulted` is eksctl's own default, not a declaration;
   say so. Relay every `scan_notes` line under it verbatim: an estate in CDK or Pulumi
   produces an empty section, and the note is what distinguishes that from an estate
   whose ranges are all unresolved.
7. The graph moves to the data review (`STATE_DISCOVERY_DATA_REVIEW`) — the step where
   the user corrects the mapping. Follow its instructions; extraction comes after it.

## What the dispositions mean

| Disposition | Meaning |
|---|---|
| `migrate` | Real data to move. Will hold up the application team until it is done. |
| `rebuild` | Starts empty on the target; nothing to copy (a cache). |
| `replatform` | The wiring is rebuilt; no data carried across (queues, streams). |
| `escalate` | No clean equivalent on Google Cloud — the customer decides. Keeping the service in AWS is one of the things they may decide; the review step after this one records it. |
| `undecided` | Recognised but with no plan. A human decides. |

An entry marked `identifier_is_fallback` is named after its Terraform address rather
than its real AWS name, because the declaration builds the name from variables the
files do not resolve. Say so if you quote it; it is not the name in the console.

## Entries known only from an ARN or endpoint

`known_only_from_an_arn_or_endpoint` counts entries with `detection: referenced`: data
services the files reach — an IAM policy grants a role access to them, an IRSA module names
them, a Helm value carries them, a ConfigMap or a manifest's env holds their hostname or URL
— but no declaration in the scanned repository states their name.
Two things can be true, and the scan cannot tell which: they were provisioned somewhere
else (another team's Terraform, the console, a pipeline), or this repository declares them
under a name built from variables the scan does not resolve. **The first kind are the
dependencies that get forgotten**, because no file here owns them, so say explicitly that
they exist, that their engine, size and settings are unknown to the scan, and that the
review step asks the user which kind each one is. Their `address` is the ARN itself, or the
canonical endpoint when no ARN was seen; that is how the review step's tools name them.
Their `account` is read from the ARN (an S3 ARN never states one). When the estate's own
`aws` provider block names its account and a referenced entry's differs, the entry carries a
note starting "in AWS account …": a **cross-account dependency**, which the assessment grades
at its worst band for a database without a replica path. Call those out by name; such an
entry is never merged with a same-named declared resource. That verdict is only given when
every `aws` provider block states its account literally and nothing in scope went unread.
When any provider is silent or states it with an expression, a file was excluded or could
not be read, or none states one, an entry whose ARN names an unlisted
account — including a declared entry an ARN was merged into — carries a note starting "its
ARN names AWS account …", and a scan note counts them. Report those as unknown, not as
same-account and not as cross-account: the ARN may name a same-named resource in another
account, and only the user can say.

Regions work the same way from each provider block's `region`: an entry whose ARN names a
region no provider block states carries a note starting "in AWS region …" and is never merged
with a same-named declared resource — a cross-region dependency, which needs its own
connectivity and latency conversation.

That verdict, like the account one, is only given when every `aws` provider block states its
region literally **and nothing in scope went unread** — and excluding a file at scope sign-off
counts as unread, so on a scoped run it usually will not fire. When it cannot, an entry whose
ARN names an unlisted region carries a note starting "its ARN places it in AWS …" instead,
including a declared entry the ARN was merged into, whose recorded region then came from that
ARN. Report those the same way as the account case: unknown, neither same-region nor
cross-region, and only the user can settle it.

One thing the scan records with a caveat rather than merging: a literal ARN whose name and kind
match a resource the scan counts through its primary (an ElastiCache group member, a read replica,
an Aurora cluster instance). It may be that same resource recorded twice, or a different one of the
same name elsewhere — the ARN states an account and a region the declaration does not, so only the
user can say. The entry carries a note beginning "possibly a duplicate:"; put those to the user and
say so at the review if they are one: attach the consumers to the entry you keep and
annotate the other — the review has no merge tool, and a duplicate that stays graded
`migrate` gates the consuming component until it is reported or annotated.

One referenced entry is NOT an out-of-band dependency: an ARN naming a declared secret or
table in a region its declaration replicates into (`replica { region = … }`), in an account
not known to be another's — when no provider states the estate's account, the replica note
comes with the unknown-account note beside it, and the pair means "the estate's replica, or a
same-named secret in an account the files do not name"; relay both. Its note begins "the
replica in AWS region …". It is the estate's own replica —
re-created by the primary's replication on the target rather than moved — so it stands apart,
is graded `rebuild` for Secrets Manager, nothing waits on it, and any workload granted only
its ARN is already copied onto the primary's consumers. Relay it as the primary's replica, not
as a resource provisioned elsewhere.

## Guesses

`guesses_needing_a_yes_or_no` counts entries with `detection: inferred`. A configuration
key's name typed its value — `INVOICE_BUCKET=acme-invoice-archive` in a Deployment's env, a
chart's `invoiceBucket:` value, `SQS_QUEUE` in a ConfigMap — and nothing in the files declares
such a resource or names it by ARN or endpoint. The scan reads Terraform **and** the chart
values and manifests in scope for these. A guess has disposition `undecided`, gates nothing,
and is answered at the review step (confirm or dismiss); the sign-off there refuses while any
is open. Report them as questions, not findings.

The scan also reads endpoints — an RDS hostname in a ConfigMap, a queue URL in a Helm value —
as literally as ARNs; those are `referenced` entries, not guesses, and the account and region
verdicts above apply to them as to an ARN.

Three things the scan deliberately does not record, and says so in `scan_notes`: an ARN
with a wildcard in the resource name (`arn:aws:s3:::acme-*` names a family, not a
bucket), an ARN whose name is built from a variable, and an ARN- or endpoint-shaped string it could
not read (an anonymised account, a missing field). Relay those notes — a wildcard grant
still means the workload reaches *something* the scan could not name. Prose is never
read: a `description` argument is skipped to the end of its value — the line, a call or
list it opens, or a heredoc — so an example ARN in a description does not become a
dependency — a variable's
`default` or an output's `value` is configuration and is read.

## Entries with no consumers

`unattributed_to_any_workload` counts the data services that **the two reference chains
this scan follows** did not connect to a workload. Report that number — it is not noise,
and it is not a count of unused resources. Say what it means precisely, because the
difference matters to whoever goes looking: the scan follows a Kubernetes-deploying
Terraform resource reaching a datastore through a module output, and an IRSA role
granting access to one. A link made any other way is not counted, and there are two
common ones **inside** the Terraform — an endpoint passed through a `local`, or into a
child module as an input variable — besides the ones outside it, like a deploy pipeline
or a console-created resource. Those entries still need migrating; somebody has to say
which team they belong to before a gate can hold anything on them.

`consumer_unknown_unreadable_terraform` is a different number and must not be folded
into the first. It counts entries where part of the Terraform could not be read to the
end, so the scan does not know whether a workload references them — the answer is
missing, not negative. Report it separately, and say the file could not be fully read.

Do not guess the owner from a name that looks similar. The scan deliberately does not,
and a wrong attribution stops the wrong team's release. Attaching one is the next step's
job, where the user decides and a ranked guess is only ever offered as a prompt to them.

## If the scan fails

Report the error and stop. The state stays here, so the step can be retried once the
problem is fixed — a missing checkout, a path typo, a concurrent write. Do not skip
ahead to extraction: the section this step fills is what later decides which
application teams can proceed.
