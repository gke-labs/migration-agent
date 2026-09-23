# Moving the data services

Reference for `STATE_DEPLOYMENT_DATA_MIGRATION`. The estate's data services were
harvested at discovery, attributed to the workloads that use them, and graded at
the data review. This document is how the ones graded `migrate` actually move.

**Every command in the runbooks this document points at is one the USER runs —
printed, never run.** The commands moved to `runbooks/`; the rule did not move
with them, and it holds for the agent as much as for the server: an agent on
the operator's workstation may well have AWS and GCP credentials in its
environment, and running these
would move customer data, and in the secrets case put plaintext through the
agent process and the transcript. Print the command, explain it, let the
operator run it. The rule is DESIGN.md §10's — the agent is forbidden from cloud
CLIs — and `replication._login_command` is the same discipline in code.

Nothing here runs on the server either: it never holds AWS credentials (the
no-AWS contract in DESIGN.md §10) and never observes the copy happening.
Completion is therefore an assertion the operator makes, which is why
`mark_data_service_migrated` records who said so and when.

## Before anything moves

Three preconditions, in order. Skipping the first is the common failure.

1. **The landing zone Terraform has to be applied.** The translation phase ends
   at a Pull Request; nothing in this agent applies it. Until somebody merges
   and applies that PR, the Cloud SQL instances and buckets these procedures
   target do not exist.
2. **Network reachability between the source and the target.** Most of these
   tools pull from AWS, so the AWS side needs to permit it — a security group
   rule, a VPC endpoint, or a public endpoint with an allowlist. For a service
   the customer chose to keep in AWS this is permanent infrastructure rather
   than a migration convenience; that case is out of scope here.
3. **Somewhere to fail safely.** Every procedure below is re-runnable, but a
   cutover is not. Agree the rollback (keep the AWS source readable until the
   workload has run against the target for a while) before starting.

## The general shape

Managed data services divide into three kinds, and the difference decides
everything about the cutover:

- **Continuous replication is possible** (RDS except MariaDB): a tool keeps
  the target in step with the source while the source is still serving. The
  cutover is a short pause — stop writes, wait for lag to reach zero, repoint
  the workload. Minutes.
- **Repeatable but not continuous** (S3, EFS): a tool re-runs and moves
  only what changed, so the source can stay live while it catches up. The
  cutover is a final pass after writes stop — short, but not instant, and it
  is the pass that makes the copy consistent.
- **Continuous replication is not possible** (MemoryDB, a persistent
  ElastiCache, secrets, parameters): the content is copied once, and anything
  written to the source afterwards is lost. The cutover is "copy last,
  immediately before the workload starts against the target". MemoryDB is here
  despite being durable: there is no managed replication path to Memorystore,
  only a snapshot export.

FSx belongs to none of the three until its flavour is known — Lustre copies
like EFS, ONTAP is a vendor replication, OpenZFS is a re-platform — which is
why its row and its runbook both say "decide the target first" rather than
naming a class.

Grade every service into one of those three before planning the day; the
table below states each one's class, and the runbook restates it in its own
opening line.

## Per service

**The steps live in `runbooks/`, one file per procedure**, and
`get_data_migration_runbook` is how they reach an operator: it returns the
matching template with this estate's own facts above it, and the agent adapts
it before anybody sees it. What follows is the shape of each move and the
thing about it worth knowing before you start — enough to have the
conversation, not enough to run it.

| Service | Runbook | Cutover class |
|---|---|---|
| `rds`, PostgreSQL | `rds-postgres.md` | continuous (DMS) |
| `rds`, MySQL | `rds-mysql.md` | continuous (DMS) |
| `rds`, SQL Server | `rds-sqlserver.md` | continuous (DMS, from backups) |
| `rds`, MariaDB | `rds-mariadb.md` | point-in-time — no continuous path |
| `aurora` | — | escalation; the tool declines, and the knowledge above says why |
| `memorydb` | `memorydb.md` | point-in-time |
| `elasticache` when upgraded to `migrate` | `elasticache-persistent.md` | point-in-time |
| `s3` | `s3.md` | repeatable incremental |
| `efs` | `efs.md` | repeatable incremental |
| `fsx` | `fsx.md` | decide the target first |
| `secretsmanager` | `secretsmanager.md` | point-in-time, and the value usually changes |
| `ssm` | `ssm.md` | point-in-time, split between two targets |

Everything else has no runbook on purpose. `runbooks.NO_RUNBOOK_REASON` carries
the sentence saying why, and the refusal quotes it.

### `rds` → Cloud SQL

Database Migration Service (DMS) does continuous replication for PostgreSQL and
MySQL, which is the overwhelming majority of what the harvester grades `migrate`.

Steps: `rds-postgres.md`, `rds-mysql.md`. Both carry the connection-profile and
migration-job commands, the validation gates, and the limitation list — which
is the part worth reading before the window rather than during it.

The source needs logical replication enabled (`rds.logical_replication=1` in the
parameter group for PostgreSQL, binary logging for MySQL), which is a **reboot**
on RDS — schedule it, it is not free.

**SQL Server** is also a DMS path — homogeneous, continuous, to Cloud SQL for
SQL Server (`rds-sqlserver.md`). What differs is the precondition:
full-recovery-model backups written to a Cloud Storage bucket DMS can read,
not the `rds.logical_replication` reboot. No engine change is involved. What
does not travel with the databases is everything at server level — logins, SQL
Agent jobs, linked servers — and that inventory is the part people miss.

**MariaDB** is not a DMS path at all: DMS does not accept it as a source and
Cloud SQL has no MariaDB engine. `rds-mariadb.md` takes a dump-and-import
window onto Cloud SQL for MySQL, and it opens by asking whether that is the
right call — a schema using Aria, `SEQUENCE` objects or system-versioned
tables is a MariaDB schema, and the honest answers there are self-managed
MariaDB or keeping it in AWS.

**Oracle** is not. Cloud SQL has no Oracle engine, so Oracle → Cloud SQL for
PostgreSQL is an engine change, which this repo escalates by policy. If the
harvester graded an Oracle instance `migrate`, say so and treat it as an
escalation rather than improvising.

### `memorydb` → Memorystore

MemoryDB is durable, which is why the harvester grades it `migrate` while
ElastiCache is `rebuild`. There is no managed replication path: the move is an
RDB snapshot exported to S3, staged through Cloud Storage, and imported.
Steps: `memorydb.md`, and `elasticache-persistent.md` for the Redis the
harvester upgraded.

This is a point-in-time copy. Anything written after the snapshot is lost, so it
belongs immediately before cutover. The export lands as one RDB object per shard,
named after the target snapshot; Memorystore imports a single RDB, so a
multi-shard MemoryDB cluster needs the shards merged (or a target shape decided)
before this step — treat that as an escalation rather than improvising.

### `s3` → Cloud Storage

Storage Transfer Service, which can run repeatedly and only moves what changed.
Steps: `s3.md`. Run it on a schedule while the source is live, then once more
after writes stop.

Watch for: object ACLs do not translate (Cloud Storage uses IAM and, optionally,
uniform bucket-level access), storage classes are named differently, lifecycle
rules have to be re-expressed rather than copied, and **S3 signed URLs do not
work against Cloud Storage** — that last one is an application change, found
either now or in production.

### `efs` / `fsx` → Filestore, Parallelstore or NetApp Volumes

Storage Transfer Service handles POSIX sources, but needs an agent running on a
host that has the source mounted. Steps: `efs.md`, `fsx.md`.

EFS access points are the part that is a translation rather than a copy: they
impose a root directory and a POSIX identity per client, Filestore has no
equivalent, and what they enforced becomes a directory convention plus a pod
security context.

FSx has four flavours in the harvester's table (Lustre, OpenZFS, Windows, ONTAP)
and they do not share a target. Windows and ONTAP in particular are usually a
NetApp Volumes conversation rather than a copy. If the target is not obvious,
that is a signal to keep it in AWS for now rather than to improvise.

### `secretsmanager` / `ssm` → Secret Manager

No bulk tool, and deliberately so — this is the case the harvester's own
comment singles out. Seven of thirteen entries in a typical estate are secrets
holding the passwords for the other six, and an application whose connection
string did not come across starts and immediately fails, with nothing to rebuild
it from.

Steps: `secretsmanager.md`, `ssm.md`. Parameter Store splits in two on the way
across — `SecureString` parameters are secrets and go to Secret Manager, while
`String` and `StringList` are configuration and belong in a ConfigMap in the
GitOps repository, reviewable rather than fetched at runtime.

Two things to get right. The **values change** during the migration — a Cloud
SQL instance has a different host, port and password than the RDS instance it
replaced — so copying the AWS value verbatim is usually wrong. And the workload
reads them through a different mechanism on GKE: the IRSA role that granted
access to Secrets Manager has no equivalent unless Workload Identity is wired
up, which is the translation phase's job, not this one.

## Services the harvester did not grade `migrate`

The grades that are not `migrate` are `rebuild`, `replatform`, `escalate`,
`keep-in-aws` and `undecided` — the last is what the scan assigns to a service
it could not place, and like the others it gates nothing.

These do not gate a component, and should not be treated as work owed here —
**unless the listing says otherwise**. A grade is a default, and two things
override it: the harvester upgrades some services on evidence (the persistent
Redis below), and a human can upgrade any of them at this step with
`annotate_data_dependency(address=..., disposition="migrate")`, which is the
ordinary way to record "we decided to move it after all". `list_data_migrations`
is authoritative; this list explains the defaults it starts from.

- `elasticache` is **usually** `rebuild` — an empty cache is a working cache,
  and the warm-up cost is real while the data loss is not. But the harvester
  upgrades it to `migrate` when the declaration sets
  `snapshot_retention_limit > 0`: that Redis persists, and treating it as
  rebuildable would silently discard data. If the listing shows an
  `elasticache` entry as owed, that is why — move it the way MemoryDB moves,
  by RDB snapshot export and import, and do not go looking for a mistake.
- `kinesis`, `msk`, `sqs`, `sns`, `eventbridge`, `firehose` and `mq` are
  `replatform` — the shape changes (Pub/Sub, Managed Service for Kafka,
  Cloud Logging) rather than the bytes moving. Drain the source, cut over
  producers, and accept that in-flight messages are the cutover's cost.
- `opensearch` is graded `replatform` with them and does **not** behave like
  them. It is a document store: there is no producer to cut over and there IS
  data to move — a snapshot-and-restore, or a re-index from the source of
  truth — so never tell an operator an empty target is a working one. The
  realistic destinations are Elastic Cloud on GCP or a self-hosted cluster on
  GKE, and choosing between them is a decision before it is a procedure.
  `runbooks.NO_RUNBOOK_REASON` says the same; if you are editing one, edit
  both.
- `aurora`, `docdb`, `neptune`, `dynamodb`, `redshift` are `escalate` — there is
  no equivalent to move to, and the answer is a decision rather than a
  procedure. Aurora → AlloyDB is an engine change. DynamoDB has no GCP
  equivalent worth pretending about; keeping it in AWS and reaching it across
  the boundary is frequently the right answer.

## Deciding to keep one in AWS instead

Legitimate, and the point at which people usually realise it: a migration that
looked routine turns out to need an engine change, a licence, or a downtime
window nobody will approve. `annotate_data_dependency(address=...,
disposition="keep-in-aws", note=...)` records that decision durably, and the
service stops being owed work and stops gating any component.

It is not free. A kept service needs cross-cloud connectivity, a credential path
for a pod that used to get one from IRSA, and egress on every call. Say so when
recording it — the note is the durable record of why, and nothing else derives
those consequences today (DESIGN.md issue 30).

## Abandoning an image copy

The image equivalent of keeping a data service in AWS, and the same kind of
decision. A copy that failed and nobody is going to chase, or a runbook nobody
is going to run:

```
abandon_image_replication(refs=["<source ref>"], reason="...")
```

The reason is required. The image stays in ECR, workload translation leaves its
reference untouched (nothing is at the Artifact Registry address), and the
cluster therefore needs ECR pull credentials and pays cross-cloud egress on
every pull that misses the node cache. Say that when you record it.

If the image is copied after all, naming it in
`mark_replication_complete(refs=[...])` re-opens the decision — a bulk call
will not, on purpose.

## Reporting back

- `mark_data_service_migrated(address=..., target=..., note=...)` when it is
  live and verified. `target` is recorded verbatim; the server cannot check it.
- `mark_data_service_migrating(address=..., note=...)` for a move that is under
  way. It does **not** satisfy the gate — a component must not ship against a
  database that is still copying — but it tells the next person where things
  stand, which for a multi-week migration is most of the value.

Both carry `note` forward across repeated calls at the same status, so omitting
it keeps what is there rather than clearing it. `note=""` is the only way to
remove one — worth knowing, because the note goes into the runbook a platform
engineer reads before shipping a component, and a stale one there is read as
current. A note is dropped automatically when the status changes, since it
described the status it was written against.

Both are called from this state, and the workspace stays in it until every
service graded `migrate` has been reported migrated **or excused with
`keep-in-aws` (`annotate_data_dependency`)**, **and every planned or failed image
copy has been confirmed (`mark_replication_complete`) or abandoned
(`abandon_image_replication`)** — both halves hold the step open, and the close
refuses naming whichever is left. A six-week migration parks the platform graph,
which is the honest thing for it to do. Nothing else is held up by that:
application teams run on their own graph, and the deployment exports publish
from here.

What the close does **not** wait on is a move reported in progress against a
service graded something other than `migrate` — a Redis being rebuilt whose
data somebody is copying anyway, say. Nothing gates on it, so the close
proceeds; and because `mark_data_service_migrated` is callable only from this
state, that record can then never be completed. The record itself survives and
the artifacts keep showing it, but it stays "in progress" for good. The tools
say this wherever they offer the close. If the operator wants the completion on
record, the move has to be reported before the step closes.
