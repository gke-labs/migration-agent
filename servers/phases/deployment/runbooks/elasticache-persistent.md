# ElastiCache (persistent) → Memorystore

**Moves by:** RDB snapshot export, staged through Cloud Storage, then import.
**Cutover class:** point-in-time copy.

**Why you are reading this.** ElastiCache is graded `rebuild` by default — an
empty cache is a working cache, and the warm-up cost is real while the data
loss is not. This runbook is offered only when the harvester found
`snapshot_retention_limit > 0` on the declaration, or a human upgraded the
grade by hand. That Redis persists, so something in the estate is treating it
as a store rather than a cache.

**The first question is whether that is true.** If the data really is
rebuildable, the cheaper answer is to provision the target empty, cut over, and
accept a cold start — and to record the decision with
`annotate_data_dependency(<TARGET_ARGS>, disposition="rebuild",
note="...")`. Ask before running any of this. A cache stampede at cutover is a
real cost and it is a smaller one than a multi-hour window.

If the data is durable — counters, rate-limit state, sessions nobody wants to
drop, anything with no other source of truth — continue.

Every command below is yours to run, with your own credentials.

## Substitutions

| Placeholder | What it is |
|---|---|
| `<TARGET_ARGS>` | The targeting arguments exactly as the tool printed them — `address="…"`, plus `directory=`/`identifier=` when it showed them. Two root modules can declare one address, and the bare address is then refused |
| `<REPLICATION_GROUP>` | The ElastiCache replication group id, or the cache cluster id for a standalone one |
| `<SNAPSHOT>` | A name for the snapshot you take |
| `<S3_BUCKET>` | An S3 bucket ElastiCache exports the snapshot to |
| `<STAGING_BUCKET>` | A Cloud Storage bucket to stage the RDB in |
| `<GCP_PROJECT>` | The project holding the Memorystore instance |
| `<GCP_REGION>` | The Memorystore region — match the GKE cluster's |
| `<TARGET_INSTANCE>` | The Memorystore instance id |

## 1. Prepare the source

- The ElastiCache service principal needs write on `<S3_BUCKET>`, granted
  through the bucket policy that ElastiCache's export requires.
- Cluster mode on: the export produces **one RDB per shard**, and the same
  merge question as MemoryDB applies. Cluster mode off: one RDB, and this is
  straightforward.

## 2. Prepare the target

The landing zone Terraform has to be applied first. Size the Memorystore
instance at or above the source's used memory, with headroom for the import.

Decide the target shape now, and note what this procedure can actually
finish. The import below is `gcloud redis instances import`, which targets
**Memorystore for Redis** — a single instance, one RDB. If the source is
cluster-mode-on and has to stay sharded, Memorystore for Redis Cluster is the
right target and this runbook is not the path to it: the export produces one
RDB per shard and there is no supported merge. That case is the escalation the
opening and the limitations both name — settle it before the window, not in
it.

## 3. Run the copy

The snapshot flag depends on which ElastiCache shape the declaration
created, and they are mutually exclusive. The harvester maps three resource
types onto `elasticache`: a replication group takes `--replication-group-id`,
a standalone `aws_elasticache_cluster` takes `--cache-cluster-id`, and
`aws_elasticache_serverless_cache` has neither — it uses its own verb. Use the
one that matches the entry.

```bash
# Replication group (cluster mode on, or a group with replicas):
aws elasticache create-snapshot \
    --replication-group-id <REPLICATION_GROUP> --snapshot-name <SNAPSHOT>

# Standalone cache cluster:
aws elasticache create-snapshot \
    --cache-cluster-id <REPLICATION_GROUP> --snapshot-name <SNAPSHOT>

# Serverless cache — see the note below; its snapshots are a separate
# resource family and the export path differs from everything after this line.
aws elasticache create-serverless-cache-snapshot \
    --serverless-cache-name <REPLICATION_GROUP> \
    --serverless-cache-snapshot-name <SNAPSHOT>

# Non-serverless only. A serverless snapshot is NOT a snapshot for the
# purposes of this call and will not be found — see the note below.
aws elasticache copy-snapshot \
    --source-snapshot-name <SNAPSHOT> \
    --target-snapshot-name <SNAPSHOT>-export \
    --target-bucket <S3_BUCKET>

# Run from a host holding both credentials. The export names one object per
# shard, `<SNAPSHOT>-export-000N.rdb`, including for a single-shard cache —
# so list what actually landed before writing the import line.
gcloud storage cp "s3://<S3_BUCKET>/<SNAPSHOT>-export-*.rdb" \
    gs://<STAGING_BUCKET>/
gcloud storage ls gs://<STAGING_BUCKET>/

# Positionals are SOURCE then INSTANCE — the RDB first, the instance second.
gcloud redis instances import \
    gs://<STAGING_BUCKET>/<SNAPSHOT>-export-0001.rdb <TARGET_INSTANCE> \
    --region=<GCP_REGION> --project=<GCP_PROJECT>
```

**If this is a serverless cache**, stop after the create and work out the
export before the window. Serverless snapshots live in their own resource
family — `describe-serverless-cache-snapshots`, `copy-serverless-cache-snapshot`
and an export verb of their own — so `copy-snapshot` above does not see them,
and neither does anything downstream of it. This runbook does not spell that
path out because the surface has been moving; check `aws elasticache help` for
the current verbs and confirm the export lands an RDB in S3 before you commit
to a cutover time. If it does not, keeping the cache in AWS or rebuilding it
cold are both better answers than improvising in the window.

## 4. Validation gates

- The import completes and the instance is `READY`.
- `DBSIZE` matches the source at snapshot time.
- A sample of keys returns equal values; TTLs are shorter by the elapsed time,
  which is expected.
- The application's read path works against the target.

## 5. Cutover

1. Stop the writers, or accept that writes after the snapshot are lost — for a
   rate-limit counter that may genuinely be acceptable, and saying so out loud
   is better than pretending the window is free.
2. Snapshot, export, import.
3. Repoint the application at the Memorystore endpoint.
4. Restart the writers.

**Warm-up.** If you did take the empty-target route after all, expect a
thundering herd on the backing store while the cache fills. Pre-fill it, or
shadow traffic at it, before sending real requests.

## 6. After cutover

Keep the source for the soak, then delete it, then report:

```
mark_data_service_migrated(<TARGET_ARGS>, target="<TARGET_INSTANCE>")
```

## Rollback

Repoint at ElastiCache. Writes to Memorystore after cutover are lost.

## Known limitations

- No continuous replication path. Snapshot only.
- Cluster mode on exports one RDB per shard; Memorystore imports one RDB.
  Multi-shard needs a merge or a target-shape decision — treat it as an
  escalation.
- Redis version parity matters: an RDB written by a newer Redis may not import
  into an older target. Check both versions before the window.
- The export writes one RDB per shard (`-0001`, `-0002`, …) and Memorystore
  imports a single RDB, so a multi-shard cache needs the merge decision above
  settled before the window rather than during it.
- ElastiCache-specific features (Global Datastore, data tiering) have no
  direct Memorystore counterpart to carry across.
