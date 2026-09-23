# MemoryDB for Redis → Memorystore

**Moves by:** RDB snapshot export, staged through Cloud Storage, then import.
**Cutover class:** point-in-time copy. **There is no continuous path**, so
everything written to the source after the snapshot is lost.

MemoryDB is durable, which is why it is graded `migrate` while ElastiCache is
graded `rebuild`. Treating it as a cache you can start cold discards data
nobody else holds.

Every command below is yours to run, with your own credentials.

## Substitutions

| Placeholder | What it is |
|---|---|
| `<TARGET_ARGS>` | The targeting arguments exactly as the tool printed them — `address="…"`, plus `directory=`/`identifier=` when it showed them. Two root modules can declare one address, and the bare address is then refused |
| `<CLUSTER_NAME>` | The MemoryDB cluster name |
| `<SNAPSHOT>` | A name for the snapshot you take |
| `<S3_BUCKET>` | An S3 bucket MemoryDB exports the snapshot to |
| `<STAGING_BUCKET>` | A Cloud Storage bucket to stage the RDB in |
| `<GCP_PROJECT>` | The project holding the Memorystore instance |
| `<GCP_REGION>` | The Memorystore region — match the GKE cluster's |
| `<TARGET_INSTANCE>` | The Memorystore instance id |

## Before you start: how many shards?

A multi-shard MemoryDB cluster exports **one RDB object per shard**, and
Memorystore imports a single RDB. If this cluster has more than one shard, the
shards have to be merged, or the target shape decided differently, before the
import will work.

That is a design decision, not a step. Treat it as an escalation rather than
improvising a merge — and say so to the operator before they take the
snapshot, not after.

## 1. Prepare the source

- `<S3_BUCKET>` needs a bucket policy granting the AWS snapshot-export
  service principal write access — the export is not performed under a
  role you attach to the cluster, so an IAM policy on a role does not
  authorise it. `elasticache-persistent.md` says the same about the same
  mechanism. Get this wrong and `copy-snapshot` fails with the writers
  already stopped.
- Snapshot the cluster at a moment you can name. Because the copy is
  point-in-time, this belongs immediately before the cutover rather than days
  ahead.

## 2. Prepare the target

The landing zone Terraform has to be applied first. Size the Memorystore
instance at or above the source's used memory, plus headroom for the import
itself.

## 3. Run the copy

```bash
aws memorydb create-snapshot \
    --cluster-name <CLUSTER_NAME> --snapshot-name <SNAPSHOT>

aws memorydb copy-snapshot \
    --source-snapshot-name <SNAPSHOT> \
    --target-snapshot-name <SNAPSHOT>-export \
    --target-bucket <S3_BUCKET>

# One object per shard. Run this from a host that holds both credentials.
gcloud storage cp "s3://<S3_BUCKET>/<SNAPSHOT>-export-*.rdb" \
    gs://<STAGING_BUCKET>/

# Positionals are SOURCE then INSTANCE — the RDB first, the instance second.
gcloud redis instances import \
    gs://<STAGING_BUCKET>/<SNAPSHOT>-export-0001.rdb <TARGET_INSTANCE> \
    --region=<GCP_REGION> --project=<GCP_PROJECT>
```

The Memorystore service account needs read on `<STAGING_BUCKET>`.

## 4. Validation gates

- The import completes and the instance returns to `READY`.
- Key count on the target matches the source's at snapshot time
  (`DBSIZE` on both).
- A sample of keys returns equal values and comparable TTLs — TTLs continue
  counting down from the snapshot, so they will be shorter, not equal.
- The application's own read path works against the target.

## 5. Cutover

1. Stop the writers.
2. Take the snapshot and run the export and import above.
3. Repoint the application at the Memorystore endpoint.
4. Restart the writers.

The gap between snapshot and repoint is data loss. That is the whole cost of
this procedure, and it is why the snapshot happens in the window rather than
before it.

## 6. After cutover

Keep the MemoryDB cluster for the soak period as the rollback, then delete it,
then report:

```
mark_data_service_migrated(<TARGET_ARGS>, target="<TARGET_INSTANCE>")
```

## Rollback

Repoint at the MemoryDB cluster. Anything written to Memorystore after cutover
is lost — there is no reverse import that preserves it.

## Known limitations

- No managed continuous replication exists between MemoryDB and Memorystore.
  A snapshot is the only supported transport.
- One RDB per shard on export; one RDB per import. Multi-shard is the
  escalation described above.
- TTLs survive the snapshot but keep expiring against wall-clock time.
- Redis module usage, if any, does not travel. Check before the window.
- MemoryDB's durability guarantee (multi-AZ transaction log) has no
  Memorystore equivalent to configure identically; decide the target's
  persistence and HA settings deliberately rather than assuming parity.
