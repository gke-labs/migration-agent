# RDS for PostgreSQL → Cloud SQL for PostgreSQL

**Moves by:** Database Migration Service (DMS), continuous CDC.
**Cutover class:** continuous replication — the target is kept in step while
the source still serves, and the cutover is a short pause rather than an
outage.
**Engine change:** none. Same engine both sides, which is why this is a
planned move rather than an escalation.

Every command below is yours to run, with your own credentials. The agent
prints them and explains them; it does not run them, and neither does the
server — nothing in this system ever holds AWS credentials or watches the copy
happen. That is why completion is something you report at the end.

## Substitutions

| Placeholder | What it is |
|---|---|
| `<TARGET_ARGS>` | The targeting arguments exactly as the tool printed them — `address="…"`, plus `directory=`/`identifier=` when it showed them. Two root modules can declare one address, and the bare address is then refused |
| `<SOURCE_HOST>` | The RDS endpoint, `<name>.<id>.<region>.rds.amazonaws.com` |
| `<SOURCE_PORT>` | Usually `5432` |
| `<MIGRATION_USER>` | The Postgres role DMS connects as |
| `<GCP_PROJECT>` | The project the landing zone created the target in |
| `<GCP_REGION>` | The Cloud SQL region — match the GKE cluster's |
| `<SOURCE_PROFILE>` | A name for the DMS source connection profile |
| `<TARGET_PROFILE>` | A name for the DMS destination connection profile |
| `<JOB>` | A name for the migration job |
| `<TARGET_INSTANCE>` | The Cloud SQL instance id |
| `<TARGET_TIER>` | Destination machine tier, e.g. `db-perf-optimized-N-8` |
| `<TARGET_DB_VERSION>` | Destination version, e.g. `POSTGRES_15` |
| `<TARGET_DISK_GB>` | Destination disk size, at or above the source |
| `<AVAILABILITY_TYPE>` | `REGIONAL` for HA, `ZONAL` otherwise |
| `<ADMIN_USER>` | The destination's administrator user (`postgres`) |

## 1. Prepare the source

Logical replication on RDS is a **parameter group change and a reboot**.
Schedule it; it is not free and it is the step people forget until the job
refuses to start.

In a new parameter group attached to the instance:

- `rds.logical_replication = 1` — this is the RDS-managed way to get
  `wal_level = logical`. Do not also set `wal_level` directly; on RDS it is
  not yours to set.
- `shared_preload_libraries` includes `pglogical`.
- `wal_sender_timeout = 0`.
- `max_replication_slots` ≥ (databases being migrated × concurrent jobs) +
  whatever already uses slots. The default is 10.
- `max_wal_senders` ≥ `max_replication_slots` + existing senders.
- `max_worker_processes` ≥ databases being migrated + existing usage.

Then reboot the instance, and on **every** database except `template0`,
`template1` and `rdsadmin`:

```sql
CREATE EXTENSION IF NOT EXISTS pglogical;
```

Grant the migration user, per database, for every schema other than
`information_schema` and any beginning with `pg_`. `:schema` below is the loop
variable, not a substitution — run the block once per schema:

```sql
GRANT USAGE  ON SCHEMA :schema                      TO <MIGRATION_USER>;
GRANT USAGE  ON SCHEMA pglogical                    TO PUBLIC;
GRANT SELECT ON ALL TABLES    IN SCHEMA pglogical   TO <MIGRATION_USER>;
GRANT SELECT ON ALL TABLES    IN SCHEMA :schema     TO <MIGRATION_USER>;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA :schema     TO <MIGRATION_USER>;
GRANT rds_replication TO <MIGRATION_USER>;
```

`rds_replication` is the RDS substitute for `ALTER USER ... WITH REPLICATION`,
which RDS does not allow because it withholds SUPERUSER.

**Reachability.** DMS has to reach the source: an IP allowlist on a public
endpoint, VPC peering, or a reverse SSH tunnel. For a private-IP target,
enable the Service Networking API in `<GCP_PROJECT>`; the operator needs
`servicenetworking.services.addPeering` and `compute.networkAdmin`.

## 2. Prepare the target

The landing zone Terraform has to be applied first — until that PR is merged
and applied, the target project and network exist only as a plan.

Create both connection profiles. The destination one is what creates the
Cloud SQL instance:

```bash
# 1. The SOURCE profile — the RDS instance DMS reads from.
gcloud database-migration connection-profiles create postgresql <SOURCE_PROFILE> \
    --region=<GCP_REGION> --project=<GCP_PROJECT> \
    --host=<SOURCE_HOST> --port=<SOURCE_PORT> \
    --username=<MIGRATION_USER> --prompt-for-password

# 2. The DESTINATION profile. This CREATES the Cloud SQL replica DMS migrates
#    into, so the sizing arguments are the target instance's rather than a
#    description of one that already exists.
gcloud database-migration connection-profiles create cloudsql <TARGET_PROFILE> \
    --region=<GCP_REGION> --project=<GCP_PROJECT> \
    --source-id=<SOURCE_PROFILE> \
    --tier=<TARGET_TIER> --database-version=<TARGET_DB_VERSION> \
    --data-disk-size=<TARGET_DISK_GB> --availability-type=<AVAILABILITY_TYPE>
```

Both profiles have to exist before the migration job below: the job takes them
by name and fails immediately on a destination that was never created.

`<TARGET_TIER>` and `--edition` go together: `db-perf-optimized-N-8` and the
other `db-perf-optimized-*` machine types are Enterprise Plus, and the flag
defaults to `enterprise`, so a tier from that family without
`--edition=enterprise-plus` is rejected. Pick the pair deliberately.

`--root-password` is optional on this command and is omitted above on purpose:
it would put the destination's administrator password in the process's command
line and in your shell history. It is set **after promotion**, in step 5 — not
here. What this command creates is a Cloud SQL *replica* of the source (gcloud
says so in its own description), and user management on a replica is not
available; running it at this point fails and leaves you looking for the
reason mid-window.


Size `<TARGET_DISK_GB>` at or above the source: Cloud SQL storage grows and
does not shrink. Match the source's database flags where Cloud SQL supports
them — `--database-flags` on the destination profile, or the instance
afterwards; a `max_connections` that silently drops from the RDS parameter
group is the classic post-cutover surprise.

Connectivity: when DMS creates the destination instance, private IP is
**VPC peering only**. If your landing zone standardised on Private Service
Connect, pre-create the Cloud SQL instance and migrate into it instead of
using the new-instance flow.

## 3. Run the copy

```bash
gcloud database-migration migration-jobs create <JOB> \
    --region=<GCP_REGION> --project=<GCP_PROJECT> --type=CONTINUOUS \
    --source=<SOURCE_PROFILE> --destination=<TARGET_PROFILE>
gcloud database-migration migration-jobs start <JOB> \
    --region=<GCP_REGION> --project=<GCP_PROJECT>
gcloud database-migration migration-jobs describe <JOB> \
    --region=<GCP_REGION> --project=<GCP_PROJECT>
```

The initial snapshot runs first, then CDC keeps the target current. `describe`
is how you watch the lag.

## 4. Validation gates

Do not cut over until all four hold:

- Replication lag under 30 s for at least 5 minutes.
- Row counts match per critical table.
- A random sample of row hashes matches.
- No errors in the target Cloud SQL log for 30 minutes.

## 5. Cutover

1. Stop the writers — drain the EKS-side workload.
2. Wait for lag to reach zero.
3. Promote the target and disable replication.
4. Repoint the application at the Cloud SQL instance. On GKE that is the Cloud
   SQL Auth Proxy sidecar or the Workload Identity-aware connector, not a
   password in an env var.
5. **Re-enable point-in-time recovery and re-apply your backup settings** —
   promotion resets them.

Set the administrator password now, and not before: until the promotion above,
the instance was a DMS-managed replica and this call was not available on it.

```bash
gcloud sql users set-password <ADMIN_USER> --instance=<TARGET_INSTANCE> \
    --prompt-for-password --project=<GCP_PROJECT>
```

6. Run the application's own smoke tests against the target.

## 6. After cutover

Keep the RDS instance **read-only for 14 days** rather than deleting it; that
window is the rollback. Then decommission it, and report the move:

```
mark_data_service_migrated(<TARGET_ARGS>, target="<TARGET_INSTANCE>")
```

## Rollback

Before promotion: stop the job, restart the writers against RDS. Nothing has
moved.

After promotion: repoint the application back at the read-only RDS instance
and accept the loss of anything written to Cloud SQL since promotion. There is
no reverse replication set up by this procedure, which is why the read-only
window matters more than it looks.

## Known limitations

From the DMS PostgreSQL documentation. Surface the ones that apply **before**
starting, not after:

- Tables with no primary key migrate their snapshot and subsequent `INSERT`s
  only. `UPDATE` and `DELETE` do not replicate — add a key or handle those
  tables by hand.
- DDL is not replicated through ordinary SQL, only via
  `pglogical.replicate_ddl_command`. A new table needs
  `pglogical.replication_set_add_table`.
- Materialized views migrate as schema only. `REFRESH MATERIALIZED VIEW` after
  cutover.
- Sequence `last_value` may differ on the destination. Verify it if application
  logic depends on it.
- `UNLOGGED` and `TEMPORARY` tables are not replicated, and cannot be.
- Large Object data is not supported.
- Generated columns are not replicated by `pglogical` on PostgreSQL 12+.
- Only Cloud-SQL-supported extensions and procedural languages migrate;
  unsupported ones are skipped silently at test or start.
- `pg_cron` and its schedules do not migrate — reinstall them on the
  destination.
- Users and roles do not migrate. Recreate them.
- Custom tablespaces collapse to `pg_default`.
- Table and schema selection is not offered: DMS migrates everything except
  `information_schema` and the `pg_*` catalogs.
- Databases added after the job starts are not picked up.
- A source in recovery mode (a read replica) cannot be migrated from.
- Replication slots can be dropped silently on a managed-database failover, and
  CDC then loses rows. Monitor slot existence, not just lag, through the
  cutover window.
