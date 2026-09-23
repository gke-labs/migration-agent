# RDS for MySQL → Cloud SQL for MySQL

**Moves by:** Database Migration Service (DMS), continuous CDC.
**Cutover class:** continuous replication.
**Engine change:** none.

Every command below is yours to run, with your own credentials. The agent
prints them; it does not run them, and the server never sees the copy happen.

Supported sources: Amazon RDS MySQL 5.6, 5.7, 8.0 and 8.4, and the Multi-AZ DB
cluster the harvester grades as `rds` rather than Aurora. **MariaDB is not a
MySQL source for DMS** — if this instance is MariaDB, use `rds-mariadb.md`.

**Aurora MySQL does not arrive here.** The harvester grades `aurora` as an
escalation and the tool declines to hand out a runbook for it, because Aurora's
storage engine has no GCP counterpart and the move is a decision before it is a
procedure. The Aurora notes below are for a Multi-AZ DB cluster or an
escalation somebody has already accepted — they are not an invitation to plan
an Aurora migration from this file.

## Substitutions

| Placeholder | What it is |
|---|---|
| `<TARGET_ARGS>` | The targeting arguments exactly as the tool printed them — `address="…"`, plus `directory=`/`identifier=` when it showed them. Two root modules can declare one address, and the bare address is then refused |
| `<SOURCE_HOST>` | The RDS endpoint |
| `<SOURCE_PORT>` | Usually `3306` |
| `<MIGRATION_USER>` | The MySQL user DMS connects as |
| `<GCP_PROJECT>` | The project the landing zone created the target in |
| `<GCP_REGION>` | The Cloud SQL region — match the GKE cluster's |
| `<SOURCE_PROFILE>`, `<TARGET_PROFILE>`, `<JOB>` | Names you choose |
| `<TARGET_INSTANCE>` | The Cloud SQL instance id |
| `<TARGET_TIER>` | Destination machine tier, e.g. `db-perf-optimized-N-8` |
| `<TARGET_DB_VERSION>` | Destination version, e.g. `MYSQL_8_0` |
| `<TARGET_DISK_GB>` | Destination disk size, at or above the source |
| `<AVAILABILITY_TYPE>` | `REGIONAL` for HA, `ZONAL` otherwise |
| `<ADMIN_USER>` | The destination's administrator user (`root`) |

## 1. Prepare the source

In the parameter group:

- `server-id` — 1 or greater.
- `GTID_MODE` — `ON` or `OFF`. **`ON_PERMISSIVE` is not supported**, and it is
  a common RDS setting, so check rather than assume. It must be `ON` if the
  destination will have read replicas, or if you use a manual dump.
- Binary logging enabled, format **`ROW`**. `STATEMENT` or `MIXED` will make
  replication fail.
- Binlog retention long enough to cover the copy. On RDS:

```sql
call mysql.rds_set_configuration('binlog retention hours', 168);
```

168 hours is the RDS maximum and the recommended value. The same call works on
Aurora.

The migration user:

- `host = '%'`.
- **Password 32 characters or fewer.** A longer one breaks replication quietly
  (MySQL bug #43439) — an easy thing to lose a day to.
- On MySQL 8.0+, the account must **not** hold `BACKUP_ADMIN`.

Privileges depend on the migration type:

| Type | Privileges |
|---|---|
| Continuous + managed dump | `REPLICATION SLAVE`, `EXECUTE`, `SELECT`, `SHOW VIEW`, `REPLICATION CLIENT`, `RELOAD`, `TRIGGER`, plus `LOCK TABLES` on RDS/Aurora |
| Continuous + manual dump | `REPLICATION SLAVE`, `EXECUTE` |
| One-time + managed dump | `SELECT`, `SHOW VIEW`, `TRIGGER`, plus `LOCK TABLES` on RDS/Aurora, plus `RELOAD` when `GTID_MODE = ON` |
| One-time + manual dump | none |

**Storage engine:** every table outside the system databases must be InnoDB.
MyISAM tables risk inconsistency and Cloud SQL does not run them.

**Stop DDL during the full-dump phase.** It may resume once CDC has begun.

**Aurora:** you cannot migrate from an Aurora *read replica* — its binary logs
are not retrievable. Point the job at the writer.

## 2. Prepare the target

The landing zone Terraform has to be applied first.

```bash
# 1. The SOURCE profile — the RDS instance DMS reads from.
gcloud database-migration connection-profiles create mysql <SOURCE_PROFILE> \
    --region=<GCP_REGION> --project=<GCP_PROJECT> \
    --host=<SOURCE_HOST> --port=<SOURCE_PORT> \
    --username=<MIGRATION_USER> --prompt-for-password

# 2. The DESTINATION profile, which CREATES the Cloud SQL replica.
gcloud database-migration connection-profiles create cloudsql <TARGET_PROFILE> \
    --region=<GCP_REGION> --project=<GCP_PROJECT> \
    --source-id=<SOURCE_PROFILE> \
    --tier=<TARGET_TIER> --database-version=<TARGET_DB_VERSION> \
    --data-disk-size=<TARGET_DISK_GB> --availability-type=<AVAILABILITY_TYPE>
```

The migration job below takes both by name and fails immediately on a
destination that was never created.

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


Storage at or above the source size (it grows, it does not shrink). For a
5.7 → 8.0 or 8.0 → 8.4 migration the destination needs `local_infile = ON`.
Private IP on the new-instance flow is VPC peering only; for Private Service
Connect, pre-create the instance and migrate into it.

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

## 4. Validation gates

- Replication lag under 30 s for at least 5 minutes.
- Row counts match for the top tables by size and by write rate.
- A random sample of row hashes matches.
- No errors in the target log for 30 minutes.

## 5. Cutover

Google's own documentation is blunt about this: an RDS or Aurora source, which
does not grant SUPERUSER, needs **a brief write downtime on the source** to
finish. Budget it in the window rather than discovering it at 02:00.

1. Stop the writers.
2. Wait for lag to reach zero.
3. Promote the destination.
4. Repoint the application — Cloud SQL Auth Proxy sidecar or the connector.
5. Re-apply backup and PITR settings.

Set the administrator password now, and not before: until the promotion above,
the instance was a DMS-managed replica and this call was not available on it.

```bash
gcloud sql users set-password <ADMIN_USER> --instance=<TARGET_INSTANCE> \
    --prompt-for-password --project=<GCP_PROJECT>
```

6. Smoke tests.

## 6. After cutover

Keep the source read-only for 14 days, then decommission, then report:

```
mark_data_service_migrated(<TARGET_ARGS>, target="<TARGET_INSTANCE>")
```

## Rollback

Before promotion: stop the job and restart the writers against RDS.

After promotion: repoint at the read-only source and accept the loss of
writes since promotion. There is no reverse replication.

## Known limitations

- DMS is not compatible with MariaDB.
- The `mysql` system database does not migrate; user roles are not included.
  Recreate them.
- `mysql`, `performance_schema`, `information_schema` and `sys` are always
  excluded. Objects that reference them fail with
  `ERROR 1109 (42S02): Unknown table in <schema name>`.
- Data-dump parallelism is available only for MySQL 5.7 and 8 destinations,
  and it briefly locks the source: roughly 1 s at 100 tables, 9 s at 10k,
  49 s at 50k. Use a read replica as the dump source if that lock is
  unacceptable.
- Do not use `mysqldump` from MySQL 5.7.36 for a manual dump (bug #105761).
- Migrating to MySQL 5.6 or 8.4 from a Percona XtraBackup physical file is not
  supported.
- Only InnoDB is supported on Cloud SQL.
