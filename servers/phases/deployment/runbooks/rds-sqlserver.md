# RDS for SQL Server → Cloud SQL for SQL Server

**Moves by:** Database Migration Service (DMS), continuous, from backups.
**Cutover class:** continuous replication, seeded from full backups.
**Engine change:** none — SQL Server both sides.

Every command below is yours to run, with your own credentials.

The shape differs from the PostgreSQL and MySQL runbooks in one place: the
source precondition is **backups in Cloud Storage**, not a replication
parameter and a reboot.

## Substitutions

| Placeholder | What it is |
|---|---|
| `<TARGET_ARGS>` | The targeting arguments exactly as the tool printed them — `address="…"`, plus `directory=`/`identifier=` when it showed them. Two root modules can declare one address, and the bare address is then refused |
| `<BACKUP_BUCKET>` | The Cloud Storage bucket holding the source backups |
| `<BACKUP_PREFIX>` | The path within that bucket the backups are written under |
| `<GCP_PROJECT>` | The project the landing zone created the target in |
| `<GCP_REGION>` | The Cloud SQL region — match the GKE cluster's |
| `<SOURCE_PROFILE>`, `<TARGET_PROFILE>`, `<JOB>` | Names you choose |
| `<DATABASES>` | Comma-separated list of databases to migrate |
| `<TARGET_INSTANCE>` | The Cloud SQL instance id |
| `<TARGET_TIER>` | Destination machine tier |
| `<TARGET_DB_VERSION>` | Destination version, e.g. `SQLSERVER_2019_STANDARD` |
| `<TARGET_DISK_GB>` | Destination disk size, at or above the source |
| `<AVAILABILITY_TYPE>` | `REGIONAL` for HA, `ZONAL` otherwise |
| `<ADMIN_USER>` | The destination's administrator user, e.g. `sqlserver` |

## 1. Prepare the source

- The databases must be in the **full recovery model**. Simple recovery has no
  log chain, so there is nothing for the differential and log backups to
  continue from.
- Write full backups — and, if you will use them, differential backups — to a
  location DMS can read, which means a Cloud Storage bucket. On RDS this is
  the native backup-to-S3 feature plus a copy into `<BACKUP_BUCKET>`, or a
  backup written directly by an agent that has both credentials.
- Grant the DMS service account read on `<BACKUP_BUCKET>`.

## 2. Prepare the target

The landing zone Terraform has to be applied first.

```bash
# 1. The SOURCE profile — the backups, not a live connection.
gcloud database-migration connection-profiles create sqlserver <SOURCE_PROFILE> \
    --region=<GCP_REGION> --project=<GCP_PROJECT> \
    --gcs-bucket=<BACKUP_BUCKET> --gcs-prefix=<BACKUP_PREFIX> \
    --provider=RDS

# 2. The DESTINATION profile, which CREATES the Cloud SQL instance.
gcloud database-migration connection-profiles create cloudsql <TARGET_PROFILE> \
    --region=<GCP_REGION> --project=<GCP_PROJECT> \
    --source-id=<SOURCE_PROFILE> \
    --tier=<TARGET_TIER> --database-version=<TARGET_DB_VERSION> \
    --data-disk-size=<TARGET_DISK_GB> --availability-type=<AVAILABILITY_TYPE>
```

**The administrator password is deliberately not on that command.** gcloud
offers `--root-password` but no file-based equivalent, so any form of it —
including `--root-password="$(cat file)"` — puts the plaintext in the
process's command line, readable with `ps` by anyone else on the host, and in
your shell history. An adapted runbook is also saved to the ledger and
rendered in the review UI, so a literal there is durable. Set the password
**after promotion**, in step 5: what this command creates is a Cloud SQL
replica of the source, and user management on a replica is not available.

If your DMS flow genuinely needs the password at profile-creation time, accept
the argv exposure knowingly and rotate afterwards — do not pretend a `cat`
hides it.

The migration job below takes both by name.

Size the destination at or above the source. Match collation deliberately:
a mismatch imports cleanly and then compares strings differently, which is the
kind of defect that surfaces weeks later in one query.

## 3. Run the copy

```bash
gcloud database-migration migration-jobs create <JOB> \
    --region=<GCP_REGION> --project=<GCP_PROJECT> --type=CONTINUOUS \
    --source=<SOURCE_PROFILE> --destination=<TARGET_PROFILE> \
    --sqlserver-databases=<DATABASES>
gcloud database-migration migration-jobs start <JOB> \
    --region=<GCP_REGION> --project=<GCP_PROJECT>
gcloud database-migration migration-jobs describe <JOB> \
    --region=<GCP_REGION> --project=<GCP_PROJECT>
```

Add `--sqlserver-diff-backup` when you are seeding from differential backups
as well as full ones.

Keep writing log backups to the bucket while the job runs — that is what CDC
consumes here. If the chain breaks, the job cannot continue and the seed has
to be redone.

## 4. Validation gates

- The job reports every database in `<DATABASES>` as replicating.
- Row counts match per critical table.
- Collation and character set match on a sample of tables.
- SQL Agent jobs, logins and linked servers are accounted for — see the
  limitations; they do not come across.
- No errors in the target log for 30 minutes.

## 5. Cutover

1. Stop the writers.
2. Take a final log backup and let the job consume it.
3. Promote the destination.
4. Repoint the application.
5. Recreate logins and re-map orphaned database users, and set the
   administrator password now that the instance is a primary rather than a
   replica:

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

Before promotion: stop the job, restart the writers on RDS.
After promotion: repoint at the read-only source; writes to Cloud SQL since
promotion are lost.

## Known limitations

- Server-level objects do not migrate with the databases: logins, SQL Agent
  jobs, linked servers, server-level triggers and certificates. Inventory them
  before the window and recreate them on the target.
- Cloud SQL for SQL Server does not offer every edition-level feature RDS
  does. Check anything that depends on Enterprise-only behaviour.
- Cross-database queries need every referenced database in the same migration
  job.
- The backup chain is the transport: a missing log backup is a broken
  migration, not a delayed one.
