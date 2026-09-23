# RDS for MariaDB → Cloud SQL for MySQL (dump and import)

**Moves by:** logical dump and import. **There is no continuous path.**
**Cutover class:** point-in-time copy — everything written to the source after
the dump is lost unless you take the downtime.
**Engine change:** yes, in effect. Cloud SQL has no MariaDB engine, and
Database Migration Service does not accept MariaDB as a source. What follows
moves a MariaDB database onto Cloud SQL for MySQL by compatibility, which
works for ordinary schemas and does not work for MariaDB-specific ones.

**Read the decision section before the procedure.** Like
`elasticache-persistent.md`, `fsx.md` and `memorydb.md`, this file opens with a
question about whether to proceed at all — and here the answer is "no" more
often than in any of them, because the target is a different database engine.

Every command below is yours to run, with your own credentials.

## The decision

Three honest options, and the right one depends on what the schema uses:

1. **Move to Cloud SQL for MySQL** (this procedure). Fits a schema that stays
   inside the MySQL-compatible subset. Costs a maintenance window sized by the
   dump and import.
2. **Run MariaDB yourself** on GKE or Compute Engine. Keeps every MariaDB
   feature and gives up the managed service.
3. **Keep it in AWS.** Legitimate, and recorded with
   `annotate_data_dependency(<TARGET_ARGS>, disposition="keep-in-aws",
   note="...")`. It costs cross-cloud connectivity, a credential path for a pod
   that used to use IRSA, and egress on every call.

Check the schema for these before choosing option 1 — each one means MariaDB,
not MySQL:

- Storage engines other than InnoDB: Aria, ColumnStore, CONNECT, SPIDER.
- `SEQUENCE` objects (MariaDB 10.3+). MySQL has no equivalent.
- Dynamic columns, `INVISIBLE` columns, application-time periods,
  system-versioned tables.
- MariaDB-only functions and UDFs, and `mysql.*` system tables that differ.
- MariaDB GTIDs, which are not MySQL GTIDs — relevant if anything downstream
  replicates from this instance.
- `CHECK` constraint and `JSON` type semantics, which differ subtly enough to
  pass an import and fail at runtime.

If any of those are load-bearing, this procedure is the wrong one. Say so and
take option 2 or 3.

## Substitutions

| Placeholder | What it is |
|---|---|
| `<TARGET_ARGS>` | The targeting arguments exactly as the tool printed them — `address="…"`, plus `directory=`/`identifier=` when it showed them. Two root modules can declare one address, and the bare address is then refused |
| `<SOURCE_HOST>` | The RDS endpoint |
| `<SOURCE_USER>` | A user that can read everything being dumped |
| `<DATABASE>` | The database to move |
| `<DUMP_FILE>` | Local dump path, e.g. `/tmp/orders.sql` |
| `<DUMP_OBJECT>` | The object name in the bucket, e.g. `orders.sql` — not the local path, which `cp` reduces to its basename |
| `<STAGING_BUCKET>` | A Cloud Storage bucket the import reads from |
| `<GCP_PROJECT>` | The project the landing zone created the target in |
| `<TARGET_INSTANCE>` | The Cloud SQL for MySQL instance id |

## 1. Prepare the source

Nothing to enable — there is no replication to set up. What you need is a
consistent dump and a window in which writes stop.

Size the window first. Dump time, transfer time and import time are all
roughly linear in data size, and the import is usually the longest of the
three. Measure on a restored snapshot rather than guessing.

Count the objects the DEFINER strip will touch, so the check after it has
something to compare against:

```sql
SELECT COUNT(*) FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA = '<DATABASE>';
SELECT COUNT(*) FROM information_schema.VIEWS   WHERE TABLE_SCHEMA   = '<DATABASE>';
```

## 2. Prepare the target

The landing zone Terraform has to be applied first. Create the Cloud SQL for
MySQL instance if the landing zone did not, and check the version pairing:
MariaDB 10.x schemas generally land on MySQL 8.0.

## 3. Run the copy

Stop the writers, then:

```bash
# Run MariaDB's own mysqldump (from mariadb-client), not Oracle MySQL's:
# --set-gtid-purged is a MySQL-client option MariaDB does not implement, and
# the MySQL 8 client probes @@GTID_MODE against a MariaDB server and fails.
mysqldump --host=<SOURCE_HOST> --user=<SOURCE_USER> --password \
    --single-transaction --no-tablespaces \
    --routines --triggers --events \
    <DATABASE> > <DUMP_FILE>

# Strip DEFINER clauses. --routines/--triggers/--events emit them, and Cloud
# SQL grants no SUPER (nor SET_USER_ID), so the import aborts on
# "ERROR 1227 (42000): Access denied" — with the writers already stopped.
#
# Anchored on the backticks mysqldump always quotes the user and host with.
# A pattern that runs to the next SPACE instead looks equivalent and is not:
# triggers and events are emitted as /*!50017 DEFINER=`root`@`localhost`*/
# with no space before the */, so it eats the comment terminator and mangles
# the trigger. GNU sed; BSD/macOS needs -i '' .
sed -i -E 's/DEFINER=`[^`]*`@`[^`]*`//g' <DUMP_FILE>

# Check the strip did what you think before uploading 2 TiB of it. Count the
# CLAUSE, not the word: mysqldump writes views as
# /*!50013 DEFINER=`x`@`y` SQL SECURITY DEFINER */, so the word survives the
# strip by design and `grep -c DEFINER` would report your view count as a
# failure. A view that keeps SQL SECURITY DEFINER without a DEFINER= clause
# defaults to the importing user and imports fine.
grep -c 'DEFINER=' <DUMP_FILE>              # expect 0
grep -c 'CREATE.*TRIGGER' <DUMP_FILE>      # expect the TRIGGERS count from step 1
grep -c 'SQL SECURITY DEFINER' <DUMP_FILE> # expect the VIEWS count from step 1

gcloud storage cp <DUMP_FILE> gs://<STAGING_BUCKET>/<DUMP_OBJECT>

gcloud sql import sql <TARGET_INSTANCE> \
    gs://<STAGING_BUCKET>/<DUMP_OBJECT> \
    --database=<DATABASE> --project=<GCP_PROJECT>
```

The import service account needs read access on the bucket; `gcloud sql
instances describe <TARGET_INSTANCE>` prints it under `serviceAccountEmailAddress`.

If the import fails on MariaDB-specific syntax, that is the schema telling you
option 1 was the wrong call. Do not hand-edit the dump into submission without
saying what you changed — a silently patched schema is a production incident
scheduled for later.

## 4. Validation gates

- The import exits clean, with no skipped statements.
- Row counts match per table between source and target.
- `SHOW CREATE TABLE` on a sample of tables matches what you expect, including
  character set and collation — MariaDB and MySQL defaults differ.
- Routines, triggers and events are present and enabled on the target.
- The application's own test suite passes against the target.

## 5. Cutover

Writes are already stopped — the dump required it. So:

1. Repoint the application at Cloud SQL (Auth Proxy sidecar or connector).
2. Restart the writers.
3. Smoke tests.

If the window is too long to accept, the answer is not a faster dump; it is
option 2 or 3.

## 6. After cutover

Keep the source read-only for 14 days as the rollback, then decommission, and
report:

```
mark_data_service_migrated(<TARGET_ARGS>, target="<TARGET_INSTANCE>")
```

## Rollback

Repoint at the source and restart the writers. Anything written to Cloud SQL
after cutover is lost — there is no reverse path — so the rollback decision is
worth making early rather than late.

## Known limitations

- Database Migration Service does not support MariaDB. There is no supported
  continuous replication into Cloud SQL from a MariaDB source, which is why
  this procedure takes a window.
- Cloud SQL has no MariaDB engine; the target is MySQL and the compatibility is
  a subset, not a guarantee.
- The two `mysqldump` clients are not interchangeable. MariaDB's own client
  is the one to use against a MariaDB server; the MySQL 8 client fails on its
  GTID probe, and its `--set-gtid-purged` flag does not exist on MariaDB's.
- MariaDB and MySQL GTIDs are different mechanisms. Anything replicating from
  this instance has to be re-pointed, not re-parented.
- The dump is a point in time. Nothing written after it exists on the target.
- `mysqldump <DATABASE>` rather than `--databases <DATABASE>`: the latter emits
  `CREATE DATABASE`/`USE`, and `gcloud sql import sql` refuses a dump that
  names its own database while `--database=` is also passed.
- DEFINER clauses on routines, triggers and events have to be stripped before
  the import — Cloud SQL has no `SUPER` to accept them. Strip them with a
  backtick-anchored pattern: triggers and events carry no space between the
  DEFINER clause and the closing `*/`, so a pattern that runs to the next
  space deletes the comment terminator and corrupts the object.
