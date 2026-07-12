# Dagster Retention & PostgreSQL Backups

How this deployment prunes Dagster history safely and how to back up / restore the
Dagster PostgreSQL database. Addresses issue #132 (the previous cleanup deleted
materialization/observation/asset-check state, which broke automation and audit
history).

## Retention by event class

| Class | What it is | Where it lives | Retention | Managed by |
|---|---|---|---|---|
| **Materializations / observations / asset checks** | Asset state that AutomationConditions, "latest materialization", asset checks, and lineage read | `event_logs` (structured) + `asset_materializations`, `asset_observations`, `asset_check_executions` | **Never deleted** | — |
| **Structured run/step events** | `RUN_*`, `STEP_*`, `ENGINE_EVENT`, etc. — run history | `event_logs` (`dagster_event_type IS NOT NULL`) | **Never deleted** | — |
| **Plain log-message rows** | High-volume `context.log.*` lines | `event_logs` (`dagster_event_type IS NULL`) | `DAGSTER_LOG_RETENTION_DAYS` (default **30d**) | `dagster_db_cleanup` (`cleanup_event_logs.sh`) |
| **Schedule ticks** | Schedule evaluation ticks | `job_ticks` | `purge_after_days: 90` | Dagster `retention:` (dagster.yaml) |
| **Sensor ticks** | Sensor evaluation ticks | `job_ticks` | skipped 7d / failure 30d / success 90d | Dagster `retention:` (dagster.yaml) |

Key rule: **orchestration and audit state is never deleted by a cleanup job.**
Only disposable log-message rows and schedule/sensor ticks age out, and the tick
purge uses Dagster's own supported `retention:` config rather than raw SQL
against internal tables.

### Why not delete the asset index tables?

`asset_materializations` / `asset_observations` / `asset_check_executions` are how
Dagster answers "what is the latest materialization of this asset?" Deleting them
makes materialized assets look un-materialized, which can retrigger
AutomationConditions and destroys asset-check and lineage history. The cleanup
job therefore never touches them, and only deletes `event_logs` rows that carry
no structured event (`dagster_event_type IS NULL`) — those never have a row in
the index tables, so no state can be orphaned.

## Configuration

Set in `.env` (see `.env.dagster.example`):

| Variable | Default | Effect |
|---|---|---|
| `DAGSTER_LOG_RETENTION_DAYS` | `30` | Age after which plain log rows are pruned |
| `DAGSTER_LOG_CLEANUP_BATCH_SIZE` | `5000` | Rows deleted per batch (bounds lock time) |
| `DAGSTER_EVENT_CLEANUP_INTERVAL_SECONDS` | `3600` | Cleanup loop interval |
| `DAGSTER_BACKUP_INTERVAL_SECONDS` | `86400` | Backup cadence (daily) |
| `DAGSTER_BACKUP_RETENTION_DAYS` | `14` | Age after which backups are pruned |
| `DAGSTER_BACKUP_GPG_PASSPHRASE` | _(empty)_ | If set (and `gpg` present), symmetrically encrypts each dump |

> The legacy `DAGSTER_EVENT_RETENTION_HOURS` is still honored by
> `cleanup_event_logs.sh` (converted to days) for backward compatibility, but it
> now affects **only** plain log rows — never structured events.

## Backups

The `dagster_db_backup` service runs `pg_dump` on `DAGSTER_BACKUP_INTERVAL_SECONDS`,
writing `dagster_<UTC-timestamp>.sql.gz` to the `dagster_backups` volume and
pruning dumps older than `DAGSTER_BACKUP_RETENTION_DAYS`.

**Encryption.** GCP persistent disks are encrypted at rest by default (AES-256),
so a backups volume on a GCP PD is already encrypted. Set
`DAGSTER_BACKUP_GPG_PASSPHRASE` to add application-level symmetric encryption
(produces `.sql.gz.gpg`); this requires `gpg` in the image.

**Off-host copies (recommended).** The volume lives on the same VM as the DB. For
real disaster recovery, sync `dagster_backups` to object storage, e.g.:

```bash
gcloud storage rsync -r /var/lib/docker/volumes/<project>_dagster_backups/_data \
  gs://<your-backup-bucket>/dagster-postgres/
```

## Objectives

- **RPO (Recovery Point Objective): 24 hours.** With daily backups, at most one
  day of Dagster metadata (run/materialization history) can be lost. Lower
  `DAGSTER_BACKUP_INTERVAL_SECONDS` for a tighter RPO.
- **RTO (Recovery Time Objective): ~15 minutes.** Restore is a single
  `gunzip | psql` into a fresh database plus a service restart.

Note: the warehouse data itself (BigQuery) is the system of record for analytics;
these backups protect Dagster's **orchestration metadata**, not the analytical
tables.

## Restore drill

Run this in a non-production environment periodically to prove backups are valid.

```bash
# 1. Pick a backup from the volume.
docker compose run --rm --entrypoint sh dagster_db_backup \
  -c 'ls -1t /backups/dagster_*.sql.gz* | head'

# 2. Create a scratch database to restore into (does not touch the live DB).
docker compose exec dagster_postgresql \
  psql -U dagster_user -d postgres -c 'CREATE DATABASE dagster_restore_test;'

# 3. Restore the dump into the scratch DB.
#    (If the file ends in .gpg, first: gpg --batch --passphrase "$PASS" -d file.sql.gz.gpg > file.sql.gz)
docker compose exec dagster_db_backup sh -c \
  'gunzip -c /backups/<chosen>.sql.gz | psql -h dagster_postgresql -U dagster_user -d dagster_restore_test'

# 4. Verify asset state survived — expect a non-zero count.
docker compose exec dagster_postgresql psql -U dagster_user -d dagster_restore_test \
  -c 'SELECT count(*) AS materializations FROM asset_materializations;'

# 5. Clean up.
docker compose exec dagster_postgresql \
  psql -U dagster_user -d postgres -c 'DROP DATABASE dagster_restore_test;'
```

**Production restore:** stop the Dagster services, restore the dump into the
`dagster` database (drop/recreate or restore into an empty DB), then restart:

```bash
docker compose stop dagster_webserver dagster_daemon dagster_user_code
gunzip -c /backups/<chosen>.sql.gz | \
  docker compose exec -T dagster_postgresql psql -U dagster_user -d dagster
docker compose start dagster_user_code dagster_daemon dagster_webserver
```
