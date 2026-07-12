#!/bin/sh
# Safe Dagster log pruning (issue #132).
#
# WHAT THIS PRUNES
#   Only plain log-message rows in `event_logs` — i.e. rows where
#   `dagster_event_type IS NULL`. Those are the high-volume compute-log lines
#   emitted via context.log.*, not structured Dagster events.
#
# WHAT THIS DELIBERATELY NEVER TOUCHES
#   - Structured events (dagster_event_type IS NOT NULL): ASSET_MATERIALIZATION,
#     ASSET_OBSERVATION, ASSET_CHECK_EVALUATION, STEP_* , RUN_* , etc.
#   - The asset-state index tables asset_materializations, asset_observations,
#     asset_check_executions, and event_log_tags.
#
#   Those tables are orchestration/automation state, not disposable logs:
#   AutomationConditions, "latest materialization", asset checks, and lineage
#   all read from them. The previous version of this script deleted every
#   non-error event older than 24h AND cascade-deleted those index tables,
#   which made materialized assets look un-materialized and could retrigger
#   automation. See docker/DAGSTER_RETENTION_AND_BACKUP.md.
#
# Schedule/sensor tick retention is handled by Dagster's supported `retention:`
# config in dagster.yaml — not here.
set -eu

: "${DAGSTER_DB_HOST:=dagster_postgresql}"
: "${DAGSTER_DB_PORT:=5432}"
: "${DAGSTER_DB_NAME:=dagster}"
: "${DAGSTER_DB_USER:=dagster_user}"
: "${DAGSTER_DB_PASSWORD:?DAGSTER_DB_PASSWORD must be set}"
# Retention for plain log-message rows only. Default 30 days. The legacy
# DAGSTER_EVENT_RETENTION_HOURS is still honored (converted to days) so existing
# deployments keep working, but it now only affects NULL-type log rows.
: "${DAGSTER_LOG_RETENTION_DAYS:=30}"
: "${DAGSTER_EVENT_CLEANUP_INTERVAL_SECONDS:=3600}"
# Delete in bounded batches so a large first pass can't hold a long lock.
: "${DAGSTER_LOG_CLEANUP_BATCH_SIZE:=5000}"

retention_days="$DAGSTER_LOG_RETENTION_DAYS"
if [ -n "${DAGSTER_EVENT_RETENTION_HOURS:-}" ]; then
  # Legacy override (hours -> days, rounded up, min 1).
  retention_days=$(( (DAGSTER_EVENT_RETENTION_HOURS + 23) / 24 ))
  [ "$retention_days" -lt 1 ] && retention_days=1
  echo "cleanup: DAGSTER_EVENT_RETENTION_HOURS=${DAGSTER_EVENT_RETENTION_HOURS} -> ${retention_days}d (log rows only)"
fi

export PGPASSWORD="$DAGSTER_DB_PASSWORD"

psql_do() {
  psql -At -h "$DAGSTER_DB_HOST" -p "$DAGSTER_DB_PORT" \
    -U "$DAGSTER_DB_USER" -d "$DAGSTER_DB_NAME" "$@"
}

echo "cleanup: pruning event_logs rows with dagster_event_type IS NULL older than ${retention_days}d, every ${DAGSTER_EVENT_CLEANUP_INTERVAL_SECONDS}s"

while true; do
  table_exists=$(psql_do -c \
    "SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'event_logs' LIMIT 1;" || true)

  if [ -z "$table_exists" ]; then
    sleep "$DAGSTER_EVENT_CLEANUP_INTERVAL_SECONDS"
    continue
  fi

  # The timestamp column has been epoch-numeric in some Dagster versions and a
  # true timestamp in others; adapt the comparison expression accordingly.
  ts_type=$(psql_do -c \
    "SELECT data_type FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'event_logs' AND column_name = 'timestamp' LIMIT 1;" || true)

  ts_expr="\"timestamp\""
  case "$ts_type" in
    "double precision"|"numeric"|"bigint"|"integer")
      ts_expr="to_timestamp(\"timestamp\")"
      ;;
  esac

  # Batched delete of ONLY plain log rows (dagster_event_type IS NULL).
  # The CTE returns the number of rows deleted; loop until a batch deletes
  # fewer than BATCH_SIZE (nothing left within the retention window).
  while true; do
    n=$(psql_do -v ON_ERROR_STOP=1 \
      -v retention_days="$retention_days" \
      -v batch_size="$DAGSTER_LOG_CLEANUP_BATCH_SIZE" <<SQL
WITH doomed AS (
  SELECT id
  FROM event_logs
  WHERE dagster_event_type IS NULL
    AND ${ts_expr} < NOW() - make_interval(days => :retention_days::int)
  ORDER BY id
  LIMIT :batch_size::int
),
del AS (
  DELETE FROM event_logs e USING doomed d
  WHERE e.id = d.id
  RETURNING 1
)
SELECT count(*) FROM del;
SQL
)
    n=${n:-0}
    echo "cleanup: deleted ${n} log rows"
    [ "$n" -lt "$DAGSTER_LOG_CLEANUP_BATCH_SIZE" ] && break
  done

  sleep "$DAGSTER_EVENT_CLEANUP_INTERVAL_SECONDS"
done
