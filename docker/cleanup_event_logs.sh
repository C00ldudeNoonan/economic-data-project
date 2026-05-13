#!/bin/sh
set -eu

: "${DAGSTER_DB_HOST:=dagster_postgresql}"
: "${DAGSTER_DB_PORT:=5432}"
: "${DAGSTER_DB_NAME:=dagster}"
: "${DAGSTER_DB_USER:=dagster_user}"
: "${DAGSTER_DB_PASSWORD:?DAGSTER_DB_PASSWORD must be set}"
: "${DAGSTER_EVENT_RETENTION_HOURS:=24}"
: "${DAGSTER_EVENT_CLEANUP_INTERVAL_SECONDS:=3600}"
: "${DAGSTER_EVENT_CLEANUP_ERROR_REGEX:=ERROR|FAILURE}"

export PGPASSWORD="$DAGSTER_DB_PASSWORD"

while true; do
  table_exists=$(psql -At -h "$DAGSTER_DB_HOST" -p "$DAGSTER_DB_PORT" -U "$DAGSTER_DB_USER" -d "$DAGSTER_DB_NAME" \
    -c "SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'event_logs' LIMIT 1;" || true)

  if [ -z "$table_exists" ]; then
    sleep "$DAGSTER_EVENT_CLEANUP_INTERVAL_SECONDS"
    continue
  fi

  ts_type=$(psql -At -h "$DAGSTER_DB_HOST" -p "$DAGSTER_DB_PORT" -U "$DAGSTER_DB_USER" -d "$DAGSTER_DB_NAME" \
    -c "SELECT data_type FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'event_logs' AND column_name = 'timestamp' LIMIT 1;" || true)

  ts_expr="\"timestamp\""
  case "$ts_type" in
    "double precision"|"numeric"|"bigint"|"integer")
      ts_expr="to_timestamp(\"timestamp\")"
      ;;
  esac

  asset_check_exists=$(psql -At -h "$DAGSTER_DB_HOST" -p "$DAGSTER_DB_PORT" -U "$DAGSTER_DB_USER" -d "$DAGSTER_DB_NAME" \
    -c "SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'asset_check_executions' LIMIT 1;" || true)
  asset_check_delete=""
  if [ -n "$asset_check_exists" ]; then
    asset_check_delete="DELETE FROM asset_check_executions WHERE evaluation_event_storage_id IN (SELECT id FROM event_log_ids_to_delete) OR materialization_event_storage_id IN (SELECT id FROM event_log_ids_to_delete);"
  fi

  event_log_tags_exists=$(psql -At -h "$DAGSTER_DB_HOST" -p "$DAGSTER_DB_PORT" -U "$DAGSTER_DB_USER" -d "$DAGSTER_DB_NAME" \
    -c "SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'event_log_tags' LIMIT 1;" || true)
  event_log_tags_delete=""
  if [ -n "$event_log_tags_exists" ]; then
    event_log_tags_delete="DELETE FROM event_log_tags WHERE event_id IN (SELECT id FROM event_log_ids_to_delete);"
  fi

  asset_materializations_exists=$(psql -At -h "$DAGSTER_DB_HOST" -p "$DAGSTER_DB_PORT" -U "$DAGSTER_DB_USER" -d "$DAGSTER_DB_NAME" \
    -c "SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'asset_materializations' LIMIT 1;" || true)
  asset_materializations_delete=""
  if [ -n "$asset_materializations_exists" ]; then
    asset_materializations_delete="DELETE FROM asset_materializations WHERE event_id IN (SELECT id FROM event_log_ids_to_delete);"
  fi

  asset_observations_exists=$(psql -At -h "$DAGSTER_DB_HOST" -p "$DAGSTER_DB_PORT" -U "$DAGSTER_DB_USER" -d "$DAGSTER_DB_NAME" \
    -c "SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'asset_observations' LIMIT 1;" || true)
  asset_observations_delete=""
  if [ -n "$asset_observations_exists" ]; then
    asset_observations_delete="DELETE FROM asset_observations WHERE event_id IN (SELECT id FROM event_log_ids_to_delete);"
  fi

  psql -v ON_ERROR_STOP=1 -h "$DAGSTER_DB_HOST" -p "$DAGSTER_DB_PORT" -U "$DAGSTER_DB_USER" -d "$DAGSTER_DB_NAME" \
    -v retention_hours="$DAGSTER_EVENT_RETENTION_HOURS" \
    -v error_regex="$DAGSTER_EVENT_CLEANUP_ERROR_REGEX" <<SQL
BEGIN;

CREATE TEMP TABLE event_log_ids_to_delete ON COMMIT DROP AS
SELECT id
FROM event_logs
WHERE ${ts_expr} < NOW() - make_interval(hours => :retention_hours::int)
  AND (dagster_event_type IS NULL OR dagster_event_type !~* :'error_regex');

${event_log_tags_delete}

${asset_materializations_delete}

${asset_observations_delete}

${asset_check_delete}

DELETE FROM event_logs
WHERE id IN (SELECT id FROM event_log_ids_to_delete);

COMMIT;
SQL

  sleep "$DAGSTER_EVENT_CLEANUP_INTERVAL_SECONDS"
done
