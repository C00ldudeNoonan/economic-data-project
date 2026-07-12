#!/bin/sh
# Automated Dagster PostgreSQL backups (issue #132).
#
# Runs pg_dump on an interval, writes a compressed timestamped dump to the
# mounted backups volume, prunes dumps older than the retention window, and
# (optionally) encrypts each dump with a GPG passphrase.
#
# Encryption: GCP persistent disks are encrypted at rest by default (AES-256),
# so a backups volume on a GCP PD is already encrypted. Set BACKUP_GPG_PASSPHRASE
# to add application-level symmetric encryption on top (requires gpg in the
# image). See docker/DAGSTER_RETENTION_AND_BACKUP.md for the restore drill.
set -eu

: "${DAGSTER_DB_HOST:=dagster_postgresql}"
: "${DAGSTER_DB_PORT:=5432}"
: "${DAGSTER_DB_NAME:=dagster}"
: "${DAGSTER_DB_USER:=dagster_user}"
: "${DAGSTER_DB_PASSWORD:?DAGSTER_DB_PASSWORD must be set}"
: "${BACKUP_DIR:=/backups}"
: "${BACKUP_INTERVAL_SECONDS:=86400}"   # daily
: "${BACKUP_RETENTION_DAYS:=14}"
: "${BACKUP_GPG_PASSPHRASE:=}"          # empty = rely on disk-level encryption

export PGPASSWORD="$DAGSTER_DB_PASSWORD"

mkdir -p "$BACKUP_DIR"

if [ -n "$BACKUP_GPG_PASSPHRASE" ] && ! command -v gpg >/dev/null 2>&1; then
  echo "backup: WARNING BACKUP_GPG_PASSPHRASE set but gpg not found; writing unencrypted (disk-level encryption still applies)" >&2
fi

echo "backup: dir=${BACKUP_DIR} interval=${BACKUP_INTERVAL_SECONDS}s retention=${BACKUP_RETENTION_DAYS}d"

while true; do
  # Wait for the database to accept connections before dumping.
  if ! pg_isready -h "$DAGSTER_DB_HOST" -p "$DAGSTER_DB_PORT" -U "$DAGSTER_DB_USER" >/dev/null 2>&1; then
    echo "backup: database not ready, retrying shortly"
    sleep 30
    continue
  fi

  stamp=$(date -u +%Y%m%dT%H%M%SZ)
  tmp="${BACKUP_DIR}/dagster_${stamp}.sql.gz.partial"
  final="${BACKUP_DIR}/dagster_${stamp}.sql.gz"

  # -Fc would be smaller/parallel-restorable, but plain SQL + gzip keeps the
  # restore drill dependency-free (psql only). Fail the cycle (not the loop)
  # if the dump errors, so a transient failure doesn't kill the service.
  if pg_dump -h "$DAGSTER_DB_HOST" -p "$DAGSTER_DB_PORT" -U "$DAGSTER_DB_USER" \
       -d "$DAGSTER_DB_NAME" --no-owner --no-privileges | gzip -c > "$tmp"; then
    if [ -n "$BACKUP_GPG_PASSPHRASE" ] && command -v gpg >/dev/null 2>&1; then
      gpg --batch --yes --passphrase "$BACKUP_GPG_PASSPHRASE" \
        --symmetric --cipher-algo AES256 -o "${final}.gpg" "$tmp"
      rm -f "$tmp"
      echo "backup: wrote ${final}.gpg"
    else
      mv "$tmp" "$final"
      echo "backup: wrote ${final}"
    fi
  else
    echo "backup: ERROR pg_dump failed for ${stamp}" >&2
    rm -f "$tmp"
  fi

  # Prune old backups (both plain and encrypted).
  find "$BACKUP_DIR" -maxdepth 1 -type f -name 'dagster_*.sql.gz*' \
    -mtime "+${BACKUP_RETENTION_DAYS}" -print -delete 2>/dev/null || true

  sleep "$BACKUP_INTERVAL_SECONDS"
done
