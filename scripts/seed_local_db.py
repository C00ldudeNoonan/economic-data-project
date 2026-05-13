# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "duckdb>=1.4.0,<1.5.0",
#     "python-dotenv>=1.0.0",
# ]
# ///
"""Seed local DuckDB from production MotherDuck tables.

Connects to production MotherDuck, samples data from each table,
and writes it into a local DuckDB file for development.

Usage:
    uv run scripts/seed_local_db.py
    uv run scripts/seed_local_db.py --tables fred_raw,housing_inventory_raw
    uv run scripts/seed_local_db.py --force
    uv run scripts/seed_local_db.py --since-commit main
"""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
import time
from pathlib import Path

import duckdb
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LOCAL_DB_PATH = PROJECT_ROOT / "local.duckdb"

# Databases and schemas to seed from production
PROD_SOURCES: list[dict[str, str]] = [
    {"database": "econ_agent", "schema": "main"},
    {"database": "prod_econ", "schema": "main"},
]

# Tables to always skip (telemetry, staging, internal)
SKIP_PREFIXES = ("stg_", "user_journeys", "stg_sessions", "stg_telemetry")

DEFAULT_SAMPLE_SIZE = 1000


def get_motherduck_token() -> str:
    """Load MotherDuck token from environment or .env file."""
    load_dotenv(PROJECT_ROOT / ".env")
    token = os.getenv("MOTHERDUCK_TOKEN", "")
    if not token:
        log.error("MOTHERDUCK_TOKEN not set. Add it to .env or export it.")
        sys.exit(1)
    return token


def connect_motherduck(token: str) -> duckdb.DuckDBPyConnection:
    """Connect to MotherDuck cloud."""
    log.info("Connecting to MotherDuck...")
    conn = duckdb.connect(f"md:?motherduck_token={token}")
    log.info("Connected to MotherDuck")
    return conn


def connect_local(force: bool) -> duckdb.DuckDBPyConnection:
    """Connect to local DuckDB file."""
    if force and LOCAL_DB_PATH.exists():
        LOCAL_DB_PATH.unlink()
        log.info("Removed existing local.duckdb (--force)")
    conn = duckdb.connect(str(LOCAL_DB_PATH))
    log.info("Connected to local DuckDB at %s", LOCAL_DB_PATH)
    return conn


def list_tables(
    md_conn: duckdb.DuckDBPyConnection,
    database: str,
    schema: str,
) -> list[str]:
    """List all base tables (not views) in a database.schema."""
    rows = md_conn.execute(
        """
        SELECT table_name
        FROM duckdb_tables()
        WHERE database_name = ? AND schema_name = ?
        ORDER BY table_name
        """,
        [database, schema],
    ).fetchall()
    return [r[0] for r in rows]


def should_skip(table_name: str) -> bool:
    """Check if a table should be skipped."""
    return any(table_name.startswith(prefix) for prefix in SKIP_PREFIXES)


def get_changed_tables_since(commit: str) -> set[str]:
    """Detect which tables may need re-seeding based on changed asset files.

    Looks at changed Python/SQL files since the given commit and extracts
    table names from file names and common patterns.
    """
    try:
        result = subprocess.run(
            ["git", "diff", "--name-only", commit, "HEAD"],
            capture_output=True,
            text=True,
            cwd=PROJECT_ROOT,
            check=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        log.warning("Could not run git diff against %s, seeding all tables", commit)
        return set()

    changed_files = result.stdout.strip().splitlines()
    tables: set[str] = set()

    for filepath in changed_files:
        path = Path(filepath)
        # Asset Python files
        if path.suffix == ".py" and "defs/" in filepath:
            stem = path.stem
            # Common pattern: asset file name matches table name
            if stem not in ("__init__", "assets", "resources", "definitions"):
                tables.add(stem)
        # dbt SQL models
        if path.suffix == ".sql" and "dbt_project/" in filepath:
            tables.add(path.stem)

    if tables:
        log.info(
            "Detected %d potentially changed tables from git diff: %s",
            len(tables),
            ", ".join(sorted(tables)),
        )
    else:
        log.info("No table-related changes detected since %s", commit)

    return tables


def seed_table(
    md_conn: duckdb.DuckDBPyConnection,
    local_conn: duckdb.DuckDBPyConnection,
    database: str,
    schema: str,
    table: str,
    sample_size: int,
    force: bool,
) -> int:
    """Copy sampled data from a MotherDuck table to local DuckDB.

    Returns the number of rows copied.
    """
    qualified = f'"{database}"."{schema}"."{table}"'
    local_table = f'"{table}"'

    # Check if table already exists locally
    existing = local_conn.execute(
        "SELECT count(*) FROM duckdb_tables() WHERE table_name = ?",
        [table],
    ).fetchone()

    if existing and existing[0] > 0 and not force:
        log.debug("Skipping %s (already exists, use --force to overwrite)", table)
        return -1

    if existing and existing[0] > 0 and force:
        local_conn.execute(f"DROP TABLE IF EXISTS {local_table}")

    # Get row count to decide sampling strategy
    try:
        count_result = md_conn.execute(
            f"SELECT count(*) FROM {qualified}"
        ).fetchone()
    except duckdb.CatalogException:
        log.warning("Table %s not accessible, skipping", qualified)
        return 0
    except Exception as e:
        log.warning("Error reading %s: %s, skipping", qualified, e)
        return 0

    total_rows = count_result[0] if count_result else 0

    if total_rows == 0:
        # Create empty table with schema
        try:
            md_conn.execute(
                f"CREATE TABLE IF NOT EXISTS __seed_temp AS SELECT * FROM {qualified} LIMIT 0"
            )
            schema_sql = md_conn.execute(
                "SELECT sql FROM duckdb_tables() WHERE table_name = '__seed_temp'"
            ).fetchone()
            md_conn.execute("DROP TABLE IF EXISTS __seed_temp")
            if schema_sql:
                local_conn.execute(
                    schema_sql[0].replace("__seed_temp", local_table)
                )
        except Exception:
            pass
        return 0

    # Sample the data using Arrow (no pandas dependency)
    if total_rows <= sample_size:
        query = f"SELECT * FROM {qualified}"
    else:
        query = f"SELECT * FROM {qualified} USING SAMPLE {sample_size} ROWS"

    try:
        arrow_table = md_conn.execute(query).fetch_arrow_table()
        local_conn.execute(
            f"CREATE OR REPLACE TABLE {local_table} AS SELECT * FROM arrow_table"
        )
        row_count = arrow_table.num_rows
        return row_count
    except Exception as e:
        log.warning("Failed to seed %s: %s", table, e)
        return 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Seed local DuckDB from production MotherDuck"
    )
    parser.add_argument(
        "--tables",
        type=str,
        default="",
        help="Comma-separated list of specific tables to seed",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Drop and recreate existing tables",
    )
    parser.add_argument(
        "--since-commit",
        type=str,
        default="",
        help="Only seed tables affected by changes since this commit/ref (e.g. main)",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=DEFAULT_SAMPLE_SIZE,
        help=f"Max rows per table (default: {DEFAULT_SAMPLE_SIZE})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List tables that would be seeded without actually seeding",
    )
    args = parser.parse_args()

    token = get_motherduck_token()
    md_conn = connect_motherduck(token)
    local_conn = connect_local(args.force)

    # Build filter sets
    explicit_tables = (
        {t.strip() for t in args.tables.split(",") if t.strip()}
        if args.tables
        else set()
    )
    changed_tables = (
        get_changed_tables_since(args.since_commit)
        if args.since_commit
        else set()
    )

    total_seeded = 0
    total_rows = 0
    start = time.time()

    for source in PROD_SOURCES:
        db, schema = source["database"], source["schema"]
        tables = list_tables(md_conn, db, schema)
        log.info("Found %d tables in %s.%s", len(tables), db, schema)

        for table in tables:
            if should_skip(table):
                continue

            # Apply filters
            if explicit_tables and table not in explicit_tables:
                continue
            if args.since_commit and changed_tables and table not in changed_tables:
                continue

            if args.dry_run:
                log.info("[DRY RUN] Would seed: %s.%s.%s", db, schema, table)
                total_seeded += 1
                continue

            rows = seed_table(
                md_conn, local_conn, db, schema, table,
                args.sample_size, args.force,
            )
            if rows == -1:
                continue  # skipped (already exists)
            if rows > 0:
                log.info("Seeded %-50s %6d rows (from %s)", table, rows, db)
            elif rows == 0:
                log.info("Seeded %-50s  empty (from %s)", table, db)
            total_seeded += 1
            total_rows += max(rows, 0)

    elapsed = time.time() - start

    md_conn.close()
    local_conn.close()

    log.info("=" * 60)
    log.info(
        "Done: %d tables seeded, %d total rows in %.1fs",
        total_seeded,
        total_rows,
        elapsed,
    )
    log.info("Local DB: %s", LOCAL_DB_PATH)
    if LOCAL_DB_PATH.exists():
        size_mb = LOCAL_DB_PATH.stat().st_size / (1024 * 1024)
        log.info("DB size: %.1f MB", size_mb)


if __name__ == "__main__":
    main()
