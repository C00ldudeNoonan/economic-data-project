"""Migrate data tables from MotherDuck to BigQuery.

Usage:
    # Dry run — print what would be migrated without doing anything
    uv run python scripts/migrate_motherduck_to_bq.py --dry-run

    # Migrate all non-empty tables from econ_agent
    uv run python scripts/migrate_motherduck_to_bq.py

    # Migrate a specific database
    uv run python scripts/migrate_motherduck_to_bq.py --database prod_econ

    # Migrate specific tables only
    uv run python scripts/migrate_motherduck_to_bq.py --tables sp500_companies_prices_raw fred_raw

    # Include empty tables (skipped by default)
    uv run python scripts/migrate_motherduck_to_bq.py --include-empty

Prerequisites:
    - MOTHERDUCK_TOKEN set in environment
    - MOTHERDUCK_DATABASE set (default: econ_agent)
    - BIGQUERY_PROJECT set
    - GOOGLE_APPLICATION_CREDENTIALS pointing to service account JSON, or gcloud ADC active
    - uv environment with duckdb and google-cloud-bigquery installed

Strategy:
    Reads each table from MotherDuck into memory via Arrow and loads directly into
    BigQuery. At ~11 MB total this is faster and cheaper than routing through GCS.
"""

import argparse
import logging
import os
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


def get_motherduck_connection(database: str):
    try:
        import duckdb
    except ImportError:
        log.error("duckdb not installed. Run: uv add duckdb --dev")
        sys.exit(1)

    token = os.environ.get("MOTHERDUCK_TOKEN")
    if not token:
        log.error("MOTHERDUCK_TOKEN environment variable not set")
        sys.exit(1)

    conn = duckdb.connect(f"md:{database}?motherduck_token={token}")
    log.info(f"Connected to MotherDuck database: {database}")
    return conn


def get_bq_client():
    from google.cloud import bigquery
    project = os.environ.get("BIGQUERY_PROJECT")
    return bigquery.Client(project=project)


def list_tables(md_conn, database: str, schema: str = "main", include_empty: bool = False) -> list[str]:
    """List tables in the given database/schema, optionally filtering out empty ones."""
    rows = md_conn.execute(
        f"""
        SELECT t.table_name
        FROM information_schema.tables t
        JOIN duckdb_tables() d
          ON d.table_name = t.table_name
          AND d.schema_name = t.table_schema
          AND d.database_name = t.table_catalog
        WHERE t.table_catalog = '{database}'
          AND t.table_schema = '{schema}'
          AND t.table_type = 'BASE TABLE'
          {'AND d.estimated_size > 0' if not include_empty else ''}
        ORDER BY d.estimated_size DESC
        """
    ).fetchall()
    return [r[0] for r in rows]


def get_row_count(md_conn, table: str) -> int:
    row = md_conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
    return row[0] if row else 0


def migrate_table(
    md_conn,
    bq_client,
    table: str,
    project: str,
    dataset: str,
    dry_run: bool = False,
) -> dict:
    """Load table directly from MotherDuck into BigQuery via in-memory Arrow."""
    from google.cloud import bigquery

    bq_table_ref = f"{project}.{dataset}.{table}"
    md_count = get_row_count(md_conn, table)
    log.info(f"  MotherDuck rows: {md_count:,}")

    if dry_run:
        return {"table": table, "md_rows": md_count, "bq_rows": 0, "status": "dry_run"}

    df = md_conn.execute(f"SELECT * FROM {table}").df()
    log.info(f"  Loaded {len(df):,} rows into memory")

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )
    bq_client.load_table_from_dataframe(df, bq_table_ref, job_config=job_config).result()

    bq_table = bq_client.get_table(bq_table_ref)
    bq_count = bq_table.num_rows
    log.info(f"  BigQuery rows: {bq_count:,}")

    status = "ok" if md_count == bq_count else "row_count_mismatch"
    return {"table": table, "md_rows": md_count, "bq_rows": bq_count, "status": status}


def main():
    parser = argparse.ArgumentParser(description="Migrate MotherDuck tables to BigQuery")
    parser.add_argument(
        "--database",
        default=os.environ.get("MOTHERDUCK_DATABASE", "econ_agent"),
        help="MotherDuck source database (default: econ_agent)",
    )
    parser.add_argument(
        "--schema",
        default=os.environ.get("MOTHERDUCK_PROD_SCHEMA", "main"),
        help="MotherDuck source schema (default: main)",
    )
    parser.add_argument("--tables", nargs="*", help="Specific tables to migrate (default: all non-empty)")
    parser.add_argument("--include-empty", action="store_true", help="Also migrate empty tables")
    parser.add_argument("--dry-run", action="store_true", help="Print plan without migrating")
    parser.add_argument(
        "--dataset",
        default=os.environ.get("BIGQUERY_DATASET", "economics_raw"),
        help="Target BigQuery dataset (default: economics_raw)",
    )
    args = parser.parse_args()

    project = os.environ.get("BIGQUERY_PROJECT")
    if not project:
        log.error("BIGQUERY_PROJECT environment variable not set")
        sys.exit(1)

    md_conn = get_motherduck_connection(args.database)
    bq_client = get_bq_client()

    available = list_tables(md_conn, database=args.database, schema=args.schema, include_empty=args.include_empty)

    if args.tables:
        tables_to_migrate = [t for t in args.tables if t in available]
        missing = [t for t in args.tables if t not in available]
        if missing:
            log.warning(f"Tables not found in MotherDuck (skipping): {missing}")
    else:
        tables_to_migrate = available

    log.info(f"{'DRY RUN: ' if args.dry_run else ''}Migrating {len(tables_to_migrate)} tables")
    log.info(f"  Source: MotherDuck {args.database}.{args.schema}")
    log.info(f"  Target: {project}.{args.dataset}")

    results = []
    for table in tables_to_migrate:
        log.info(f"\nMigrating: {table}")
        try:
            result = migrate_table(
                md_conn, bq_client, table, project, args.dataset, args.dry_run
            )
        except Exception as e:
            log.error(f"  FAILED: {e}")
            result = {"table": table, "md_rows": -1, "bq_rows": -1, "status": f"error: {e}"}
        results.append(result)

    print("\n" + "=" * 70)
    print("MIGRATION REPORT")
    print(f"Source: MotherDuck {args.database} -> Target: {project}.{args.dataset}")
    print("=" * 70)
    print(f"{'Table':<45} {'MD Rows':>10} {'BQ Rows':>10} {'Status'}")
    print("-" * 75)
    ok_count = 0
    for r in results:
        if r["status"] == "ok":
            ok_count += 1
        print(f"{r['table']:<45} {r['md_rows']:>10,} {r['bq_rows']:>10,} {r['status']}")
    print("-" * 75)
    print(f"\nCompleted: {ok_count}/{len(results)} tables migrated successfully")

    if any(r["status"] not in ("ok", "dry_run") for r in results):
        sys.exit(1)


if __name__ == "__main__":
    main()
