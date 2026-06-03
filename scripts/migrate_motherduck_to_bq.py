"""Migrate raw data tables from MotherDuck to BigQuery.

Phase 4 of the BigQuery migration (issue #79).

Usage:
    # Dry run — print what would be migrated without doing anything
    uv run python scripts/migrate_motherduck_to_bq.py --dry-run

    # Migrate all raw tables
    uv run python scripts/migrate_motherduck_to_bq.py

    # Migrate specific tables only
    uv run python scripts/migrate_motherduck_to_bq.py --tables sp500_companies_raw fred_series_raw

    # Skip the GCS intermediate step and load directly (requires enough local memory)
    uv run python scripts/migrate_motherduck_to_bq.py --direct

Prerequisites:
    - MOTHERDUCK_TOKEN set in environment (for duckdb motherduck connection)
    - BIGQUERY_PROJECT, BIGQUERY_DATASET set (or defaults used)
    - GCS_BUCKET_NAME set (used as staging area for large tables)
    - GOOGLE_APPLICATION_CREDENTIALS pointing to service account JSON
    - uv environment with both duckdb and google-cloud-bigquery installed

Strategy:
    1. Connect to MotherDuck
    2. For each raw table: export to GCS as Parquet via DuckDB httpfs
    3. Load each GCS Parquet into BigQuery via load job
    4. Print row count reconciliation

For tables too large to fit in memory, use GCS as an intermediate staging area.
"""

import argparse
import logging
import os
import sys
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# Raw tables written by Dagster assets (in economics_raw dataset)
RAW_TABLES = [
    "sp500_companies_raw",
    "nasdaq_companies_raw",
    "stock_prices_raw",
    "stock_splits_raw",
    "stock_dividends_raw",
    "commodity_prices_raw",
    "fred_series_raw",
    "fred_observations_raw",
    "housing_zillow_raw",
    "housing_realtor_raw",
    "fomc_transcripts_raw",
    "fomc_meeting_dates_raw",
    "reddit_posts_raw",
    "reddit_comments_raw",
    "sec_company_cik",
    "sec_company_cik_history",
    "sec_filings",
    "sec_filing_documents",
    "sec_filing_content",
    "sec_filing_markdown",
    "sec_filing_llm_metadata",
    "sec_filing_search_terms",
    "sec_filing_chunks",
    "telemetry_events_raw",
    "economic_calendar_raw",
    "ai_model_benchmarks_raw",
]


def get_motherduck_connection():
    """Connect to MotherDuck using MOTHERDUCK_TOKEN env var."""
    try:
        import duckdb
    except ImportError:
        log.error("duckdb not installed. Run: uv add duckdb --dev")
        sys.exit(1)

    token = os.environ.get("MOTHERDUCK_TOKEN")
    database = os.environ.get("MOTHERDUCK_DATABASE", "my_db")
    if not token:
        log.error("MOTHERDUCK_TOKEN environment variable not set")
        sys.exit(1)

    conn = duckdb.connect(f"md:{database}?motherduck_token={token}")
    log.info(f"Connected to MotherDuck database: {database}")
    return conn


def get_bq_client():
    """Get a BigQuery client using ADC or service account credentials."""
    from google.cloud import bigquery

    project = os.environ.get("BIGQUERY_PROJECT")
    return bigquery.Client(project=project)


def list_existing_tables(md_conn, schema: str = "main") -> list[str]:
    """List all tables in the given MotherDuck schema."""
    rows = md_conn.execute(
        f"SELECT table_name FROM information_schema.tables "
        f"WHERE table_schema = '{schema}' AND table_type = 'BASE TABLE'"
    ).fetchall()
    return [r[0] for r in rows]


def get_row_count(md_conn, table: str) -> int:
    """Get approximate row count from MotherDuck."""
    row = md_conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
    return row[0] if row else 0


def migrate_table_via_gcs(
    md_conn,
    bq_client,
    table: str,
    project: str,
    dataset: str,
    gcs_bucket: str,
    dry_run: bool = False,
) -> dict:
    """Export table to GCS Parquet then load into BigQuery."""
    from google.cloud import bigquery, storage

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    gcs_path = f"gs://{gcs_bucket}/migration/{timestamp}/{table}/*.parquet"
    bq_table_ref = f"{project}.{dataset}.{table}"

    md_count = get_row_count(md_conn, table)
    log.info(f"  MotherDuck rows: {md_count:,}")

    if dry_run:
        return {"table": table, "md_rows": md_count, "bq_rows": 0, "status": "dry_run"}

    # Export to GCS via DuckDB httpfs
    log.info(f"  Exporting to {gcs_path}")
    md_conn.execute("INSTALL httpfs; LOAD httpfs;")
    md_conn.execute(
        f"COPY (SELECT * FROM {table}) TO '{gcs_path}' (FORMAT PARQUET, OVERWRITE TRUE)"
    )
    log.info(f"  Export complete")

    # Load from GCS into BigQuery
    log.info(f"  Loading into {bq_table_ref}")
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )
    load_job = bq_client.load_table_from_uri(
        gcs_path,
        bq_table_ref,
        job_config=job_config,
    )
    load_job.result()
    log.info(f"  Load complete")

    bq_table = bq_client.get_table(bq_table_ref)
    bq_count = bq_table.num_rows
    log.info(f"  BigQuery rows: {bq_count:,}")

    # Clean up GCS staging files
    storage_client = storage.Client()
    bucket = storage_client.bucket(gcs_bucket)
    prefix = f"migration/{timestamp}/{table}/"
    blobs = list(bucket.list_blobs(prefix=prefix))
    for blob in blobs:
        blob.delete()
    log.info(f"  Cleaned up {len(blobs)} staging files from GCS")

    return {
        "table": table,
        "md_rows": md_count,
        "bq_rows": bq_count,
        "status": "ok" if md_count == bq_count else "row_count_mismatch",
    }


def migrate_table_direct(
    md_conn,
    bq_client,
    table: str,
    project: str,
    dataset: str,
    dry_run: bool = False,
) -> dict:
    """Load table directly from MotherDuck into BigQuery via in-memory Pandas."""
    import polars as pl
    from google.cloud import bigquery

    bq_table_ref = f"{project}.{dataset}.{table}"
    md_count = get_row_count(md_conn, table)
    log.info(f"  MotherDuck rows: {md_count:,}")

    if dry_run:
        return {"table": table, "md_rows": md_count, "bq_rows": 0, "status": "dry_run"}

    df = md_conn.execute(f"SELECT * FROM {table}").pl()
    log.info(f"  Loaded {len(df):,} rows into memory")

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )
    bq_client.load_table_from_dataframe(
        df.to_pandas(), bq_table_ref, job_config=job_config
    ).result()

    bq_table = bq_client.get_table(bq_table_ref)
    bq_count = bq_table.num_rows
    log.info(f"  BigQuery rows: {bq_count:,}")

    return {
        "table": table,
        "md_rows": md_count,
        "bq_rows": bq_count,
        "status": "ok" if md_count == bq_count else "row_count_mismatch",
    }


def main():
    parser = argparse.ArgumentParser(description="Migrate MotherDuck tables to BigQuery")
    parser.add_argument("--tables", nargs="*", help="Specific tables to migrate (default: all)")
    parser.add_argument("--dry-run", action="store_true", help="Print plan without migrating")
    parser.add_argument("--direct", action="store_true", help="Load directly without GCS staging")
    parser.add_argument("--dataset", default=os.environ.get("BIGQUERY_DATASET", "economics_raw"))
    args = parser.parse_args()

    project = os.environ.get("BIGQUERY_PROJECT")
    gcs_bucket = os.environ.get("GCS_BUCKET_NAME")

    if not project:
        log.error("BIGQUERY_PROJECT environment variable not set")
        sys.exit(1)
    if not args.direct and not gcs_bucket:
        log.error("GCS_BUCKET_NAME not set. Use --direct to skip GCS staging.")
        sys.exit(1)

    md_conn = get_motherduck_connection()
    bq_client = get_bq_client()

    available_tables = list_existing_tables(md_conn)
    tables_to_migrate = args.tables if args.tables else RAW_TABLES

    # Filter to tables that actually exist in MotherDuck
    tables_to_migrate = [t for t in tables_to_migrate if t in available_tables]
    missing = [t for t in (args.tables or RAW_TABLES) if t not in available_tables]
    if missing:
        log.warning(f"Tables not found in MotherDuck (will skip): {missing}")

    log.info(f"{'DRY RUN: ' if args.dry_run else ''}Migrating {len(tables_to_migrate)} tables")
    log.info(f"  Source: MotherDuck → Target: {project}.{args.dataset}")

    results = []
    for table in tables_to_migrate:
        log.info(f"\nMigrating: {table}")
        try:
            if args.direct:
                result = migrate_table_direct(
                    md_conn, bq_client, table, project, args.dataset, args.dry_run
                )
            else:
                result = migrate_table_via_gcs(
                    md_conn, bq_client, table, project, args.dataset, gcs_bucket, args.dry_run
                )
        except Exception as e:
            log.error(f"  FAILED: {e}")
            result = {"table": table, "md_rows": -1, "bq_rows": -1, "status": f"error: {e}"}
        results.append(result)

    # Print reconciliation summary
    print("\n" + "=" * 60)
    print("MIGRATION RECONCILIATION REPORT")
    print("=" * 60)
    print(f"{'Table':<40} {'MD Rows':>10} {'BQ Rows':>10} {'Status'}")
    print("-" * 70)
    ok_count = 0
    for r in results:
        status = r["status"]
        if status == "ok":
            ok_count += 1
        print(f"{r['table']:<40} {r['md_rows']:>10,} {r['bq_rows']:>10,} {status}")
    print("-" * 70)
    print(f"\nCompleted: {ok_count}/{len(results)} tables migrated successfully")

    if any(r["status"] not in ("ok", "dry_run") for r in results):
        sys.exit(1)


if __name__ == "__main__":
    main()
