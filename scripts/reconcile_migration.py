"""Compare row counts between MotherDuck and BigQuery after migration.

Phase 4 reconciliation (issue #79).

Usage:
    uv run python scripts/reconcile_migration.py
    uv run python scripts/reconcile_migration.py --tables sp500_companies_raw fred_series_raw
    uv run python scripts/reconcile_migration.py --tolerance 0.01  # allow 1% discrepancy
"""

import argparse
import logging
import os
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

from scripts.migrate_motherduck_to_bq import RAW_TABLES


def main():
    parser = argparse.ArgumentParser(description="Reconcile MotherDuck vs BigQuery row counts")
    parser.add_argument("--tables", nargs="*")
    parser.add_argument("--tolerance", type=float, default=0.0,
                        help="Fractional row count tolerance (default: 0 = exact match)")
    parser.add_argument("--dataset", default=os.environ.get("BIGQUERY_DATASET", "economics_raw"))
    args = parser.parse_args()

    project = os.environ.get("BIGQUERY_PROJECT")
    if not project:
        log.error("BIGQUERY_PROJECT not set")
        sys.exit(1)

    try:
        import duckdb
        token = os.environ.get("MOTHERDUCK_TOKEN")
        database = os.environ.get("MOTHERDUCK_DATABASE", "my_db")
        md_conn = duckdb.connect(f"md:{database}?motherduck_token={token}")
    except Exception as e:
        log.error(f"Failed to connect to MotherDuck: {e}")
        sys.exit(1)

    from google.cloud import bigquery
    bq_client = bigquery.Client(project=project)

    tables = args.tables or RAW_TABLES
    results = []

    for table in tables:
        md_count = -1
        bq_count = -1
        try:
            row = md_conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
            md_count = row[0] if row else 0
        except Exception as e:
            log.warning(f"{table}: MotherDuck error — {e}")

        try:
            bq_table = bq_client.get_table(f"{project}.{args.dataset}.{table}")
            bq_count = bq_table.num_rows
        except Exception as e:
            log.warning(f"{table}: BigQuery error — {e}")

        if md_count >= 0 and bq_count >= 0:
            if md_count == 0:
                match = bq_count == 0
            else:
                diff_pct = abs(md_count - bq_count) / md_count
                match = diff_pct <= args.tolerance
        else:
            match = False

        results.append({
            "table": table,
            "md_rows": md_count,
            "bq_rows": bq_count,
            "match": match,
        })

    print("\n" + "=" * 70)
    print("RECONCILIATION REPORT")
    print(f"Source: MotherDuck | Target: {project}.{args.dataset}")
    print("=" * 70)
    print(f"{'Table':<40} {'MD Rows':>10} {'BQ Rows':>10} {'Match'}")
    print("-" * 70)

    ok = sum(1 for r in results if r["match"])
    for r in results:
        flag = "✓" if r["match"] else "✗ MISMATCH"
        print(f"{r['table']:<40} {r['md_rows']:>10,} {r['bq_rows']:>10,} {flag}")

    print("-" * 70)
    print(f"\n{ok}/{len(results)} tables match")

    if ok < len(results):
        sys.exit(1)


if __name__ == "__main__":
    main()
