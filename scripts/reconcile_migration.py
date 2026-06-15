"""Compare row counts between MotherDuck and BigQuery and surface data gaps.

Usage:
    uv run python scripts/reconcile_migration.py
    uv run python scripts/reconcile_migration.py --database econ_agent
    uv run python scripts/reconcile_migration.py --dataset economics_raw
    uv run python scripts/reconcile_migration.py --tolerance 0.01  # allow 1% discrepancy
"""

import argparse
import logging
import os
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Reconcile MotherDuck vs BigQuery row counts")
    parser.add_argument(
        "--database",
        default=os.environ.get("MOTHERDUCK_DATABASE", "econ_agent"),
        help="MotherDuck source database (default: econ_agent)",
    )
    parser.add_argument(
        "--schema",
        default=os.environ.get("MOTHERDUCK_PROD_SCHEMA", "main"),
    )
    parser.add_argument("--tables", nargs="*", help="Specific tables to check (default: all non-empty MD tables)")
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
        md_conn = duckdb.connect(f"md:{args.database}?motherduck_token={token}")
    except Exception as e:
        log.error(f"Failed to connect to MotherDuck: {e}")
        sys.exit(1)

    from google.cloud import bigquery
    bq_client = bigquery.Client(project=project)

    # Get all non-empty tables from MotherDuck
    if args.tables:
        md_tables = set(args.tables)
    else:
        rows = md_conn.execute(
            f"""
            SELECT t.table_name
            FROM information_schema.tables t
            JOIN duckdb_tables() d ON d.table_name = t.table_name AND d.schema_name = t.table_schema
            WHERE t.table_schema = '{args.schema}' AND t.table_type = 'BASE TABLE'
            AND d.estimated_size > 0
            ORDER BY d.estimated_size DESC
            """
        ).fetchall()
        md_tables = {r[0] for r in rows}

    # Get all tables in the target BQ dataset
    try:
        bq_tables = {t.table_id for t in bq_client.list_tables(f"{project}.{args.dataset}")}
    except Exception:
        bq_tables = set()

    all_tables = md_tables | bq_tables

    results = []
    for table in sorted(all_tables):
        md_count = -1
        bq_count = -1
        in_md = table in md_tables
        in_bq = table in bq_tables

        if in_md:
            try:
                row = md_conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                md_count = row[0] if row else 0
            except Exception as e:
                log.warning(f"{table}: MotherDuck error — {e}")

        if in_bq:
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
            "in_md": in_md,
            "in_bq": in_bq,
            "match": match,
        })

    # Categorize
    matched = [r for r in results if r["match"]]
    mismatched = [r for r in results if r["in_md"] and r["in_bq"] and not r["match"]]
    md_only = [r for r in results if r["in_md"] and not r["in_bq"]]
    bq_only = [r for r in results if r["in_bq"] and not r["in_md"]]

    print("\n" + "=" * 75)
    print("MIGRATION RECONCILIATION REPORT")
    print(f"Source: MotherDuck {args.database} | Target: {project}.{args.dataset}")
    print("=" * 75)

    if matched:
        print(f"\nMATCHED ({len(matched)} tables)")
        print(f"  {'Table':<45} {'MD Rows':>10} {'BQ Rows':>10}")
        print("  " + "-" * 67)
        for r in matched:
            print(f"  {r['table']:<45} {r['md_rows']:>10,} {r['bq_rows']:>10,}")

    if mismatched:
        print(f"\nMISMATCHED ({len(mismatched)} tables)")
        print(f"  {'Table':<45} {'MD Rows':>10} {'BQ Rows':>10} {'Diff':>10}")
        print("  " + "-" * 77)
        for r in mismatched:
            diff = r['bq_rows'] - r['md_rows']
            print(f"  {r['table']:<45} {r['md_rows']:>10,} {r['bq_rows']:>10,} {diff:>+10,}")

    if md_only:
        print(f"\nGAPS - in MotherDuck only, not yet in BigQuery ({len(md_only)} tables)")
        print(f"  {'Table':<45} {'MD Rows':>10}")
        print("  " + "-" * 57)
        for r in md_only:
            print(f"  {r['table']:<45} {r['md_rows']:>10,}")

    if bq_only:
        print(f"\nBQ ONLY - in BigQuery only, not in MotherDuck ({len(bq_only)} tables)")
        print(f"  {'Table':<45} {'BQ Rows':>10}")
        print("  " + "-" * 57)
        for r in bq_only:
            print(f"  {r['table']:<45} {r['bq_rows']:>10,}")

    print("\n" + "=" * 75)
    print(f"Summary: {len(matched)} matched | {len(mismatched)} mismatched | "
          f"{len(md_only)} MD-only gaps | {len(bq_only)} BQ-only")

    if mismatched or md_only:
        sys.exit(1)


if __name__ == "__main__":
    main()
