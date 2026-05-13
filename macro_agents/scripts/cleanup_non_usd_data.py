"""One-time cleanup script to remove non-USD price data from MotherDuck.

MarketStack API was returning data from international exchanges in local
currencies (MXN, EUR, GBP, etc.) alongside USD data. This script removes
those rows, keeping only USD and NULL currency rows.

Usage:
    cd macro_agents
    uv run python scripts/cleanup_non_usd_data.py

Requires MOTHERDUCK_TOKEN environment variable to be set.
"""

import os
import sys

import duckdb

TABLES_WITH_CURRENCY = [
    "sp500_companies_prices_raw",
    "us_sector_etfs_raw",
    "currency_etfs_raw",
    "fixed_income_etfs_raw",
    "major_indices_raw",
    "global_markets_raw",
]

DATABASE = os.environ.get("MOTHERDUCK_DATABASE", "econ_agent")
SCHEMA = os.environ.get("MOTHERDUCK_PROD_SCHEMA", "main")


def main() -> None:
    token = os.environ.get("MOTHERDUCK_TOKEN")
    if not token:
        print("ERROR: MOTHERDUCK_TOKEN environment variable is not set")
        sys.exit(1)

    conn = duckdb.connect(f"md:{DATABASE}?motherduck_token={token}")

    for table in TABLES_WITH_CURRENCY:
        full_table = f"{DATABASE}.{SCHEMA}.{table}"

        # Check current non-USD row count
        count_result = conn.execute(
            f"""
            SELECT COUNT(*) as cnt
            FROM {full_table}
            WHERE price_currency IS NOT NULL
              AND LOWER(price_currency) != 'usd'
            """
        ).fetchone()
        non_usd_count = count_result[0] if count_result else 0

        if non_usd_count == 0:
            print(f"  {table}: no non-USD rows, skipping")
            continue

        print(f"  {table}: deleting {non_usd_count} non-USD rows...")

        conn.execute(
            f"""
            DELETE FROM {full_table}
            WHERE price_currency IS NOT NULL
              AND LOWER(price_currency) != 'usd'
            """
        )
        print(f"  {table}: done")

    conn.close()
    print("\nCleanup complete.")


if __name__ == "__main__":
    print(f"Cleaning non-USD data from {DATABASE}.{SCHEMA}\n")

    # Dry run first - show what would be deleted
    print("Run this script to delete non-USD rows from MotherDuck.")
    print("Tables to clean:", ", ".join(TABLES_WITH_CURRENCY))
    print()

    response = input("Proceed with deletion? (yes/no): ").strip().lower()
    if response != "yes":
        print("Aborted.")
        sys.exit(0)

    main()
