# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "duckdb>=1.4.0,<1.5.0",
#     "requests>=2.31.0",
# ]
# ///
"""Reconcile Dagster asset materializations with actual MotherDuck data.

For each multi-partitioned MarketStack asset, this script:
1. Queries MotherDuck for partition keys that have data
2. Queries Dagster for partition keys that are already materialized
3. Reports the gap partitions as materialized via reportRunlessAssetEvents
"""

import json
import os
import sys
import time

import duckdb
import requests

DAGSTER_URL = "http://localhost:3000/graphql"
BATCH_SIZE = 500  # partitions per GraphQL mutation call

# MotherDuck connection
MOTHERDUCK_TOKEN = os.environ.get("MOTHERDUCK_TOKEN")
MOTHERDUCK_DATABASE = os.environ.get("MOTHERDUCK_DATABASE", "econ_agent")


def get_motherduck_conn():
    return duckdb.connect(f"md:{MOTHERDUCK_DATABASE}?motherduck_token={MOTHERDUCK_TOKEN}")


# Asset configs: asset_name -> (table, partition_key_sql)
# ETF-type: partition key = "YYYY-MM-DD|TICKER" (date|ticker dimensions)
# Commodity-type: partition key = "commodity|YYYY-MM-DD" (commodity|date dimensions)
ASSET_CONFIGS = {
    "us_sector_etfs_raw": {
        "table": "us_sector_etfs_raw",
        "sql": """
            SELECT DISTINCT
                strftime(date_trunc('month', date::DATE), '%Y-%m-%d') || '|' || symbol as partition_key
            FROM main.us_sector_etfs_raw
        """,
    },
    "currency_etfs_raw": {
        "table": "currency_etfs_raw",
        "sql": """
            SELECT DISTINCT
                strftime(date_trunc('month', date::DATE), '%Y-%m-%d') || '|' || symbol as partition_key
            FROM main.currency_etfs_raw
        """,
    },
    "major_indices_raw": {
        "table": "major_indices_raw",
        "sql": """
            SELECT DISTINCT
                strftime(date_trunc('month', date::DATE), '%Y-%m-%d') || '|' || symbol as partition_key
            FROM main.major_indices_raw
        """,
    },
    "fixed_income_etfs_raw": {
        "table": "fixed_income_etfs_raw",
        "sql": """
            SELECT DISTINCT
                strftime(date_trunc('month', date::DATE), '%Y-%m-%d') || '|' || symbol as partition_key
            FROM main.fixed_income_etfs_raw
        """,
    },
    "global_markets_raw": {
        "table": "global_markets_raw",
        "sql": """
            SELECT DISTINCT
                strftime(date_trunc('month', date::DATE), '%Y-%m-%d') || '|' || symbol as partition_key
            FROM main.global_markets_raw
        """,
    },
    "sp500_companies_prices_raw": {
        "table": "sp500_companies_prices_raw",
        "sql": """
            SELECT DISTINCT
                strftime(date_trunc('month', date::DATE), '%Y-%m-%d') || '|' || symbol as partition_key
            FROM main.sp500_companies_prices_raw
        """,
    },
    "energy_commodities_raw": {
        "table": "energy_commodities_raw",
        "sql": """
            SELECT DISTINCT
                replace(commodity_name, ' ', '_') || '|' || strftime(date_trunc('month', date::DATE), '%Y-%m-%d') as partition_key
            FROM main.energy_commodities_raw
        """,
    },
    "input_commodities_raw": {
        "table": "input_commodities_raw",
        "sql": """
            SELECT DISTINCT
                replace(commodity_name, ' ', '_') || '|' || strftime(date_trunc('month', date::DATE), '%Y-%m-%d') as partition_key
            FROM main.input_commodities_raw
        """,
    },
    "agriculture_commodities_raw": {
        "table": "agriculture_commodities_raw",
        "sql": """
            SELECT DISTINCT
                replace(commodity_name, ' ', '_') || '|' || strftime(date_trunc('month', date::DATE), '%Y-%m-%d') as partition_key
            FROM main.agriculture_commodities_raw
        """,
    },
}


def get_motherduck_partitions(conn, asset_name: str) -> set[str]:
    """Get partition keys that have data in MotherDuck."""
    config = ASSET_CONFIGS[asset_name]
    result = conn.execute(config["sql"]).fetchall()
    return {row[0] for row in result}


def get_dagster_materialized_partitions(asset_name: str) -> set[str]:
    """Get partition keys that Dagster already knows are materialized."""
    partitions = set()
    cursor = None
    limit = 10000

    while True:
        after_clause = f', afterTimestampMillis: "{cursor}"' if cursor else ""
        query = f"""{{
            assetOrError(assetKey: {{path: ["{asset_name}"]}}) {{
                ... on Asset {{
                    assetMaterializations(limit: {limit}{after_clause}) {{
                        partition
                        timestamp
                    }}
                }}
            }}
        }}"""
        resp = requests.post(DAGSTER_URL, json={"query": query})
        data = resp.json()
        mats = data["data"]["assetOrError"]["assetMaterializations"]

        if not mats:
            break

        for m in mats:
            if m["partition"]:
                partitions.add(m["partition"])
            cursor = m["timestamp"]

        if len(mats) < limit:
            break

    return partitions


def report_materializations(asset_name: str, partition_keys: list[str]) -> dict:
    """Report partition keys as materialized using reportRunlessAssetEvents."""
    mutation = """
    mutation ReportRunlessAssetEvents($eventParams: ReportRunlessAssetEventsParams!) {
        reportRunlessAssetEvents(eventParams: $eventParams) {
            __typename
            ... on ReportRunlessAssetEventsSuccess {
                assetKey { path }
            }
            ... on UnauthorizedError {
                message
            }
            ... on PythonError {
                message
            }
        }
    }
    """
    variables = {
        "eventParams": {
            "eventType": "ASSET_MATERIALIZATION",
            "assetKey": {"path": [asset_name]},
            "partitionKeys": partition_keys,
            "description": "Reconciled from MotherDuck data",
        }
    }
    resp = requests.post(DAGSTER_URL, json={"query": mutation, "variables": variables})
    return resp.json()


def reconcile_asset(conn, asset_name: str, dry_run: bool = False) -> dict:
    """Reconcile a single asset. Returns stats."""
    print(f"\n{'='*60}")
    print(f"Reconciling: {asset_name}")
    print(f"{'='*60}")

    # Get data from both sources
    md_partitions = get_motherduck_partitions(conn, asset_name)
    print(f"  MotherDuck data partitions: {len(md_partitions)}")

    dagster_partitions = get_dagster_materialized_partitions(asset_name)
    print(f"  Dagster materialized partitions: {len(dagster_partitions)}")

    # Find gap
    gap = md_partitions - dagster_partitions
    print(f"  Gap (data exists, not materialized): {len(gap)}")

    if not gap:
        print("  No reconciliation needed.")
        return {"asset": asset_name, "gap": 0, "reported": 0}

    if dry_run:
        # Show sample gap partitions
        sample = sorted(gap)[:10]
        print(f"  Sample gap partitions: {sample}")
        return {"asset": asset_name, "gap": len(gap), "reported": 0}

    # Report in batches
    gap_list = sorted(gap)
    reported = 0
    for i in range(0, len(gap_list), BATCH_SIZE):
        batch = gap_list[i : i + BATCH_SIZE]
        result = report_materializations(asset_name, batch)

        response_data = result.get("data", {}).get("reportRunlessAssetEvents", {})
        typename = response_data.get("__typename", "Unknown")

        if typename == "ReportRunlessAssetEventsSuccess":
            reported += len(batch)
            print(f"  Reported batch {i//BATCH_SIZE + 1}: {len(batch)} partitions")
        else:
            msg = response_data.get("message", "Unknown error")
            print(f"  ERROR on batch {i//BATCH_SIZE + 1}: {typename} - {msg}")
            break

        # Small delay between batches to avoid overwhelming the server
        if i + BATCH_SIZE < len(gap_list):
            time.sleep(0.5)

    return {"asset": asset_name, "gap": len(gap), "reported": reported}


def main():
    dry_run = "--dry-run" in sys.argv
    # Allow filtering to specific assets
    asset_filter = [a for a in sys.argv[1:] if not a.startswith("--")]

    if dry_run:
        print("DRY RUN MODE - no changes will be made")

    assets_to_process = asset_filter if asset_filter else list(ASSET_CONFIGS.keys())

    conn = get_motherduck_conn()
    results = []

    for asset_name in assets_to_process:
        if asset_name not in ASSET_CONFIGS:
            print(f"Unknown asset: {asset_name}, skipping")
            continue
        stats = reconcile_asset(conn, asset_name, dry_run=dry_run)
        results.append(stats)

    conn.close()

    # Summary
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    total_gap = 0
    total_reported = 0
    for r in results:
        status = f"reported {r['reported']}" if r["reported"] else f"gap={r['gap']}"
        print(f"  {r['asset']}: {status}")
        total_gap += r["gap"]
        total_reported += r["reported"]
    print(f"\n  Total gap: {total_gap}")
    print(f"  Total reported: {total_reported}")


if __name__ == "__main__":
    main()
