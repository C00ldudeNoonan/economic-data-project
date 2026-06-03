from datetime import date, timedelta

import dagster as dg

from macro_agents.defs.domains.markets.assets import (
    agriculture_commodities_raw,
    currency_etfs_raw,
    energy_commodities_raw,
    fixed_income_etfs_raw,
    global_markets_raw,
    input_commodities_raw,
    major_indices_raw,
    nasdaq_companies_prices_raw,
    sp500_companies_prices_raw,
    us_sector_etfs_raw,
)
from macro_agents.defs.resources.bigquery_warehouse import BigQueryWarehouseResource


def check_weekly_data_coverage(
    table_name: str,
    partition_column: str,
    date_column: str,
    bq: BigQueryWarehouseResource,
) -> dg.AssetCheckResult:
    """
    Check if every partition has at least one value per week in the last year.
    Returns degraded status if any weeks are missing data.
    """
    logger = dg.get_dagster_logger()

    today = date.today()
    one_year_ago = today - timedelta(days=365)

    if not bq.table_exists(table_name):
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.ERROR,
            metadata={
                "error": f"Table {table_name} does not exist",
            },
        )

    query = f"""
    WITH date_range AS (
        SELECT DISTINCT
            DATE_TRUNC('week', date_col) as week_start
        FROM (
            SELECT CAST('{one_year_ago}' AS DATE) + (INTERVAL '1 day' * days) as date_col
            FROM (SELECT * FROM generate_series(0, 365)) AS t(days)
        )
    ),
    data_weeks AS (
        SELECT DISTINCT
            {partition_column},
            DATE_TRUNC('week', CAST({date_column} AS DATE)) as week_start
        FROM {table_name}
        WHERE CAST({date_column} AS DATE) >= '{one_year_ago}'
            AND CAST({date_column} AS DATE) <= '{today}'
    ),
    expected_weeks AS (
        SELECT DISTINCT
            dr.week_start,
            p.{partition_column}
        FROM date_range dr
        CROSS JOIN (SELECT DISTINCT {partition_column} FROM {table_name}) p
    ),
    missing_weeks AS (
        SELECT 
            ew.{partition_column},
            ew.week_start,
            CAST(ew.week_start AS VARCHAR) || ' to ' || CAST(ew.week_start + INTERVAL '6 days' AS VARCHAR) as week_range
        FROM expected_weeks ew
        LEFT JOIN data_weeks dw 
            ON ew.{partition_column} = dw.{partition_column} 
            AND ew.week_start = dw.week_start
        WHERE dw.week_start IS NULL
    )
    SELECT 
        {partition_column},
        CAST(week_start AS DATE) as week_start,
        week_range
    FROM missing_weeks
    ORDER BY {partition_column}, week_start
    """

    try:
        result_df = bq.execute_query(query)

        if result_df.shape[0] == 0:
            return dg.AssetCheckResult(
                passed=True,
                metadata={
                    "status": "All partitions have at least one value per week in the last year",
                    "checked_period": f"{one_year_ago} to {today}",
                },
            )

        missing_data = result_df.to_dicts()
        failed_partitions = {}

        for row in missing_data:
            partition_key = str(row[partition_column])
            week_range = str(row.get("week_range", row.get("week_start", "unknown")))

            if partition_key not in failed_partitions:
                failed_partitions[partition_key] = []
            failed_partitions[partition_key].append(week_range)

        metadata = {
            "status": "Some partitions are missing data for certain weeks",
            "checked_period": f"{one_year_ago} to {today}",
            "total_missing_weeks": result_df.shape[0],
            "partitions_with_missing_weeks": len(failed_partitions),
        }

        partition_list = []
        for partition, weeks in list(failed_partitions.items())[:50]:
            partition_list.append(
                {
                    "partition": partition,
                    "missing_weeks": weeks[:20],
                }
            )

        metadata["failed_partitions"] = str(partition_list)

        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.WARN,
            metadata=metadata,
        )

    except Exception as e:
        logger.error(f"Error checking weekly data coverage for {table_name}: {str(e)}")
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.ERROR,
            metadata={
                "error": f"Failed to check weekly data coverage: {str(e)}",
            },
        )


@dg.asset_check(asset=us_sector_etfs_raw)
def us_sector_etfs_weekly_coverage_check(
    bq: BigQueryWarehouseResource,
) -> dg.AssetCheckResult:
    """Check if every ticker has at least one value per week in the last year."""
    return check_weekly_data_coverage(
        "us_sector_etfs_raw",
        "symbol",
        "date",
        md,
    )


@dg.asset_check(asset=currency_etfs_raw)
def currency_etfs_weekly_coverage_check(
    bq: BigQueryWarehouseResource,
) -> dg.AssetCheckResult:
    """Check if every ticker has at least one value per week in the last year."""
    return check_weekly_data_coverage(
        "currency_etfs_raw",
        "symbol",
        "date",
        md,
    )


@dg.asset_check(asset=major_indices_raw)
def major_indices_weekly_coverage_check(
    bq: BigQueryWarehouseResource,
) -> dg.AssetCheckResult:
    """Check if every ticker has at least one value per week in the last year."""
    return check_weekly_data_coverage(
        "major_indices_raw",
        "symbol",
        "date",
        md,
    )


@dg.asset_check(asset=fixed_income_etfs_raw)
def fixed_income_etfs_weekly_coverage_check(
    bq: BigQueryWarehouseResource,
) -> dg.AssetCheckResult:
    """Check if every ticker has at least one value per week in the last year."""
    return check_weekly_data_coverage(
        "fixed_income_etfs_raw",
        "symbol",
        "date",
        md,
    )


@dg.asset_check(asset=global_markets_raw)
def global_markets_weekly_coverage_check(
    bq: BigQueryWarehouseResource,
) -> dg.AssetCheckResult:
    """Check if every ticker has at least one value per week in the last year."""
    return check_weekly_data_coverage(
        "global_markets_raw",
        "symbol",
        "date",
        md,
    )


@dg.asset_check(asset=energy_commodities_raw)
def energy_commodities_weekly_coverage_check(
    bq: BigQueryWarehouseResource,
) -> dg.AssetCheckResult:
    """Check if every commodity has at least one value per week in the last year."""
    return check_weekly_data_coverage(
        "energy_commodities_raw",
        "commodity_name",
        "date",
        md,
    )


@dg.asset_check(asset=input_commodities_raw)
def input_commodities_weekly_coverage_check(
    bq: BigQueryWarehouseResource,
) -> dg.AssetCheckResult:
    """Check if every commodity has at least one value per week in the last year."""
    return check_weekly_data_coverage(
        "input_commodities_raw",
        "commodity_name",
        "date",
        md,
    )


@dg.asset_check(asset=agriculture_commodities_raw)
def agriculture_commodities_weekly_coverage_check(
    bq: BigQueryWarehouseResource,
) -> dg.AssetCheckResult:
    """Check if every commodity has at least one value per week in the last year."""
    return check_weekly_data_coverage(
        "agriculture_commodities_raw",
        "commodity_name",
        "date",
        md,
    )


@dg.asset_check(asset=sp500_companies_prices_raw)
def sp500_companies_prices_weekly_coverage_check(
    bq: BigQueryWarehouseResource,
) -> dg.AssetCheckResult:
    """Check if every ticker has at least one value per week in the last year."""
    return check_weekly_data_coverage(
        "sp500_companies_prices_raw",
        "symbol",
        "date",
        md,
    )


@dg.asset_check(asset=nasdaq_companies_prices_raw)
def nasdaq_companies_prices_weekly_coverage_check(
    bq: BigQueryWarehouseResource,
) -> dg.AssetCheckResult:
    """Check if every ticker has at least one value per week in the last year."""
    return check_weekly_data_coverage(
        "nasdaq_companies_prices_raw",
        "symbol",
        "date",
        md,
    )
