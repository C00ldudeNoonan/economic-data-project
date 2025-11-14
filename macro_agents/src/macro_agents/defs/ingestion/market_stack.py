from datetime import timedelta
import dagster as dg
from datetime import datetime, date

from macro_agents.defs.resources.motherduck import MotherDuckResource
from macro_agents.defs.constants.market_stack_constants import (
    US_SECTOR_ETFS,
    CURRENCY_ETFS,
    MAJOR_INDICES_TICKERS,
    FIXED_INCOME_ETFS,
    GLOBAL_MARKETS,
    ENERGY_COMMODITIES,
    INPUT_COMMODITIES,
    AGRICULTURE_COMMODITIES,
)
from macro_agents.defs.resources.market_stack import MarketStackResource


monthly_partitions = dg.MonthlyPartitionsDefinition(
    start_date="2012-01-01", end_offset=1
)

us_sector_etfs_static = dg.StaticPartitionsDefinition(US_SECTOR_ETFS)
currency_etfs_static = dg.StaticPartitionsDefinition(CURRENCY_ETFS)
major_indices_tickers_static = dg.StaticPartitionsDefinition(MAJOR_INDICES_TICKERS)
fixed_income_etfs_static = dg.StaticPartitionsDefinition(FIXED_INCOME_ETFS)
global_markets_static = dg.StaticPartitionsDefinition(GLOBAL_MARKETS)

us_sector_etfs_partitions = dg.MultiPartitionsDefinition(
    {"ticker": us_sector_etfs_static, "date": monthly_partitions}
)

currency_etfs_partitions = dg.MultiPartitionsDefinition(
    {"ticker": currency_etfs_static, "date": monthly_partitions}
)

major_indices_tickers_partitions = dg.MultiPartitionsDefinition(
    {"ticker": major_indices_tickers_static, "date": monthly_partitions}
)

fixed_income_etfs_partitions = dg.MultiPartitionsDefinition(
    {"ticker": fixed_income_etfs_static, "date": monthly_partitions}
)

global_markets_partitions = dg.MultiPartitionsDefinition(
    {"ticker": global_markets_static, "date": monthly_partitions}
)

energy_commodities_static = dg.StaticPartitionsDefinition(ENERGY_COMMODITIES)
input_commodities_static = dg.StaticPartitionsDefinition(INPUT_COMMODITIES)
agriculture_commodities_static = dg.StaticPartitionsDefinition(AGRICULTURE_COMMODITIES)

energy_commodities_partitions = dg.MultiPartitionsDefinition(
    {"commodity": energy_commodities_static, "date": monthly_partitions}
)

input_commodities_partitions = dg.MultiPartitionsDefinition(
    {"commodity": input_commodities_static, "date": monthly_partitions}
)

agriculture_commodities_partitions = dg.MultiPartitionsDefinition(
    {"commodity": agriculture_commodities_static, "date": monthly_partitions}
)


def get_month_dates(partition_key: str) -> tuple[str, str]:
    """Convert monthly partition key (YYYY-MM-DD or YYYY-MM) to start and end dates"""
    parts = partition_key.split("-")
    year = int(parts[0])
    month = int(parts[1])
    start_date = datetime(year, month, 1).strftime("%Y-%m-%d")
    if month == 12:
        end_date = datetime(year + 1, 1, 1) - timedelta(days=1)
    else:
        end_date = datetime(year, month + 1, 1) - timedelta(days=1)
    end_date_str = end_date.strftime("%Y-%m-%d")
    return start_date, end_date_str


@dg.asset(
    group_name="ingestion",
    kinds={"polars", "duckdb"},
    partitions_def=us_sector_etfs_partitions,
    description="Raw data from MarketStack API for US Sector ETFs",
)
def us_sector_etfs_raw(
    context: dg.AssetExecutionContext,
    md: MotherDuckResource,
    marketstack: MarketStackResource,
) -> dg.MaterializeResult:
    ticker = context.partition_key.keys_by_dimension["ticker"]
    date_partition = context.partition_key.keys_by_dimension["date"]
    start_date, end_date = get_month_dates(date_partition)

    context.log.info(
        f"Fetching data for ticker: {ticker}, month: {start_date} to {end_date}"
    )

    df = marketstack.get_ticker_historical_data(ticker, start_date, end_date)
    context.log.info(f"DataFrame shape before upsert: {df.shape}")
    if df.shape[0] > 0:
        context.log.info(f"DataFrame columns: {df.columns}")
        md.upsert_data("us_sector_etfs_raw", df, ["symbol", "date"], context=context)
    else:
        context.log.warning(f"No data returned for ticker: {ticker}")

    return dg.MaterializeResult(
        metadata={
            "rows": df.shape[0],
            "ticker": ticker,
            "start_date": start_date,
            "end_date": end_date,
            "first_10_rows": str(df.head(10)) if df.shape[0] > 0 else "No data",
        }
    )


@dg.asset(
    group_name="ingestion",
    kinds={"polars", "duckdb"},
    partitions_def=currency_etfs_partitions,
    description="Raw data from MarketStack API for Currency ETFs",
)
def currency_etfs_raw(
    context: dg.AssetExecutionContext,
    md: MotherDuckResource,
    marketstack: MarketStackResource,
) -> dg.MaterializeResult:
    ticker = context.partition_key.keys_by_dimension["ticker"]
    date_partition = context.partition_key.keys_by_dimension["date"]
    start_date, end_date = get_month_dates(date_partition)

    context.log.info(
        f"Fetching data for ticker: {ticker}, month: {start_date} to {end_date}"
    )

    df = marketstack.get_ticker_historical_data(ticker, start_date, end_date)
    context.log.info(f"DataFrame shape before upsert: {df.shape}")
    if df.shape[0] > 0:
        context.log.info(f"DataFrame columns: {df.columns}")
        md.upsert_data("currency_etfs_raw", df, ["symbol", "date"], context=context)
    else:
        context.log.warning(f"No data returned for ticker: {ticker}")

    return dg.MaterializeResult(
        metadata={
            "rows": df.shape[0],
            "ticker": ticker,
            "start_date": start_date,
            "end_date": end_date,
            "first_10_rows": str(df.head(10)) if df.shape[0] > 0 else "No data",
        }
    )


@dg.asset(
    group_name="ingestion",
    kinds={"polars", "duckdb"},
    partitions_def=major_indices_tickers_partitions,
    description="Raw data from MarketStack API for Major Indices",
)
def major_indices_raw(
    context: dg.AssetExecutionContext,
    md: MotherDuckResource,
    marketstack: MarketStackResource,
) -> dg.MaterializeResult:
    ticker = context.partition_key.keys_by_dimension["ticker"]
    date_partition = context.partition_key.keys_by_dimension["date"]
    start_date, end_date = get_month_dates(date_partition)

    context.log.info(
        f"Fetching data for ticker: {ticker}, month: {start_date} to {end_date}"
    )

    df = marketstack.get_ticker_historical_data(ticker, start_date, end_date)
    context.log.info(f"DataFrame shape before upsert: {df.shape}")
    if df.shape[0] > 0:
        context.log.info(f"DataFrame columns: {df.columns}")
        md.upsert_data("major_indices_raw", df, ["symbol", "date"], context=context)
    else:
        context.log.warning(f"No data returned for ticker: {ticker}")

    return dg.MaterializeResult(
        metadata={
            "rows": df.shape[0],
            "ticker": ticker,
            "start_date": start_date,
            "end_date": end_date,
            "first_10_rows": str(df.head(10)) if df.shape[0] > 0 else "No data",
        }
    )


@dg.asset(
    group_name="ingestion",
    kinds={"polars", "duckdb"},
    partitions_def=fixed_income_etfs_partitions,
    description="Raw data from MarketStack API for Fixed Income ETFs",
)
def fixed_income_etfs_raw(
    context: dg.AssetExecutionContext,
    md: MotherDuckResource,
    marketstack: MarketStackResource,
) -> dg.MaterializeResult:
    ticker = context.partition_key.keys_by_dimension["ticker"]
    date_partition = context.partition_key.keys_by_dimension["date"]
    start_date, end_date = get_month_dates(date_partition)

    context.log.info(
        f"Fetching data for ticker: {ticker}, month: {start_date} to {end_date}"
    )

    df = marketstack.get_ticker_historical_data(ticker, start_date, end_date)
    context.log.info(f"DataFrame shape before upsert: {df.shape}")
    if df.shape[0] > 0:
        context.log.info(f"DataFrame columns: {df.columns}")
        md.upsert_data("fixed_income_etfs_raw", df, ["symbol", "date"], context=context)
    else:
        context.log.warning(f"No data returned for ticker: {ticker}")

    return dg.MaterializeResult(
        metadata={
            "rows": df.shape[0],
            "ticker": ticker,
            "start_date": start_date,
            "end_date": end_date,
            "first_10_rows": str(df.head(10)) if df.shape[0] > 0 else "No data",
        }
    )


@dg.asset(
    group_name="ingestion",
    kinds={"polars", "duckdb"},
    partitions_def=global_markets_partitions,
    description="Raw data from MarketStack API for Global Markets",
)
def global_markets_raw(
    context: dg.AssetExecutionContext,
    md: MotherDuckResource,
    marketstack: MarketStackResource,
) -> dg.MaterializeResult:
    ticker = context.partition_key.keys_by_dimension["ticker"]
    date_partition = context.partition_key.keys_by_dimension["date"]
    start_date, end_date = get_month_dates(date_partition)

    context.log.info(
        f"Fetching data for ticker: {ticker}, month: {start_date} to {end_date}"
    )

    df = marketstack.get_ticker_historical_data(ticker, start_date, end_date)
    context.log.info(
        f"Fetched {df.shape[0]} rows for {ticker} from {start_date} to {end_date}"
    )
    context.log.info(f"DataFrame shape before upsert: {df.shape}")
    if df.shape[0] > 0:
        context.log.info(f"DataFrame columns: {df.columns}")
        context.log.info(f"DataFrame head:\n{df.head()}")
        md.upsert_data("global_markets_raw", df, ["symbol", "date"], context=context)
    else:
        context.log.warning(f"No data returned for ticker: {ticker}")

    return dg.MaterializeResult(
        metadata={
            "rows": df.shape[0],
            "ticker": ticker,
            "start_date": start_date,
            "end_date": end_date,
            "first_10_rows": str(df.head(10)) if df.shape[0] > 0 else "No data",
        }
    )


@dg.asset(
    group_name="ingestion",
    kinds={"polars", "duckdb"},
    partitions_def=energy_commodities_partitions,
    description="Raw data from MarketStack API for Energy Commodities",
)
def energy_commodities_raw(
    context: dg.AssetExecutionContext,
    md: MotherDuckResource,
    marketstack: MarketStackResource,
) -> dg.MaterializeResult:
    commodity = context.partition_key.keys_by_dimension["commodity"]
    date_partition = context.partition_key.keys_by_dimension["date"]
    start_date, end_date = get_month_dates(date_partition)

    context.log.info(
        f"Fetching data for commodity: {commodity}, month: {start_date} to {end_date}"
    )

    df = marketstack.get_commodity_historical_data(commodity, start_date, end_date)
    context.log.info(f"Fetched {df.shape[0]} rows for {commodity}")
    context.log.info(f"DataFrame shape before upsert: {df.shape}")
    if df.shape[0] > 0:
        context.log.info(f"DataFrame columns: {df.columns}")
        context.log.info(f"DataFrame head:\n{df.head()}")
        md.upsert_data(
            "energy_commodities_raw", df, ["commodity_name", "date"], context=context
        )
    else:
        context.log.warning(f"No data returned for commodity: {commodity}")

    return dg.MaterializeResult(
        metadata={
            "rows": df.shape[0],
            "commodity": commodity,
            "start_date": start_date,
            "end_date": end_date,
            "first_10_rows": str(df.head(10)) if df.shape[0] > 0 else "No data",
        }
    )


@dg.asset(
    group_name="ingestion",
    kinds={"polars", "duckdb"},
    partitions_def=input_commodities_partitions,
    description="Raw data from MarketStack API for Input/Industrial Commodities",
)
def input_commodities_raw(
    context: dg.AssetExecutionContext,
    md: MotherDuckResource,
    marketstack: MarketStackResource,
) -> dg.MaterializeResult:
    commodity = context.partition_key.keys_by_dimension["commodity"]
    date_partition = context.partition_key.keys_by_dimension["date"]
    start_date, end_date = get_month_dates(date_partition)

    context.log.info(
        f"Fetching data for commodity: {commodity}, month: {start_date} to {end_date}"
    )

    df = marketstack.get_commodity_historical_data(commodity, start_date, end_date)
    context.log.info(f"Fetched {df.shape[0]} rows for {commodity}")
    context.log.info(f"DataFrame shape before upsert: {df.shape}")
    if df.shape[0] > 0:
        context.log.info(f"DataFrame columns: {df.columns}")
        context.log.info(f"DataFrame head:\n{df.head()}")
        md.upsert_data(
            "input_commodities_raw", df, ["commodity_name", "date"], context=context
        )
    else:
        context.log.warning(f"No data returned for commodity: {commodity}")

    return dg.MaterializeResult(
        metadata={
            "rows": df.shape[0],
            "commodity": commodity,
            "start_date": start_date,
            "end_date": end_date,
            "first_10_rows": str(df.head(10)) if df.shape[0] > 0 else "No data",
        }
    )


@dg.asset(
    group_name="ingestion",
    kinds={"polars", "duckdb"},
    partitions_def=agriculture_commodities_partitions,
    description="Raw data from MarketStack API for Agricultural Commodities",
)
def agriculture_commodities_raw(
    context: dg.AssetExecutionContext,
    md: MotherDuckResource,
    marketstack: MarketStackResource,
) -> dg.MaterializeResult:
    commodity = context.partition_key.keys_by_dimension["commodity"]
    date_partition = context.partition_key.keys_by_dimension["date"]
    start_date, end_date = get_month_dates(date_partition)

    context.log.info(
        f"Fetching data for commodity: {commodity}, month: {start_date} to {end_date}"
    )

    df = marketstack.get_commodity_historical_data(commodity, start_date, end_date)
    context.log.info(f"Fetched {df.shape[0]} rows for {commodity}")
    context.log.info(f"DataFrame shape before upsert: {df.shape}")
    if df.shape[0] > 0:
        context.log.info(f"DataFrame columns: {df.columns}")
        context.log.info(f"DataFrame head:\n{df.head()}")
        md.upsert_data(
            "agriculture_commodities_raw",
            df,
            ["commodity_name", "date"],
            context=context,
        )
    else:
        context.log.warning(f"No data returned for commodity: {commodity}")

    return dg.MaterializeResult(
        metadata={
            "rows": df.shape[0],
            "commodity": commodity,
            "start_date": start_date,
            "end_date": end_date,
            "first_10_rows": str(df.head(10)) if df.shape[0] > 0 else "No data",
        }
    )


def check_weekly_data_coverage(
    table_name: str,
    partition_column: str,
    date_column: str,
    md: MotherDuckResource,
) -> dg.AssetCheckResult:
    """
    Check if every partition has at least one value per week in the last year.
    Returns degraded status if any weeks are missing data.
    """
    logger = dg.get_dagster_logger()

    # Calculate date range for last year
    today = date.today()
    one_year_ago = today - timedelta(days=365)

    # Check if table exists
    if not md.table_exists(table_name):
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.ERROR,
            metadata={
                "error": f"Table {table_name} does not exist",
            },
        )

    # Query to get all weeks in the last year with data, grouped by partition
    # Using DuckDB's generate_series function with proper syntax
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
        result_df = md.execute_query(query)

        if result_df.shape[0] == 0:
            # All weeks have data
            return dg.AssetCheckResult(
                passed=True,
                metadata={
                    "status": "All partitions have at least one value per week in the last year",
                    "checked_period": f"{one_year_ago} to {today}",
                },
            )
        else:
            # Some weeks are missing data
            missing_data = result_df.to_dicts()
            failed_partitions = {}

            for row in missing_data:
                partition_key = str(row[partition_column])
                week_range = str(
                    row.get("week_range", row.get("week_start", "unknown"))
                )

                if partition_key not in failed_partitions:
                    failed_partitions[partition_key] = []
                failed_partitions[partition_key].append(week_range)

            # Format metadata
            metadata = {
                "status": "Some partitions are missing data for certain weeks",
                "checked_period": f"{one_year_ago} to {today}",
                "total_missing_weeks": result_df.shape[0],
                "partitions_with_missing_weeks": len(failed_partitions),
            }

            # Add failed partitions and weeks (limit to first 50 to avoid metadata size issues)
            partition_list = []
            for partition, weeks in list(failed_partitions.items())[:50]:
                partition_list.append(
                    {
                        "partition": partition,
                        "missing_weeks": weeks[:20],  # Limit weeks per partition
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


# Asset checks for ticker-based assets (using "symbol" as partition column)
@dg.asset_check(asset=us_sector_etfs_raw)
def us_sector_etfs_weekly_coverage_check(
    md: MotherDuckResource,
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
    md: MotherDuckResource,
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
    md: MotherDuckResource,
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
    md: MotherDuckResource,
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
    md: MotherDuckResource,
) -> dg.AssetCheckResult:
    """Check if every ticker has at least one value per week in the last year."""
    return check_weekly_data_coverage(
        "global_markets_raw",
        "symbol",
        "date",
        md,
    )


# Asset checks for commodity-based assets (using "commodity_name" as partition column)
@dg.asset_check(asset=energy_commodities_raw)
def energy_commodities_weekly_coverage_check(
    md: MotherDuckResource,
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
    md: MotherDuckResource,
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
    md: MotherDuckResource,
) -> dg.AssetCheckResult:
    """Check if every commodity has at least one value per week in the last year."""
    return check_weekly_data_coverage(
        "agriculture_commodities_raw",
        "commodity_name",
        "date",
        md,
    )
