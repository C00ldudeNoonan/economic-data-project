from datetime import timedelta
import dagster as dg
from datetime import datetime

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


weekly_partitions = dg.WeeklyPartitionsDefinition(start_date="2012-01-01")

us_sector_etfs_static = dg.StaticPartitionsDefinition(US_SECTOR_ETFS)
currency_etfs_static = dg.StaticPartitionsDefinition(CURRENCY_ETFS)
major_indices_tickers_static = dg.StaticPartitionsDefinition(MAJOR_INDICES_TICKERS)
fixed_income_etfs_static = dg.StaticPartitionsDefinition(FIXED_INCOME_ETFS)
global_markets_static = dg.StaticPartitionsDefinition(GLOBAL_MARKETS)

# Multi-dimensional partitions
us_sector_etfs_partitions = dg.MultiPartitionsDefinition(
    {"ticker": us_sector_etfs_static, "date": weekly_partitions}
)

currency_etfs_partitions = dg.MultiPartitionsDefinition(
    {"ticker": currency_etfs_static, "date": weekly_partitions}
)

major_indices_tickers_partitions = dg.MultiPartitionsDefinition(
    {"ticker": major_indices_tickers_static, "date": weekly_partitions}
)

fixed_income_etfs_partitions = dg.MultiPartitionsDefinition(
    {"ticker": fixed_income_etfs_static, "date": weekly_partitions}
)

global_markets_partitions = dg.MultiPartitionsDefinition(
    {"ticker": global_markets_static, "date": weekly_partitions}
)

# Commodities partitions
energy_commodities_static = dg.StaticPartitionsDefinition(ENERGY_COMMODITIES)
input_commodities_static = dg.StaticPartitionsDefinition(INPUT_COMMODITIES)
agriculture_commodities_static = dg.StaticPartitionsDefinition(AGRICULTURE_COMMODITIES)

energy_commodities_partitions = dg.MultiPartitionsDefinition(
    {"commodity": energy_commodities_static, "date": weekly_partitions}
)

input_commodities_partitions = dg.MultiPartitionsDefinition(
    {"commodity": input_commodities_static, "date": weekly_partitions}
)

agriculture_commodities_partitions = dg.MultiPartitionsDefinition(
    {"commodity": agriculture_commodities_static, "date": weekly_partitions}
)


def get_week_dates(partition_key: str) -> tuple[str, str]:
    """Convert weekly partition key to start and end dates"""
    start_date = partition_key
    end_date = (datetime.strptime(start_date, "%Y-%m-%d") + timedelta(days=6)).strftime(
        "%Y-%m-%d"
    )
    return start_date, end_date


# US Sector ETFs Asset
@dg.asset(
    group_name="ingestion",
    kinds={"polars", "duckdb"},
    partitions_def=us_sector_etfs_partitions,
    automation_condition=dg.AutomationCondition.on_cron("0 0 * * 5"),
    description="Raw data from MarketStack API for US Sector ETFs",
)
def us_sector_etfs_raw(
    context: dg.AssetExecutionContext,
    md: MotherDuckResource,
    marketstack: MarketStackResource,
) -> dg.MaterializeResult:
    # Extract partition dimensions
    ticker = context.partition_key.keys_by_dimension["ticker"]
    date_partition = context.partition_key.keys_by_dimension["date"]

    # Get week start and end dates
    start_date, end_date = get_week_dates(date_partition)

    context.log.info(
        f"Fetching data for ticker: {ticker}, week: {start_date} to {end_date}"
    )

    # Fetch data from MarketStack API
    df = marketstack.get_ticker_historical_data(ticker, start_date, end_date)

    # Upsert to MotherDuck
    md.upsert_data("us_sector_etfs_raw", df, ["symbol", "date"])

    return dg.MaterializeResult(
        metadata={
            "rows": df.shape[0],
            "ticker": ticker,
            "start_date": start_date,
            "end_date": end_date,
        }
    )


# Currency ETFs Asset
@dg.asset(
    group_name="ingestion",
    kinds={"polars", "duckdb"},
    partitions_def=currency_etfs_partitions,
    automation_condition=dg.AutomationCondition.on_cron("0 0 * * 5"),
    description="Raw data from MarketStack API for Currency ETFs",
)
def currency_etfs_raw(
    context: dg.AssetExecutionContext,
    md: MotherDuckResource,
    marketstack: MarketStackResource,
) -> dg.MaterializeResult:
    ticker = context.partition_key.keys_by_dimension["ticker"]
    date_partition = context.partition_key.keys_by_dimension["date"]
    start_date, end_date = get_week_dates(date_partition)

    context.log.info(
        f"Fetching data for ticker: {ticker}, week: {start_date} to {end_date}"
    )

    df = marketstack.get_ticker_historical_data(ticker, start_date, end_date)
    md.upsert_data("currency_etfs_raw", df, ["symbol", "date"])

    return dg.MaterializeResult(
        metadata={
            "rows": df.shape[0],
            "ticker": ticker,
            "start_date": start_date,
            "end_date": end_date,
        }
    )


# Major Indices Asset
@dg.asset(
    group_name="ingestion",
    kinds={"polars", "duckdb"},
    partitions_def=major_indices_tickers_partitions,
    automation_condition=dg.AutomationCondition.on_cron("0 0 * * 5"),
    description="Raw data from MarketStack API for Major Indices",
)
def major_indices_raw(
    context: dg.AssetExecutionContext,
    md: MotherDuckResource,
    marketstack: MarketStackResource,
) -> dg.MaterializeResult:
    ticker = context.partition_key.keys_by_dimension["ticker"]
    date_partition = context.partition_key.keys_by_dimension["date"]
    start_date, end_date = get_week_dates(date_partition)

    context.log.info(
        f"Fetching data for ticker: {ticker}, week: {start_date} to {end_date}"
    )

    df = marketstack.get_ticker_historical_data(ticker, start_date, end_date)
    md.upsert_data("major_indices_raw", df, ["symbol", "date"])

    return dg.MaterializeResult(
        metadata={
            "rows": df.shape[0],
            "ticker": ticker,
            "start_date": start_date,
            "end_date": end_date,
        }
    )


@dg.asset(
    group_name="ingestion",
    kinds={"polars", "duckdb"},
    partitions_def=fixed_income_etfs_partitions,
    automation_condition=dg.AutomationCondition.on_cron("0 0 * * 5"),
    description="Raw data from MarketStack API for Fixed Income ETFs",
)
def fixed_income_etfs_raw(
    context: dg.AssetExecutionContext,
    md: MotherDuckResource,
    marketstack: MarketStackResource,
) -> dg.MaterializeResult:
    ticker = context.partition_key.keys_by_dimension["ticker"]
    date_partition = context.partition_key.keys_by_dimension["date"]
    start_date, end_date = get_week_dates(date_partition)

    context.log.info(
        f"Fetching data for ticker: {ticker}, week: {start_date} to {end_date}"
    )

    df = marketstack.get_ticker_historical_data(ticker, start_date, end_date)
    md.upsert_data("fixed_income_etfs_raw", df, ["symbol", "date"])

    return dg.MaterializeResult(
        metadata={
            "rows": df.shape[0],
            "ticker": ticker,
            "start_date": start_date,
            "end_date": end_date,
        }
    )


# Global Markets Asset
@dg.asset(
    group_name="ingestion",
    kinds={"polars", "duckdb"},
    partitions_def=global_markets_partitions,
    automation_condition=dg.AutomationCondition.on_cron("0 0 * * 5"),
    description="Raw data from MarketStack API for Global Markets",
)
def global_markets_raw(
    context: dg.AssetExecutionContext,
    md: MotherDuckResource,
    marketstack: MarketStackResource,
) -> dg.MaterializeResult:
    ticker = context.partition_key.keys_by_dimension["ticker"]
    date_partition = context.partition_key.keys_by_dimension["date"]
    start_date, end_date = get_week_dates(date_partition)

    context.log.info(
        f"Fetching data for ticker: {ticker}, week: {start_date} to {end_date}"
    )

    df = marketstack.get_ticker_historical_data(ticker, start_date, end_date)
    context.log.info(
        f"Fetched {df.shape[0]} rows for {ticker} from {start_date} to {end_date}"
    )
    context.log.info(f"DataFrame columns: {df.columns}")
    context.log.info(f"DataFrame head:\n{df.head()}")
    md.upsert_data("global_markets_raw", df, ["symbol", "date"])

    return dg.MaterializeResult(
        metadata={
            "rows": df.shape[0],
            "ticker": ticker,
            "start_date": start_date,
            "end_date": end_date,
        }
    )


# Energy Commodities Asset
@dg.asset(
    group_name="ingestion",
    kinds={"polars", "duckdb"},
    partitions_def=energy_commodities_partitions,
    automation_condition=dg.AutomationCondition.on_cron("0 0 * * 5"),
    description="Raw data from MarketStack API for Energy Commodities",
)
def energy_commodities_raw(
    context: dg.AssetExecutionContext,
    md: MotherDuckResource,
    marketstack: MarketStackResource,
) -> dg.MaterializeResult:
    # Extract partition dimensions
    commodity = context.partition_key.keys_by_dimension["commodity"]
    date_partition = context.partition_key.keys_by_dimension["date"]

    # Get week start and end dates
    start_date, end_date = get_week_dates(date_partition)

    context.log.info(
        f"Fetching data for commodity: {commodity}, week: {start_date} to {end_date}"
    )

    # Fetch data from MarketStack API
    df = marketstack.get_commodity_historical_data(commodity, start_date, end_date)
    
    # Log DataFrame info for debugging
    context.log.info(f"Fetched {df.shape[0]} rows for {commodity}")
    if df.shape[0] > 0:
        context.log.info(f"DataFrame columns: {df.columns}")
        context.log.info(f"DataFrame head:\n{df.head()}")
        md.upsert_data("energy_commodities_raw", df, ["commodity_name", "date"])
    else:
        context.log.warning(f"No data returned for commodity: {commodity}")

    return dg.MaterializeResult(
        metadata={
            "rows": df.shape[0],
            "commodity": commodity,
            "start_date": start_date,
            "end_date": end_date,
        }
    )


# Input Commodities Asset
@dg.asset(
    group_name="ingestion",
    kinds={"polars", "duckdb"},
    partitions_def=input_commodities_partitions,
    automation_condition=dg.AutomationCondition.on_cron("0 0 * * 5"),
    description="Raw data from MarketStack API for Input/Industrial Commodities",
)
def input_commodities_raw(
    context: dg.AssetExecutionContext,
    md: MotherDuckResource,
    marketstack: MarketStackResource,
) -> dg.MaterializeResult:
    commodity = context.partition_key.keys_by_dimension["commodity"]
    date_partition = context.partition_key.keys_by_dimension["date"]
    start_date, end_date = get_week_dates(date_partition)

    context.log.info(
        f"Fetching data for commodity: {commodity}, week: {start_date} to {end_date}"
    )

    df = marketstack.get_commodity_historical_data(commodity, start_date, end_date)
    
    # Log DataFrame info for debugging
    context.log.info(f"Fetched {df.shape[0]} rows for {commodity}")
    if df.shape[0] > 0:
        context.log.info(f"DataFrame columns: {df.columns}")
        context.log.info(f"DataFrame head:\n{df.head()}")
        md.upsert_data("input_commodities_raw", df, ["commodity_name", "date"])
    else:
        context.log.warning(f"No data returned for commodity: {commodity}")

    return dg.MaterializeResult(
        metadata={
            "rows": df.shape[0],
            "commodity": commodity,
            "start_date": start_date,
            "end_date": end_date,
        }
    )


# Agriculture Commodities Asset
@dg.asset(
    group_name="ingestion",
    kinds={"polars", "duckdb"},
    partitions_def=agriculture_commodities_partitions,
    automation_condition=dg.AutomationCondition.on_cron("0 0 * * 5"),
    description="Raw data from MarketStack API for Agricultural Commodities",
)
def agriculture_commodities_raw(
    context: dg.AssetExecutionContext,
    md: MotherDuckResource,
    marketstack: MarketStackResource,
) -> dg.MaterializeResult:
    commodity = context.partition_key.keys_by_dimension["commodity"]
    date_partition = context.partition_key.keys_by_dimension["date"]
    start_date, end_date = get_week_dates(date_partition)

    context.log.info(
        f"Fetching data for commodity: {commodity}, week: {start_date} to {end_date}"
    )

    df = marketstack.get_commodity_historical_data(commodity, start_date, end_date)
    
    # Log DataFrame info for debugging
    context.log.info(f"Fetched {df.shape[0]} rows for {commodity}")
    if df.shape[0] > 0:
        context.log.info(f"DataFrame columns: {df.columns}")
        context.log.info(f"DataFrame head:\n{df.head()}")
        md.upsert_data("agriculture_commodities_raw", df, ["commodity_name", "date"])
    else:
        context.log.warning(f"No data returned for commodity: {commodity}")

    return dg.MaterializeResult(
        metadata={
            "rows": df.shape[0],
            "commodity": commodity,
            "start_date": start_date,
            "end_date": end_date,
        }
    )
