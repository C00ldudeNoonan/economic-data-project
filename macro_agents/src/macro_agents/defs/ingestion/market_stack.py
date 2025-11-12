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
    md.upsert_data("us_sector_etfs_raw", df, ["symbol", "date"])

    return dg.MaterializeResult(
        metadata={
            "rows": df.shape[0],
            "ticker": ticker,
            "start_date": start_date,
            "end_date": end_date,
            "first_10_rows": str(df.head(10)),
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
    md.upsert_data("currency_etfs_raw", df, ["symbol", "date"])

    return dg.MaterializeResult(
        metadata={
            "rows": df.shape[0],
            "ticker": ticker,
            "start_date": start_date,
            "end_date": end_date,
            "first_10_rows": str(df.head(10)),
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
    md.upsert_data("major_indices_raw", df, ["symbol", "date"])

    return dg.MaterializeResult(
        metadata={
            "rows": df.shape[0],
            "ticker": ticker,
            "start_date": start_date,
            "end_date": end_date,
            "first_10_rows": str(df.head(10)),
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
    md.upsert_data("fixed_income_etfs_raw", df, ["symbol", "date"])

    return dg.MaterializeResult(
        metadata={
            "rows": df.shape[0],
            "ticker": ticker,
            "start_date": start_date,
            "end_date": end_date,
            "first_10_rows": str(df.head(10)),
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
    context.log.info(f"DataFrame columns: {df.columns}")
    context.log.info(f"DataFrame head:\n{df.head()}")
    md.upsert_data("global_markets_raw", df, ["symbol", "date"])

    return dg.MaterializeResult(
        metadata={
            "rows": df.shape[0],
            "ticker": ticker,
            "start_date": start_date,
            "end_date": end_date,
            "first_10_rows": str(df.head(10)),
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
            "first_10_rows": str(df.head(10)) if df.shape[0] > 0 else "No data",
        }
    )
