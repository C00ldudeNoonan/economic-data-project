from datetime import timedelta
import polars as pl
from typing import Dict, Any
import dagster as dg
from datetime import datetime

from macro_agents.defs.resources.motherduck import MotherDuckResource
from macro_agents.defs.assets.constants.market_stack_constants import US_SECTOR_ETFS, CURRENCY_ETFS, MAJOR_INDICES_TICKERS, FIXED_INCOME_ETFS, GLOBAL_MARKETS
from macro_agents.defs.resources.market_stack import MarketStackResource


# Create partition definitions
us_sector_etfs_partitions = dg.StaticPartitionsDefinition(US_SECTOR_ETFS)
currency_etfs_partitions = dg.StaticPartitionsDefinition(CURRENCY_ETFS)
major_indices_tickers_partitions = dg.StaticPartitionsDefinition(MAJOR_INDICES_TICKERS)
fixed_income_etfs_partitions = dg.StaticPartitionsDefinition(FIXED_INCOME_ETFS)
global_markets_partitions = dg.StaticPartitionsDefinition(GLOBAL_MARKETS)

weekly_partitions = dg.WeeklyPartitionsDefinition(start_date="2020-01-01")

# Multi-dimensional partitions
us_sector_etfs_partitions = dg.MultiPartitionsDefinition({
    "ticker": us_sector_etfs_partitions,
    "date": weekly_partitions
})


dg.asset(
    group_name="ingestion",
    kinds={"polars", "duckdb"},
    partitions_def=us_sector_etfs_partitions,
    automation_condition=dg.AutomationCondition.on_cron("0 0 * * 1"),
    description="Raw data from MarketStack API for US Sector ETFs",
)
def marketstack_us_sector_etfs_data_raw(context: dg.Context, md: MotherDuckResource, marketstack: MarketStackResource) -> dg.MaterializeResult:
    # Fetch data from MarketStack API

    context.log.info(f"Fetching data for partition: {context.partition_key}")
    ticker, date = context.partition_key.split("|") # Split partition key
    start_date = context.partition_key
    end_date = (datetime.strptime(start_date, "%Y-%m-%d") + timedelta(days=6)).strftime("%Y-%m-%d")
    df = marketstack.find_etf_holdings(ticker, start_date, end_date)
    md.upsert_data(df, "marketstack_us_sector_etfs_data_raw", key_columns=["ticker", "date"])

    return dg.MaterializeResult(
        metadata={"rows": df.shape[0], "ticker": ticker, "date": date}
    )