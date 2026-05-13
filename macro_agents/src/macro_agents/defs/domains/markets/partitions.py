from datetime import datetime, timedelta

import dagster as dg

from macro_agents.defs.constants.market_stack_constants import (
    AGRICULTURE_COMMODITIES,
    CURRENCY_ETFS,
    ENERGY_COMMODITIES,
    FIXED_INCOME_ETFS,
    GLOBAL_MARKETS,
    INPUT_COMMODITIES,
    MAJOR_INDICES_TICKERS,
    NASDAQ_COMPANY_TICKERS_PARTITION_NAME,
    SP500_COMPANY_TICKERS_PARTITION_NAME,
    US_SECTOR_ETFS,
)

MARKETS_GROUP = "markets_ingestion"
COMMODITIES_GROUP = "commodities_ingestion"
MARKET_STACK_SCHEDULE_TIMEZONE = "America/New_York"

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

sp500_company_tickers = dg.DynamicPartitionsDefinition(
    name=SP500_COMPANY_TICKERS_PARTITION_NAME
)
nasdaq_company_tickers = dg.DynamicPartitionsDefinition(
    name=NASDAQ_COMPANY_TICKERS_PARTITION_NAME
)

sp500_companies_partitions = dg.MultiPartitionsDefinition(
    {"ticker": sp500_company_tickers, "date": monthly_partitions}
)

nasdaq_companies_partitions = dg.MultiPartitionsDefinition(
    {"ticker": nasdaq_company_tickers, "date": monthly_partitions}
)


def _sync_company_ticker_partitions(
    context: dg.AssetExecutionContext, partition_name: str, symbols: list[str]
) -> None:
    unique_symbols = sorted({symbol for symbol in symbols if symbol})
    if not unique_symbols:
        context.log.warning(f"No symbols available to sync for {partition_name}")
        return

    instance = context.instance
    try:
        existing_symbols = set(instance.get_dynamic_partitions(partition_name))
    except Exception as exc:
        context.log.warning(
            f"Could not read existing dynamic partitions for {partition_name}: {exc}"
        )
        existing_symbols = set()

    new_symbols = [
        symbol for symbol in unique_symbols if symbol not in existing_symbols
    ]
    if new_symbols:
        instance.add_dynamic_partitions(partition_name, new_symbols)
        context.log.debug(
            f"Added {len(new_symbols)} dynamic partitions to {partition_name}"
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
