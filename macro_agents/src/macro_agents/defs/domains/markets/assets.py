from datetime import date

import dagster as dg
import polars as pl
from google.cloud import bigquery as bq

from macro_agents.defs.constants.market_stack_constants import (
    NASDAQ_COMPANY_TICKERS_PARTITION_NAME,
    SP500_COMPANY_TICKERS_PARTITION_NAME,
)
from macro_agents.defs.domains.markets.partitions import (
    COMMODITIES_GROUP,
    MARKETS_GROUP,
    agriculture_commodities_partitions,
    currency_etfs_partitions,
    energy_commodities_partitions,
    fixed_income_etfs_partitions,
    get_month_dates,
    global_markets_partitions,
    input_commodities_partitions,
    major_indices_tickers_partitions,
    nasdaq_companies_partitions,
    sp500_companies_partitions,
    us_sector_etfs_partitions,
    _sync_company_ticker_partitions,
)
from macro_agents.defs.resources.company_list_scraper import (
    CompanyListScraperResource,
)
from macro_agents.defs.resources.market_stack import MarketStackResource
from macro_agents.defs.resources.bigquery_warehouse import BigQueryWarehouseResource


def _unify_splits_schema(
    df: pl.DataFrame, schema: dict[str, pl.DataType]
) -> pl.DataFrame:
    """Normalize splits DataFrame schema so concatenation succeeds across tickers."""
    cols: list[pl.Expr] = []
    for col_name, dtype in schema.items():
        if col_name in df.columns:
            cols.append(pl.col(col_name).cast(dtype))
        else:
            cols.append(pl.lit(None).cast(dtype).alias(col_name))
    return df.select(cols)


def _ensure_sp500_scd2_columns(
    bq: BigQueryWarehouseResource, context: dg.AssetExecutionContext
) -> None:
    """One-time migration: add date_started/date_ended columns and backfill existing rows."""
    try:
        client = bq.get_client()
        project = client.project
        dataset = bq.dataset
        cols = bq.fetchall(
            f"SELECT column_name FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS` "
            f"WHERE table_name = 'sp500_companies_raw' AND column_name = 'date_started'"
        )
        if cols:
            return

        context.log.info(
            "Migrating sp500_companies_raw: adding date_started/date_ended columns"
        )
        client.query(
            f"ALTER TABLE `{project}.{dataset}.sp500_companies_raw` ADD COLUMN date_started DATE"
        ).result()
        client.query(
            f"ALTER TABLE `{project}.{dataset}.sp500_companies_raw` ADD COLUMN date_ended DATE"
        ).result()
        client.query(
            f"UPDATE `{project}.{dataset}.sp500_companies_raw` "
            f"SET date_started = CAST(fetched_at AS DATE) "
            f"WHERE date_started IS NULL"
        ).result()
        context.log.info("Migration complete: backfilled date_started from fetched_at")
    except Exception as e:
        context.log.debug(f"Migration skipped (table may not exist yet): {e}")


@dg.asset(
    group_name=MARKETS_GROUP,
    kinds={"web_scraping", "duckdb"},
    description="S&P 500 company list from Wikipedia with SCD2 history tracking",
)
def sp500_companies_raw(
    context: dg.AssetExecutionContext,
    company_scraper: CompanyListScraperResource,
    bq: BigQueryWarehouseResource,
) -> dg.MaterializeResult:
    """
    Scrape S&P 500 companies from Wikipedia with SCD Type 2 history.

    Appends new rows and closes old ones instead of overwriting.
    - New companies get date_started=today, date_ended=NULL
    - Removed companies get date_ended=today on their active row
    - Unchanged companies keep their existing active row
    """
    df = company_scraper.scrape_sp500_companies(context)

    if df.is_empty():
        context.log.warning("No S&P 500 companies scraped - check Wikipedia structure")
        return dg.MaterializeResult(
            metadata={"num_companies": 0, "error": "Empty result"}
        )

    if df["symbol"].n_unique() != len(df):
        duplicates = df.filter(pl.col("symbol").is_duplicated())
        context.log.warning(f"Found {len(duplicates)} duplicate symbols")
        df = df.unique(subset=["symbol"], keep="first")

    today = date.today()
    scraped_symbols = set(df["symbol"].to_list())

    # Run one-time migration if needed (adds date_started/date_ended to existing table)
    _ensure_sp500_scd2_columns(bq, context)

    # Load currently active rows from the table
    added_count = 0
    removed_count = 0
    unchanged_count = 0

    client = bq.get_client()
    project = client.project
    dataset = bq.dataset
    table_ref = f"`{project}.{dataset}.sp500_companies_raw`"

    # Create table if it doesn't exist (first run)
    client.query(
        f"CREATE TABLE IF NOT EXISTS {table_ref} ("
        "symbol STRING, company_name STRING, sector STRING, "
        "sub_industry STRING, headquarters STRING, date_added STRING, "
        "cik STRING, founded STRING, source STRING, fetched_at TIMESTAMP, "
        "date_started DATE, date_ended DATE)"
    ).result()

    active_df = bq.execute_query(
        f"SELECT symbol FROM {table_ref} WHERE date_ended IS NULL"
    )
    active_symbols = (
        set(active_df["symbol"].to_list()) if not active_df.is_empty() else set()
    )

    new_symbols = scraped_symbols - active_symbols
    removed_symbols = active_symbols - scraped_symbols
    unchanged_count = len(scraped_symbols & active_symbols)

    # Guard against mass deactivation from partial/incomplete scrapes.
    min_expected = 400
    if active_symbols and len(scraped_symbols) < min_expected:
        context.log.warning(
            f"Scrape returned only {len(scraped_symbols)} companies "
            f"(expected >= {min_expected}). Skipping removals to avoid "
            f"mass deactivation from an incomplete snapshot."
        )
        removed_symbols = set()

    # 1. Close removed companies (set date_ended) — use IN list directly
    if removed_symbols:
        symbols_list = ", ".join(f"'{s}'" for s in sorted(removed_symbols))
        client.query(
            f"UPDATE {table_ref} SET date_ended = DATE('{today}') "
            f"WHERE symbol IN ({symbols_list}) AND date_ended IS NULL"
        ).result()
        removed_count = len(removed_symbols)
        context.log.info(f"Closed {removed_count} removed companies: {sorted(removed_symbols)}")

    # 2. Insert new additions via BigQuery load job
    if new_symbols:
        new_df = df.filter(pl.col("symbol").is_in(list(new_symbols)))
        new_df = new_df.with_columns(
            pl.lit(today).alias("date_started"),
            pl.lit(None).cast(pl.Date).alias("date_ended"),
        )
        client.load_table_from_dataframe(
            new_df.to_pandas(),
            f"{project}.{dataset}.sp500_companies_raw",
            job_config=bq.LoadJobConfig(write_disposition="WRITE_APPEND"),
        ).result()
        added_count = len(new_symbols)
        context.log.info(f"Added {added_count} new companies: {sorted(new_symbols)}")

    # 3. Update metadata for still-active companies via staging table + MERGE
    continuing_symbols = scraped_symbols & active_symbols
    if continuing_symbols:
        continuing_df = df.filter(pl.col("symbol").is_in(list(continuing_symbols)))
        staging_ref = f"{project}.{dataset}.sp500_companies_raw_staging"
        client.load_table_from_dataframe(
            continuing_df.to_pandas(),
            staging_ref,
            job_config=bq.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
        ).result()
        client.query(
            f"MERGE {table_ref} AS T "
            f"USING `{staging_ref}` AS S ON T.symbol = S.symbol AND T.date_ended IS NULL "
            f"WHEN MATCHED THEN UPDATE SET "
            f"T.company_name = S.company_name, T.sector = S.sector, "
            f"T.sub_industry = S.sub_industry, T.headquarters = S.headquarters, "
            f"T.cik = S.cik, T.founded = S.founded, T.fetched_at = S.fetched_at"
        ).result()
        client.delete_table(staging_ref, not_found_ok=True)

    # Sync partitions with only active symbols
    active_tickers = sorted(scraped_symbols)
    _sync_company_ticker_partitions(
        context, SP500_COMPANY_TICKERS_PARTITION_NAME, active_tickers
    )

    return dg.MaterializeResult(
        metadata={
            "num_active_companies": len(scraped_symbols),
            "companies_added": added_count,
            "companies_removed": removed_count,
            "companies_unchanged": unchanged_count,
            "sectors": sorted(df["sector"].unique().to_list()),
            "sample_companies": df.head(5)
            .select(["symbol", "company_name", "sector"])
            .to_dicts(),
            "fetched_at": str(df["fetched_at"][0]),
        }
    )


@dg.asset(
    group_name=MARKETS_GROUP,
    kinds={"web_scraping", "duckdb"},
    description="NASDAQ company list from Stock Analysis with aggressive rate limiting",
)
def nasdaq_companies_raw(
    context: dg.AssetExecutionContext,
    company_scraper: CompanyListScraperResource,
    bq: BigQueryWarehouseResource,
) -> dg.MaterializeResult:
    """
    Scrape NASDAQ companies from Stock Analysis.

    Updates monthly to capture new listings and delistings.
    Uses aggressive rate limiting (8-20s delays) due to anti-scraping measures.
    """
    df = company_scraper.scrape_nasdaq_companies(context)

    if df.is_empty():
        context.log.warning(
            "No NASDAQ companies scraped - check Stock Analysis structure"
        )
        return dg.MaterializeResult(
            metadata={"num_companies": 0, "error": "Empty result"}
        )

    if df["symbol"].n_unique() != len(df):
        duplicates = df.filter(pl.col("symbol").is_duplicated())
        context.log.warning(f"Found {len(duplicates)} duplicate symbols")
        df = df.unique(subset=["symbol"], keep="first")

    bq.upsert_data("nasdaq_companies_raw", df, ["symbol"], context=context)
    _sync_company_ticker_partitions(
        context, NASDAQ_COMPANY_TICKERS_PARTITION_NAME, df["symbol"].to_list()
    )

    return dg.MaterializeResult(
        metadata={
            "num_companies": len(df),
            "sectors": sorted(df["sector"].unique().to_list()),
            "sample_companies": df.head(5)
            .select(["symbol", "company_name", "sector"])
            .to_dicts(),
            "fetched_at": str(df["fetched_at"][0]),
        }
    )


@dg.asset(
    group_name=MARKETS_GROUP,
    kinds={"polars", "duckdb"},
    deps=[dg.AssetKey(["sp500_companies_raw"])],
    description="Stock split history from MarketStack /splits API for S&P 500 companies",
)
def sp500_splits_raw(
    context: dg.AssetExecutionContext,
    bq: BigQueryWarehouseResource,
    marketstack: MarketStackResource,
) -> dg.MaterializeResult:
    """
    Fetch full split history for all S&P 500 tickers from the MarketStack /splits endpoint.

    Unpartitioned asset — splits are rare events and the full history is small.
    Runs monthly after the company list refreshes.
    """
    # Use date_ended filter when SCD2 columns exist, otherwise fetch all symbols
    try:
        tickers_df = bq.execute_query(
            "SELECT DISTINCT symbol FROM sp500_companies_raw WHERE date_ended IS NULL"
        )
    except Exception:
        context.log.info("date_ended column not found, fetching all symbols")
        tickers_df = bq.execute_query("SELECT DISTINCT symbol FROM sp500_companies_raw")
    tickers = sorted(tickers_df["symbol"].to_list())
    context.log.info(f"Fetching splits for {len(tickers)} S&P 500 tickers")

    SPLITS_SCHEMA: dict[str, pl.DataType] = {
        "symbol": pl.Utf8(),
        "date": pl.Utf8(),
        "split_factor": pl.Float64(),
    }
    all_splits: list[pl.DataFrame] = []
    tickers_with_splits = 0
    failed_tickers: list[str] = []

    for i, ticker in enumerate(tickers):
        context.log.info(f"[{i + 1}/{len(tickers)}] Fetching splits for {ticker}")
        try:
            df = marketstack.get_splits_data(ticker)
            if not df.is_empty():
                tickers_with_splits += 1
                unified = _unify_splits_schema(df, SPLITS_SCHEMA)
                all_splits.append(unified)
        except Exception as e:
            context.log.warning(f"Failed to fetch splits for {ticker}: {e}")
            failed_tickers.append(ticker)

    if failed_tickers:
        raise RuntimeError(
            f"Split ingestion failed for {len(failed_tickers)}/{len(tickers)} tickers: "
            f"{failed_tickers[:20]}{'...' if len(failed_tickers) > 20 else ''}"
        )

    if all_splits:
        combined = pl.concat(all_splits, how="vertical")
        context.log.info(
            f"Total split events: {len(combined)} across {tickers_with_splits} tickers"
        )
    else:
        context.log.info("No split events found — creating empty table schema")
        combined = pl.DataFrame(
            schema={"symbol": pl.Utf8, "date": pl.Utf8, "split_factor": pl.Float64}
        )

    bq.upsert_data("sp500_splits_raw", combined, ["symbol", "date"], context=context)

    sample_events = (
        combined.sort("date", descending=True).head(5).to_dicts()
        if not combined.is_empty()
        else []
    )

    return dg.MaterializeResult(
        metadata={
            "total_split_events": len(combined) if not combined.is_empty() else 0,
            "tickers_with_splits": tickers_with_splits,
            "total_tickers_checked": len(tickers),
            "sample_recent_splits": sample_events,
        }
    )


def _fetch_ticker_partitions(
    context: dg.AssetExecutionContext,
    bq: BigQueryWarehouseResource,
    marketstack: MarketStackResource,
    table_name: str,
) -> dg.MaterializeResult:
    """Fetch MarketStack data for batched ticker × date partitions."""
    total_rows = 0
    empty_partitions: list[str] = []
    for partition_key in context.partition_keys:
        ticker = partition_key.keys_by_dimension["ticker"]
        date_partition = partition_key.keys_by_dimension["date"]
        start_date, end_date = get_month_dates(date_partition)

        context.log.info(
            f"Fetching data for ticker: {ticker}, month: {start_date} to {end_date}"
        )

        df = marketstack.get_ticker_historical_data(ticker, start_date, end_date)
        context.log.info(
            f"[{ticker}/{date_partition}] rows after filtering: {df.shape[0]}"
        )
        if df.shape[0] > 0:
            context.log.debug(f"DataFrame columns: {df.columns}")
            bq.upsert_data(table_name, df, ["symbol", "date"], context=context)
            total_rows += df.shape[0]
        else:
            empty_partitions.append(f"{ticker}/{date_partition}")
            context.log.warning(
                f"No data returned for ticker: {ticker}, month: {date_partition}"
            )

    total_partitions = len(context.partition_keys)
    empty_count = len(empty_partitions)

    if empty_partitions:
        context.log.warning(
            f"{empty_count}/{total_partitions} partitions returned 0 rows: "
            f"{empty_partitions[:20]}"
        )

    # Fail the run if ALL partitions returned empty data, so Dagster
    # records them as unmaterialized instead of falsely marking success.
    if empty_count == total_partitions and total_partitions > 0:
        raise RuntimeError(
            f"All {total_partitions} partitions returned 0 rows. "
            f"First 10: {empty_partitions[:10]}"
        )

    return dg.MaterializeResult(
        metadata={
            "total_rows": total_rows,
            "partitions_processed": total_partitions,
            "partitions_empty": empty_count,
            "empty_partition_keys": empty_partitions[:50],
        },
    )


def _fetch_commodity_partitions(
    context: dg.AssetExecutionContext,
    bq: BigQueryWarehouseResource,
    marketstack: MarketStackResource,
    table_name: str,
) -> dg.MaterializeResult:
    """Fetch MarketStack data for batched commodity × date partitions."""
    total_rows = 0
    for partition_key in context.partition_keys:
        commodity = partition_key.keys_by_dimension["commodity"]
        date_partition = partition_key.keys_by_dimension["date"]
        start_date, end_date = get_month_dates(date_partition)

        context.log.debug(
            f"Fetching data for commodity: {commodity}, month: {start_date} to {end_date}"
        )

        df = marketstack.get_commodity_historical_data(commodity, start_date, end_date)
        context.log.debug(f"Fetched {df.shape[0]} rows for {commodity}")
        context.log.debug(f"DataFrame shape before upsert: {df.shape}")
        if df.shape[0] > 0:
            context.log.debug(f"DataFrame columns: {df.columns}")
            context.log.debug(f"DataFrame head:\n{df.head()}")
            bq.upsert_data(table_name, df, ["commodity_name", "date"], context=context)
            total_rows += df.shape[0]
        else:
            context.log.warning(f"No data returned for commodity: {commodity}")

    return dg.MaterializeResult(
        metadata={
            "total_rows": total_rows,
            "partitions_processed": len(context.partition_keys),
        },
    )


@dg.asset(
    group_name=MARKETS_GROUP,
    kinds={"polars", "duckdb"},
    partitions_def=us_sector_etfs_partitions,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=50),
    automation_condition=dg.AutomationCondition.on_cron(
        "0 18 * * 5", "America/New_York"
    ),
    description="Raw data from MarketStack API for US Sector ETFs",
)
def us_sector_etfs_raw(
    context: dg.AssetExecutionContext,
    bq: BigQueryWarehouseResource,
    marketstack: MarketStackResource,
) -> dg.MaterializeResult:
    return _fetch_ticker_partitions(context, md, marketstack, "us_sector_etfs_raw")


@dg.asset(
    group_name=MARKETS_GROUP,
    kinds={"polars", "duckdb"},
    partitions_def=currency_etfs_partitions,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=50),
    automation_condition=dg.AutomationCondition.on_cron(
        "30 18 * * 5", "America/New_York"
    ),
    description="Raw data from MarketStack API for Currency ETFs",
)
def currency_etfs_raw(
    context: dg.AssetExecutionContext,
    bq: BigQueryWarehouseResource,
    marketstack: MarketStackResource,
) -> dg.MaterializeResult:
    return _fetch_ticker_partitions(context, md, marketstack, "currency_etfs_raw")


@dg.asset(
    group_name=MARKETS_GROUP,
    kinds={"polars", "duckdb"},
    partitions_def=major_indices_tickers_partitions,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=50),
    automation_condition=dg.AutomationCondition.on_cron(
        "0 19 * * 5", "America/New_York"
    ),
    description="Raw data from MarketStack API for Major Indices",
)
def major_indices_raw(
    context: dg.AssetExecutionContext,
    bq: BigQueryWarehouseResource,
    marketstack: MarketStackResource,
) -> dg.MaterializeResult:
    return _fetch_ticker_partitions(context, md, marketstack, "major_indices_raw")


@dg.asset(
    group_name=MARKETS_GROUP,
    kinds={"polars", "duckdb"},
    partitions_def=fixed_income_etfs_partitions,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=50),
    automation_condition=dg.AutomationCondition.on_cron(
        "30 19 * * 5", "America/New_York"
    ),
    description="Raw data from MarketStack API for Fixed Income ETFs",
)
def fixed_income_etfs_raw(
    context: dg.AssetExecutionContext,
    bq: BigQueryWarehouseResource,
    marketstack: MarketStackResource,
) -> dg.MaterializeResult:
    return _fetch_ticker_partitions(context, md, marketstack, "fixed_income_etfs_raw")


@dg.asset(
    group_name=MARKETS_GROUP,
    kinds={"polars", "duckdb"},
    partitions_def=global_markets_partitions,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=50),
    automation_condition=dg.AutomationCondition.on_cron(
        "0 20 * * 5", "America/New_York"
    ),
    description="Raw data from MarketStack API for Global Markets",
)
def global_markets_raw(
    context: dg.AssetExecutionContext,
    bq: BigQueryWarehouseResource,
    marketstack: MarketStackResource,
) -> dg.MaterializeResult:
    return _fetch_ticker_partitions(context, md, marketstack, "global_markets_raw")


@dg.asset(
    group_name=MARKETS_GROUP,
    kinds={"polars", "duckdb"},
    partitions_def=sp500_companies_partitions,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=500),
    automation_condition=dg.AutomationCondition.on_cron(
        "30 20 * * 5", "America/New_York"
    ),
    deps=[dg.AssetKey(["sp500_companies_raw"])],
    description="Raw daily data from MarketStack API for S&P 500 companies",
)
def sp500_companies_prices_raw(
    context: dg.AssetExecutionContext,
    bq: BigQueryWarehouseResource,
    marketstack: MarketStackResource,
) -> dg.MaterializeResult:
    return _fetch_ticker_partitions(
        context, md, marketstack, "sp500_companies_prices_raw"
    )


@dg.asset(
    group_name=MARKETS_GROUP,
    kinds={"polars", "duckdb"},
    partitions_def=nasdaq_companies_partitions,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=50),
    automation_condition=dg.AutomationCondition.on_cron(
        "0 21 * * 5", "America/New_York"
    ),
    deps=[dg.AssetKey(["nasdaq_companies_raw"])],
    description="Raw daily data from MarketStack API for NASDAQ companies",
)
def nasdaq_companies_prices_raw(
    context: dg.AssetExecutionContext,
    bq: BigQueryWarehouseResource,
    marketstack: MarketStackResource,
) -> dg.MaterializeResult:
    return _fetch_ticker_partitions(
        context, md, marketstack, "nasdaq_companies_prices_raw"
    )


@dg.asset(
    group_name=COMMODITIES_GROUP,
    kinds={"polars", "duckdb"},
    partitions_def=energy_commodities_partitions,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=50),
    automation_condition=dg.AutomationCondition.on_cron(
        "30 21 * * 5", "America/New_York"
    ),
    description="Raw data from MarketStack API for Energy Commodities",
)
def energy_commodities_raw(
    context: dg.AssetExecutionContext,
    bq: BigQueryWarehouseResource,
    marketstack: MarketStackResource,
) -> dg.MaterializeResult:
    return _fetch_commodity_partitions(
        context, md, marketstack, "energy_commodities_raw"
    )


@dg.asset(
    group_name=COMMODITIES_GROUP,
    kinds={"polars", "duckdb"},
    partitions_def=input_commodities_partitions,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=50),
    automation_condition=dg.AutomationCondition.on_cron(
        "0 22 * * 5", "America/New_York"
    ),
    description="Raw data from MarketStack API for Input/Industrial Commodities",
)
def input_commodities_raw(
    context: dg.AssetExecutionContext,
    bq: BigQueryWarehouseResource,
    marketstack: MarketStackResource,
) -> dg.MaterializeResult:
    return _fetch_commodity_partitions(
        context, md, marketstack, "input_commodities_raw"
    )


@dg.asset(
    group_name=COMMODITIES_GROUP,
    kinds={"polars", "duckdb"},
    partitions_def=agriculture_commodities_partitions,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=50),
    automation_condition=dg.AutomationCondition.on_cron(
        "30 22 * * 5", "America/New_York"
    ),
    description="Raw data from MarketStack API for Agricultural Commodities",
)
def agriculture_commodities_raw(
    context: dg.AssetExecutionContext,
    bq: BigQueryWarehouseResource,
    marketstack: MarketStackResource,
) -> dg.MaterializeResult:
    return _fetch_commodity_partitions(
        context, md, marketstack, "agriculture_commodities_raw"
    )
