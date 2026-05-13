from datetime import date

import dagster as dg
import polars as pl

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
from macro_agents.defs.resources.motherduck import MotherDuckResource


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
    md: MotherDuckResource, context: dg.AssetExecutionContext
) -> None:
    """One-time migration: add date_started/date_ended columns and backfill existing rows."""
    conn = None
    try:
        conn = md.get_connection()
        # Check if columns already exist
        cols = conn.execute(
            "SELECT column_name FROM duckdb_columns() "
            "WHERE table_name = 'sp500_companies_raw' AND column_name = 'date_started'"
        ).fetchall()
        if cols:
            return  # Already migrated

        context.log.info(
            "Migrating sp500_companies_raw: adding date_started/date_ended columns"
        )
        conn.execute("ALTER TABLE sp500_companies_raw ADD COLUMN date_started DATE")
        conn.execute("ALTER TABLE sp500_companies_raw ADD COLUMN date_ended DATE")
        conn.execute(
            "UPDATE sp500_companies_raw "
            "SET date_started = CAST(fetched_at AS DATE) "
            "WHERE date_started IS NULL"
        )
        conn.commit()
        context.log.info("Migration complete: backfilled date_started from fetched_at")
    except Exception as e:
        # Table may not exist yet on first run — that's fine
        context.log.debug(f"Migration skipped (table may not exist yet): {e}")
    finally:
        if conn:
            conn.close()


@dg.asset(
    group_name=MARKETS_GROUP,
    kinds={"web_scraping", "duckdb"},
    description="S&P 500 company list from Wikipedia with SCD2 history tracking",
)
def sp500_companies_raw(
    context: dg.AssetExecutionContext,
    company_scraper: CompanyListScraperResource,
    md: MotherDuckResource,
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
    _ensure_sp500_scd2_columns(md, context)

    # Load currently active rows from the table
    conn = None
    added_count = 0
    removed_count = 0
    unchanged_count = 0
    try:
        conn = md.get_connection()

        # Create table if it doesn't exist (first run)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS sp500_companies_raw ("
            "symbol VARCHAR, company_name VARCHAR, sector VARCHAR, "
            "sub_industry VARCHAR, headquarters VARCHAR, date_added VARCHAR, "
            "cik VARCHAR, founded VARCHAR, source VARCHAR, fetched_at TIMESTAMP, "
            "date_started DATE, date_ended DATE)"
        )

        active_df = pl.read_database(
            "SELECT symbol FROM sp500_companies_raw WHERE date_ended IS NULL",
            connection=conn,
        )
        active_symbols = (
            set(active_df["symbol"].to_list()) if not active_df.is_empty() else set()
        )

        new_symbols = scraped_symbols - active_symbols
        removed_symbols = active_symbols - scraped_symbols
        unchanged_count = len(scraped_symbols & active_symbols)

        # Guard against mass deactivation from partial/incomplete scrapes.
        # The S&P 500 has ~500 companies; if we scraped far fewer than active,
        # it likely means parse failures — skip removals to avoid false closures.
        min_expected = 400
        if active_symbols and len(scraped_symbols) < min_expected:
            context.log.warning(
                f"Scrape returned only {len(scraped_symbols)} companies "
                f"(expected >= {min_expected}). Skipping removals to avoid "
                f"mass deactivation from an incomplete snapshot."
            )
            removed_symbols = set()

        # 1. Close removed companies (set date_ended) using temp table join
        if removed_symbols:
            removed_df = pl.DataFrame({"symbol": sorted(removed_symbols)})
            conn.register("removed_symbols_df", removed_df)
            conn.execute(
                "UPDATE sp500_companies_raw SET date_ended = ? "
                "WHERE symbol IN (SELECT symbol FROM removed_symbols_df) "
                "AND date_ended IS NULL",
                [today],
            )
            conn.unregister("removed_symbols_df")
            removed_count = len(removed_symbols)
            context.log.info(
                f"Closed {removed_count} removed companies: {sorted(removed_symbols)}"
            )

        # 2. Insert new additions
        if new_symbols:
            new_df = df.filter(pl.col("symbol").is_in(list(new_symbols)))
            new_df = new_df.with_columns(
                pl.lit(today).alias("date_started"),
                pl.lit(None).cast(pl.Date).alias("date_ended"),
            )
            # Register and insert
            conn.register("new_companies_df", new_df)
            conn.execute(
                "INSERT INTO sp500_companies_raw "
                "SELECT symbol, company_name, sector, sub_industry, headquarters, "
                "date_added, cik, founded, source, fetched_at, date_started, date_ended "
                "FROM new_companies_df"
            )
            conn.unregister("new_companies_df")
            added_count = len(new_symbols)
            context.log.info(
                f"Added {added_count} new companies: {sorted(new_symbols)}"
            )

        # 3. Update metadata for still-active companies (sector, name changes, etc.)
        continuing_symbols = scraped_symbols & active_symbols
        if continuing_symbols:
            continuing_df = df.filter(pl.col("symbol").is_in(list(continuing_symbols)))
            conn.register("continuing_df", continuing_df)
            conn.execute(
                "UPDATE sp500_companies_raw SET "
                "company_name = c.company_name, "
                "sector = c.sector, "
                "sub_industry = c.sub_industry, "
                "headquarters = c.headquarters, "
                "cik = c.cik, "
                "founded = c.founded, "
                "fetched_at = c.fetched_at "
                "FROM continuing_df c "
                "WHERE sp500_companies_raw.symbol = c.symbol "
                "AND sp500_companies_raw.date_ended IS NULL"
            )
            conn.unregister("continuing_df")

        conn.commit()
    finally:
        if conn:
            conn.close()

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
    md: MotherDuckResource,
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

    md.upsert_data("nasdaq_companies_raw", df, ["symbol"], context=context)
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
    md: MotherDuckResource,
    marketstack: MarketStackResource,
) -> dg.MaterializeResult:
    """
    Fetch full split history for all S&P 500 tickers from the MarketStack /splits endpoint.

    Unpartitioned asset — splits are rare events and the full history is small.
    Runs monthly after the company list refreshes.
    """
    # Use date_ended filter when SCD2 columns exist, otherwise fetch all symbols
    try:
        tickers_df = md.execute_query(
            "SELECT DISTINCT symbol FROM sp500_companies_raw WHERE date_ended IS NULL"
        )
    except Exception:
        context.log.info("date_ended column not found, fetching all symbols")
        tickers_df = md.execute_query("SELECT DISTINCT symbol FROM sp500_companies_raw")
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

    md.upsert_data("sp500_splits_raw", combined, ["symbol", "date"], context=context)

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
    md: MotherDuckResource,
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
            md.upsert_data(table_name, df, ["symbol", "date"], context=context)
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
    md: MotherDuckResource,
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
            md.upsert_data(table_name, df, ["commodity_name", "date"], context=context)
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
    md: MotherDuckResource,
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
    md: MotherDuckResource,
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
    md: MotherDuckResource,
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
    md: MotherDuckResource,
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
    md: MotherDuckResource,
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
    md: MotherDuckResource,
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
    md: MotherDuckResource,
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
    md: MotherDuckResource,
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
    md: MotherDuckResource,
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
    md: MotherDuckResource,
    marketstack: MarketStackResource,
) -> dg.MaterializeResult:
    return _fetch_commodity_partitions(
        context, md, marketstack, "agriculture_commodities_raw"
    )
