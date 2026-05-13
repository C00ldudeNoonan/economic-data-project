import dagster as dg

from macro_agents.defs.domains.markets.jobs import (
    nasdaq_companies_list_job,
    sp500_companies_list_job,
    sp500_splits_ingestion_job,
)

# ---------------------------------------------------------------------------
# All MarketStack ingestion assets (tickers + commodities) now use
# declarative automation (AutomationCondition.on_cron) with BackfillPolicy
# to batch partitions into a single run.  The per-partition schedules that
# previously lived here have been removed.
# ---------------------------------------------------------------------------

# Empty list kept for backward-compatible imports in defs.py / __init__.py
market_stack_ingestion_schedules: list[dg.ScheduleDefinition] = []

# ---------------------------------------------------------------------------
# Simple cron schedules (company lists + splits — one run each, no partitions)
# ---------------------------------------------------------------------------

monthly_sp500_splits_schedule = dg.ScheduleDefinition(
    name="monthly_sp500_splits_schedule",
    cron_schedule="0 4 1 * *",
    execution_timezone="America/New_York",
    job=sp500_splits_ingestion_job,
    description="Monthly S&P 500 stock splits ingestion on 1st at 4 AM EST (after company list at 2 AM)",
)

monthly_sp500_companies_list_schedule = dg.ScheduleDefinition(
    name="monthly_sp500_companies_list_schedule",
    cron_schedule="0 2 1 * *",
    execution_timezone="America/New_York",
    job=sp500_companies_list_job,
    description="Monthly S&P 500 company list scraping on 1st at 2 AM EST",
)

monthly_nasdaq_companies_list_schedule = dg.ScheduleDefinition(
    name="monthly_nasdaq_companies_list_schedule",
    cron_schedule="0 3 1 * *",
    execution_timezone="America/New_York",
    job=nasdaq_companies_list_job,
    description="Monthly NASDAQ company list scraping on 1st at 3 AM EST",
)
