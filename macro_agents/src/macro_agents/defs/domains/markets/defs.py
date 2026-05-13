import dagster as dg

from macro_agents.defs.domains.markets.data_quality_assets import (
    dq_anomalies_to_sheets,
    dq_apply_corrections,
)
from macro_agents.defs.domains.markets.assets import (
    agriculture_commodities_raw,
    currency_etfs_raw,
    energy_commodities_raw,
    fixed_income_etfs_raw,
    global_markets_raw,
    input_commodities_raw,
    major_indices_raw,
    nasdaq_companies_prices_raw,
    nasdaq_companies_raw,
    sp500_companies_prices_raw,
    sp500_companies_raw,
    sp500_splits_raw,
    us_sector_etfs_raw,
)
from macro_agents.defs.domains.markets.checks import (
    agriculture_commodities_weekly_coverage_check,
    currency_etfs_weekly_coverage_check,
    energy_commodities_weekly_coverage_check,
    fixed_income_etfs_weekly_coverage_check,
    global_markets_weekly_coverage_check,
    input_commodities_weekly_coverage_check,
    major_indices_weekly_coverage_check,
    nasdaq_companies_prices_weekly_coverage_check,
    sp500_companies_prices_weekly_coverage_check,
    us_sector_etfs_weekly_coverage_check,
)
from macro_agents.defs.domains.markets.jobs import (
    agriculture_commodities_ingestion_job,
    currency_etfs_ingestion_job,
    energy_commodities_ingestion_job,
    fixed_income_etfs_ingestion_job,
    global_markets_ingestion_job,
    input_commodities_ingestion_job,
    major_indices_ingestion_job,
    nasdaq_companies_list_job,
    nasdaq_companies_prices_ingestion_job,
    sp500_companies_list_job,
    sp500_companies_prices_ingestion_job,
    sp500_splits_ingestion_job,
    us_sector_etfs_ingestion_job,
)
from macro_agents.defs.domains.markets.schedules import (
    market_stack_ingestion_schedules,
    monthly_nasdaq_companies_list_schedule,
    monthly_sp500_companies_list_schedule,
    monthly_sp500_splits_schedule,
)
from macro_agents.defs.resources.company_list_scraper import (
    CompanyListScraperResource,
)
from macro_agents.defs.resources.market_stack import marketstack_resource

# Explicit automation sensor for MarketStack on_cron assets.
# These are weekly Friday cron assets with MultiPartitionsDefinition
# (ticker x month), some with 86,000+ partitions. The implicit
# default_automation_condition_sensor evaluates every ~30 seconds,
# which saturates daemon CPU and the gRPC server. A 10-minute interval
# reduces evaluations from 2,880/day to 144/day (20x reduction).
marketstack_automation_sensor = dg.AutomationConditionSensorDefinition(
    name="marketstack_automation_sensor",
    target=dg.AssetSelection.groups("markets_ingestion", "commodities_ingestion"),
    minimum_interval_seconds=600,
    default_status=dg.DefaultSensorStatus.RUNNING,
)

market_stack_jobs = [
    sp500_companies_list_job,
    nasdaq_companies_list_job,
    us_sector_etfs_ingestion_job,
    currency_etfs_ingestion_job,
    major_indices_ingestion_job,
    fixed_income_etfs_ingestion_job,
    global_markets_ingestion_job,
    sp500_companies_prices_ingestion_job,
    nasdaq_companies_prices_ingestion_job,
    energy_commodities_ingestion_job,
    input_commodities_ingestion_job,
    agriculture_commodities_ingestion_job,
    sp500_splits_ingestion_job,
]


defs = dg.Definitions(
    assets=[
        us_sector_etfs_raw,
        currency_etfs_raw,
        major_indices_raw,
        fixed_income_etfs_raw,
        global_markets_raw,
        sp500_companies_raw,
        nasdaq_companies_raw,
        sp500_companies_prices_raw,
        sp500_splits_raw,
        nasdaq_companies_prices_raw,
        energy_commodities_raw,
        input_commodities_raw,
        agriculture_commodities_raw,
        dq_anomalies_to_sheets,
        dq_apply_corrections,
    ],
    asset_checks=[
        us_sector_etfs_weekly_coverage_check,
        currency_etfs_weekly_coverage_check,
        major_indices_weekly_coverage_check,
        fixed_income_etfs_weekly_coverage_check,
        global_markets_weekly_coverage_check,
        sp500_companies_prices_weekly_coverage_check,
        nasdaq_companies_prices_weekly_coverage_check,
        energy_commodities_weekly_coverage_check,
        input_commodities_weekly_coverage_check,
        agriculture_commodities_weekly_coverage_check,
    ],
    jobs=market_stack_jobs,
    sensors=[marketstack_automation_sensor],
    schedules=[
        *market_stack_ingestion_schedules,
        monthly_sp500_companies_list_schedule,
        monthly_nasdaq_companies_list_schedule,
        monthly_sp500_splits_schedule,
    ],
    resources={
        "marketstack": marketstack_resource,
        "company_scraper": CompanyListScraperResource(),
    },
)
