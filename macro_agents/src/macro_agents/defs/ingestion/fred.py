import dagster as dg
from macro_agents.defs.resources.motherduck import MotherDuckResource
from macro_agents.defs.resources.fred import FredResource
from macro_agents.defs.constants.fred_series_lists import (
    labor_market_series,
    inflation_series,
    interest_rates_series,
    money_credit_series,
    gdp_production_series,
    consumer_series,
    housing_series,
    trade_series,
    financial_conditions_series,
    leading_indicators_series,
    regional_indicators_series,
)


# Combine all series lists and extract unique series codes
_all_series_lists = [
    labor_market_series,
    inflation_series,
    interest_rates_series,
    money_credit_series,
    gdp_production_series,
    consumer_series,
    housing_series,
    trade_series,
    financial_conditions_series,
    leading_indicators_series,
    regional_indicators_series,
]

# Extract series codes (first element of each tuple) and deduplicate
_all_series_codes = []
for series_list in _all_series_lists:
    for series_tuple in series_list:
        series_code = series_tuple[0]
        if series_code not in _all_series_codes:
            _all_series_codes.append(series_code)

# Sort for consistency
_all_series_codes.sort()

fred_series_partition = dg.StaticPartitionsDefinition(_all_series_codes)


@dg.asset(
    group_name="ingestion",
    kinds={"polars", "duckdb"},
    partitions_def=fred_series_partition,
    automation_condition=dg.AutomationCondition.on_cron("0 2 * * 0"),
    description="Raw data from FRED API - runs weekly on Sundays at 2 AM EST",
)
def fred_raw(
    context: dg.AssetExecutionContext, fred: FredResource, md: MotherDuckResource
) -> dg.MaterializeResult:
    series_code = context.partition_key

    data = fred.get_fred_data(series_code)
    md.upsert_data("fred_raw", data, ["date", "series_code"])

    return dg.MaterializeResult(
        metadata={
            "series_code": series_code,
            "num_records": len(data),
            "max_date": str(data["date"].max()),
            "min_date": str(data["date"].min()),
            "first_10_rows": str(data.head(10)) if data.shape[0] > 0 else "No data",
        }
    )
