import dagster as dg
from macro_agents.defs.resources.motherduck import MotherDuckResource
from macro_agents.defs.resources.fred import FredResource
from macro_agents.defs.assets.constants.fred_series_lists import (
    inflation_series,
    interest_rates_series,
    money_credit_series,
    gdp_production_series,
    labor_market_series,
    gdp_production_series,
    consumer_series,
    housing_series,
    trade_series,
    financial_conditions_series,
    leading_indicators_series,
    regional_indicators_series,
)


fred_series_partition = dg.StaticPartitionsDefinition(
    [
        "BAMLH0A0HYM2",
        "DJIA",
        "DFF",
        "MORTGAGE30US",
        "USAUCSFRCONDOSMSAMID",
        "DTWEXBGS",
        "DGS10",
        "USCONS",
        "LFWA64TTUSM647S",
        "EXHOSLUSM495S",
        "MDSP",
        "MSPUS",
        "CDSP",
        "MEDDAYONMARUS",
        "MEDLISPRIPERSQUFEEUS",
        "WPUIP2311102",
        "TTLHH",
        "TTLFHH",
        "TTLHHM156N",
        "T4232MM157NCEN",
        "MEHOINUSA672N",
    ]
)


## different lists for fred series


@dg.asset(
    group_name="ingestion",
    kinds={"polars", "duckdb"},
    partitions_def=fred_series_partition,
    automation_condition=dg.AutomationCondition.on_cron("0 0 * * 1"),
    description="Raw data from FRED API",
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
        }
    )
