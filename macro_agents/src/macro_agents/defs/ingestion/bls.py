import dagster as dg
import polars as pl
import requests
from datetime import datetime
from macro_agents.defs.resources.motherduck import MotherDuckResource
from pydantic import Field
import os

census_api_key = os.getenv("CENSUS_API_KEY")


class HousingInventoryConfig(dg.Config):
    year: str = Field(
        default_factory=lambda: str(datetime.now().year),
        description="Year to fetch housing inventory data for",
    )


@dg.asset(
    group_name="ingestion",
    kinds={"polars", "duckdb"},
    automation_condition=dg.AutomationCondition.on_cron("0 3 * * 0"),
    description="Raw data from BLS API for housing inventory - runs weekly on Sundays at 3 AM EST for current year",
)
def housing_inventory_raw(
    context: dg.AssetExecutionContext,
    md: MotherDuckResource,
    config: HousingInventoryConfig,
) -> dg.MaterializeResult:
    year = config.year

    url = f"https://api.census.gov/data/timeseries/eits/hv?get=data_type_code,time_slot_id,seasonally_adj,category_code,cell_value,error_data&for=us:*&time={year}&key={census_api_key}"
    response = requests.get(url)

    columns = response.json()[0]
    rows = response.json()[1:]

    df = pl.DataFrame(rows, schema=columns, orient="row")
    df = df.with_columns(pl.lit(year).alias("year"))
    context.log.info(f"Columns: {df.columns}")

    md.upsert_data("housing_inventory_raw", df, ["year"])

    return dg.MaterializeResult(
        metadata={
            "year": year,
            "num_records": len(df),
            "columns": df.columns,
            "first_10_rows": str(df.head(10)) if df.shape[0] > 0 else "No data",
        }
    )


@dg.asset(
    group_name="ingestion",
    kinds={"polars", "duckdb"},
    automation_condition=dg.AutomationCondition.on_cron("0 4 * * 0"),
    description="Raw data from BLS API for housing pulse - runs weekly on Sundays at 4 AM EST",
)
def housing_pulse_raw(
    context: dg.AssetExecutionContext, md: MotherDuckResource
) -> dg.MaterializeResult:
    main_df = pl.DataFrame()
    iterator = True
    while iterator:
        for cycle in list(range(1, datetime.now().month)):
            try:
                url = f"https://api.census.gov/data/timeseries/hhpulse?get=SURVEY_YEAR,NAME,MEASURE_NAME,COL_START_DATE,COL_END_DATE,RATE,TOTAL,MEASURE_DESCRIPTION&for=state:*&time=2024&CYCLE=0{str(cycle)}&key={census_api_key}"
                response = requests.get(url)
                columns = response.json()[0]
                rows = response.json()[1:]
                data = [dict(zip(columns, row)) for row in rows]
                df = pl.DataFrame(data)
                main_df = pl.concat([main_df, df])
            except Exception as e:
                context.log.info(f"{str(cycle)}- series doesnt exist")
                context.log.info(e)
                break

        break

    md.drop_create_duck_db_table("housing_pulse_raw", main_df)

    return dg.MaterializeResult(
        metadata={
            "num_records": len(main_df),
            "first_10_rows": str(main_df.head(10))
            if main_df.shape[0] > 0
            else "No data",
        }
    )
