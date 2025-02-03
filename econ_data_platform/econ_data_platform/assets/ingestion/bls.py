import dagster as dg
import polars as pl
import requests
from datetime import datetime
from econ_data_platform.resources.motherduck import MotherDuckResource

census_api_key = dg.EnvVar("CENSUS_API_KEY")
year_partition = dg.StaticPartitionsDefinition(
    [str(year) for year in range(1999, 2025)]
)


@dg.asset(
    group_name="ingestion",
    kinds={"polars", "duckdb"},
    partitions_def=year_partition,
    description="Raw data from BLS API for housing inventory",
    automation_condition=dg.AutomationCondition.on_cron("0 0 * * 1"),
)
def housing_inventory_raw(
    context: dg.AssetExecutionContext, md: MotherDuckResource
) -> dg.MaterializeResult:
    # Get the data from the Census API
    year = context.partition_key

    url = f"https://api.census.gov/data/timeseries/eits/hv?get=data_type_code,time_slot_id,seasonally_adj,category_code,cell_value,error_data&for=us:*&time={year}&key={census_api_key}"
    response = requests.get(url)

    columns = response.json()[0]
    rows = response.json()[1:]

    df = pl.DataFrame(rows, schema=columns, orient="row")

    md.drop_create_duck_db_table("housing_inventory", df)

    return dg.MaterializeResult(
        metadata={
            "year": year,
            "num_records": len(df),
        }
    )


@dg.asset(
    group_name="ingestion",
    kinds={"polars", "duckdb"},
    description="Raw data from BLS API for housing pulse",
    automation_condition=dg.AutomationCondition.on_cron("0 0 * * 1"),
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
        }
    )
