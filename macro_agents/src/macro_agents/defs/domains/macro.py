"""Macro-economic ingestion assets."""

from datetime import datetime, timezone
from typing import cast

import dagster as dg
import polars as pl
import requests
import pytz
from bs4 import BeautifulSoup
from bs4.element import Tag

from macro_agents.defs.constants.fred_series_lists import (
    consumer_series,
    financial_conditions_series,
    fiscal_series,
    gdp_production_series,
    housing_series,
    inflation_series,
    interest_rates_series,
    labor_market_series,
    manufacturing_activity_series,
    leading_indicators_series,
    money_credit_series,
    regional_indicators_series,
    trade_series,
)
from macro_agents.defs.resources.federal_reserve import FederalReserveResource
from macro_agents.defs.resources.fred import FredResource, fred_resource
from macro_agents.defs.resources.gcs import GCSResource
from macro_agents.defs.resources.bigquery_warehouse import BigQueryWarehouseResource
from macro_agents.defs.domains.macro_checks import macro_checks

MACRO_GROUP = "macro_ingestion"
TREASURY_YIELD_COLUMNS = [
    "bc_1month",
    "bc_2month",
    "bc_3month",
    "bc_4month",
    "bc_6month",
    "bc_1year",
    "bc_2year",
    "bc_3year",
    "bc_5year",
    "bc_7year",
    "bc_10year",
    "bc_20year",
    "bc_30year",
]
TREASURY_YIELD_COLUMN_TYPES = {column: "FLOAT64" for column in TREASURY_YIELD_COLUMNS}

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
    fiscal_series,
    manufacturing_activity_series,
]

_all_series_codes = []
for series_list in _all_series_lists:
    for series_tuple in series_list:
        series_code = series_tuple[0]
        if series_code not in _all_series_codes:
            _all_series_codes.append(series_code)

_all_series_codes.sort()

fred_series_partition = dg.StaticPartitionsDefinition(_all_series_codes)


@dg.asset(
    group_name=MACRO_GROUP,
    kinds={"polars", "duckdb"},
    partitions_def=fred_series_partition,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=10),
    description="Raw data from FRED API - runs weekly on Sundays at 2 AM EST",
)
def fred_raw(
    context: dg.AssetExecutionContext, fred: FredResource, bq: BigQueryWarehouseResource
) -> dg.MaterializeResult:
    total_records = 0
    for series_code in context.partition_keys:
        data = fred.get_fred_data(series_code)
        bq.upsert_data("fred_raw", data, ["date", "series_code"])
        total_records += len(data)

    return dg.MaterializeResult(
        metadata={
            "total_records": total_records,
            "partitions_processed": len(context.partition_keys),
        },
    )


current_year = datetime.now().year
year_partition = dg.StaticPartitionsDefinition(
    [str(year) for year in range(1990, current_year + 1)]
)


@dg.asset(
    group_name=MACRO_GROUP,
    kinds={"polars", "duckdb", "web_scraping"},
    partitions_def=year_partition,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=10),
    description="Raw Treasury yield curve data scraped from Treasury.gov XML feed",
)
def treasury_yields_raw(
    context: dg.AssetExecutionContext, bq: BigQueryWarehouseResource
) -> dg.MaterializeResult:
    """Scrape Treasury yield curve data for batched year partitions."""
    total_records = 0
    for year in context.partition_keys:
        total_records += _fetch_treasury_yields_for_year(context, bq, year)

    return dg.MaterializeResult(
        metadata={
            "total_records": total_records,
            "partitions_processed": len(context.partition_keys),
        },
    )


def _fetch_treasury_yields_for_year(
    context: dg.AssetExecutionContext, bq: BigQueryWarehouseResource, year: str
) -> int:
    """Scrape Treasury yield curve data for a specific year. Returns record count."""
    url = (
        "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/"
        f"xmlview?data=daily_treasury_yield_curve&field_tdr_date_value={year}"
    )

    context.log.info(f"Fetching Treasury yield data for year {year} from: {url}")

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, "xml")
        entries = soup.find_all("entry")
        context.log.info(f"Found {len(entries)} entries for year {year}")

        if not entries:
            context.log.warning(f"No data found for year {year}")
            return 0

        records = []
        for entry in entries:
            entry_tag = entry
            properties = entry_tag.find("m:properties")
            if not properties:
                continue
            properties_tag = cast(Tag, properties)

            date_elem = properties_tag.find("d:NEW_DATE")
            if not date_elem:
                continue

            date_str = date_elem.get_text().strip()
            date_only = date_str.split("T")[0]

            yields: dict[str, str | float | None] = {"date": date_only}
            for yield_elem in TREASURY_YIELD_COLUMNS:
                xml_field = yield_elem.upper()
                elem = properties_tag.find(f"d:{xml_field}")
                if elem and elem.get_text().strip():
                    try:
                        yields[xml_field.lower()] = float(elem.get_text().strip())
                    except ValueError:
                        yields[xml_field.lower()] = None
                else:
                    yields[xml_field.lower()] = None

            records.append(yields)

        df = pl.DataFrame(records)
        # Ensure yield columns are Float64 even when all values are null,
        # to prevent Polars inferring Utf8 and creating VARCHAR columns in DuckDB.
        yield_cols = [col for col in df.columns if col.startswith("bc_")]
        df = df.cast({col: pl.Float64 for col in yield_cols})
        context.log.info(f"Successfully parsed {len(df)} records for year {year}")

        if len(df) > 0:
            bq.normalize_column_types(
                "treasury_yields_raw",
                TREASURY_YIELD_COLUMN_TYPES,
                context=context,
            )
            bq.upsert_data("treasury_yields_raw", df, ["date"], context=context)

        return len(df)

    except Exception as e:
        context.log.error(f"Error fetching Treasury yield data for year {year}: {e}")
        raise


@dg.asset(
    group_name=MACRO_GROUP,
    kinds={"web_scraping", "gcs"},
    description=(
        "FOMC minutes from the Federal Reserve - runs daily to check for new minutes"
    ),
)
def fomc_minutes_raw(
    context: dg.AssetExecutionContext,
    bq: BigQueryWarehouseResource,
    fed: FederalReserveResource,
    gcs: GCSResource,
) -> dg.MaterializeResult:
    """
    Fetch FOMC minutes and store them in GCS and DuckDB.

    - Raw minutes stored in GCS for retrieval
    - Metadata tracked in DuckDB: meeting_date, gcs_path, fetched_at, etc.
    """
    now = datetime.now(timezone.utc)
    year = now.year
    context.log.info(f"Fetching FOMC minutes for year: {year}")
    context.log.info(f"GCS path prefix: fomc/{year}/")

    meeting_dates = fed.get_meeting_calendar(year, context)
    context.log.info(f"Found {len(meeting_dates)} meetings for {year}")

    minutes_data = []

    for meeting_date in meeting_dates:
        try:
            context.log.debug(f"Processing FOMC minutes for meeting: {meeting_date}")

            minutes = fed.get_meeting_minutes(meeting_date, context)
            minutes_date = minutes["meeting_date"]

            gcs_path = f"fomc/{year}/{minutes_date}.json"
            gcs_uri = gcs.upload_json(gcs_path, minutes, context)

            context.log.debug(f"Uploaded to GCS path: {gcs_path}")

            minutes_data.append(
                {
                    "meeting_date": minutes_date,
                    "gcs_path": gcs_path,
                    "gcs_uri": gcs_uri,
                    "meeting_link": minutes.get("source_url"),
                    "meeting_year": year,
                    "fetched_at": now,
                }
            )
        except Exception as exc:
            context.log.warning(
                f"Failed to process minutes entry {meeting_date}: {exc}"
            )

    if not minutes_data:
        return dg.MaterializeResult(
            metadata={
                "status": "no_minutes_found",
                "meeting_year": year,
                "gcs_bucket": gcs.bucket_name,
                "gcs_prefix": f"fomc/{year}/",
            }
        )

    df = pl.DataFrame(minutes_data)
    bq.upsert_data("fomc_minutes_raw", df, ["meeting_date"], context=context)

    return dg.MaterializeResult(
        metadata={
            "meeting_year": year,
            "minutes_count": len(df),
            "gcs_bucket": gcs.bucket_name,
            "gcs_prefix": f"fomc/{year}/",
            "first_10_rows": str(df.head(10)) if df.shape[0] > 0 else "No data",
        }
    )


fred_ingestion_job = dg.define_asset_job(
    name="fred_ingestion_job",
    tags={"dagster/priority": "10", "dagster/max_runtime": 1800},
    selection=dg.AssetSelection.assets("fred_raw"),
    description="FRED data ingestion job - runs weekly on Sundays at 2 AM EST",
)

fomc_minutes_ingestion_job = dg.define_asset_job(
    name="fomc_minutes_ingestion_job",
    tags={
        "dagster/priority": "10",
        "dagster/max_runtime": 1800,
        "job_type": "daily_ingestion",
    },
    selection=dg.AssetSelection.assets("fomc_minutes_raw"),
    description="FOMC minutes ingestion job - runs daily at 2:15 AM EST",
)

treasury_yields_ingestion_job = dg.define_asset_job(
    name="treasury_yields_ingestion_job",
    tags={"dagster/priority": "10", "dagster/max_runtime": 1800},
    selection=dg.AssetSelection.assets("treasury_yields_raw"),
    description="Treasury Yields ingestion job - runs current year partition weekly on Sundays",
)


@dg.sensor(
    name="treasury_yields_ingestion_schedule",
    job=treasury_yields_ingestion_job,
    description="Weekly schedule for Treasury Yields - runs current year partition every Sunday at 3 AM EST",
    minimum_interval_seconds=3600,
)
def treasury_yields_sensor(context: dg.SensorEvaluationContext):
    now = datetime.now(pytz.timezone("America/New_York"))
    if now.weekday() != 6:
        return

    current_year = str(now.year)

    yield dg.RunRequest(
        run_key=f"treasury_yields_{current_year}_{now.strftime('%Y%m%d')}",
        partition_key=current_year,
        tags={
            "trigger": "weekly_schedule",
            "year": current_year,
        },
    )


@dg.schedule(
    name="weekly_fred_ingestion_schedule",
    cron_schedule="0 2 * * 0",
    execution_timezone="America/New_York",
    job=fred_ingestion_job,
    description="Weekly FRED data ingestion on Sundays at 2 AM EST - generates run requests for all series partitions",
)
def weekly_fred_ingestion_schedule(context: dg.ScheduleEvaluationContext):
    scheduled_time = context.scheduled_execution_time or datetime.now(
        pytz.timezone("America/New_York")
    )
    run_requests = []
    for series_code in _all_series_codes:
        run_requests.append(
            dg.RunRequest(
                run_key=f"fred_{series_code}_{scheduled_time.strftime('%Y%m%d')}",
                partition_key=series_code,
                tags={
                    "trigger": "weekly_schedule",
                    "series_code": series_code,
                },
            )
        )
    return run_requests


daily_fomc_minutes_schedule = dg.ScheduleDefinition(
    name="daily_fomc_minutes_schedule",
    cron_schedule="15 2 * * *",
    execution_timezone="America/New_York",
    job=fomc_minutes_ingestion_job,
    description="Daily FOMC minutes check at 2:15 AM EST",
)

defs = dg.Definitions(
    assets=[fred_raw, treasury_yields_raw, fomc_minutes_raw],
    asset_checks=macro_checks,
    jobs=[
        fred_ingestion_job,
        fomc_minutes_ingestion_job,
        treasury_yields_ingestion_job,
    ],
    schedules=[weekly_fred_ingestion_schedule, daily_fomc_minutes_schedule],
    sensors=[treasury_yields_sensor],
    resources={
        "fred": fred_resource,
    },
)
