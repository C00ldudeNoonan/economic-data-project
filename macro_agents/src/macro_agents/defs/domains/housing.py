"""Housing-related ingestion assets."""

import io
import json
import os
from datetime import datetime
from pathlib import Path

import dagster as dg
import polars as pl
import pytz
import requests
from pydantic import Field

from macro_agents.defs.resources._url_secrets import get_safe
from macro_agents.defs.resources.google_drive import GoogleDriveResource
from macro_agents.defs.resources.bigquery_warehouse import BigQueryWarehouseResource
from macro_agents.defs.domains.housing_checks import housing_checks

HOUSING_GROUP = "housing_ingestion"

census_api_key = os.getenv("CENSUS_API_KEY")


class HousingInventoryConfig(dg.Config):
    year: str = Field(
        default_factory=lambda: str(datetime.now().year),
        description="Year to fetch housing inventory data for",
    )


@dg.asset(
    group_name=HOUSING_GROUP,
    kinds={"polars", "duckdb"},
    description="Raw data from BLS API for housing inventory - runs weekly on Sundays at 3 AM EST for current year",
)
def housing_inventory_raw(
    context: dg.AssetExecutionContext,
    bq: BigQueryWarehouseResource,
    config: HousingInventoryConfig,
) -> dg.MaterializeResult:
    year = config.year

    response = get_safe(
        "https://api.census.gov/data/timeseries/eits/hv",
        params={
            "get": "data_type_code,time_slot_id,seasonally_adj,category_code,cell_value,error_data",
            "for": "us:*",
            "time": year,
            "key": census_api_key,
        },
    )
    if response.status_code != 200:
        context.log.error(
            f"Failed to fetch housing inventory data: {response.status_code}"
        )
        context.log.error(f"Response: {response.text[:200]}")
        raise Exception(
            f"Failed to fetch housing inventory data: {response.status_code}"
        )

    try:
        data = response.json()
    except json.JSONDecodeError:
        context.log.warning(
            f"Census API returned non-JSON response for year {year}: {response.text[:200]}"
        )
        return dg.MaterializeResult(
            metadata={
                "year": year,
                "num_records": 0,
                "skipped": "Non-JSON API response",
            }
        )

    if not isinstance(data, list) or len(data) < 2:
        context.log.warning(
            f"Census API returned empty or unexpected response for year {year}: {str(data)[:200]}"
        )
        return dg.MaterializeResult(
            metadata={"year": year, "num_records": 0, "skipped": "Empty API response"}
        )

    columns = data[0]
    rows = data[1:]

    df = pl.DataFrame(rows, schema=columns, orient="row")
    df = df.with_columns(pl.lit(year).alias("year"))
    context.log.info(f"Columns: {df.columns}")

    bq.upsert_data("housing_inventory_raw", df, ["year"])

    return dg.MaterializeResult(
        metadata={
            "year": year,
            "num_records": len(df),
            "columns": df.columns,
            "first_10_rows": str(df.head(10)) if df.shape[0] > 0 else "No data",
        }
    )


@dg.asset(
    group_name=HOUSING_GROUP,
    kinds={"polars", "duckdb"},
    description="Raw data from BLS API for housing pulse - runs weekly on Sundays at 4 AM EST",
)
def housing_pulse_raw(
    context: dg.AssetExecutionContext, bq: BigQueryWarehouseResource
) -> dg.MaterializeResult:
    current_year = datetime.now().year
    main_df = pl.DataFrame()

    for cycle in range(1, datetime.now().month):
        cycle_str = f"0{cycle}" if cycle < 10 else str(cycle)
        try:
            response = get_safe(
                "https://api.census.gov/data/timeseries/hhpulse",
                params={
                    "get": "SURVEY_YEAR,NAME,MEASURE_NAME,COL_START_DATE,COL_END_DATE,RATE,TOTAL,MEASURE_DESCRIPTION",
                    "for": "state:*",
                    "time": current_year,
                    "CYCLE": cycle_str,
                    "key": census_api_key,
                },
            )
        except requests.RequestException as e:
            context.log.warning(f"Request failed for cycle {cycle}: {e}")
            break

        if response.status_code != 200:
            context.log.warning(
                f"Census API returned {response.status_code} for cycle {cycle}, stopping"
            )
            break

        try:
            data = response.json()
        except json.JSONDecodeError:
            context.log.warning(
                f"Census API returned non-JSON response for cycle {cycle}: {response.text[:200]}"
            )
            break

        if not isinstance(data, list) or len(data) < 2:
            context.log.warning(f"No data for cycle {cycle}, stopping")
            break

        columns = data[0]
        rows = data[1:]
        df = pl.DataFrame(rows, schema=columns, orient="row")
        main_df = pl.concat([main_df, df])

    if main_df.is_empty():
        context.log.warning("No housing pulse data collected from any cycle")
        return dg.MaterializeResult(
            metadata={"num_records": 0, "skipped": "No data from Census API"}
        )

    bq.upsert_data("housing_pulse_raw", main_df, ["COL_START_DATE"])

    return dg.MaterializeResult(
        metadata={
            "num_records": len(main_df),
            "columns": main_df.columns,
            "first_10_rows": str(main_df.head(10))
            if main_df.shape[0] > 0
            else "No data",
        }
    )


realtor_partition = dg.StaticPartitionsDefinition(
    ["country", "state", "metro", "county", "zip"]
)


@dg.asset(
    group_name=HOUSING_GROUP,
    kinds={"polars", "duckdb"},
    partitions_def=realtor_partition,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=10),
    description="Raw monthly housing data from Realtor.com via Google Drive",
)
def realtor_raw(
    context: dg.AssetExecutionContext,
    gdrive: GoogleDriveResource,
    bq: BigQueryWarehouseResource,
) -> dg.MaterializeResult:
    """
    Ingest Realtor.com housing data from Google Drive CSV files.

    Each CSV file is loaded directly into memory (no local download) and written to
    a MotherDuck table with a name matching the file name (without .csv extension).
    """
    all_files = gdrive.list_files_in_folder(file_extension="csv", context=context)
    total_files_processed = 0

    for geo_level in context.partition_keys:
        context.log.info(
            f"Processing Realtor.com data for geographic level: {geo_level}"
        )

        files = [f for f in all_files if geo_level in f["name"].lower()]

        if not files:
            context.log.warning(f"No CSV files found with '{geo_level}' in filename")
            continue

        context.log.info(
            f"Found {len(files)} CSV file(s) with '{geo_level}' in filename"
        )

        for file_info in files:
            file_id = file_info["id"]
            file_name = file_info["name"]

            file_content = gdrive.get_file_content(file_id, context=context)
            df = pl.read_csv(io.BytesIO(file_content))

            table_name = Path(file_name).stem
            context.log.info(f"Writing {len(df)} rows to table: {table_name}")
            bq.upsert_data(table_name, df, ["month_date_yyyymm"], context=context)
            total_files_processed += 1

    return dg.MaterializeResult(
        metadata={
            "total_files_processed": total_files_processed,
            "partitions_processed": len(context.partition_keys),
        },
    )


housing_inventory_job = dg.define_asset_job(
    name="housing_inventory_job",
    tags={"dagster/priority": "10", "dagster/max_runtime": 600},
    selection=dg.AssetSelection.assets("housing_inventory_raw"),
    description="Housing inventory ingestion job - runs weekly on Sundays at 3 AM EST",
)

housing_pulse_job = dg.define_asset_job(
    name="housing_pulse_job",
    tags={"dagster/priority": "10", "dagster/max_runtime": 600},
    selection=dg.AssetSelection.assets("housing_pulse_raw"),
    description="Housing pulse ingestion job - runs weekly on Sundays at 4 AM EST",
)

realtor_ingestion_job = dg.define_asset_job(
    name="realtor_ingestion_job",
    tags={"dagster/priority": "10", "dagster/max_runtime": 1800},
    selection=dg.AssetSelection.assets("realtor_raw"),
    description="Realtor.com data ingestion job - triggered by Google Drive sensor when new files are detected",
)


@dg.sensor(
    name="realtor_gdrive_file_monitor",
    description="Monitors Google Drive folder for new Realtor.com CSV files and triggers ingestion",
    minimum_interval_seconds=60,  # Check every minute
    required_resource_keys={"gdrive"},
    job=realtor_ingestion_job,
)
def realtor_gdrive_sensor(context: dg.SensorEvaluationContext):
    """
    Monitor Google Drive folder for new or updated Realtor.com CSV files.

    This sensor checks all CSV files in the main folder and categorizes them by
    geographic level based on the filename containing: country, state, metro, county, or zip.

    Triggers the realtor_raw asset partition if:
    1. Any file has been modified since the last check, OR
    2. A new file is detected, OR
    3. The partition has never been materialized (no cursor exists)

    The sensor uses a cursor to track the modification time of each individual file,
    structured as:
    {
        "country": {"file1.csv": "2024-01-15T...", "file2.csv": "2024-01-10T..."},
        "state": {"file3.csv": "2024-01-12T..."},
        ...
    }
    """

    gdrive: GoogleDriveResource = context.resources.gdrive
    gdrive.setup_for_execution(context)

    geo_levels = ["country", "state", "metro", "county", "zip"]

    # Parse cursor - now tracks individual file modification times per geo_level
    cursor = context.cursor
    file_timestamps: dict[str, dict[str, str]] = {level: {} for level in geo_levels}

    if cursor:
        stripped_cursor = cursor.strip()
        if stripped_cursor and stripped_cursor.startswith("{"):
            try:
                parsed_cursor = json.loads(cursor)
                # Handle migration from old cursor format (geo_level -> single timestamp)
                # to new format (geo_level -> {filename: timestamp})
                for geo_level in geo_levels:
                    value = parsed_cursor.get(geo_level)
                    if isinstance(value, dict):
                        # New format
                        file_timestamps[geo_level] = value
                    elif isinstance(value, str):
                        # Old format - will trigger re-detection of all files
                        context.log.info(
                            f"Migrating cursor for '{geo_level}' from old format"
                        )
                        file_timestamps[geo_level] = {}
            except json.JSONDecodeError:
                context.log.warning("Cursor is not valid JSON, starting fresh")
        else:
            context.log.warning("Cursor is not valid JSON format, starting fresh")

    # Get all CSV files from the main folder
    all_files = gdrive.list_files_in_folder(file_extension="csv", context=context)

    # Categorize files by geo_level based on filename
    files_by_category: dict[str, list] = {level: [] for level in geo_levels}

    for file_info in all_files:
        file_name_lower = file_info["name"].lower()
        for geo_level in geo_levels:
            if geo_level in file_name_lower:
                files_by_category[geo_level].append(file_info)
                break  # Each file belongs to only one category

    context.log.info(
        f"Found {len(all_files)} CSV files, categorized: "
        + ", ".join(f"{k}: {len(v)}" for k, v in files_by_category.items())
    )

    # Track new file timestamps for cursor update
    new_file_timestamps: dict[str, dict[str, str]] = {level: {} for level in geo_levels}

    for geo_level in geo_levels:
        files = files_by_category.get(geo_level, [])
        last_file_times = file_timestamps.get(geo_level, {})

        if not files:
            context.log.debug(
                f"No CSV files found with '{geo_level}' in filename, skipping"
            )
            # Preserve existing timestamps for this geo_level
            new_file_timestamps[geo_level] = last_file_times
            continue

        updated_files = []

        for file_info in files:
            file_name = file_info["name"]
            file_modified = file_info["modifiedTime"]

            # Always update the timestamp in the new cursor
            new_file_timestamps[geo_level][file_name] = file_modified

            # Check if this is a new file or has been modified
            last_modified = last_file_times.get(file_name)

            if last_modified is None:
                # New file
                updated_files.append(
                    {
                        "name": file_name,
                        "modified": file_modified,
                        "reason": "new_file",
                    }
                )
                context.log.info(
                    f"New file detected in '{geo_level}': {file_name} "
                    f"(modified: {file_modified})"
                )
            elif file_modified != last_modified:
                # File has been modified (different timestamp)
                updated_files.append(
                    {
                        "name": file_name,
                        "modified": file_modified,
                        "reason": "file_updated",
                    }
                )
                context.log.info(
                    f"Updated file detected in '{geo_level}': {file_name} "
                    f"(previous: {last_modified}, current: {file_modified})"
                )

        if updated_files:
            # Determine trigger reason (prefer "new_file" if any new files)
            has_new_files = any(f["reason"] == "new_file" for f in updated_files)
            trigger_reason = "new_file_detected" if has_new_files else "file_updated"

            file_names = ", ".join(f["name"] for f in updated_files)
            max_modified_time = max(f["modified"] for f in updated_files)
            run_key = (
                f"realtor_{geo_level}_{max_modified_time}_"
                f"{datetime.now(pytz.timezone('America/New_York')).strftime('%Y%m%d%H%M%S')}"
            )

            yield dg.RunRequest(
                run_key=run_key,
                asset_selection=[dg.AssetKey("realtor_raw")],
                partition_key=geo_level,
                tags={
                    "trigger": trigger_reason,
                    "geo_level": geo_level,
                    "file_names": file_names,
                    "file_count": str(len(updated_files)),
                    "max_modified": max_modified_time,
                    "source": "google_drive",
                },
            )
        else:
            context.log.debug(
                f"No changes in '{geo_level}': checked {len(files)} files"
            )

    context.update_cursor(json.dumps(new_file_timestamps))


weekly_housing_inventory_schedule = dg.ScheduleDefinition(
    name="weekly_housing_inventory_schedule",
    cron_schedule="0 3 * * 0",
    execution_timezone="America/New_York",
    job=housing_inventory_job,
    description="Weekly housing inventory ingestion on Sundays at 3 AM EST",
)

weekly_housing_pulse_schedule = dg.ScheduleDefinition(
    name="weekly_housing_pulse_schedule",
    cron_schedule="30 3 * * 0",
    execution_timezone="America/New_York",
    job=housing_pulse_job,
    description="Weekly housing pulse ingestion on Sundays at 3:30 AM EST",
)

defs = dg.Definitions(
    assets=[housing_inventory_raw, housing_pulse_raw, realtor_raw],
    asset_checks=housing_checks,
    jobs=[housing_inventory_job, housing_pulse_job, realtor_ingestion_job],
    schedules=[weekly_housing_inventory_schedule, weekly_housing_pulse_schedule],
    sensors=[realtor_gdrive_sensor],
    resources={
        "gdrive": GoogleDriveResource(
            credentials_path=dg.EnvVar("GOOGLE_APPLICATION_CREDENTIALS"),
            folder_id="1_-IXxcEbBzMk1p7TygEqIBOnaZBh0gei",
        ),
    },
)
