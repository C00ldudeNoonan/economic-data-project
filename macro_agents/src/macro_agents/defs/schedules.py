"""
Scheduled execution configuration for economic analysis assets.
"""

import dagster as dg
from dagster import ScheduleDefinition, DefaultSensorStatus
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pytz


monthly_sector_analysis_schedule = ScheduleDefinition(
    name="monthly_sector_analysis_schedule",
    cron_schedule="0 9 1 * *",
    execution_timezone="America/New_York",
    job_name="monthly_sector_analysis_job",
    description="Monthly sector inflation analysis on the 1st of each month at 9 AM EST",
)

weekly_cycle_analysis_schedule = ScheduleDefinition(
    name="weekly_cycle_analysis_schedule",
    cron_schedule="0 8 * * 1",
    execution_timezone="America/New_York",
    job_name="weekly_cycle_analysis_job",
    description="Weekly economic cycle analysis every Monday at 8 AM EST",
)

weekly_allocation_schedule = ScheduleDefinition(
    name="weekly_allocation_schedule",
    cron_schedule="0 9 * * 1",
    execution_timezone="America/New_York",
    job_name="weekly_allocation_job",
    description="Weekly asset allocation recommendations every Monday at 9 AM EST",
)

daily_monitoring_schedule = ScheduleDefinition(
    name="daily_monitoring_schedule",
    cron_schedule="0 6 * * 1-5",
    execution_timezone="America/New_York",
    job_name="daily_monitoring_job",
    description="Daily economic monitoring and alerts on weekdays at 6 AM EST",
)

weekly_replication_schedule = ScheduleDefinition(
    name="weekly_replication_schedule",
    cron_schedule="0 2 * * 0",
    execution_timezone="America/New_York",
    job_name="weekly_replication_job",
    description="Weekly replication of tables from MotherDuck to BigQuery every Sunday at 2 AM EST",
)

# Note: weekly_ingestion_schedule removed because ingestion assets have incompatible
# partition definitions. Use weekly_ingestion_sensor instead, which handles partition
# selection dynamically for different asset types.


def create_data_freshness_sensor():
    """Create a sensor that triggers analysis based on data freshness."""

    @dg.sensor(
        name="data_freshness_sensor",
        description="Triggers analysis when new economic data is available",
        default_status=DefaultSensorStatus.RUNNING,
    )
    def data_freshness_sensor(context):
        """Sensor that checks for fresh economic data and triggers appropriate analysis."""
        current_time = datetime.now(pytz.timezone("America/New_York"))

        if current_time.weekday() == 0 and current_time.hour == 8:
            yield dg.RunRequest(
                run_key=f"weekly_cycle_analysis_{current_time.strftime('%Y%m%d_%H%M')}",
                tags={"trigger": "weekly_schedule", "analysis_type": "cycle"},
            )

        if current_time.day == 1 and current_time.hour == 9:
            yield dg.RunRequest(
                run_key=f"monthly_sector_analysis_{current_time.strftime('%Y%m%d_%H%M')}",
                tags={"trigger": "monthly_schedule", "analysis_type": "sector"},
            )

    return data_freshness_sensor


def create_ingestion_sensor():
    """Create a sensor that triggers ingestion job with appropriate partition selection for market_stack assets."""

    @dg.sensor(
        name="weekly_ingestion_sensor",
        description="Triggers ingestion job weekly, with dynamic partition selection for market_stack assets (current month or previous month if within first 5 days)",
        default_status=DefaultSensorStatus.RUNNING,
        minimum_interval_seconds=3600,
    )
    def weekly_ingestion_sensor(context):
        """Sensor that triggers ingestion job with partition filtering for market_stack assets."""
        from macro_agents.defs.ingestion.market_stack import (
            us_sector_etfs_partitions,
            currency_etfs_partitions,
            major_indices_tickers_partitions,
            fixed_income_etfs_partitions,
            global_markets_partitions,
            energy_commodities_partitions,
            input_commodities_partitions,
            agriculture_commodities_partitions,
        )
        from macro_agents.defs.constants.market_stack_constants import (
            US_SECTOR_ETFS,
            CURRENCY_ETFS,
            MAJOR_INDICES_TICKERS,
            FIXED_INCOME_ETFS,
            GLOBAL_MARKETS,
            ENERGY_COMMODITIES,
            INPUT_COMMODITIES,
            AGRICULTURE_COMMODITIES,
        )

        now = datetime.now(pytz.timezone("America/New_York"))
        current_month = now.strftime("%Y-%m")

        target_months = [current_month]
        if now.day <= 5:
            previous_month = (now - relativedelta(months=1)).strftime("%Y-%m")
            target_months.append(previous_month)

        if now.weekday() == 0:
            market_stack_configs = [
                (
                    "us_sector_etfs_raw",
                    us_sector_etfs_partitions,
                    US_SECTOR_ETFS,
                    "ticker",
                ),
                (
                    "currency_etfs_raw",
                    currency_etfs_partitions,
                    CURRENCY_ETFS,
                    "ticker",
                ),
                (
                    "major_indices_raw",
                    major_indices_tickers_partitions,
                    MAJOR_INDICES_TICKERS,
                    "ticker",
                ),
                (
                    "fixed_income_etfs_raw",
                    fixed_income_etfs_partitions,
                    FIXED_INCOME_ETFS,
                    "ticker",
                ),
                (
                    "global_markets_raw",
                    global_markets_partitions,
                    GLOBAL_MARKETS,
                    "ticker",
                ),
                (
                    "energy_commodities_raw",
                    energy_commodities_partitions,
                    ENERGY_COMMODITIES,
                    "commodity",
                ),
                (
                    "input_commodities_raw",
                    input_commodities_partitions,
                    INPUT_COMMODITIES,
                    "commodity",
                ),
                (
                    "agriculture_commodities_raw",
                    agriculture_commodities_partitions,
                    AGRICULTURE_COMMODITIES,
                    "commodity",
                ),
            ]

            for (
                asset_name,
                partition_def,
                static_keys,
                static_dimension_name,
            ) in market_stack_configs:
                partition_keys = []
                for static_key in static_keys:
                    for month in target_months:
                        partition_key = dg.MultiPartitionKey(
                            {
                                static_dimension_name: static_key,
                                "date": month,
                            }
                        )
                        partition_keys.append(partition_key)

                for partition_key in partition_keys:
                    yield dg.MaterializeRequest(
                        asset_key=asset_name,
                        partition_key=partition_key,
                        tags={
                            "trigger": "weekly_schedule",
                            "asset": asset_name,
                            "target_months": ",".join(target_months),
                        },
                    )

            # Materialize non-market_stack ingestion assets (FRED, BLS, treasury_yields, housing)
            # These don't have compatible partitions, so materialize them individually
            non_market_stack_assets = [
                "fred_raw",
                "bls_raw", 
                "treasury_yields_raw",
                "housing_inventory_raw",
                "housing_pulse_raw",
            ]
            for asset_name in non_market_stack_assets:
                yield dg.MaterializeRequest(
                    asset_key=asset_name,
                    tags={
                        "trigger": "weekly_schedule",
                        "asset": asset_name,
                    },
                )

    return weekly_ingestion_sensor


def create_scheduled_jobs():
    """Create job definitions for scheduled execution."""

    monthly_sector_job = dg.define_asset_job(
        name="monthly_sector_analysis_job",
        selection=["sector_inflation_analysis", "sector_inflation_specific_analysis"],
        description="Monthly sector inflation analysis job",
    )

    weekly_cycle_job = dg.define_asset_job(
        name="weekly_cycle_analysis_job",
        selection=["economic_cycle_analysis"],
        description="Weekly economic cycle analysis job",
    )

    weekly_allocation_job = dg.define_asset_job(
        name="weekly_allocation_job",
        selection=["asset_allocation_recommendations", "custom_asset_allocation"],
        description="Weekly asset allocation recommendations job",
    )

    daily_monitoring_job = dg.define_asset_job(
        name="daily_monitoring_job",
        selection=["economic_monitoring_alerts"],
        description="Daily economic monitoring job",
    )

    weekly_replication_job = dg.define_asset_job(
        name="weekly_replication_job",
        selection=dg.AssetSelection.groups("replication"),
        description="Weekly replication job for syncing MotherDuck tables to BigQuery",
    )

    # Note: weekly_ingestion_job is not defined here because ingestion assets have
    # incompatible partition definitions. Instead, use the weekly_ingestion_sensor
    # which handles partition selection dynamically.

    return {
        "monthly_sector_job": monthly_sector_job,
        "weekly_cycle_job": weekly_cycle_job,
        "weekly_allocation_job": weekly_allocation_job,
        "daily_monitoring_job": daily_monitoring_job,
        "weekly_replication_job": weekly_replication_job,
    }


schedules = [
    monthly_sector_analysis_schedule,
    weekly_cycle_analysis_schedule,
    weekly_allocation_schedule,
    daily_monitoring_schedule,
    weekly_replication_schedule,
]

sensors = [create_data_freshness_sensor(), create_ingestion_sensor()]

jobs = create_scheduled_jobs()
