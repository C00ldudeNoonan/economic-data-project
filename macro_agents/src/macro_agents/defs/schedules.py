import dagster as dg
from dagster import ScheduleDefinition, DefaultSensorStatus
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pytz


weekly_replication_schedule = ScheduleDefinition(
    name="weekly_replication_schedule",
    cron_schedule="0 2 * * 0",
    execution_timezone="America/New_York",
    job_name="weekly_replication_job",
    description="Weekly replication of tables from MotherDuck to BigQuery every Sunday at 2 AM EST",
)

monthly_economic_analysis_schedule = ScheduleDefinition(
    name="monthly_economic_analysis_schedule",
    cron_schedule="0 9 1-7 * 0",
    execution_timezone="America/New_York",
    job_name="monthly_economic_analysis_job",
    description="Monthly economic analysis pipeline on first Sunday of each month at 9 AM EST (runs on Sundays in days 1-7, which always includes the first Sunday)",
)


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

    weekly_replication_job = dg.define_asset_job(
        name="weekly_replication_job",
        selection=dg.AssetSelection.groups("replication"),
        description="Weekly replication job for syncing MotherDuck tables to BigQuery",
    )

    monthly_economic_analysis_job = dg.define_asset_job(
        name="monthly_economic_analysis_job",
        selection=[
            "analyze_economy_state",
            "analyze_asset_class_relationships",
            "generate_investment_recommendations",
        ],
        description="Monthly economic analysis pipeline: economy state -> asset relationships -> recommendations",
    )

    # Note: weekly_ingestion_job is not defined here because ingestion assets have
    # incompatible partition definitions. Instead, use the weekly_ingestion_sensor
    # which handles partition selection dynamically.

    return {
        "weekly_replication_job": weekly_replication_job,
        "monthly_economic_analysis_job": monthly_economic_analysis_job,
    }


schedules = [
    weekly_replication_schedule,
    monthly_economic_analysis_schedule,
]

sensors = [create_ingestion_sensor()]

jobs = create_scheduled_jobs()
