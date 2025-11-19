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

monthly_economic_analysis_schedule_skeptical = ScheduleDefinition(
    name="monthly_economic_analysis_schedule_skeptical",
    cron_schedule="0 9 1-7 * 0",
    execution_timezone="America/New_York",
    job_name="monthly_economic_analysis_job_skeptical",
    description="Monthly economic analysis pipeline (skeptical/bearish) on first Sunday of each month at 9 AM EST",
    run_config={
        "ops": {
            "analyze_economy_state": {
                "config": {
                    "personality": "skeptical",
                }
            },
            "generate_investment_recommendations": {
                "config": {
                    "personality": "skeptical",
                }
            },
        }
    },
)

monthly_economic_analysis_schedule_neutral = ScheduleDefinition(
    name="monthly_economic_analysis_schedule_neutral",
    cron_schedule="0 10 1-7 * 0",
    execution_timezone="America/New_York",
    job_name="monthly_economic_analysis_job_neutral",
    description="Monthly economic analysis pipeline (neutral) on first Sunday of each month at 10 AM EST",
    run_config={
        "ops": {
            "analyze_economy_state": {
                "config": {
                    "personality": "neutral",
                }
            },
            "generate_investment_recommendations": {
                "config": {
                    "personality": "neutral",
                }
            },
        }
    },
)

monthly_economic_analysis_schedule_bullish = ScheduleDefinition(
    name="monthly_economic_analysis_schedule_bullish",
    cron_schedule="0 11 1-7 * 0",
    execution_timezone="America/New_York",
    job_name="monthly_economic_analysis_job_bullish",
    description="Monthly economic analysis pipeline (bullish/optimistic) on first Sunday of each month at 11 AM EST",
    run_config={
        "ops": {
            "analyze_economy_state": {
                "config": {
                    "personality": "bullish",
                }
            },
            "generate_investment_recommendations": {
                "config": {
                    "personality": "bullish",
                }
            },
        }
    },
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


def create_backtesting_model_sensor(model_name: str = "gpt-4-turbo-preview"):
    """
    Create a sensor that triggers backtesting job for each month from 2018 to 1 year ago.

    Args:
        model_name: LLM model to use for backtesting (e.g., 'gpt-4-turbo-preview', 'gpt-4o', 'gpt-3.5-turbo')

    Returns:
        Sensor that generates RunRequests for each month
    """
    # Sanitize model_name for sensor name (replace dots and dashes with underscores)
    sensor_name_suffix = model_name.replace(".", "_").replace("-", "_")

    @dg.sensor(
        name=f"backtesting_model_sensor_{sensor_name_suffix}",
        description=f"Triggers backtesting job for each month from 2018-01-01 to 1 year ago with model {model_name}",
        default_status=DefaultSensorStatus.STOPPED,  # Start stopped, enable manually
        minimum_interval_seconds=3600,
        job_name="backtesting_model_run",
    )
    def backtesting_model_sensor(context):
        """Sensor that generates backtest runs for each month."""
        now = datetime.now(pytz.timezone("America/New_York"))
        one_year_ago = now - relativedelta(years=1)

        # Start from 2018-01-01
        start_date = datetime(2018, 1, 1, tzinfo=pytz.timezone("America/New_York"))

        # Generate monthly dates (first day of each month)
        current_date = start_date
        dates = []
        while current_date <= one_year_ago:
            dates.append(current_date.strftime("%Y-%m-%d"))
            current_date += relativedelta(months=1)

        context.log.info(
            f"Generating {len(dates)} backtest runs for model {model_name} "
            f"from {dates[0]} to {dates[-1]}"
        )

        # Generate RunRequest for each date
        for backtest_date in dates:
            yield dg.RunRequest(
                run_key=f"backtest-{model_name}-{backtest_date}",
                run_config={
                    "ops": {
                        "backtest_analyze_economy_state": {
                            "config": {
                                "backtest_date": backtest_date,
                                "model_name": model_name,
                            }
                        },
                        "backtest_analyze_asset_class_relationships": {
                            "config": {
                                "backtest_date": backtest_date,
                                "model_name": model_name,
                            }
                        },
                        "backtest_generate_investment_recommendations": {
                            "config": {
                                "backtest_date": backtest_date,
                                "model_name": model_name,
                            }
                        },
                        "evaluate_backtest_recommendations": {
                            "config": {
                                "backtest_date": backtest_date,
                                "model_name": model_name,
                            }
                        },
                    }
                },
                tags={
                    "trigger": "backtesting_model_sensor",
                    "model_name": model_name,
                    "backtest_date": backtest_date,
                },
            )

    return backtesting_model_sensor


def create_scheduled_jobs():
    """Create job definitions for scheduled execution."""

    weekly_replication_job = dg.define_asset_job(
        name="weekly_replication_job",
        selection=dg.AssetSelection.groups("replication"),
        description="Weekly replication job for syncing MotherDuck tables to BigQuery",
    )

    # Create three separate jobs for different analytical personalities
    # Config with personality should be provided at runtime when materializing
    monthly_economic_analysis_job_skeptical = dg.define_asset_job(
        name="monthly_economic_analysis_job_skeptical",
        selection=[
            "analyze_economy_state",
            "analyze_asset_class_relationships",
            "generate_investment_recommendations",
        ],
        description="Monthly economic analysis pipeline (skeptical/bearish personality): economy state -> asset relationships -> recommendations. Provide config with personality='skeptical' at runtime.",
    )

    monthly_economic_analysis_job_neutral = dg.define_asset_job(
        name="monthly_economic_analysis_job_neutral",
        selection=[
            "analyze_economy_state",
            "analyze_asset_class_relationships",
            "generate_investment_recommendations",
        ],
        description="Monthly economic analysis pipeline (neutral personality): economy state -> asset relationships -> recommendations. Provide config with personality='neutral' at runtime.",
    )

    monthly_economic_analysis_job_bullish = dg.define_asset_job(
        name="monthly_economic_analysis_job_bullish",
        selection=[
            "analyze_economy_state",
            "analyze_asset_class_relationships",
            "generate_investment_recommendations",
        ],
        description="Monthly economic analysis pipeline (bullish/optimistic personality): economy state -> asset relationships -> recommendations. Provide config with personality='bullish' at runtime.",
    )

    backtesting_job = dg.define_asset_job(
        name="backtesting_job",
        selection=dg.AssetSelection.groups("backtesting"),
        description="Backtesting pipeline: economy state -> asset relationships -> recommendations -> evaluation. Requires config with 'backtest_date' (YYYY-MM-DD), optional 'model_name' (default: gpt-4-turbo-preview), and optional 'personality' (default: skeptical) for each asset. Provide config at runtime.",
    )

    # Monthly partitioned backtesting job - runs from 2018 to 1 year ago
    # Note: Partitioning is inferred from selected assets, so partitions_def is not needed
    backtesting_model_run = dg.define_asset_job(
        name="backtesting_model_run",
        selection=dg.AssetSelection.groups("backtesting"),
        description="Monthly partitioned backtesting job: runs backtests for each month from 2018-01-01 to 1 year ago. Provide 'model_name' as config parameter when materializing. The backtest_date should be provided in config for each asset.",
    )

    return {
        "weekly_replication_job": weekly_replication_job,
        "monthly_economic_analysis_job_skeptical": monthly_economic_analysis_job_skeptical,
        "monthly_economic_analysis_job_neutral": monthly_economic_analysis_job_neutral,
        "monthly_economic_analysis_job_bullish": monthly_economic_analysis_job_bullish,
        "backtesting_job": backtesting_job,
        "backtesting_model_run": backtesting_model_run,
    }


schedules = [
    weekly_replication_schedule,
    monthly_economic_analysis_schedule_skeptical,
    monthly_economic_analysis_schedule_neutral,
    monthly_economic_analysis_schedule_bullish,
]

# Create sensors for different models
backtesting_gpt4_sensor = create_backtesting_model_sensor("gpt-4-turbo-preview")
backtesting_gpt4o_sensor = create_backtesting_model_sensor("gpt-4o")
backtesting_gpt35_sensor = create_backtesting_model_sensor("gpt-3.5-turbo")

sensors = [
    create_ingestion_sensor(),
    backtesting_gpt4_sensor,
    backtesting_gpt4o_sensor,
    backtesting_gpt35_sensor,
]

jobs = create_scheduled_jobs()
