import dagster as dg
from dagster import ScheduleDefinition, DefaultSensorStatus
from datetime import datetime
import pytz
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


def create_scheduled_jobs():
    """Create job definitions for scheduled execution."""

    us_sector_etfs_ingestion_job = dg.define_asset_job(
        name="us_sector_etfs_ingestion_job",
        selection=dg.AssetSelection.assets("us_sector_etfs_raw"),
        description="US Sector ETFs ingestion job - runs current month partition weekly on Sundays",
    )

    currency_etfs_ingestion_job = dg.define_asset_job(
        name="currency_etfs_ingestion_job",
        selection=dg.AssetSelection.assets("currency_etfs_raw"),
        description="Currency ETFs ingestion job - runs current month partition weekly on Sundays",
    )

    major_indices_ingestion_job = dg.define_asset_job(
        name="major_indices_ingestion_job",
        selection=dg.AssetSelection.assets("major_indices_raw"),
        description="Major Indices ingestion job - runs current month partition weekly on Sundays",
    )

    fixed_income_etfs_ingestion_job = dg.define_asset_job(
        name="fixed_income_etfs_ingestion_job",
        selection=dg.AssetSelection.assets("fixed_income_etfs_raw"),
        description="Fixed Income ETFs ingestion job - runs current month partition weekly on Sundays",
    )

    global_markets_ingestion_job = dg.define_asset_job(
        name="global_markets_ingestion_job",
        selection=dg.AssetSelection.assets("global_markets_raw"),
        description="Global Markets ingestion job - runs current month partition weekly on Sundays",
    )

    energy_commodities_ingestion_job = dg.define_asset_job(
        name="energy_commodities_ingestion_job",
        selection=dg.AssetSelection.assets("energy_commodities_raw"),
        description="Energy Commodities ingestion job - runs current month partition weekly on Sundays",
    )

    input_commodities_ingestion_job = dg.define_asset_job(
        name="input_commodities_ingestion_job",
        selection=dg.AssetSelection.assets("input_commodities_raw"),
        description="Input Commodities ingestion job - runs current month partition weekly on Sundays",
    )

    agriculture_commodities_ingestion_job = dg.define_asset_job(
        name="agriculture_commodities_ingestion_job",
        selection=dg.AssetSelection.assets("agriculture_commodities_raw"),
        description="Agriculture Commodities ingestion job - runs current month partition weekly on Sundays",
    )

    treasury_yields_ingestion_job = dg.define_asset_job(
        name="treasury_yields_ingestion_job",
        selection=dg.AssetSelection.assets("treasury_yields_raw"),
        description="Treasury Yields ingestion job - runs current year partition weekly on Sundays",
    )

    weekly_replication_job = dg.define_asset_job(
        name="weekly_replication_job",
        selection=dg.AssetSelection.groups("replication"),
        description="Weekly replication job for syncing MotherDuck tables to BigQuery",
    )

    monthly_economic_analysis_job_skeptical = dg.define_asset_job(
        name="monthly_economic_analysis_job_skeptical",
        selection=[
            "analyze_economy_state",
            "analyze_asset_class_relationships",
            "generate_investment_recommendations",
        ],
        description="""Monthly economic analysis pipeline (skeptical/bearish personality): economy state -> asset relationships -> recommendations.

        Configuration (provide at runtime via launchpad):
        - personality: 'skeptical' (required)
        - model_provider: Optional LLM provider override ('openai', 'anthropic', 'gemini'). Defaults to resource/env var.
        - model_name: Optional LLM model name override (e.g., 'gpt-4-turbo-preview', 'claude-3-5-haiku-20241022'). Defaults to resource/env var.

        Example config:
        ```yaml
        ops:
          analyze_economy_state:
            config:
              personality: "skeptical"
              model_provider: "anthropic"
              model_name: "claude-3-5-haiku-20241022"
          analyze_asset_class_relationships:
            config:
              model_provider: "anthropic"
              model_name: "claude-3-5-haiku-20241022"
          generate_investment_recommendations:
            config:
              personality: "skeptical"
              model_provider: "anthropic"
              model_name: "claude-3-5-haiku-20241022"
        ```""",
    )

    monthly_economic_analysis_job_neutral = dg.define_asset_job(
        name="monthly_economic_analysis_job_neutral",
        selection=[
            "analyze_economy_state",
            "analyze_asset_class_relationships",
            "generate_investment_recommendations",
        ],
        description="""Monthly economic analysis pipeline (neutral personality): economy state -> asset relationships -> recommendations.

        Configuration (provide at runtime via launchpad):
        - personality: 'neutral' (required)
        - model_provider: Optional LLM provider override ('openai', 'anthropic', 'gemini'). Defaults to resource/env var.
        - model_name: Optional LLM model name override (e.g., 'gpt-4-turbo-preview', 'claude-3-5-haiku-20241022'). Defaults to resource/env var.

        Example config:
        ```yaml
        ops:
          analyze_economy_state:
            config:
              personality: "neutral"
              model_provider: "openai"
              model_name: "gpt-4-turbo-preview"
          analyze_asset_class_relationships:
            config:
              model_provider: "openai"
              model_name: "gpt-4-turbo-preview"
          generate_investment_recommendations:
            config:
              personality: "neutral"
              model_provider: "openai"
              model_name: "gpt-4-turbo-preview"
        ```""",
    )

    monthly_economic_analysis_job_bullish = dg.define_asset_job(
        name="monthly_economic_analysis_job_bullish",
        selection=[
            "analyze_economy_state",
            "analyze_asset_class_relationships",
            "generate_investment_recommendations",
        ],
        description="""Monthly economic analysis pipeline (bullish/optimistic personality): economy state -> asset relationships -> recommendations.

        Configuration (provide at runtime via launchpad):
        - personality: 'bullish' (required)
        - model_provider: Optional LLM provider override ('openai', 'anthropic', 'gemini'). Defaults to resource/env var.
        - model_name: Optional LLM model name override (e.g., 'gpt-4-turbo-preview', 'claude-3-5-haiku-20241022'). Defaults to resource/env var.

        Example config:
        ```yaml
        ops:
          analyze_economy_state:
            config:
              personality: "bullish"
              model_provider: "openai"
              model_name: "gpt-4o"
          analyze_asset_class_relationships:
            config:
              model_provider: "openai"
              model_name: "gpt-4o"
          generate_investment_recommendations:
            config:
              personality: "bullish"
              model_provider: "openai"
              model_name: "gpt-4o"
        ```""",
    )

    dbt_models_job = dg.define_asset_job(
        name="dbt_models_job",
        selection=dg.AssetSelection.groups(
            "staging", "government", "markets", "commodities", "analysis"
        )
        - dg.AssetSelection.groups("backtesting"),
        description="Run all dbt models excluding backtesting models. This job should run before DSPy analysis jobs.",
    )

    dbt_backtesting_models_job = dg.define_asset_job(
        name="dbt_backtesting_models_job",
        selection=dg.AssetSelection.groups("backtesting")
        - dg.AssetSelection.assets(
            "backtest_analyze_economy_state",
            "backtest_analyze_asset_class_relationships",
            "backtest_generate_investment_recommendations",
            "evaluate_backtest_recommendations",
        ),
        description="Run only dbt backtesting snapshot models (excludes DSPy backtesting assets).",
    )

    dspy_analysis_job = dg.define_asset_job(
        name="dspy_analysis_job",
        selection=dg.AssetSelection.groups("economic_analysis"),
        description="""Run DSPy analysis assets (economy state, asset relationships, investment recommendations). Depends on dbt models.

        Configuration (provide at runtime via launchpad):
        - personality: 'skeptical', 'neutral', or 'bullish' (required for analyze_economy_state and generate_investment_recommendations)
        - model_provider: Optional LLM provider override ('openai', 'anthropic', 'gemini'). Defaults to resource/env var.
        - model_name: Optional LLM model name override (e.g., 'gpt-4-turbo-preview', 'claude-3-5-haiku-20241022'). Defaults to resource/env var.

        Example config:
        ```yaml
        ops:
          analyze_economy_state:
            config:
              personality: "neutral"
              model_provider: "openai"
              model_name: "gpt-4-turbo-preview"
          analyze_asset_class_relationships:
            config:
              model_provider: "openai"
              model_name: "gpt-4-turbo-preview"
          generate_investment_recommendations:
            config:
              personality: "neutral"
              model_provider: "openai"
              model_name: "gpt-4-turbo-preview"
        ```""",
    )

    backtesting_job = dg.define_asset_job(
        name="backtesting_job",
        selection=dg.AssetSelection.assets(
            "backtest_analyze_economy_state",
            "backtest_analyze_asset_class_relationships",
            "backtest_generate_investment_recommendations",
            "evaluate_backtest_recommendations",
        ),
        description="""Backtesting pipeline: economy state -> asset relationships -> recommendations -> evaluation.
        
        Note: This job only includes DSPy backtesting assets, not dbt backtesting models.

        Configuration:
        - Single date: Use 'backtest_date' (YYYY-MM-DD, first day of month)
        - Date range: Use 'backtest_date_start' and 'backtest_date_end' (YYYY-MM-DD, first day of month)
        - Optional: 'model_provider' (default: openai), 'model_name' (default: gpt-4-turbo-preview), 'personality' (default: skeptical)

        Example config for single date:
        ```yaml
        ops:
          backtest_analyze_economy_state:
            config:
              backtest_date: "2024-01-01"
              model_provider: "anthropic"
              model_name: "claude-3-5-haiku-20241022"
              personality: "skeptical"
          backtest_analyze_asset_class_relationships:
            config:
              backtest_date: "2024-01-01"
              model_provider: "anthropic"
              model_name: "claude-3-5-haiku-20241022"
          backtest_generate_investment_recommendations:
            config:
              backtest_date: "2024-01-01"
              model_provider: "anthropic"
              model_name: "claude-3-5-haiku-20241022"
              personality: "skeptical"
          evaluate_backtest_recommendations:
            config:
              backtest_date: "2024-01-01"
              model_provider: "anthropic"
              model_name: "claude-3-5-haiku-20241022"
        ```

        Example config for date range:
        ```yaml
        ops:
          backtest_analyze_economy_state:
            config:
              backtest_date_start: "2018-01-01"
              backtest_date_end: "2023-12-01"
              model_provider: "anthropic"
              model_name: "claude-3-5-haiku-20241022"
              personality: "skeptical"
          backtest_analyze_asset_class_relationships:
            config:
              backtest_date_start: "2018-01-01"
              backtest_date_end: "2023-12-01"
              model_provider: "anthropic"
              model_name: "claude-3-5-haiku-20241022"
          backtest_generate_investment_recommendations:
            config:
              backtest_date_start: "2018-01-01"
              backtest_date_end: "2023-12-01"
              model_provider: "anthropic"
              model_name: "claude-3-5-haiku-20241022"
              personality: "skeptical"
          evaluate_backtest_recommendations:
            config:
              backtest_date_start: "2018-01-01"
              backtest_date_end: "2023-12-01"
              model_provider: "anthropic"
              model_name: "claude-3-5-haiku-20241022"
        ```

        Provide config at runtime when launching the job in the UI or via CLI.""",
    )

    backtesting_job_skeptical = dg.define_asset_job(
        name="backtesting_job_skeptical",
        selection=dg.AssetSelection.assets(
            "backtest_analyze_economy_state",
            "backtest_analyze_asset_class_relationships",
            "backtest_generate_investment_recommendations",
            "evaluate_backtest_recommendations",
        ),
        description="""Backtesting pipeline with skeptical/bearish personality: economy state -> asset relationships -> recommendations -> evaluation.

        Use this job to run backtests with skeptical personality. Set personality='skeptical' in config.

        Configuration:
        - Single date: Use 'backtest_date' (YYYY-MM-DD, first day of month)
        - Date range: Use 'backtest_date_start' and 'backtest_date_end' (YYYY-MM-DD, first day of month)
        - Optional: 'model_provider' (default: openai), 'model_name' (default: gpt-4-turbo-preview)
        - Set 'personality' to 'skeptical' for assets that use it

        Example config for date range:
        ```yaml
        ops:
          backtest_analyze_economy_state:
            config:
              backtest_date_start: "2018-01-01"
              backtest_date_end: "2023-12-01"
              model_provider: "anthropic"
              model_name: "claude-3-5-haiku-20241022"
              personality: "skeptical"
          backtest_analyze_asset_class_relationships:
            config:
              backtest_date_start: "2018-01-01"
              backtest_date_end: "2023-12-01"
              model_provider: "anthropic"
              model_name: "claude-3-5-haiku-20241022"
          backtest_generate_investment_recommendations:
            config:
              backtest_date_start: "2018-01-01"
              backtest_date_end: "2023-12-01"
              model_provider: "anthropic"
              model_name: "claude-3-5-haiku-20241022"
              personality: "skeptical"
          evaluate_backtest_recommendations:
            config:
              backtest_date_start: "2018-01-01"
              backtest_date_end: "2023-12-01"
              model_provider: "anthropic"
              model_name: "claude-3-5-haiku-20241022"
        ```""",
    )

    backtesting_job_neutral = dg.define_asset_job(
        name="backtesting_job_neutral",
        selection=dg.AssetSelection.assets(
            "backtest_analyze_economy_state",
            "backtest_analyze_asset_class_relationships",
            "backtest_generate_investment_recommendations",
            "evaluate_backtest_recommendations",
        ),
        description="""Backtesting pipeline with neutral personality: economy state -> asset relationships -> recommendations -> evaluation.

        Use this job to run backtests with neutral personality. Set personality='neutral' in config.

        Configuration:
        - Single date: Use 'backtest_date' (YYYY-MM-DD, first day of month)
        - Date range: Use 'backtest_date_start' and 'backtest_date_end' (YYYY-MM-DD, first day of month)
        - Optional: 'model_provider' (default: openai), 'model_name' (default: gpt-4-turbo-preview)
        - Set 'personality' to 'neutral' for assets that use it

        Example config for date range:
        ```yaml
        ops:
          backtest_analyze_economy_state:
            config:
              backtest_date_start: "2018-01-01"
              backtest_date_end: "2023-12-01"
              model_provider: "anthropic"
              model_name: "claude-3-5-haiku-20241022"
              personality: "neutral"
          backtest_analyze_asset_class_relationships:
            config:
              backtest_date_start: "2018-01-01"
              backtest_date_end: "2023-12-01"
              model_provider: "anthropic"
              model_name: "claude-3-5-haiku-20241022"
          backtest_generate_investment_recommendations:
            config:
              backtest_date_start: "2018-01-01"
              backtest_date_end: "2023-12-01"
              model_provider: "anthropic"
              model_name: "claude-3-5-haiku-20241022"
              personality: "neutral"
          evaluate_backtest_recommendations:
            config:
              backtest_date_start: "2018-01-01"
              backtest_date_end: "2023-12-01"
              model_provider: "anthropic"
              model_name: "claude-3-5-haiku-20241022"
        ```""",
    )

    backtesting_job_bullish = dg.define_asset_job(
        name="backtesting_job_bullish",
        selection=dg.AssetSelection.assets(
            "backtest_analyze_economy_state",
            "backtest_analyze_asset_class_relationships",
            "backtest_generate_investment_recommendations",
            "evaluate_backtest_recommendations",
        ),
        description="""Backtesting pipeline with bullish/optimistic personality: economy state -> asset relationships -> recommendations -> evaluation.

        Use this job to run backtests with bullish personality. Set personality='bullish' in config.

        Configuration:
        - Single date: Use 'backtest_date' (YYYY-MM-DD, first day of month)
        - Date range: Use 'backtest_date_start' and 'backtest_date_end' (YYYY-MM-DD, first day of month)
        - Optional: 'model_provider' (default: openai), 'model_name' (default: gpt-4-turbo-preview)
        - Set 'personality' to 'bullish' for assets that use it

        Example config for date range:
        ```yaml
        ops:
          backtest_analyze_economy_state:
            config:
              backtest_date_start: "2018-01-01"
              backtest_date_end: "2023-12-01"
              model_provider: "anthropic"
              model_name: "claude-3-5-haiku-20241022"
              personality: "bullish"
          backtest_analyze_asset_class_relationships:
            config:
              backtest_date_start: "2018-01-01"
              backtest_date_end: "2023-12-01"
              model_provider: "anthropic"
              model_name: "claude-3-5-haiku-20241022"
          backtest_generate_investment_recommendations:
            config:
              backtest_date_start: "2018-01-01"
              backtest_date_end: "2023-12-01"
              model_provider: "anthropic"
              model_name: "claude-3-5-haiku-20241022"
              personality: "bullish"
          evaluate_backtest_recommendations:
            config:
              backtest_date_start: "2018-01-01"
              backtest_date_end: "2023-12-01"
              model_provider: "anthropic"
              model_name: "claude-3-5-haiku-20241022"
        ```""",
    )

    return {
        "us_sector_etfs_ingestion_job": us_sector_etfs_ingestion_job,
        "currency_etfs_ingestion_job": currency_etfs_ingestion_job,
        "major_indices_ingestion_job": major_indices_ingestion_job,
        "fixed_income_etfs_ingestion_job": fixed_income_etfs_ingestion_job,
        "global_markets_ingestion_job": global_markets_ingestion_job,
        "energy_commodities_ingestion_job": energy_commodities_ingestion_job,
        "input_commodities_ingestion_job": input_commodities_ingestion_job,
        "agriculture_commodities_ingestion_job": agriculture_commodities_ingestion_job,
        "treasury_yields_ingestion_job": treasury_yields_ingestion_job,
        "weekly_replication_job": weekly_replication_job,
        "dbt_models_job": dbt_models_job,
        "dbt_backtesting_models_job": dbt_backtesting_models_job,
        "dspy_analysis_job": dspy_analysis_job,
        "monthly_economic_analysis_job_skeptical": monthly_economic_analysis_job_skeptical,
        "monthly_economic_analysis_job_neutral": monthly_economic_analysis_job_neutral,
        "monthly_economic_analysis_job_bullish": monthly_economic_analysis_job_bullish,
        "backtesting_job": backtesting_job,
        "backtesting_job_skeptical": backtesting_job_skeptical,
        "backtesting_job_neutral": backtesting_job_neutral,
        "backtesting_job_bullish": backtesting_job_bullish,
    }


def create_market_stack_ingestion_schedule(
    asset_name, job, tickers_or_commodities, dimension_name
):
    """Create a sensor that runs MarketStack assets for current month on Sundays."""

    @dg.sensor(
        name=f"{asset_name}_ingestion_schedule",
        job=job,
        description=f"Weekly schedule for {asset_name} - runs current month partition for all {dimension_name}s every Sunday at 2 AM EST",
        default_status=DefaultSensorStatus.RUNNING,
        minimum_interval_seconds=3600,
    )
    def market_stack_schedule(context):
        now = datetime.now(pytz.timezone("America/New_York"))
        if now.weekday() != 6:
            return

        current_month = now.strftime("%Y-%m")

        for ticker_or_commodity in tickers_or_commodities:
            partition_key = dg.MultiPartitionKey(
                {"date": current_month, dimension_name: ticker_or_commodity}
            )
            yield dg.RunRequest(
                run_key=f"{asset_name}_{ticker_or_commodity}_{current_month}_{now.strftime('%Y%m%d')}",
                partition_key=partition_key,
                tags={
                    "trigger": "weekly_schedule",
                    "asset": asset_name,
                    "month": current_month,
                    dimension_name: ticker_or_commodity,
                },
            )

    return market_stack_schedule


jobs = create_scheduled_jobs()

us_sector_etfs_schedule = create_market_stack_ingestion_schedule(
    "us_sector_etfs_raw",
    jobs["us_sector_etfs_ingestion_job"],
    US_SECTOR_ETFS,
    "ticker",
)

currency_etfs_schedule = create_market_stack_ingestion_schedule(
    "currency_etfs_raw",
    jobs["currency_etfs_ingestion_job"],
    CURRENCY_ETFS,
    "ticker",
)

major_indices_schedule = create_market_stack_ingestion_schedule(
    "major_indices_raw",
    jobs["major_indices_ingestion_job"],
    MAJOR_INDICES_TICKERS,
    "ticker",
)

fixed_income_etfs_schedule = create_market_stack_ingestion_schedule(
    "fixed_income_etfs_raw",
    jobs["fixed_income_etfs_ingestion_job"],
    FIXED_INCOME_ETFS,
    "ticker",
)

global_markets_schedule = create_market_stack_ingestion_schedule(
    "global_markets_raw",
    jobs["global_markets_ingestion_job"],
    GLOBAL_MARKETS,
    "ticker",
)

energy_commodities_schedule = create_market_stack_ingestion_schedule(
    "energy_commodities_raw",
    jobs["energy_commodities_ingestion_job"],
    ENERGY_COMMODITIES,
    "commodity",
)

input_commodities_schedule = create_market_stack_ingestion_schedule(
    "input_commodities_raw",
    jobs["input_commodities_ingestion_job"],
    INPUT_COMMODITIES,
    "commodity",
)

agriculture_commodities_schedule = create_market_stack_ingestion_schedule(
    "agriculture_commodities_raw",
    jobs["agriculture_commodities_ingestion_job"],
    AGRICULTURE_COMMODITIES,
    "commodity",
)


@dg.sensor(
    name="treasury_yields_ingestion_schedule",
    job=jobs["treasury_yields_ingestion_job"],
    description="Weekly schedule for Treasury Yields - runs current year partition every Sunday at 3 AM EST",
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=3600,
)
def treasury_yields_schedule(context):
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


schedules = [
    weekly_replication_schedule,
    monthly_economic_analysis_schedule_skeptical,
    monthly_economic_analysis_schedule_neutral,
    monthly_economic_analysis_schedule_bullish,
]

sensors = [
    us_sector_etfs_schedule,
    currency_etfs_schedule,
    major_indices_schedule,
    fixed_income_etfs_schedule,
    global_markets_schedule,
    energy_commodities_schedule,
    input_commodities_schedule,
    agriculture_commodities_schedule,
    treasury_yields_schedule,
]
