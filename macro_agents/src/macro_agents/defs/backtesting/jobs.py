import dagster as dg


backtesting_job = dg.define_asset_job(
    name="backtesting_job",
    tags={"dagster/priority": "-5", "dagster/max_runtime": 3600},
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
    tags={"dagster/priority": "-5", "dagster/max_runtime": 3600},
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
    tags={"dagster/priority": "-5", "dagster/max_runtime": 3600},
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
    tags={"dagster/priority": "-5", "dagster/max_runtime": 3600},
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


backtesting_jobs = [
    backtesting_job,
    backtesting_job_skeptical,
    backtesting_job_neutral,
    backtesting_job_bullish,
]
