import dagster as dg


economic_analysis_job_skeptical = dg.define_asset_job(
    name="weekly_economic_analysis_job_skeptical",
    tags={"dagster/priority": "5", "dagster/max_runtime": 2700},
    selection=[
        "analyze_economy_state",
        "generate_economy_state_charts",
        "analyze_asset_class_relationships",
        "generate_investment_recommendations",
        "generate_investment_recommendation_charts",
        "generate_economic_narratives",
        "generate_indicator_forecasts",
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
          model_provider: "gemini"
          model_name: "gemini-3-pro-preview"
      analyze_asset_class_relationships:
        config:
          model_provider: "gemini"
          model_name: "gemini-3-pro-preview"
      generate_investment_recommendations:
        config:
          personality: "skeptical"
          model_provider: "gemini"
          model_name: "gemini-3-pro-preview"
    ```""",
)

economic_analysis_job_neutral = dg.define_asset_job(
    name="weekly_economic_analysis_job_neutral",
    tags={"dagster/priority": "5", "dagster/max_runtime": 2700},
    selection=[
        "analyze_economy_state",
        "generate_economy_state_charts",
        "analyze_asset_class_relationships",
        "generate_investment_recommendations",
        "generate_investment_recommendation_charts",
        "generate_economic_narratives",
        "generate_indicator_forecasts",
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

economic_analysis_job_bullish = dg.define_asset_job(
    name="weekly_economic_analysis_job_bullish",
    tags={"dagster/priority": "5", "dagster/max_runtime": 2700},
    selection=[
        "analyze_economy_state",
        "generate_economy_state_charts",
        "analyze_asset_class_relationships",
        "generate_investment_recommendations",
        "generate_investment_recommendation_charts",
        "generate_economic_narratives",
        "generate_indicator_forecasts",
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
          model_provider: "gemini"
          model_name: "gemini-3-pro-preview"
      analyze_asset_class_relationships:
        config:
          model_provider: "gemini"
          model_name: "gemini-3-pro-preview"
      generate_investment_recommendations:
        config:
          personality: "bullish"
          model_provider: "gemini"
          model_name: "gemini-3-pro-preview"
    ```""",
)


dspy_analysis_job = dg.define_asset_job(
    name="dspy_analysis_job",
    tags={"dagster/priority": "5", "dagster/max_runtime": 2700},
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
          model_provider: "gemini"
          model_name: "gemini-3-pro-preview"
      analyze_asset_class_relationships:
        config:
          model_provider: "gemini"
          model_name: "gemini-3-pro-preview"
      generate_investment_recommendations:
        config:
          personality: "neutral"
          model_provider: "gemini"
          model_name: "gemini-3-pro-preview"
    ```""",
)


reddit_daily_summary_job = dg.define_asset_job(
    name="reddit_daily_summary_job",
    tags={
        "dagster/priority": "3",
        "dagster/max_runtime": 1800,
        "job_type": "daily_ingestion",
    },
    selection=dg.AssetSelection.assets("reddit_daily_summary"),
    description="Daily AI-powered Reddit summary generation",
)

news_weekly_summary_job = dg.define_asset_job(
    name="news_weekly_summary_job",
    tags={"dagster/priority": "3", "dagster/max_runtime": 1800},
    selection=dg.AssetSelection.assets("news_weekly_summary"),
    description="Weekly cross-source news summary combining Reddit and FOMC data",
)

interesting_data_points_job = dg.define_asset_job(
    name="interesting_data_points_job",
    tags={"dagster/priority": "3", "dagster/max_runtime": 1800},
    selection=dg.AssetSelection.assets("detect_interesting_data_points_weekly"),
    description="Weekly AI-powered analysis identifying interesting data points",
)

analysis_jobs = [
    economic_analysis_job_skeptical,
    economic_analysis_job_neutral,
    economic_analysis_job_bullish,
    dspy_analysis_job,
    reddit_daily_summary_job,
    news_weekly_summary_job,
    interesting_data_points_job,
]
