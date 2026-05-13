import dagster as dg

from macro_agents.defs.analysis.jobs import (
    economic_analysis_job_bullish,
    economic_analysis_job_neutral,
    economic_analysis_job_skeptical,
    interesting_data_points_job,
    news_weekly_summary_job,
    reddit_daily_summary_job,
)


weekly_economic_analysis_schedule_skeptical = dg.ScheduleDefinition(
    name="weekly_economic_analysis_schedule_skeptical",
    cron_schedule="0 14 * * 0",
    execution_timezone="America/New_York",
    job=economic_analysis_job_skeptical,
    description="Weekly economic analysis pipeline (skeptical/bearish) on Sundays at 2 PM EST",
    run_config={
        "ops": {
            "analyze_economy_state": {
                "config": {
                    "personality": "skeptical",
                    "model_provider": "gemini",
                    "model_name": "gemini-3-pro-preview",
                }
            },
            "generate_investment_recommendations": {
                "config": {
                    "personality": "skeptical",
                    "model_provider": "gemini",
                    "model_name": "gemini-3-pro-preview",
                }
            },
            "generate_economic_narratives": {
                "config": {
                    "personality": "skeptical",
                    "model_provider": "gemini",
                    "model_name": "gemini-3-pro-preview",
                }
            },
            "generate_indicator_forecasts": {
                "config": {
                    "personality": "skeptical",
                    "model_provider": "gemini",
                    "model_name": "gemini-3-pro-preview",
                }
            },
        }
    },
)

weekly_economic_analysis_schedule_neutral = dg.ScheduleDefinition(
    name="weekly_economic_analysis_schedule_neutral",
    cron_schedule="15 14 * * 0",
    execution_timezone="America/New_York",
    job=economic_analysis_job_neutral,
    description="Weekly economic analysis pipeline (neutral) on Sundays at 2:15 PM EST",
    run_config={
        "ops": {
            "analyze_economy_state": {
                "config": {
                    "personality": "neutral",
                    "model_provider": "gemini",
                    "model_name": "gemini-3-pro-preview",
                }
            },
            "generate_investment_recommendations": {
                "config": {
                    "personality": "neutral",
                    "model_provider": "gemini",
                    "model_name": "gemini-3-pro-preview",
                }
            },
            "generate_economic_narratives": {
                "config": {
                    "personality": "neutral",
                    "model_provider": "gemini",
                    "model_name": "gemini-3-pro-preview",
                }
            },
            "generate_indicator_forecasts": {
                "config": {
                    "personality": "neutral",
                    "model_provider": "gemini",
                    "model_name": "gemini-3-pro-preview",
                }
            },
        }
    },
)

weekly_economic_analysis_schedule_bullish = dg.ScheduleDefinition(
    name="weekly_economic_analysis_schedule_bullish",
    cron_schedule="30 14 * * 0",
    execution_timezone="America/New_York",
    job=economic_analysis_job_bullish,
    description="Weekly economic analysis pipeline (bullish/optimistic) on Sundays at 2:30 PM EST",
    run_config={
        "ops": {
            "analyze_economy_state": {
                "config": {
                    "personality": "bullish",
                    "model_provider": "gemini",
                    "model_name": "gemini-3-pro-preview",
                }
            },
            "generate_investment_recommendations": {
                "config": {
                    "personality": "bullish",
                    "model_provider": "gemini",
                    "model_name": "gemini-3-pro-preview",
                }
            },
            "generate_economic_narratives": {
                "config": {
                    "personality": "bullish",
                    "model_provider": "gemini",
                    "model_name": "gemini-3-pro-preview",
                }
            },
            "generate_indicator_forecasts": {
                "config": {
                    "personality": "bullish",
                    "model_provider": "gemini",
                    "model_name": "gemini-3-pro-preview",
                }
            },
        }
    },
)

daily_reddit_summary_schedule = dg.ScheduleDefinition(
    name="daily_reddit_summary_schedule",
    cron_schedule="45 2 * * *",
    execution_timezone="America/New_York",
    job=reddit_daily_summary_job,
    description="Daily Reddit summary generation at 2:45 AM EST",
)

weekly_news_summary_schedule = dg.ScheduleDefinition(
    name="weekly_news_summary_schedule",
    cron_schedule="0 4 * * 1",
    execution_timezone="America/New_York",
    job=news_weekly_summary_job,
    description="Weekly cross-source news summary on Mondays at 4 AM EST",
)

weekly_interesting_data_points_schedule = dg.ScheduleDefinition(
    name="weekly_interesting_data_points_schedule",
    cron_schedule="0 8 * * 1",
    execution_timezone="America/New_York",
    job=interesting_data_points_job,
    description="Weekly interesting data points analysis on Mondays at 8 AM EST",
)

analysis_schedules = [
    weekly_economic_analysis_schedule_skeptical,
    weekly_economic_analysis_schedule_neutral,
    weekly_economic_analysis_schedule_bullish,
    daily_reddit_summary_schedule,
    weekly_news_summary_schedule,
    weekly_interesting_data_points_schedule,
]
