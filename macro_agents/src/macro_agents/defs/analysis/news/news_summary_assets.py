"""Assets for generating AI-powered news summaries from Reddit and FOMC data."""

from datetime import datetime, timezone

import io
from datetime import timedelta

import dagster as dg
import polars as pl

from macro_agents.defs.analysis.news.news_summarizer import NewsSummarizerResource
from macro_agents.defs.resources.motherduck import MotherDuckResource


@dg.asset(
    group_name="news_summaries",
    kinds={"ai", "duckdb"},
    partitions_def=dg.DailyPartitionsDefinition(start_date="2024-01-01"),
    deps=[dg.AssetKey(["agent_reddit_posts_daily"])],
    description="AI-generated daily summary of Reddit posts from financial/economic subreddits",
)
def reddit_daily_summary(
    context: dg.AssetExecutionContext,
    news_summarizer: NewsSummarizerResource,
    md: MotherDuckResource,
) -> dg.MaterializeResult:
    """
    Generate daily AI summary of Reddit posts across all financial/economic subreddits.

    Aggregates posts from r/investing, r/stocks, r/wallstreetbets, r/economics, r/economy
    and generates a comprehensive summary with sentiment analysis and key themes.
    """
    if context.has_partition_key:
        partition_date = context.partition_key
    else:
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).date()
        partition_date = yesterday.strftime("%Y-%m-%d")
        context.log.info(
            f"No partition key — defaulting to yesterday: {partition_date}"
        )

    context.log.info(f"Generating daily Reddit summary for {partition_date}")

    # Fetch all posts for this date across all subreddits
    query = f"""
        SELECT
            title,
            score,
            num_comments,
            subreddit,
            author,
            url
        FROM agent_reddit_posts_daily
        WHERE partition_date = '{partition_date}'
        ORDER BY score DESC
        LIMIT 100
    """

    posts_df = md.execute_query(query, read_only=True)

    if posts_df.is_empty():
        context.log.warning(f"No posts found for {partition_date}")
        return dg.MaterializeResult(
            metadata={
                "partition_date": partition_date,
                "num_posts": 0,
                "skipped": "No data available",
            }
        )

    # Convert to CSV format for DSPy
    content_buffer = io.StringIO()
    posts_df.write_csv(content_buffer)
    content_csv = content_buffer.getvalue()

    context.log.info(f"Analyzing {len(posts_df)} posts with AI summarizer")

    # Generate summary using DSPy
    try:
        result = news_summarizer.summarize_daily(
            date=partition_date, content_data=content_csv, source_type="reddit"
        )
    except Exception as e:
        context.log.error(f"AI summarization failed: {e}")
        return dg.MaterializeResult(
            metadata={
                "partition_date": partition_date,
                "num_posts": len(posts_df),
                "error": f"Summarization failed: {str(e)}",
            }
        )

    # Store summary in database
    summary_data = {
        "summary_date": partition_date,
        "summary_type": "daily",
        "source_type": "reddit",
        "summary_content": result.summary,
        "key_themes": result.key_themes,
        "sentiment": result.sentiment,
        "notable_items": result.notable_items,
        "num_posts_analyzed": len(posts_df),
        "created_at": datetime.now(timezone.utc),
        "model_name": news_summarizer._model_name
        if hasattr(news_summarizer, "_model_name")
        else "unknown",
    }

    summary_df = pl.DataFrame([summary_data])

    # Upsert to database
    md.upsert_data("reddit_summaries", summary_df, ["summary_date"], context=context)

    context.log.info("Successfully generated and stored Reddit summary")

    return dg.MaterializeResult(
        metadata={
            "partition_date": partition_date,
            "num_posts": len(posts_df),
            "sentiment": result.sentiment,
            "key_themes": result.key_themes[:100] + "..."
            if len(result.key_themes) > 100
            else result.key_themes,
            "summary_preview": result.summary[:200] + "..."
            if len(result.summary) > 200
            else result.summary,
        }
    )


@dg.asset(
    group_name="news_summaries",
    kinds={"ai", "duckdb"},
    partitions_def=dg.WeeklyPartitionsDefinition(start_date="2024-01-01"),
    description="AI-generated weekly cross-source summary combining Reddit and FOMC data",
)
def news_weekly_summary(
    context: dg.AssetExecutionContext,
    news_summarizer: NewsSummarizerResource,
    md: MotherDuckResource,
) -> dg.MaterializeResult:
    """
    Generate weekly AI summary combining Reddit posts and FOMC minutes.

    Synthesizes daily summaries from both sources to provide comprehensive
    weekly overview of market sentiment and policy context.
    """
    # Get week boundaries from partition key (or current week for non-partitioned runs)
    if context.has_partition_key:
        week_start = context.partition_key
    else:
        today = datetime.now(timezone.utc).date()
        # WeeklyPartitionsDefinition uses Sunday as start
        days_since_sunday = (today.weekday() + 1) % 7
        sunday = today - timedelta(days=days_since_sunday)
        week_start = sunday.strftime("%Y-%m-%d")
        context.log.info(f"No partition key — defaulting to current week: {week_start}")
    # WeeklyPartitionsDefinition uses Sunday as start, so week_end is 6 days later
    week_start_date = datetime(
        year=int(week_start[:4]),
        month=int(week_start[5:7]),
        day=int(week_start[8:10]),
    ).date()
    week_end = (week_start_date + timedelta(days=6)).strftime("%Y-%m-%d")

    context.log.info(f"Generating weekly summary for {week_start} to {week_end}")

    if not md.table_exists("reddit_summaries"):
        context.log.warning("reddit_summaries table does not exist — skipping weekly summary")
        return dg.MaterializeResult(
            metadata={
                "week_start": week_start,
                "week_end": week_end,
                "skipped": "reddit_summaries table not yet materialized",
            }
        )

    # Fetch daily summaries from both sources for this week
    query = f"""
        SELECT
            summary_date,
            source_type,
            summary_content,
            sentiment,
            key_themes
        FROM reddit_summaries
        WHERE summary_date >= '{week_start}'
          AND summary_date <= '{week_end}'
          AND summary_type = 'daily'
        ORDER BY summary_date, source_type
    """

    daily_summaries_df = md.execute_query(query, read_only=True)

    if daily_summaries_df.is_empty():
        context.log.warning(f"No daily summaries found for week {week_start}")
        return dg.MaterializeResult(
            metadata={
                "week_start": week_start,
                "week_end": week_end,
                "num_daily_summaries": 0,
                "skipped": "No daily summaries available",
            }
        )

    # Convert to CSV for DSPy
    csv_buffer = io.StringIO()
    daily_summaries_df.write_csv(csv_buffer)
    daily_summaries_csv = csv_buffer.getvalue()

    context.log.info(
        f"Analyzing {len(daily_summaries_df)} daily summaries with AI summarizer"
    )

    # Generate weekly summary using DSPy
    try:
        result = news_summarizer.summarize_weekly(
            week_start=week_start,
            week_end=week_end,
            daily_summaries=daily_summaries_csv,
        )
    except Exception as e:
        context.log.error(f"AI summarization failed: {e}")
        return dg.MaterializeResult(
            metadata={
                "week_start": week_start,
                "week_end": week_end,
                "num_daily_summaries": len(daily_summaries_df),
                "error": f"Summarization failed: {str(e)}",
            }
        )

    # Store summary in database
    summary_data = {
        "summary_date": week_start,  # Store week start as the date
        "summary_type": "weekly",
        "source_type": "combined",  # Both Reddit and FOMC
        "summary_content": result.summary,
        "trends": result.trends,
        "sentiment_evolution": result.sentiment_evolution,
        "policy_context": result.policy_context,
        "num_daily_summaries": len(daily_summaries_df),
        "created_at": datetime.now(timezone.utc),
        "model_name": news_summarizer._model_name
        if hasattr(news_summarizer, "_model_name")
        else "unknown",
    }

    summary_df = pl.DataFrame([summary_data])

    # Upsert to database
    md.upsert_data(
        "news_weekly_summaries", summary_df, ["summary_date"], context=context
    )

    context.log.info("Successfully generated and stored weekly summary")

    return dg.MaterializeResult(
        metadata={
            "week_start": week_start,
            "week_end": week_end,
            "num_daily_summaries": len(daily_summaries_df),
            "trends_preview": result.trends[:200] + "..."
            if len(result.trends) > 200
            else result.trends,
            "summary_preview": result.summary[:200] + "..."
            if len(result.summary) > 200
            else result.summary,
        }
    )
