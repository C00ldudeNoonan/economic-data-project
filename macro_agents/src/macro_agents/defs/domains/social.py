"""Social and community ingestion assets."""

from decimal import Decimal
from datetime import datetime, timezone

import dagster as dg
import polars as pl

from macro_agents.defs.resources.motherduck import MotherDuckResource
from macro_agents.defs.resources.ollama import OllamaResource
from macro_agents.defs.resources.reddit import RedditResource
from macro_agents.defs.domains.social_checks import social_checks

SOCIAL_GROUP = "social_ingestion"
EMBEDDING_GROUP = "social_embeddings"

SUBREDDITS = ["investing", "stocks", "wallstreetbets", "economics", "economy"]
subreddit_partition = dg.StaticPartitionsDefinition(SUBREDDITS)

date_partition = dg.DailyPartitionsDefinition(start_date="2024-01-01")

reddit_partitions = dg.MultiPartitionsDefinition(
    {"subreddit": subreddit_partition, "date": date_partition}
)


@dg.asset(
    group_name=SOCIAL_GROUP,
    kinds={"web_scraping", "duckdb"},
    partitions_def=reddit_partitions,
    description="Reddit posts from financial/economic subreddits scraped from old.reddit.com with aggressive rate limiting",
)
def reddit_posts_raw(
    context: dg.AssetExecutionContext,
    reddit: RedditResource,
    md: MotherDuckResource,
) -> dg.MaterializeResult:
    """
    Scrape Reddit posts for a subreddit and date partition with rate limiting.

    Rate limiting strategy:
    - 8-20 second random delays between requests (more conservative than Fed)
    - No authentication required (anonymous scraping)
    - User agent rotation
    - Exponential backoff on failures (3 retries, 30s -> 5min)
    - Scrapes old.reddit.com for stable HTML structure
    """

    def ensure_table_exists() -> None:
        empty_df = pl.DataFrame(
            schema={
                "post_id": pl.Utf8,
                "title": pl.Utf8,
                "score": pl.Int64,
                "num_comments": pl.Int64,
                "created_utc": pl.Datetime,
                "author": pl.Utf8,
                "url": pl.Utf8,
                "permalink": pl.Utf8,
                "subreddit": pl.Utf8,
                "domain": pl.Utf8,
                "partition_date": pl.Utf8,
                "fetched_at": pl.Datetime,
            }
        )
        md.upsert_data("reddit_posts_raw", empty_df, ["post_id"], context=context)

    subreddit = context.partition_key.keys_by_dimension["subreddit"]
    partition_date = context.partition_key.keys_by_dimension["date"]

    context.log.info(f"Starting Reddit scraping for r/{subreddit} on {partition_date}")

    try:
        df = reddit.get_top_posts(
            subreddit=subreddit,
            time_filter="day",
            limit=100,
            context=context,
        )
    except Exception as e:
        context.log.error(f"Failed to scrape r/{subreddit}: {e}")
        ensure_table_exists()
        return dg.MaterializeResult(
            metadata={
                "subreddit": subreddit,
                "partition_date": partition_date,
                "num_posts": 0,
                "error": str(e),
            }
        )

    if df.is_empty():
        context.log.warning(f"No posts found for r/{subreddit} on {partition_date}")
        ensure_table_exists()
        return dg.MaterializeResult(
            metadata={
                "subreddit": subreddit,
                "partition_date": partition_date,
                "num_posts": 0,
            }
        )

    df = df.with_columns(pl.lit(partition_date).alias("partition_date"))
    df = df.with_columns(pl.lit(datetime.now(timezone.utc)).alias("fetched_at"))

    context.log.info(
        f"Successfully scraped {len(df)} posts from r/{subreddit}, upserting to database"
    )

    md.upsert_data("reddit_posts_raw", df, ["post_id"], context=context)

    def _coerce_float(value: object) -> float:
        if isinstance(value, (int, float, Decimal)):
            return float(value)
        return 0.0

    total_score = int(df["score"].sum())
    avg_score = _coerce_float(df["score"].mean())
    total_comments = int(df["num_comments"].sum())
    avg_comments = _coerce_float(df["num_comments"].mean())

    top_post = (
        df.sort("score", descending=True)
        .select(["title", "score", "num_comments", "author", "url"])
        .head(1)
        .to_dicts()[0]
    )

    return dg.MaterializeResult(
        metadata={
            "subreddit": subreddit,
            "partition_date": partition_date,
            "num_posts": len(df),
            "total_score": total_score,
            "avg_score": avg_score,
            "total_comments": total_comments,
            "avg_comments": avg_comments,
            "top_post": top_post,
            "first_10_rows": str(df.head(10)),
        }
    )


@dg.asset(
    group_name=SOCIAL_GROUP,
    kinds={"web_scraping", "duckdb"},
    partitions_def=reddit_partitions,
    deps=["reddit_posts_raw"],
    description="Full post content and selftext fetched per post from old.reddit.com",
)
def reddit_post_content_raw(
    context: dg.AssetExecutionContext,
    reddit: RedditResource,
    md: MotherDuckResource,
) -> dg.MaterializeResult:
    """Fetch full post content (selftext, links) for each post in a partition."""

    subreddit = context.partition_key.keys_by_dimension["subreddit"]
    partition_date = context.partition_key.keys_by_dimension["date"]

    context.log.info(f"Fetching post content for r/{subreddit} on {partition_date}")

    # Read existing posts for this partition
    query = (
        f"SELECT post_id, permalink FROM reddit_posts_raw "
        f"WHERE subreddit = '{subreddit}' AND partition_date = '{partition_date}'"
    )
    try:
        posts_df = md.execute_query(query)
    except Exception as e:
        context.log.error(f"Failed to query posts: {e}")
        return dg.MaterializeResult(metadata={"error": str(e), "num_posts": 0})

    if posts_df.is_empty():
        context.log.warning(f"No posts found for r/{subreddit} on {partition_date}")
        return dg.MaterializeResult(
            metadata={
                "subreddit": subreddit,
                "partition_date": partition_date,
                "num_posts": 0,
            }
        )

    content_rows: list[dict] = []
    failed = 0
    skipped = 0

    for row in posts_df.iter_rows(named=True):
        permalink = row["permalink"]
        if not permalink:
            skipped += 1
            continue
        try:
            content = reddit.get_post_content(permalink, context=context)
            if not content.get("post_id"):
                skipped += 1
                continue
            content["partition_date"] = partition_date
            content["subreddit"] = subreddit
            content["fetched_at"] = datetime.now(timezone.utc)
            # Serialize links list to string for storage
            content["links"] = ",".join(content.get("links", []))
            content_rows.append(content)
        except Exception as e:
            context.log.warning(f"Failed to fetch content for {permalink}: {e}")
            failed += 1

    if content_rows:
        df = pl.DataFrame(content_rows)
        md.upsert_data("reddit_post_content_raw", df, ["post_id"], context=context)

    return dg.MaterializeResult(
        metadata={
            "subreddit": subreddit,
            "partition_date": partition_date,
            "num_posts": len(content_rows),
            "failed": failed,
            "skipped": skipped,
        }
    )


@dg.asset(
    group_name=SOCIAL_GROUP,
    kinds={"web_scraping", "duckdb"},
    partitions_def=reddit_partitions,
    deps=["reddit_posts_raw"],
    description="Comments scraped per post from old.reddit.com",
)
def reddit_comments_raw(
    context: dg.AssetExecutionContext,
    reddit: RedditResource,
    md: MotherDuckResource,
) -> dg.MaterializeResult:
    """Fetch comments for each post in a partition."""

    subreddit = context.partition_key.keys_by_dimension["subreddit"]
    partition_date = context.partition_key.keys_by_dimension["date"]

    context.log.info(f"Fetching comments for r/{subreddit} on {partition_date}")

    query = (
        f"SELECT post_id, permalink FROM reddit_posts_raw "
        f"WHERE subreddit = '{subreddit}' AND partition_date = '{partition_date}'"
    )
    try:
        posts_df = md.execute_query(query)
    except Exception as e:
        context.log.error(f"Failed to query posts: {e}")
        return dg.MaterializeResult(metadata={"error": str(e), "num_comments": 0})

    if posts_df.is_empty():
        context.log.warning(f"No posts found for r/{subreddit} on {partition_date}")
        return dg.MaterializeResult(
            metadata={
                "subreddit": subreddit,
                "partition_date": partition_date,
                "num_comments": 0,
            }
        )

    all_comments: list[dict] = []
    failed = 0
    skipped = 0

    for row in posts_df.iter_rows(named=True):
        post_id = row["post_id"]
        permalink = row["permalink"]
        if not permalink:
            skipped += 1
            continue
        try:
            comments_df = reddit.get_post_comments(permalink, context=context)
            if comments_df.is_empty():
                skipped += 1
                continue
            for comment_row in comments_df.iter_rows(named=True):
                comment = dict(comment_row)
                comment["post_id"] = post_id
                comment["subreddit"] = subreddit
                comment["partition_date"] = partition_date
                comment["fetched_at"] = datetime.now(timezone.utc)
                # Serialize links list to string for storage
                comment["links"] = ",".join(comment.get("links", []) or [])
                all_comments.append(comment)
        except Exception as e:
            context.log.warning(f"Failed to fetch comments for {permalink}: {e}")
            failed += 1

    if all_comments:
        df = pl.DataFrame(all_comments)
        md.upsert_data("reddit_comments_raw", df, ["comment_id"], context=context)

    return dg.MaterializeResult(
        metadata={
            "subreddit": subreddit,
            "partition_date": partition_date,
            "num_comments": len(all_comments),
            "failed": failed,
            "skipped": skipped,
        }
    )


@dg.asset(
    group_name=EMBEDDING_GROUP,
    kinds={"ml", "duckdb"},
    partitions_def=reddit_partitions,
    deps=["reddit_post_content_raw", "reddit_comments_raw"],
    description="Embeddings for Reddit post text and comment text using Ollama nomic-embed-text",
)
def reddit_content_embeddings(
    context: dg.AssetExecutionContext,
    ollama: OllamaResource,
    md: MotherDuckResource,
) -> dg.MaterializeResult:
    """Build embeddings for post selftext and comment bodies in a partition."""

    subreddit = context.partition_key.keys_by_dimension["subreddit"]
    partition_date = context.partition_key.keys_by_dimension["date"]

    context.log.info(f"Building embeddings for r/{subreddit} on {partition_date}")

    embedding_rows: list[dict] = []
    embedded = 0
    skipped = 0
    failed = 0

    # Embed post selftext
    query_error = ""
    try:
        posts_df = md.execute_query(
            f"SELECT post_id, title, selftext FROM reddit_post_content_raw "
            f"WHERE subreddit = '{subreddit}' AND partition_date = '{partition_date}'"
        )
    except Exception as e:
        context.log.warning(f"Failed to query post content: {e}")
        query_error = f"post_content_query: {e}"
        posts_df = pl.DataFrame()

    for row in posts_df.iter_rows(named=True):
        text = row.get("selftext") or row.get("title") or ""
        if not text.strip():
            skipped += 1
            continue
        try:
            vectors = ollama.get_embeddings([text])
            embedding_rows.append(
                {
                    "content_id": row["post_id"],
                    "content_type": "post",
                    "subreddit": subreddit,
                    "partition_date": partition_date,
                    "text_preview": text[:200],
                    "embedding": vectors[0],
                    "embedded_at": datetime.now(timezone.utc),
                }
            )
            embedded += 1
        except Exception as e:
            context.log.warning(f"Failed to embed post {row['post_id']}: {e}")
            failed += 1

    # Embed comments
    try:
        comments_df = md.execute_query(
            f"SELECT comment_id, body FROM reddit_comments_raw "
            f"WHERE subreddit = '{subreddit}' AND partition_date = '{partition_date}'"
        )
    except Exception as e:
        context.log.warning(f"Failed to query comments: {e}")
        query_error += (
            f"; comments_query: {e}" if query_error else f"comments_query: {e}"
        )
        comments_df = pl.DataFrame()

    for row in comments_df.iter_rows(named=True):
        body = row.get("body") or ""
        if not body.strip():
            skipped += 1
            continue
        try:
            vectors = ollama.get_embeddings([body])
            embedding_rows.append(
                {
                    "content_id": row["comment_id"],
                    "content_type": "comment",
                    "subreddit": subreddit,
                    "partition_date": partition_date,
                    "text_preview": body[:200],
                    "embedding": vectors[0],
                    "embedded_at": datetime.now(timezone.utc),
                }
            )
            embedded += 1
        except Exception as e:
            context.log.warning(f"Failed to embed comment {row['comment_id']}: {e}")
            failed += 1

    if embedding_rows:
        df = pl.DataFrame(embedding_rows)
        md.upsert_data(
            "reddit_content_embeddings",
            df,
            ["content_id", "content_type"],
            context=context,
        )

    context.log.info(
        f"Embeddings: {embedded} embedded, {skipped} skipped, {failed} failed"
    )

    metadata: dict = {
        "subreddit": subreddit,
        "partition_date": partition_date,
        "embedded": embedded,
        "skipped": skipped,
        "failed": failed,
    }
    if query_error:
        metadata["query_error"] = query_error

    return dg.MaterializeResult(metadata=metadata)


reddit_posts_ingestion_job = dg.define_asset_job(
    name="reddit_posts_ingestion_job",
    tags={"dagster/priority": "5", "dagster/max_runtime": 1800},
    selection=dg.AssetSelection.assets(
        "reddit_posts_raw",
        "reddit_post_content_raw",
        "reddit_comments_raw",
        "reddit_content_embeddings",
    ),
    description="Reddit full ingestion job - posts, content, comments, and embeddings",
)


@dg.schedule(
    name="daily_reddit_posts_schedule",
    cron_schedule="30 1 * * *",
    execution_timezone="America/New_York",
    job=reddit_posts_ingestion_job,
    description="Daily Reddit posts ingestion at 1:30 AM EST - generates run requests for all subreddits",
)
def daily_reddit_posts_schedule(context: dg.ScheduleEvaluationContext):
    scheduled_time = context.scheduled_execution_time or datetime.now(timezone.utc)
    partition_date = scheduled_time.strftime("%Y-%m-%d")
    run_requests = []
    for subreddit in SUBREDDITS:
        partition_key = dg.MultiPartitionKey(
            {"subreddit": subreddit, "date": partition_date}
        )
        run_requests.append(
            dg.RunRequest(
                run_key=f"reddit_{subreddit}_{partition_date}",
                partition_key=partition_key,
                tags={
                    "trigger": "daily_schedule",
                    "subreddit": subreddit,
                    "date": partition_date,
                },
            )
        )
    return run_requests


defs = dg.Definitions(
    assets=[
        reddit_posts_raw,
        reddit_post_content_raw,
        reddit_comments_raw,
        reddit_content_embeddings,
    ],
    asset_checks=social_checks,
    jobs=[reddit_posts_ingestion_job],
    schedules=[daily_reddit_posts_schedule],
    resources={
        "reddit": RedditResource(),
    },
)
