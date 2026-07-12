"""Reddit ticker mention extraction asset."""

import re
from datetime import datetime, timezone

import dagster as dg
import polars as pl

from macro_agents.defs.domains.social import reddit_partitions
from macro_agents.defs.resources.bigquery_warehouse import BigQueryWarehouseResource

TICKER_GROUP = "social_tickers"

# Pattern matches $AAPL, $TSLA, etc. (1-5 uppercase letters after $)
TICKER_PATTERN = re.compile(r"\$([A-Z]{1,5})\b")

# Common false positives that look like tickers but aren't
TICKER_BLOCKLIST = frozenset(
    {
        "USD",
        "USA",
        "GDP",
        "CPI",
        "PPI",
        "PCE",
        "FED",
        "SEC",
        "FBI",
        "CIA",
        "CEO",
        "CFO",
        "CTO",
        "COO",
        "IPO",
        "ETF",
        "NYSE",
        "ATH",
        "ATL",
        "IMO",
        "FOMO",
        "YOLO",
        "HODL",
        "LMAO",
        "EDIT",
        "PSA",
        "TLDR",
        "EPS",
        "PE",
        "PB",
        "ROI",
        "ROE",
        "YOY",
        "QOQ",
        "MOM",
        "APR",
        "APY",
        "IRA",
        "K",  # $401K patterns
    }
)


def extract_tickers(text: str) -> list[str]:
    """Extract stock ticker symbols from text.

    Matches $AAPL-style mentions and filters out common false positives.
    Returns deduplicated list of ticker strings.
    """
    if not text:
        return []
    matches = TICKER_PATTERN.findall(text)
    return list(dict.fromkeys(m for m in matches if m not in TICKER_BLOCKLIST))


@dg.asset(
    group_name=TICKER_GROUP,
    kinds={"python", "bigquery"},
    partitions_def=reddit_partitions,
    deps=["reddit_post_content_raw", "reddit_comments_raw"],
    description="Stock ticker mentions extracted from Reddit posts and comments",
)
def reddit_ticker_mentions(
    context: dg.AssetExecutionContext,
    bq: BigQueryWarehouseResource,
) -> dg.MaterializeResult:
    """Extract $TICKER mentions from post titles, selftext, and comments."""

    subreddit = context.partition_key.keys_by_dimension["subreddit"]
    partition_date = context.partition_key.keys_by_dimension["date"]

    context.log.info(f"Extracting tickers for r/{subreddit} on {partition_date}")

    mention_rows: list[dict] = []

    # Extract from posts
    try:
        posts_df = bq.execute_query(
            f"SELECT post_id, title, selftext FROM reddit_post_content_raw "
            f"WHERE subreddit = '{subreddit}' AND partition_date = '{partition_date}'"
        )
    except Exception as e:
        context.log.warning(f"Failed to query post content: {e}")
        posts_df = pl.DataFrame()

    for row in posts_df.iter_rows(named=True):
        post_id = row["post_id"]
        # Check title
        title = row.get("title") or ""
        for ticker in extract_tickers(title):
            mention_rows.append(
                {
                    "ticker": ticker,
                    "content_id": post_id,
                    "content_type": "post_title",
                    "subreddit": subreddit,
                    "partition_date": partition_date,
                    "context_text": title[:200],
                    "extracted_at": datetime.now(timezone.utc),
                }
            )
        # Check selftext
        selftext = row.get("selftext") or ""
        for ticker in extract_tickers(selftext):
            mention_rows.append(
                {
                    "ticker": ticker,
                    "content_id": post_id,
                    "content_type": "post_body",
                    "subreddit": subreddit,
                    "partition_date": partition_date,
                    "context_text": selftext[:200],
                    "extracted_at": datetime.now(timezone.utc),
                }
            )

    # Extract from comments
    try:
        comments_df = bq.execute_query(
            f"SELECT comment_id, body FROM reddit_comments_raw "
            f"WHERE subreddit = '{subreddit}' AND partition_date = '{partition_date}' "
            f"AND body IS NOT NULL AND length(body) > 0"
        )
    except Exception as e:
        context.log.warning(f"Failed to query comments: {e}")
        comments_df = pl.DataFrame()

    for row in comments_df.iter_rows(named=True):
        body = row.get("body") or ""
        for ticker in extract_tickers(body):
            mention_rows.append(
                {
                    "ticker": ticker,
                    "content_id": row["comment_id"],
                    "content_type": "comment",
                    "subreddit": subreddit,
                    "partition_date": partition_date,
                    "context_text": body[:200],
                    "extracted_at": datetime.now(timezone.utc),
                }
            )

    if mention_rows:
        df = pl.DataFrame(mention_rows)
        bq.upsert_data(
            "reddit_ticker_mentions",
            df,
            ["ticker", "content_id", "content_type"],
            context=context,
        )

    # Compute summary stats
    ticker_counts: dict[str, int] = {}
    for row in mention_rows:
        ticker_counts[row["ticker"]] = ticker_counts.get(row["ticker"], 0) + 1
    top_tickers = sorted(ticker_counts.items(), key=lambda x: x[1], reverse=True)[:10]

    context.log.info(
        f"Extracted {len(mention_rows)} ticker mentions "
        f"({len(ticker_counts)} unique tickers)"
    )

    return dg.MaterializeResult(
        metadata={
            "subreddit": subreddit,
            "partition_date": partition_date,
            "total_mentions": len(mention_rows),
            "unique_tickers": len(ticker_counts),
            "top_tickers": str(top_tickers[:10]),
        }
    )


defs = dg.Definitions(
    assets=[reddit_ticker_mentions],
)
