"""Reddit sentiment scoring assets using VADER."""

from datetime import datetime, timezone

import dagster as dg
import polars as pl
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from macro_agents.defs.domains.social import reddit_partitions
from macro_agents.defs.resources.motherduck import MotherDuckResource

SENTIMENT_GROUP = "social_sentiment"


def _score_texts(texts: list[str]) -> list[dict]:
    """Score a batch of texts with VADER sentiment analyzer.

    Returns list of dicts with compound, pos, neg, neu scores and a label.
    """
    analyzer = SentimentIntensityAnalyzer()
    results = []
    for text in texts:
        scores = analyzer.polarity_scores(text)
        compound = scores["compound"]
        if compound >= 0.05:
            label = "positive"
        elif compound <= -0.05:
            label = "negative"
        else:
            label = "neutral"
        results.append(
            {
                "compound": compound,
                "positive": scores["pos"],
                "negative": scores["neg"],
                "neutral": scores["neu"],
                "label": label,
            }
        )
    return results


@dg.asset(
    group_name=SENTIMENT_GROUP,
    kinds={"ml", "duckdb"},
    partitions_def=reddit_partitions,
    deps=["reddit_post_content_raw", "reddit_comments_raw"],
    description="VADER sentiment scores for Reddit posts and comments",
)
def reddit_sentiment_scored(
    context: dg.AssetExecutionContext,
    md: MotherDuckResource,
) -> dg.MaterializeResult:
    """Score post titles, selftext, and comments with VADER sentiment."""

    subreddit = context.partition_key.keys_by_dimension["subreddit"]
    partition_date = context.partition_key.keys_by_dimension["date"]

    context.log.info(f"Scoring sentiment for r/{subreddit} on {partition_date}")

    scored_rows: list[dict] = []

    # Score post titles + selftext
    try:
        posts_df = md.execute_query(
            f"SELECT post_id, title, selftext FROM reddit_post_content_raw "
            f"WHERE subreddit = '{subreddit}' AND partition_date = '{partition_date}'"
        )
    except Exception as e:
        context.log.warning(f"Failed to query post content: {e}")
        posts_df = pl.DataFrame()

    for row in posts_df.iter_rows(named=True):
        # Score title
        title = row.get("title") or ""
        if title.strip():
            scores = _score_texts([title])[0]
            scored_rows.append(
                {
                    "content_id": row["post_id"],
                    "content_type": "post_title",
                    "subreddit": subreddit,
                    "partition_date": partition_date,
                    "text_preview": title[:200],
                    **scores,
                    "scored_at": datetime.now(timezone.utc),
                }
            )

        # Score selftext if present
        selftext = row.get("selftext") or ""
        if selftext.strip():
            scores = _score_texts([selftext])[0]
            scored_rows.append(
                {
                    "content_id": f"{row['post_id']}_body",
                    "content_type": "post_body",
                    "subreddit": subreddit,
                    "partition_date": partition_date,
                    "text_preview": selftext[:200],
                    **scores,
                    "scored_at": datetime.now(timezone.utc),
                }
            )

    # Score comments
    try:
        comments_df = md.execute_query(
            f"SELECT comment_id, body FROM reddit_comments_raw "
            f"WHERE subreddit = '{subreddit}' AND partition_date = '{partition_date}' "
            f"AND body IS NOT NULL AND length(body) > 0"
        )
    except Exception as e:
        context.log.warning(f"Failed to query comments: {e}")
        comments_df = pl.DataFrame()

    for row in comments_df.iter_rows(named=True):
        body = row.get("body") or ""
        if not body.strip():
            continue
        scores = _score_texts([body])[0]
        scored_rows.append(
            {
                "content_id": row["comment_id"],
                "content_type": "comment",
                "subreddit": subreddit,
                "partition_date": partition_date,
                "text_preview": body[:200],
                **scores,
                "scored_at": datetime.now(timezone.utc),
            }
        )

    if scored_rows:
        df = pl.DataFrame(scored_rows)
        md.upsert_data(
            "reddit_sentiment_scored",
            df,
            ["content_id", "content_type"],
            context=context,
        )

    # Compute aggregate stats
    post_scores = [r["compound"] for r in scored_rows if "post" in r["content_type"]]
    comment_scores = [
        r["compound"] for r in scored_rows if r["content_type"] == "comment"
    ]
    all_scores = [r["compound"] for r in scored_rows]

    def _avg(lst: list[float]) -> float:
        return sum(lst) / len(lst) if lst else 0.0

    def _pct_positive(lst: list[float]) -> float:
        return sum(1 for s in lst if s >= 0.05) / len(lst) * 100 if lst else 0.0

    context.log.info(
        f"Scored {len(scored_rows)} items: "
        f"avg={_avg(all_scores):.3f}, "
        f"pct_positive={_pct_positive(all_scores):.1f}%"
    )

    return dg.MaterializeResult(
        metadata={
            "subreddit": subreddit,
            "partition_date": partition_date,
            "total_scored": len(scored_rows),
            "posts_scored": len(post_scores),
            "comments_scored": len(comment_scores),
            "avg_sentiment": round(_avg(all_scores), 4),
            "avg_post_sentiment": round(_avg(post_scores), 4),
            "avg_comment_sentiment": round(_avg(comment_scores), 4),
            "pct_positive": round(_pct_positive(all_scores), 1),
            "pct_negative": round(
                sum(1 for s in all_scores if s <= -0.05) / len(all_scores) * 100
                if all_scores
                else 0.0,
                1,
            ),
        }
    )


defs = dg.Definitions(
    assets=[reddit_sentiment_scored],
)
