"""Unit tests for social domain assets."""

from datetime import datetime, timezone
from unittest.mock import Mock

import dagster as dg
import polars as pl
import pytest
from macro_agents.defs.domains.social import (
    reddit_comments_raw,
    reddit_content_embeddings,
    reddit_post_content_raw,
)


def _partition_key(subreddit: str = "investing", date: str = "2024-01-15"):
    return dg.MultiPartitionKey({"subreddit": subreddit, "date": date})


@pytest.fixture()
def asset_context():
    with dg.build_asset_context(
        partition_key=_partition_key(),
    ) as context:
        yield context


def _meta_val(metadata: dict, key: str):
    """Extract raw value from Dagster metadata, handling both wrapped and unwrapped."""
    val = metadata[key]
    if hasattr(val, "value"):
        return val.value
    if hasattr(val, "text"):
        return val.text
    return val


class TestRedditPostContentRaw:
    """Tests for reddit_post_content_raw asset."""

    def test_fetches_content_for_posts(self, asset_context):
        context = asset_context
        reddit = Mock()
        md = Mock()

        md.execute_query.return_value = pl.DataFrame(
            {
                "post_id": ["abc123", "def456"],
                "permalink": [
                    "/r/investing/comments/abc123/",
                    "/r/investing/comments/def456/",
                ],
            }
        )

        reddit.get_post_content.side_effect = [
            {
                "post_id": "abc123",
                "title": "Stock analysis",
                "selftext": "Detailed analysis here",
                "links": ["https://example.com"],
                "author": "analyst",
                "score": 100,
                "url": "",
                "created_utc": datetime(2024, 1, 15, tzinfo=timezone.utc),
            },
            {
                "post_id": "def456",
                "title": "Market news",
                "selftext": "",
                "links": [],
                "author": "newsbot",
                "score": 50,
                "url": "https://news.com",
                "created_utc": datetime(2024, 1, 15, tzinfo=timezone.utc),
            },
        ]

        result = reddit_post_content_raw(context, reddit, md)

        assert _meta_val(result.metadata, "num_posts") == 2
        assert _meta_val(result.metadata, "failed") == 0
        assert _meta_val(result.metadata, "skipped") == 0
        md.upsert_data.assert_called_once()

    def test_handles_empty_posts(self, asset_context):
        context = asset_context
        reddit = Mock()
        md = Mock()

        md.execute_query.return_value = pl.DataFrame(
            schema={"post_id": pl.Utf8, "permalink": pl.Utf8}
        )

        result = reddit_post_content_raw(context, reddit, md)

        assert _meta_val(result.metadata, "num_posts") == 0
        reddit.get_post_content.assert_not_called()

    def test_handles_fetch_failure(self, asset_context):
        context = asset_context
        reddit = Mock()
        md = Mock()

        md.execute_query.return_value = pl.DataFrame(
            {
                "post_id": ["abc123"],
                "permalink": ["/r/investing/comments/abc123/"],
            }
        )
        reddit.get_post_content.side_effect = Exception("Network error")

        result = reddit_post_content_raw(context, reddit, md)

        assert _meta_val(result.metadata, "failed") == 1
        assert _meta_val(result.metadata, "num_posts") == 0

    def test_skips_empty_permalink(self, asset_context):
        context = asset_context
        reddit = Mock()
        md = Mock()

        md.execute_query.return_value = pl.DataFrame(
            {"post_id": ["abc123"], "permalink": [""]}
        )

        result = reddit_post_content_raw(context, reddit, md)

        assert _meta_val(result.metadata, "skipped") == 1
        reddit.get_post_content.assert_not_called()


class TestRedditCommentsRaw:
    """Tests for reddit_comments_raw asset."""

    def test_fetches_comments_for_posts(self, asset_context):
        context = asset_context
        reddit = Mock()
        md = Mock()

        md.execute_query.return_value = pl.DataFrame(
            {
                "post_id": ["abc123"],
                "permalink": ["/r/investing/comments/abc123/"],
            }
        )

        reddit.get_post_comments.return_value = pl.DataFrame(
            {
                "comment_id": ["c1", "c2"],
                "author": ["user1", "user2"],
                "body": ["Great post!", "I disagree"],
                "score": [10, -2],
                "created_utc": [
                    datetime(2024, 1, 15, tzinfo=timezone.utc),
                    datetime(2024, 1, 15, tzinfo=timezone.utc),
                ],
                "parent_id": ["t3_abc123", "t3_abc123"],
                "depth": [0, 0],
                "links": [[], ["https://source.com"]],
            }
        )

        result = reddit_comments_raw(context, reddit, md)

        assert _meta_val(result.metadata, "num_comments") == 2
        assert _meta_val(result.metadata, "failed") == 0
        md.upsert_data.assert_called_once()

    def test_handles_empty_comments(self, asset_context):
        context = asset_context
        reddit = Mock()
        md = Mock()

        md.execute_query.return_value = pl.DataFrame(
            {
                "post_id": ["abc123"],
                "permalink": ["/r/investing/comments/abc123/"],
            }
        )
        reddit.get_post_comments.return_value = pl.DataFrame(
            schema={
                "comment_id": pl.Utf8,
                "author": pl.Utf8,
                "body": pl.Utf8,
                "score": pl.Int64,
                "created_utc": pl.Datetime,
                "parent_id": pl.Utf8,
                "depth": pl.Int64,
                "links": pl.List(pl.Utf8),
            }
        )

        result = reddit_comments_raw(context, reddit, md)

        assert _meta_val(result.metadata, "skipped") == 1
        md.upsert_data.assert_not_called()

    def test_handles_query_failure(self, asset_context):
        context = asset_context
        reddit = Mock()
        md = Mock()

        md.execute_query.side_effect = Exception("DB connection error")

        result = reddit_comments_raw(context, reddit, md)

        assert _meta_val(result.metadata, "num_comments") == 0
        error_val = _meta_val(result.metadata, "error")
        assert "DB connection error" in str(error_val)


class TestRedditContentEmbeddings:
    """Tests for reddit_content_embeddings asset."""

    def test_embeds_posts_and_comments(self, asset_context):
        context = asset_context
        ollama = Mock()
        md = Mock()

        md.execute_query.side_effect = [
            pl.DataFrame(
                {
                    "post_id": ["abc123"],
                    "title": ["Market Analysis"],
                    "selftext": ["Deep dive into markets"],
                }
            ),
            pl.DataFrame(
                {
                    "comment_id": ["c1"],
                    "body": ["Interesting analysis"],
                }
            ),
        ]

        ollama.get_embeddings.return_value = [[0.1] * 768]

        result = reddit_content_embeddings(context, ollama, md)

        assert _meta_val(result.metadata, "embedded") == 2
        assert _meta_val(result.metadata, "skipped") == 0
        assert _meta_val(result.metadata, "failed") == 0
        md.upsert_data.assert_called_once()

    def test_skips_empty_text(self, asset_context):
        context = asset_context
        ollama = Mock()
        md = Mock()

        md.execute_query.side_effect = [
            pl.DataFrame(
                {
                    "post_id": ["abc123"],
                    "title": [""],
                    "selftext": [""],
                }
            ),
            pl.DataFrame(
                {
                    "comment_id": ["c1"],
                    "body": [""],
                }
            ),
        ]

        result = reddit_content_embeddings(context, ollama, md)

        assert _meta_val(result.metadata, "embedded") == 0
        assert _meta_val(result.metadata, "skipped") == 2
        ollama.get_embeddings.assert_not_called()

    def test_falls_back_to_title_when_no_selftext(self, asset_context):
        context = asset_context
        ollama = Mock()
        md = Mock()

        md.execute_query.side_effect = [
            pl.DataFrame(
                {
                    "post_id": ["abc123"],
                    "title": ["Important Market Update"],
                    "selftext": [""],
                }
            ),
            pl.DataFrame(schema={"comment_id": pl.Utf8, "body": pl.Utf8}),
        ]

        ollama.get_embeddings.return_value = [[0.1] * 768]

        result = reddit_content_embeddings(context, ollama, md)

        assert _meta_val(result.metadata, "embedded") == 1
        ollama.get_embeddings.assert_called_once_with(["Important Market Update"])

    def test_handles_embedding_failure(self, asset_context):
        context = asset_context
        ollama = Mock()
        md = Mock()

        md.execute_query.side_effect = [
            pl.DataFrame(
                {
                    "post_id": ["abc123"],
                    "title": ["Test"],
                    "selftext": ["Some text"],
                }
            ),
            pl.DataFrame(schema={"comment_id": pl.Utf8, "body": pl.Utf8}),
        ]

        ollama.get_embeddings.side_effect = Exception("Ollama connection refused")

        result = reddit_content_embeddings(context, ollama, md)

        assert _meta_val(result.metadata, "failed") == 1
        assert _meta_val(result.metadata, "embedded") == 0

    def test_handles_query_failure_gracefully(self, asset_context):
        context = asset_context
        ollama = Mock()
        md = Mock()

        md.execute_query.side_effect = Exception("DB error")

        result = reddit_content_embeddings(context, ollama, md)

        assert _meta_val(result.metadata, "embedded") == 0


class TestRedditSentimentScored:
    """Tests for reddit_sentiment_scored asset."""

    def test_scores_posts_and_comments(self, asset_context):
        from macro_agents.defs.domains.social_sentiment import reddit_sentiment_scored

        context = asset_context
        md = Mock()

        md.execute_query.side_effect = [
            pl.DataFrame(
                {
                    "post_id": ["abc123"],
                    "title": ["Markets are soaring today!"],
                    "selftext": ["Everything is up, great news for investors."],
                }
            ),
            pl.DataFrame(
                {
                    "comment_id": ["c1"],
                    "body": ["This is terrible, the economy is crashing."],
                }
            ),
        ]

        result = reddit_sentiment_scored(context, md)

        # 1 title + 1 selftext + 1 comment = 3 scored items
        assert _meta_val(result.metadata, "total_scored") == 3
        assert _meta_val(result.metadata, "posts_scored") == 2
        assert _meta_val(result.metadata, "comments_scored") == 1
        md.upsert_data.assert_called_once()

    def test_skips_empty_text(self, asset_context):
        from macro_agents.defs.domains.social_sentiment import reddit_sentiment_scored

        context = asset_context
        md = Mock()

        md.execute_query.side_effect = [
            pl.DataFrame(
                {
                    "post_id": ["abc123"],
                    "title": [""],
                    "selftext": [""],
                }
            ),
            pl.DataFrame(
                {
                    "comment_id": ["c1"],
                    "body": [""],
                }
            ),
        ]

        result = reddit_sentiment_scored(context, md)

        assert _meta_val(result.metadata, "total_scored") == 0
        md.upsert_data.assert_not_called()

    def test_handles_query_failure(self, asset_context):
        from macro_agents.defs.domains.social_sentiment import reddit_sentiment_scored

        context = asset_context
        md = Mock()

        md.execute_query.side_effect = Exception("DB error")

        result = reddit_sentiment_scored(context, md)

        assert _meta_val(result.metadata, "total_scored") == 0

    def test_sentiment_labels_correct(self):
        from macro_agents.defs.domains.social_sentiment import _score_texts

        results = _score_texts(
            [
                "This is amazing and wonderful!",
                "This is terrible and awful!",
                "The meeting is at 3pm.",
            ]
        )

        assert results[0]["label"] == "positive"
        assert results[0]["compound"] > 0.05
        assert results[1]["label"] == "negative"
        assert results[1]["compound"] < -0.05
        assert results[2]["label"] == "neutral"

    def test_content_types_correct(self, asset_context):
        from macro_agents.defs.domains.social_sentiment import reddit_sentiment_scored

        context = asset_context
        md = Mock()

        md.execute_query.side_effect = [
            pl.DataFrame(
                {
                    "post_id": ["abc123"],
                    "title": ["Great stock pick!"],
                    "selftext": ["Buy this stock now."],
                }
            ),
            pl.DataFrame(schema={"comment_id": pl.Utf8, "body": pl.Utf8}),
        ]

        reddit_sentiment_scored(context, md)

        # Verify the upserted data has correct content types
        call_args = md.upsert_data.call_args
        df = call_args[0][1]
        content_types = df["content_type"].to_list()
        assert "post_title" in content_types
        assert "post_body" in content_types


class TestExtractTickers:
    """Tests for ticker extraction utility."""

    def test_basic_ticker_extraction(self):
        from macro_agents.defs.domains.social_tickers import extract_tickers

        assert extract_tickers("I'm buying $AAPL and $TSLA today") == ["AAPL", "TSLA"]

    def test_filters_blocklist(self):
        from macro_agents.defs.domains.social_tickers import extract_tickers

        result = extract_tickers("The $GDP report shows $CPI rising, but $AAPL is up")
        assert result == ["AAPL"]
        assert "GDP" not in result
        assert "CPI" not in result

    def test_deduplicates(self):
        from macro_agents.defs.domains.social_tickers import extract_tickers

        result = extract_tickers("$AAPL is great, I love $AAPL so much")
        assert result == ["AAPL"]

    def test_empty_text(self):
        from macro_agents.defs.domains.social_tickers import extract_tickers

        assert extract_tickers("") == []
        assert extract_tickers(None) == []

    def test_no_tickers(self):
        from macro_agents.defs.domains.social_tickers import extract_tickers

        assert extract_tickers("Just a regular comment about the market") == []

    def test_ignores_lowercase(self):
        from macro_agents.defs.domains.social_tickers import extract_tickers

        assert extract_tickers("spending $50 on lunch") == []


class TestRedditTickerMentions:
    """Tests for reddit_ticker_mentions asset."""

    def test_extracts_from_posts_and_comments(self, asset_context):
        from macro_agents.defs.domains.social_tickers import reddit_ticker_mentions

        context = asset_context
        md = Mock()

        md.execute_query.side_effect = [
            pl.DataFrame(
                {
                    "post_id": ["abc123"],
                    "title": ["Why $AAPL will beat earnings"],
                    "selftext": ["Also watching $MSFT and $GOOG"],
                }
            ),
            pl.DataFrame(
                {
                    "comment_id": ["c1"],
                    "body": ["Don't forget $TSLA!"],
                }
            ),
        ]

        result = reddit_ticker_mentions(context, md)

        assert _meta_val(result.metadata, "total_mentions") == 4
        assert _meta_val(result.metadata, "unique_tickers") == 4
        md.upsert_data.assert_called_once()

    def test_no_tickers_found(self, asset_context):
        from macro_agents.defs.domains.social_tickers import reddit_ticker_mentions

        context = asset_context
        md = Mock()

        md.execute_query.side_effect = [
            pl.DataFrame(
                {
                    "post_id": ["abc123"],
                    "title": ["General market discussion"],
                    "selftext": ["No tickers here"],
                }
            ),
            pl.DataFrame(
                {
                    "comment_id": ["c1"],
                    "body": ["Just chatting about the economy"],
                }
            ),
        ]

        result = reddit_ticker_mentions(context, md)

        assert _meta_val(result.metadata, "total_mentions") == 0
        assert _meta_val(result.metadata, "unique_tickers") == 0
        md.upsert_data.assert_not_called()

    def test_handles_query_failure(self, asset_context):
        from macro_agents.defs.domains.social_tickers import reddit_ticker_mentions

        context = asset_context
        md = Mock()

        md.execute_query.side_effect = Exception("DB error")

        result = reddit_ticker_mentions(context, md)

        assert _meta_val(result.metadata, "total_mentions") == 0
