from datetime import date, timedelta

import dagster as dg

from macro_agents.defs.resources.bigquery_warehouse import BigQueryWarehouseResource


@dg.asset_check(asset="reddit_posts_raw")
def reddit_posts_data_check(bq: BigQueryWarehouseResource) -> dg.AssetCheckResult:
    """Validate reddit posts data has recent entries with non-null text."""
    if not bq.table_exists("reddit_posts_raw"):
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.ERROR,
            metadata={"error": "reddit_posts_raw table does not exist"},
        )

    cutoff = date.today() - timedelta(days=7)
    df = bq.execute_query(
        f"""
        SELECT
            COUNT(*) AS row_count,
            COUNT(DISTINCT subreddit) AS subreddit_count,
            MAX(created_utc) AS max_date,
            SUM(CASE WHEN title IS NULL OR title = '' THEN 1 ELSE 0 END) AS null_titles
        FROM reddit_posts_raw
        WHERE CAST(created_utc AS DATE) >= '{cutoff}'
        """,
        read_only=True,
    )

    row_count = int(df["row_count"][0]) if not df.is_empty() else 0
    passed = row_count > 0

    metadata = {"recent_row_count": row_count, "cutoff": str(cutoff)}
    if passed:
        metadata["subreddit_count"] = int(df["subreddit_count"][0])
        metadata["max_date"] = str(df["max_date"][0])
        metadata["null_titles"] = int(df["null_titles"][0])

    return dg.AssetCheckResult(
        passed=passed,
        severity=dg.AssetCheckSeverity.WARN,
        metadata=metadata,
    )


@dg.asset_check(asset="reddit_post_content_raw")
def reddit_post_content_data_check(
    bq: BigQueryWarehouseResource,
) -> dg.AssetCheckResult:
    """Validate reddit post content exists and links back to posts."""
    if not bq.table_exists("reddit_post_content_raw"):
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.ERROR,
            metadata={"error": "reddit_post_content_raw table does not exist"},
        )

    df = bq.execute_query(
        """
        SELECT
            COUNT(*) AS row_count,
            SUM(CASE WHEN content IS NULL OR content = '' THEN 1 ELSE 0 END) AS empty_content
        FROM reddit_post_content_raw
        """,
        read_only=True,
    )

    if df.is_empty() or df["row_count"][0] == 0:
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.ERROR,
            metadata={"error": "reddit_post_content_raw table is empty"},
        )

    row_count = int(df["row_count"][0])
    empty_content = int(df["empty_content"][0])
    empty_pct = round(empty_content / row_count * 100, 2) if row_count > 0 else 0

    return dg.AssetCheckResult(
        passed=empty_pct < 50,
        severity=dg.AssetCheckSeverity.WARN,
        metadata={
            "row_count": row_count,
            "empty_content_pct": empty_pct,
        },
    )


@dg.asset_check(asset="reddit_comments_raw")
def reddit_comments_data_check(bq: BigQueryWarehouseResource) -> dg.AssetCheckResult:
    """Validate reddit comments exist with recent data."""
    if not bq.table_exists("reddit_comments_raw"):
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.ERROR,
            metadata={"error": "reddit_comments_raw table does not exist"},
        )

    cutoff = date.today() - timedelta(days=7)
    df = bq.execute_query(
        f"""
        SELECT
            COUNT(*) AS row_count,
            MAX(created_utc) AS max_date
        FROM reddit_comments_raw
        WHERE CAST(created_utc AS DATE) >= '{cutoff}'
        """,
        read_only=True,
    )

    row_count = int(df["row_count"][0]) if not df.is_empty() else 0

    return dg.AssetCheckResult(
        passed=row_count > 0,
        severity=dg.AssetCheckSeverity.WARN,
        metadata={
            "recent_row_count": row_count,
            "cutoff": str(cutoff),
            "max_date": str(df["max_date"][0]) if row_count > 0 else "N/A",
        },
    )


@dg.asset_check(asset="reddit_content_embeddings")
def reddit_embeddings_data_check(bq: BigQueryWarehouseResource) -> dg.AssetCheckResult:
    """Validate embeddings exist and have correct dimensionality."""
    if not bq.table_exists("reddit_content_embeddings"):
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.ERROR,
            metadata={"error": "reddit_content_embeddings table does not exist"},
        )

    df = bq.execute_query(
        """
        SELECT
            COUNT(*) AS row_count,
            SUM(CASE WHEN embedding IS NULL THEN 1 ELSE 0 END) AS null_embeddings
        FROM reddit_content_embeddings
        """,
        read_only=True,
    )

    if df.is_empty() or df["row_count"][0] == 0:
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.ERROR,
            metadata={"error": "reddit_content_embeddings table is empty"},
        )

    row_count = int(df["row_count"][0])
    null_embeddings = int(df["null_embeddings"][0])
    null_pct = round(null_embeddings / row_count * 100, 2) if row_count > 0 else 0

    return dg.AssetCheckResult(
        passed=null_pct < 10,
        severity=dg.AssetCheckSeverity.WARN,
        metadata={
            "row_count": row_count,
            "null_embedding_pct": null_pct,
        },
    )


social_checks = [
    reddit_posts_data_check,
    reddit_post_content_data_check,
    reddit_comments_data_check,
    reddit_embeddings_data_check,
]
