import dagster as dg

from macro_agents.defs.resources.bigquery_warehouse import BigQueryWarehouseResource


@dg.asset_check(asset="analyze_economy_state")
def economy_state_data_check(md: BigQueryWarehouseResource) -> dg.AssetCheckResult:
    """Validate economy state analysis has recent outputs."""
    if not md.table_exists("economy_state_analysis"):
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.ERROR,
            metadata={"error": "economy_state_analysis table does not exist"},
        )

    df = md.execute_query(
        """
        SELECT
            COUNT(*) AS row_count,
            MAX(analysis_date) AS latest_analysis
        FROM economy_state_analysis
        """,
        read_only=True,
    )

    row_count = int(df["row_count"][0]) if not df.is_empty() else 0
    return dg.AssetCheckResult(
        passed=row_count > 0,
        severity=dg.AssetCheckSeverity.WARN,
        metadata={
            "row_count": row_count,
            "latest_analysis": str(df["latest_analysis"][0])
            if row_count > 0
            else "N/A",
        },
    )


@dg.asset_check(asset="generate_economic_narratives")
def economic_narratives_data_check(md: BigQueryWarehouseResource) -> dg.AssetCheckResult:
    """Validate economic narratives table has content."""
    if not md.table_exists("economic_indicator_narratives"):
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.ERROR,
            metadata={"error": "economic_indicator_narratives table does not exist"},
        )

    df = md.execute_query(
        """
        SELECT
            COUNT(*) AS row_count,
            COUNT(DISTINCT indicator_name) AS indicator_count
        FROM economic_indicator_narratives
        """,
        read_only=True,
    )

    row_count = int(df["row_count"][0]) if not df.is_empty() else 0
    return dg.AssetCheckResult(
        passed=row_count > 0,
        severity=dg.AssetCheckSeverity.WARN,
        metadata={
            "row_count": row_count,
            "indicator_count": int(df["indicator_count"][0]) if row_count > 0 else 0,
        },
    )


@dg.asset_check(asset="generate_investment_recommendations")
def investment_recommendations_data_check(
    md: BigQueryWarehouseResource,
) -> dg.AssetCheckResult:
    """Validate investment recommendations have been generated."""
    if not md.table_exists("investment_recommendations"):
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.ERROR,
            metadata={"error": "investment_recommendations table does not exist"},
        )

    df = md.execute_query(
        """
        SELECT
            COUNT(*) AS row_count,
            MAX(analysis_date) AS latest_analysis
        FROM investment_recommendations
        """,
        read_only=True,
    )

    row_count = int(df["row_count"][0]) if not df.is_empty() else 0
    return dg.AssetCheckResult(
        passed=row_count > 0,
        severity=dg.AssetCheckSeverity.WARN,
        metadata={
            "row_count": row_count,
            "latest_analysis": str(df["latest_analysis"][0])
            if row_count > 0
            else "N/A",
        },
    )


@dg.asset_check(asset="reddit_daily_summary")
def reddit_summary_data_check(md: BigQueryWarehouseResource) -> dg.AssetCheckResult:
    """Validate Reddit daily summaries exist."""
    if not md.table_exists("reddit_summaries"):
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.ERROR,
            metadata={"error": "reddit_summaries table does not exist"},
        )

    df = md.execute_query(
        """
        SELECT
            COUNT(*) AS row_count,
            MAX(summary_date) AS latest_summary
        FROM reddit_summaries
        """,
        read_only=True,
    )

    row_count = int(df["row_count"][0]) if not df.is_empty() else 0
    return dg.AssetCheckResult(
        passed=row_count > 0,
        severity=dg.AssetCheckSeverity.WARN,
        metadata={
            "row_count": row_count,
            "latest_summary": str(df["latest_summary"][0]) if row_count > 0 else "N/A",
        },
    )


@dg.asset_check(asset="news_weekly_summary")
def news_summary_data_check(md: BigQueryWarehouseResource) -> dg.AssetCheckResult:
    """Validate weekly news summaries exist."""
    if not md.table_exists("news_weekly_summaries"):
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.ERROR,
            metadata={"error": "news_weekly_summaries table does not exist"},
        )

    df = md.execute_query(
        """
        SELECT
            COUNT(*) AS row_count,
            MAX(summary_date) AS latest_summary
        FROM news_weekly_summaries
        """,
        read_only=True,
    )

    row_count = int(df["row_count"][0]) if not df.is_empty() else 0
    return dg.AssetCheckResult(
        passed=row_count > 0,
        severity=dg.AssetCheckSeverity.WARN,
        metadata={
            "row_count": row_count,
            "latest_summary": str(df["latest_summary"][0]) if row_count > 0 else "N/A",
        },
    )


analysis_checks = [
    economy_state_data_check,
    economic_narratives_data_check,
    investment_recommendations_data_check,
    reddit_summary_data_check,
    news_summary_data_check,
]
