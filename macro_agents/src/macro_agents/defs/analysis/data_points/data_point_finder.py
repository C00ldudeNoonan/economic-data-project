"""Statistical detection functions for identifying interesting data points.

This module provides functions to detect 4 types of interesting movements:
1. Big short-term moves (>1.5 std dev from mean)
2. Trend changes/inflections (momentum reversals)
3. Correlation anomalies (strong relationships with forward returns)
4. Statistical outliers (extreme percentiles)
"""

import polars as pl

from macro_agents.defs.resources.bigquery_warehouse import BigQueryWarehouseResource


def query_data_for_findings(
    md: BigQueryWarehouseResource, week_start: str, week_end: str
) -> dict[str, pl.DataFrame]:
    """
    Query all relevant data sources for interesting data point detection.

    Args:
        md: BigQueryWarehouseResource for database queries
        week_start: Week start date (YYYY-MM-DD)
        week_end: Week end date (YYYY-MM-DD)

    Returns:
        Dict with DataFrames for: economic, market, commodity, correlation, sentiment
    """
    # Economic indicators with 3m, 6m, 1y changes
    economic_query = """
    SELECT
        series_code,
        series_name,
        month,
        current_value,
        pct_change_3m,
        pct_change_6m,
        pct_change_1y,
        date_grain
    FROM agent_fred_series_latest_aggregates
    WHERE month >= DATE_TRUNC('month', CAST(? AS DATE) - INTERVAL '12 months')
      AND month <= CAST(? AS DATE)
      AND current_value IS NOT NULL
    ORDER BY series_code, month DESC
    """

    # Market data from summary tables (12 weeks)
    market_query = """
    SELECT
        ticker AS symbol,
        time_period,
        total_return_pct,
        annualized_volatility_pct,
        win_rate_pct,
        worst_day_pct_change,
        best_day_pct_change
    FROM agent_market_performance
    WHERE time_period IN ('12_weeks', '26_weeks')
    """

    # Commodity data from summary tables
    commodity_query = """
    SELECT
        commodity AS symbol,
        time_period,
        total_return_pct,
        annualized_volatility_pct,
        win_rate_pct,
        worst_day_pct_change,
        best_day_pct_change
    FROM agent_commodity_performance
    WHERE time_period IN ('12_weeks', '26_weeks')
    """

    # Correlation data showing economic indicators vs forward returns
    correlation_query = """
    SELECT
        symbol,
        series_name,
        category,
        economic_category,
        observation_count,
        corr_econ_q1_returns,
        corr_econ_q2_returns,
        corr_econ_q3_returns,
        avg_q1_return_when_econ_growing,
        avg_q1_return_when_econ_declining
    FROM agent_leading_econ_return_indicator
    WHERE observation_count >= 12
    """

    # Sentiment trends with momentum
    sentiment_query = """
    SELECT
        date,
        avg_score,
        median_score,
        total_posts,
        weekly_avg_score,
        score_momentum_pct
    FROM agent_reddit_sentiment_trends
    WHERE date >= CAST(? AS DATE) - INTERVAL '3 months'
      AND date <= CAST(? AS DATE)
    ORDER BY date DESC
    """

    # Execute queries
    economic_df = md.execute_query(economic_query, params=[week_start, week_end])
    market_df = md.execute_query(market_query)
    commodity_df = md.execute_query(commodity_query)
    correlation_df = md.execute_query(correlation_query)
    sentiment_df = md.execute_query(sentiment_query, params=[week_start, week_end])

    return {
        "economic": economic_df,
        "market": market_df,
        "commodity": commodity_df,
        "correlation": correlation_df,
        "sentiment": sentiment_df,
    }


def detect_big_moves(df: pl.DataFrame, threshold_std_dev: float = 1.5) -> pl.DataFrame:
    """
    Detect changes exceeding threshold standard deviations.

    Analyzes percent changes (3m, 6m, 1y) and identifies values
    that are statistical outliers (>1.5 std dev from mean).

    Args:
        df: DataFrame with columns: series_code, series_name, pct_change_3m,
            pct_change_6m, pct_change_1y, current_value
        threshold_std_dev: Z-score threshold for significance (default 1.5)

    Returns:
        DataFrame with findings containing:
        - data_point: Metric name
        - metric_category: 'economic'
        - current_value: Current value
        - change_period: '3m', '6m', or '1y'
        - change_pct: Percent change value
        - z_score: Z-score of the change
        - significance_score: Absolute z-score for ranking
        - finding_type: 'big_short_term_move'
    """
    if df.is_empty():
        return _get_empty_findings_df()

    findings = []

    # Analyze each change period
    for period, col in [
        ("3m", "pct_change_3m"),
        ("6m", "pct_change_6m"),
        ("1y", "pct_change_1y"),
    ]:
        if col not in df.columns:
            continue

        # Filter non-null values
        valid_df = df.filter(pl.col(col).is_not_null())

        if valid_df.is_empty():
            continue

        # Calculate mean and std dev
        stats = valid_df.select(
            [
                pl.col(col).mean().alias("mean"),
                pl.col(col).std().alias("std"),
            ]
        ).row(0, named=True)

        if stats["std"] is None or stats["std"] == 0:
            continue

        # Calculate z-scores
        with_zscores = valid_df.with_columns(
            [
                ((pl.col(col) - stats["mean"]) / stats["std"]).alias("z_score"),
            ]
        )

        # Filter for significant moves
        significant = with_zscores.filter(pl.col("z_score").abs() >= threshold_std_dev)

        # Convert to findings
        for row in significant.iter_rows(named=True):
            findings.append(
                {
                    "data_point": row["series_name"],
                    "metric_category": "economic",
                    "current_value": row.get("current_value"),
                    "change_period": period,
                    "change_pct": row[col],
                    "z_score": row["z_score"],
                    "significance_score": abs(row["z_score"]),
                    "finding_type": "big_short_term_move",
                }
            )

    if not findings:
        return _get_empty_findings_df()

    return pl.DataFrame(findings)


def detect_trend_changes(df: pl.DataFrame, lookback_months: int = 6) -> pl.DataFrame:
    """
    Detect momentum inflections (trend reversals).

    Identifies when an economic indicator's momentum shifts direction,
    indicating a potential trend change or inflection point.

    Args:
        df: DataFrame with columns: series_code, series_name, month,
            current_value, pct_change_6m
        lookback_months: Number of months to look back for trend (default 6)

    Returns:
        DataFrame with findings containing inflection points
    """
    if df.is_empty():
        return _get_empty_findings_df()

    findings = []

    # Group by series and sort by month
    series_groups = df.group_by("series_code").agg(pl.all().sort_by("month"))

    for group_data in series_groups.iter_rows(named=True):
        series_code = group_data["series_code"]
        series_name_list = group_data["series_name"]
        values_list = group_data["current_value"]

        if len(values_list) < lookback_months + 1:
            continue

        # Get the series name (should be constant within group)
        series_name = series_name_list[0] if series_name_list else series_code

        # Calculate momentum (rolling average slope)
        # Most recent data is first (DESC order), so reverse for calculation
        values = list(reversed(values_list))

        # Calculate 3-month rolling momentum
        for i in range(3, len(values)):
            if i < lookback_months:
                continue

            # Current momentum: change over last 3 months
            current_momentum = values[i] - values[i - 3]

            # Previous momentum: change over 3 months before that
            previous_momentum = values[i - 3] - values[i - 6]

            # Detect sign change (inflection)
            if current_momentum * previous_momentum < 0:  # Opposite signs
                # Calculate magnitude of inflection
                inflection_magnitude = abs(current_momentum - previous_momentum)

                # Only report if magnitude is meaningful
                if inflection_magnitude > 0:
                    findings.append(
                        {
                            "data_point": series_name,
                            "metric_category": "economic",
                            "current_value": values[i],
                            "change_period": "6m",
                            "change_pct": None,
                            "z_score": None,
                            "significance_score": inflection_magnitude,
                            "finding_type": "trend_change_inflection",
                        }
                    )
                    break  # Only report most recent inflection per series

    if not findings:
        return _get_empty_findings_df()

    return pl.DataFrame(findings)


def detect_correlation_anomalies(
    corr_df: pl.DataFrame, threshold: float = 0.3
) -> pl.DataFrame:
    """
    Identify significant correlations between economic data and forward returns.

    Finds economic indicators with strong correlations (|r| >= 0.3) to future
    quarterly returns, which could be leading indicators.

    Args:
        corr_df: DataFrame from leading_econ_return_indicator with columns:
            series_name, symbol, corr_econ_q1_returns, corr_econ_q2_returns,
            corr_econ_q3_returns
        threshold: Minimum |correlation| to flag as significant (default 0.3)

    Returns:
        DataFrame with findings for significant correlations
    """
    if corr_df.is_empty():
        return _get_empty_findings_df()

    # Calculate max absolute correlation across Q1, Q2, Q3
    with_max_corr = corr_df.with_columns(
        [
            pl.max_horizontal(
                [
                    pl.col("corr_econ_q1_returns").abs(),
                    pl.col("corr_econ_q2_returns").abs(),
                    pl.col("corr_econ_q3_returns").abs(),
                ]
            ).alias("max_abs_corr"),
        ]
    )

    # Filter for significant correlations
    significant = with_max_corr.filter(pl.col("max_abs_corr") >= threshold)

    if significant.is_empty():
        return _get_empty_findings_df()

    # Convert to findings
    findings = []
    for row in significant.iter_rows(named=True):
        # Determine which quarter has the strongest correlation
        corrs = {
            "Q1": row.get("corr_econ_q1_returns", 0) or 0,
            "Q2": row.get("corr_econ_q2_returns", 0) or 0,
            "Q3": row.get("corr_econ_q3_returns", 0) or 0,
        }
        strongest_quarter = max(corrs, key=lambda k: abs(corrs[k]))
        strongest_corr = corrs[strongest_quarter]

        data_point_name = f"{row['series_name']} vs {row['symbol']}"

        findings.append(
            {
                "data_point": data_point_name,
                "metric_category": "correlation",
                "current_value": strongest_corr,
                "change_period": strongest_quarter,
                "change_pct": None,
                "z_score": None,
                "significance_score": abs(strongest_corr),
                "finding_type": "correlation_anomaly",
            }
        )

    return pl.DataFrame(findings)


def detect_statistical_outliers(
    df: pl.DataFrame, percentile_low: float = 10, percentile_high: float = 90
) -> pl.DataFrame:
    """
    Find extreme values (top/bottom percentiles).

    Identifies market/commodity performance that falls in extreme percentiles,
    indicating unusual strength or weakness.

    Args:
        df: DataFrame with columns: symbol, time_period, total_return_pct,
            annualized_volatility_pct
        percentile_low: Low percentile threshold (default 10)
        percentile_high: High percentile threshold (default 90)

    Returns:
        DataFrame with findings for statistical outliers
    """
    if df.is_empty():
        return _get_empty_findings_df()

    findings = []

    # Focus on 12-week returns for "recent move" detection
    recent_df = df.filter(pl.col("time_period") == "12_weeks")

    if recent_df.is_empty():
        return _get_empty_findings_df()

    # Calculate percentile thresholds for returns
    percentiles = recent_df.select(
        [
            pl.col("total_return_pct").quantile(percentile_low / 100).alias("p_low"),
            pl.col("total_return_pct").quantile(percentile_high / 100).alias("p_high"),
            pl.col("total_return_pct").mean().alias("mean"),
            pl.col("total_return_pct").std().alias("std"),
        ]
    ).row(0, named=True)

    if percentiles["std"] is None or percentiles["std"] == 0:
        return _get_empty_findings_df()

    # Find outliers (below p_low or above p_high)
    outliers = recent_df.filter(
        (pl.col("total_return_pct") <= percentiles["p_low"])
        | (pl.col("total_return_pct") >= percentiles["p_high"])
    )

    # Convert to findings
    for row in outliers.iter_rows(named=True):
        # Calculate z-score
        z_score = (row["total_return_pct"] - percentiles["mean"]) / percentiles["std"]

        findings.append(
            {
                "data_point": row["symbol"],
                "metric_category": "market",
                "current_value": row["total_return_pct"],
                "change_period": "12_weeks",
                "change_pct": row["total_return_pct"],
                "z_score": z_score,
                "significance_score": abs(z_score),
                "finding_type": "statistical_outlier",
            }
        )

    if not findings:
        return _get_empty_findings_df()

    return pl.DataFrame(findings)


def aggregate_findings(findings_list: list[pl.DataFrame]) -> pl.DataFrame:
    """
    Combine all findings and rank by significance.

    Args:
        findings_list: List of finding DataFrames from detection functions

    Returns:
        Combined DataFrame sorted by significance_score descending
    """
    # Filter out empty DataFrames
    non_empty = [df for df in findings_list if not df.is_empty()]

    if not non_empty:
        return _get_empty_findings_df()

    # Concatenate all findings
    all_findings = pl.concat(non_empty, how="diagonal")

    # Sort by significance score (highest first)
    ranked = all_findings.sort("significance_score", descending=True)

    return ranked


def _get_empty_findings_df() -> pl.DataFrame:
    """Get empty DataFrame with correct schema for findings."""
    return pl.DataFrame(
        schema={
            "data_point": pl.Utf8,
            "metric_category": pl.Utf8,
            "current_value": pl.Float64,
            "change_period": pl.Utf8,
            "change_pct": pl.Float64,
            "z_score": pl.Float64,
            "significance_score": pl.Float64,
            "finding_type": pl.Utf8,
        }
    )
