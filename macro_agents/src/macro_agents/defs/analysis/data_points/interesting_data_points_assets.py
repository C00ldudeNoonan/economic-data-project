"""Weekly Dagster asset for detecting and analyzing interesting data points."""

from datetime import datetime, timezone

from datetime import timedelta

import dagster as dg
import polars as pl

from macro_agents.defs.analysis.data_points.data_point_finder import (
    aggregate_findings,
    detect_big_moves,
    detect_correlation_anomalies,
    detect_statistical_outliers,
    detect_trend_changes,
    query_data_for_findings,
)
from macro_agents.defs.analysis.economy_state.economy_state_analyzer import (
    EconomicAnalysisResource,
)
from macro_agents.defs.resources.bigquery_warehouse import BigQueryWarehouseResource


def get_latest_economic_context(bq: BigQueryWarehouseResource) -> str:
    """
    Get latest economic analysis summary for context.

    Args:
        bq: BigQueryWarehouseResource for database queries

    Returns:
        String summary of latest economic analysis or default message
    """
    query = """
    SELECT
        analysis_content,
        analysis_timestamp
    FROM economy_state_analysis
    ORDER BY analysis_timestamp DESC
    LIMIT 1
    """

    try:
        df = bq.execute_query(query, read_only=True)
        if df.is_empty():
            return "No recent economic analysis available. Assume current conditions."

        # Extract key summary from the analysis
        analysis_content = df["analysis_content"][0]

        # Try to extract just the key findings (first few paragraphs)
        # to keep token usage reasonable
        paragraphs = analysis_content.split("\n\n")
        summary = "\n\n".join(paragraphs[:3])  # First 3 paragraphs

        return f"Latest Economic Analysis:\n{summary}"

    except Exception:
        return "No recent economic analysis available. Assume current conditions."


@dg.asset(
    group_name="economic_analysis",
    kinds={"ai", "duckdb"},
    partitions_def=dg.WeeklyPartitionsDefinition(start_date="2024-01-01"),
    deps=[
        dg.AssetKey(["agent_fred_series_latest_aggregates"]),
        dg.AssetKey(["agent_leading_econ_return_indicator"]),
        dg.AssetKey(["agent_market_performance"]),
        dg.AssetKey(["agent_commodity_performance"]),
        dg.AssetKey(["agent_reddit_sentiment_trends"]),
    ],
    description="Weekly AI-powered analysis identifying interesting data points: big moves, trend changes, correlations, outliers",
)
def detect_interesting_data_points_weekly(
    context: dg.AssetExecutionContext,
    economic_analysis: EconomicAnalysisResource,
    bq: BigQueryWarehouseResource,
) -> dg.MaterializeResult:
    """
    Weekly scan for interesting data points using statistical detection + AI analysis.

    Identifies:
    1. Big short-term moves (>1.5 std dev)
    2. Trend changes/inflections (momentum reversals)
    3. Correlation anomalies (strong relationships with forward returns)
    4. Statistical outliers (extreme percentiles)

    Returns:
        MaterializeResult with metadata about findings
    """
    # 1. Get week boundaries from partition key (or current week for non-partitioned runs)
    if context.has_partition_key:
        week_start = context.partition_key
    else:
        today = datetime.now(timezone.utc).date()
        monday = today - timedelta(days=today.weekday())
        week_start = monday.strftime("%Y-%m-%d")
        context.log.info(f"No partition key — defaulting to current week: {week_start}")

    # WeeklyPartitionsDefinition uses Sunday as start, so week_end is 6 days later
    try:
        week_start_date = datetime(
            year=int(week_start[:4]),
            month=int(week_start[5:7]),
            day=int(week_start[8:10]),
        ).date()
        week_end = (week_start_date + timedelta(days=6)).strftime("%Y-%m-%d")
    except Exception as e:
        context.log.error(f"Failed to parse week dates: {e}")
        return dg.MaterializeResult(
            metadata={
                "error": "Invalid week dates",
                "week_start": week_start,
            }
        )

    context.log.info(
        f"Detecting interesting data points for week {week_start} to {week_end}"
    )

    # 2. Setup AI resource
    try:
        economic_analysis.setup_for_execution(context)
        analyzer = economic_analysis.get_data_point_analyzer()
    except Exception as e:
        context.log.error(f"Failed to setup economic analysis resource: {e}")
        return dg.MaterializeResult(
            metadata={
                "error": f"Failed to setup AI: {str(e)}",
                "week_start": week_start,
                "week_end": week_end,
            }
        )

    # 3. Query data sources
    context.log.info("Querying data sources for statistical analysis")
    try:
        data = query_data_for_findings(bq, week_start, week_end)
    except Exception as e:
        context.log.error(f"Failed to query data: {e}")
        return dg.MaterializeResult(
            metadata={
                "error": f"Failed to query data: {str(e)}",
                "week_start": week_start,
                "week_end": week_end,
            }
        )

    # 4. Run statistical detection
    context.log.info("Running statistical detection algorithms")

    try:
        big_moves = detect_big_moves(data["economic"])
        context.log.info(f"Detected {big_moves.height} big moves")
    except Exception as e:
        context.log.warning(f"Big moves detection failed: {e}")
        big_moves = pl.DataFrame()

    try:
        trend_changes = detect_trend_changes(data["economic"])
        context.log.info(f"Detected {trend_changes.height} trend changes")
    except Exception as e:
        context.log.warning(f"Trend changes detection failed: {e}")
        trend_changes = pl.DataFrame()

    try:
        correlations = detect_correlation_anomalies(data["correlation"])
        context.log.info(f"Detected {correlations.height} correlation anomalies")
    except Exception as e:
        context.log.warning(f"Correlation detection failed: {e}")
        correlations = pl.DataFrame()

    try:
        market_outliers = detect_statistical_outliers(data["market"])
        commodity_outliers = detect_statistical_outliers(data["commodity"])
        outliers = pl.concat([market_outliers, commodity_outliers], how="diagonal")
        context.log.info(f"Detected {outliers.height} statistical outliers")
    except Exception as e:
        context.log.warning(f"Outlier detection failed: {e}")
        outliers = pl.DataFrame()

    # 5. Aggregate and rank findings
    all_findings = aggregate_findings(
        [big_moves, trend_changes, correlations, outliers]
    )

    if all_findings.is_empty():
        context.log.warning("No interesting findings detected this week")
        return dg.MaterializeResult(
            metadata={
                "week_start": week_start,
                "week_end": week_end,
                "num_findings": 0,
                "message": "No significant findings detected",
            }
        )

    top_findings = all_findings.head(20)  # Top 20 by statistical significance
    context.log.info(f"Selected top {top_findings.height} findings for AI analysis")

    # 6. Get economic context for AI
    context.log.info("Retrieving economic context")
    economic_context = get_latest_economic_context(bq)

    # 7. Prioritize with AI
    context.log.info("Running AI prioritization")
    findings_csv = top_findings.write_csv()

    try:
        prioritization = analyzer.prioritize_findings(findings_csv, economic_context)
        prioritized_list = prioritization.prioritized_findings
        context.log.info(
            f"AI prioritization complete. Rationale: {prioritization.rationale}"
        )
    except Exception as e:
        context.log.error(f"AI prioritization failed: {e}")
        # Fallback: use top findings by statistical significance
        prioritized_list = ",".join(top_findings["data_point"].to_list()[:15])
        context.log.warning("Using statistical ranking as fallback")

    # 8. Analyze each prioritized finding with AI
    context.log.info("Analyzing prioritized findings with AI")
    analyzed_findings = []

    for finding in top_findings.iter_rows(named=True):
        if finding["data_point"] not in prioritized_list:
            continue  # Skip findings not in prioritized list

        # Prepare context strings for AI
        current_value_str = (
            f"{finding['current_value']:.2f}" if finding.get("current_value") else "N/A"
        )

        change_info = []
        if finding.get("change_pct") is not None:
            change_info.append(
                f"{finding['change_period']} change: {finding['change_pct']:.2f}%"
            )
        if finding.get("z_score") is not None:
            change_info.append(f"Z-score: {finding['z_score']:.2f}")

        current_value_with_changes = f"Value: {current_value_str}. " + ", ".join(
            change_info
        )

        historical_context_str = (
            f"Significance score: {finding['significance_score']:.2f}. "
        )
        if finding.get("z_score") is not None:
            historical_context_str += f"Z-score: {finding['z_score']:.2f} (indicates {abs(finding['z_score']):.1f} standard deviations from historical mean)."

        # Call AI analyzer
        try:
            analysis = analyzer.analyze_finding(
                finding_type=finding["finding_type"],
                data_point_name=finding["data_point"],
                metric_category=finding["metric_category"],
                current_value=current_value_with_changes,
                historical_context=historical_context_str,
                economic_context=economic_context,
            )

            analyzed_findings.append(
                {
                    "week_start": week_start,
                    "week_end": week_end,
                    "finding_type": finding["finding_type"],
                    "data_point": finding["data_point"],
                    "metric_category": finding["metric_category"],
                    "current_value": finding.get("current_value"),
                    "change_3m": finding.get("change_pct")
                    if finding.get("change_period") == "3m"
                    else None,
                    "change_6m": finding.get("change_pct")
                    if finding.get("change_period") == "6m"
                    else None,
                    "z_score": finding.get("z_score"),
                    "significance_score": finding["significance_score"],
                    "ai_explanation": analysis.explanation,
                    "significance_narrative": analysis.significance_narrative,
                    "created_at": datetime.now(timezone.utc),
                }
            )

            context.log.info(
                f"Analyzed: {finding['data_point']} - {analysis.significance_narrative[:80]}..."
            )

        except Exception as e:
            context.log.error(f"Failed to analyze {finding['data_point']}: {e}")
            # Continue with other findings
            continue

    if not analyzed_findings:
        context.log.warning("No findings successfully analyzed by AI")
        return dg.MaterializeResult(
            metadata={
                "week_start": week_start,
                "week_end": week_end,
                "num_findings": 0,
                "error": "AI analysis failed for all findings",
            }
        )

    # 9. Store in database
    context.log.info(f"Storing {len(analyzed_findings)} findings in database")
    findings_df = pl.DataFrame(analyzed_findings)

    try:
        bq.upsert_data(
            "interesting_data_points_weekly",
            findings_df,
            ["week_start", "data_point"],
            context=context,
        )
    except Exception as e:
        context.log.error(f"Failed to write to database: {e}")
        return dg.MaterializeResult(
            metadata={
                "week_start": week_start,
                "week_end": week_end,
                "num_findings": len(analyzed_findings),
                "error": f"Database write failed: {str(e)}",
            }
        )

    # 10. Return metadata
    finding_types = findings_df["finding_type"].unique().to_list()
    top_finding_value = (
        analyzed_findings[0]["significance_narrative"] if analyzed_findings else None
    )
    top_finding = top_finding_value if isinstance(top_finding_value, str) else None

    context.log.info(
        f"Successfully completed weekly analysis with {len(analyzed_findings)} findings"
    )

    return dg.MaterializeResult(
        metadata={
            "week_start": week_start,
            "week_end": week_end,
            "num_findings": len(analyzed_findings),
            "finding_types": finding_types,
            "top_finding": top_finding[:200] if top_finding else None,
            "num_big_moves": big_moves.height,
            "num_trend_changes": trend_changes.height,
            "num_correlations": correlations.height,
            "num_outliers": outliers.height,
        }
    )
