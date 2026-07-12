import ast
from pathlib import Path
from unittest.mock import Mock

import dagster as dg
import polars as pl

from macro_agents.defs.analysis.data_points.data_point_finder import (
    query_data_for_findings,
)
from macro_agents.defs.analysis.economy_state.economy_state_charts import (
    _fetch_analysis_row,
)
from macro_agents.defs.analysis.investments.investment_recommendation_charts import (
    _fetch_recommendations_row,
)
from macro_agents.defs.analysis.news.news_summary_assets import reddit_daily_summary
from macro_agents.defs.resources.bigquery_warehouse import default_dataset_for_schema


def test_raw_query_calls_do_not_pass_positional_parameters() -> None:
    definitions_root = Path(__file__).parents[1] / "src" / "macro_agents" / "defs"
    violations: list[str] = []

    for path in definitions_root.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "query"
                and len(node.args) >= 2
            ):
                violations.append(f"{path.relative_to(definitions_root)}:{node.lineno}")

    assert violations == [], (
        "Use BigQueryWarehouseResource.execute_query(..., params=...) instead of "
        f"raw client query(sql, params): {violations}"
    )


def test_economy_state_chart_lookup_uses_named_run_id() -> None:
    bq = Mock()
    bq.execute_query.return_value = pl.DataFrame(
        {"dagster_run_id": ["run-123"], "analysis_content": ["analysis"]}
    )

    result = _fetch_analysis_row(bq, "run-123")

    assert result is not None
    query = bq.execute_query.call_args.args[0]
    assert "dagster_run_id = @run_id" in query
    assert bq.execute_query.call_args.kwargs == {
        "read_only": True,
        "params": {"run_id": "run-123"},
    }


def test_investment_chart_lookup_uses_named_run_id() -> None:
    bq = Mock()
    bq.execute_query.return_value = pl.DataFrame(
        {
            "dagster_run_id": ["run-456"],
            "recommendations_content": ["recommendations"],
        }
    )

    result = _fetch_recommendations_row(bq, "run-456")

    assert result is not None
    query = bq.execute_query.call_args.args[0]
    assert "dagster_run_id = @run_id" in query
    assert bq.execute_query.call_args.kwargs == {
        "read_only": True,
        "params": {"run_id": "run-456"},
    }


def test_data_point_queries_use_named_date_parameters() -> None:
    bq = Mock()
    bq.execute_query.return_value = pl.DataFrame()

    result = query_data_for_findings(bq, "2026-07-06", "2026-07-12")

    assert set(result) == {
        "economic",
        "market",
        "commodity",
        "correlation",
        "sentiment",
    }
    economic_call = bq.execute_query.call_args_list[0]
    sentiment_call = bq.execute_query.call_args_list[4]
    expected_params = {
        "week_start": "2026-07-06",
        "week_end": "2026-07-12",
    }
    assert "DATE(@week_start)" in economic_call.args[0]
    assert "DATE(@week_end)" in economic_call.args[0]
    assert economic_call.kwargs["params"] == expected_params
    assert "DATE(@week_start)" in sentiment_call.args[0]
    assert "DATE(@week_end)" in sentiment_call.args[0]
    assert sentiment_call.kwargs["params"] == expected_params


def test_data_point_queries_qualify_agents_preprocess_dataset() -> None:
    """agent_* models live in economics_analysis, not the resource's default
    economics_raw dataset — every reference must be dataset-qualified (issue #141).
    """
    bq = Mock()
    bq.execute_query.return_value = pl.DataFrame()

    query_data_for_findings(bq, "2026-07-06", "2026-07-12")

    analysis_dataset = default_dataset_for_schema("economics_analysis")
    expected_tables = {
        f"{analysis_dataset}.agent_fred_series_latest_aggregates",
        f"{analysis_dataset}.agent_market_performance",
        f"{analysis_dataset}.agent_commodity_performance",
        f"{analysis_dataset}.agent_leading_econ_return_indicator",
        f"{analysis_dataset}.agent_reddit_sentiment_trends",
    }
    queries = "\n".join(call.args[0] for call in bq.execute_query.call_args_list)
    for table in expected_tables:
        assert table in queries, f"unqualified reference to {table}"


def test_reddit_daily_summary_qualifies_agents_preprocess_dataset() -> None:
    """reddit_daily_summary reads the agent_reddit_posts_daily dbt model, which
    materializes into economics_analysis rather than the resource default (#141).
    """
    bq = Mock()
    bq.execute_query.return_value = pl.DataFrame()  # empty -> early return
    news_summarizer = Mock()

    context = dg.build_asset_context(partition_key="2026-07-11")
    reddit_daily_summary(context, news_summarizer, bq)

    analysis_dataset = default_dataset_for_schema("economics_analysis")
    query = bq.execute_query.call_args.args[0]
    assert f"FROM {analysis_dataset}.agent_reddit_posts_daily" in query
