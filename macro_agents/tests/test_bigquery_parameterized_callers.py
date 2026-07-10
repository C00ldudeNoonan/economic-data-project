from unittest.mock import Mock

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
