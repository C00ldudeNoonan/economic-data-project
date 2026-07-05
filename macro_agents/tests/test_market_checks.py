from unittest.mock import Mock

import polars as pl

from macro_agents.defs.domains.markets.checks import check_weekly_data_coverage


def test_weekly_data_coverage_query_uses_bigquery_date_array():
    bq = Mock()
    bq.table_exists.return_value = True
    bq.execute_query.return_value = pl.DataFrame()

    result = check_weekly_data_coverage(
        "commodity_etfs_raw",
        "symbol",
        "date",
        bq,
    )

    assert result.passed
    query = bq.execute_query.call_args.args[0]
    assert "GENERATE_DATE_ARRAY" in query
    assert "DATE_TRUNC(date_col, WEEK(MONDAY))" in query
    assert "generate_series" not in query
