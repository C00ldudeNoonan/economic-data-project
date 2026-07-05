from unittest.mock import Mock

import polars as pl

from macro_agents.defs.domains.macro_checks import treasury_yields_data_check


def test_treasury_yields_check_aggregates_key_series_columns():
    bq = Mock()
    bq.table_exists.return_value = True
    bq.execute_query.return_value = pl.DataFrame(
        {
            "row_count": [5],
            "max_date": ["2026-07-02"],
            "key_series_populated": [20],
        }
    )

    result = treasury_yields_data_check(bq)

    assert result.passed
    query = bq.execute_query.call_args.args[0]
    assert "SUM(CASE WHEN bc_10year IS NOT NULL" in query
    assert "SAFE_CAST(date AS DATE)" in query
