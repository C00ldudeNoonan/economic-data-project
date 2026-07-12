"""Tests for the split economy-state domain fetchers (issue #136 refactor).

The 13 fetchers moved out of the 770-line domain_data_fetchers.py into cohesive
per-sub-agent modules. These lock in behavior parity (query targets, empty-result
handling, CSV output) and the narrowed exception handling.
"""

from pathlib import Path
from unittest.mock import Mock

import polars as pl
import pytest
from google.api_core.exceptions import GoogleAPIError

from macro_agents.defs.analysis.economy_state.domain_fetchers._common import df_to_csv
from macro_agents.defs.analysis.economy_state.domain_fetchers.commodities import (
    get_agriculture_commodities_data,
    get_energy_commodities_data,
    get_input_commodities_data,
)
from macro_agents.defs.analysis.economy_state.domain_fetchers.financial import (
    get_credit_data,
    get_financial_conditions_data,
    get_yield_curve_data,
)
from macro_agents.defs.analysis.economy_state.domain_fetchers.labor import (
    get_labor_market_data,
    get_labor_trends_data,
)
from macro_agents.defs.analysis.economy_state.domain_fetchers.market_structure import (
    get_fixed_income_data,
    get_global_markets_data,
    get_major_indices_data,
)
from macro_agents.defs.analysis.economy_state.domain_fetchers.sectors import (
    get_sector_correlation_data,
    get_sector_data,
)

# fetcher -> table it must query
FETCHER_TABLE = [
    (get_labor_market_data, "agent_fred_series_latest_aggregates"),
    (get_labor_trends_data, "agent_fred_monthly_diff"),
    (get_financial_conditions_data, "agent_financial_conditions_index"),
    (get_yield_curve_data, "agent_treasury_yield_curve_spreads"),
    (get_credit_data, "agent_fred_series_latest_aggregates"),
    (get_energy_commodities_data, "agent_commodity_performance"),
    (get_input_commodities_data, "agent_commodity_performance"),
    (get_agriculture_commodities_data, "agent_commodity_performance"),
    (get_sector_data, "agent_market_performance"),
    (get_sector_correlation_data, "agent_leading_econ_return_indicator"),
    (get_major_indices_data, "agent_market_performance"),
    (get_fixed_income_data, "agent_market_performance"),
    (get_global_markets_data, "agent_market_performance"),
]


def _bq(return_df: pl.DataFrame | None = None) -> Mock:
    bq = Mock()
    bq.execute_query.return_value = (
        return_df if return_df is not None else pl.DataFrame({"a": [1]})
    )
    return bq


def test_df_to_csv_roundtrip() -> None:
    assert df_to_csv(pl.DataFrame({"a": [1, 2]})) == "a\n1\n2\n"


@pytest.mark.parametrize("fetcher,table", FETCHER_TABLE)
def test_fetcher_queries_expected_table_and_returns_csv(fetcher, table) -> None:
    bq = _bq()
    out = fetcher(bq)
    query = bq.execute_query.call_args.args[0]
    assert table in query
    assert bq.execute_query.call_args.kwargs == {"read_only": True}
    assert out.startswith("a\n")  # CSV of the mocked frame


@pytest.mark.parametrize("fetcher,table", FETCHER_TABLE)
def test_fetcher_accepts_cutoff_date(fetcher, table) -> None:
    # Every fetcher takes an optional cutoff_date positionally; ensure the
    # backtest branch still builds a query without error.
    bq = _bq()
    fetcher(bq, "2026-01-01")
    assert bq.execute_query.called


def test_commodity_fetchers_filter_by_their_category() -> None:
    for fetcher, category in (
        (get_energy_commodities_data, "energy"),
        (get_input_commodities_data, "input"),
        (get_agriculture_commodities_data, "agriculture"),
    ):
        bq = _bq()
        fetcher(bq)
        assert (
            f"commodity_category = '{category}'" in bq.execute_query.call_args.args[0]
        )


def test_empty_result_handling_matches_original() -> None:
    empty = pl.DataFrame()
    # These return "" on empty results.
    assert get_labor_trends_data(_bq(empty)) == ""
    assert get_yield_curve_data(_bq(empty)) == ""
    assert get_sector_correlation_data(_bq(empty)) == ""


def test_sector_correlation_swallows_only_warehouse_errors() -> None:
    # A warehouse error (e.g. table not materialized) degrades to "".
    bq = Mock()
    bq.execute_query.side_effect = GoogleAPIError("table not found")
    assert get_sector_correlation_data(bq) == ""

    # Unexpected errors must NOT be swallowed (no more bare except Exception).
    bq2 = Mock()
    bq2.execute_query.side_effect = ValueError("bug")
    with pytest.raises(ValueError):
        get_sector_correlation_data(bq2)


def test_domain_fetcher_modules_are_within_size_target() -> None:
    pkg = Path(get_labor_market_data.__code__.co_filename).parent
    oversized = {
        p.name: sum(1 for _ in p.open(encoding="utf-8"))
        for p in pkg.glob("*.py")
        if sum(1 for _ in p.open(encoding="utf-8")) > 400
    }
    assert not oversized, f"modules exceed 400-line target: {oversized}"
