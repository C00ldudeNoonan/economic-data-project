"""Tests for historical CIK mapping and multi-CIK filing fetch (issue #49)."""

from datetime import datetime, timezone
from unittest.mock import Mock

import polars as pl

from macro_agents.defs.domains.sec.cik_history import (
    KNOWN_CROSS_CIK_PREDECESSORS,
    curated_predecessor_records,
)
from macro_agents.defs.domains.sec.metadata import (
    build_ciks_to_fetch,
    fetch_filings_across_ciks,
)

_NOW = datetime(2026, 7, 3, tzinfo=timezone.utc)


def _filings_frame(cik: str) -> pl.DataFrame:
    return pl.DataFrame(
        {"cik": [cik], "accession_number": [f"{cik}-26-000001"], "form_type": ["10-K"]}
    )


class TestCuratedPredecessorRecords:
    def test_only_symbols_in_universe_are_emitted(self):
        records = curated_predecessor_records({"GOOGL", "MSFT"}, _NOW)

        symbols = {r["current_symbol"] for r in records}
        assert symbols == {"GOOGL", "MSFT"}

    def test_empty_universe_emits_nothing(self):
        assert curated_predecessor_records(set(), _NOW) == []

    def test_alphabet_predecessor_fields(self):
        (record,) = curated_predecessor_records({"GOOGL"}, _NOW)

        assert record["cik"] == "1288776"
        assert record["cik_padded"] == "0001288776"
        assert record["company_name"] == "Google Inc."
        assert record["relationship_type"] == "reorganized_from"
        assert record["source"] == "curated"
        assert record["former_names"] is None
        assert record["created_at"] == _NOW

    def test_ids_are_deterministic_and_unique(self):
        universe = {e["current_symbol"] for e in KNOWN_CROSS_CIK_PREDECESSORS}

        first = curated_predecessor_records(universe, _NOW)
        second = curated_predecessor_records(universe, _NOW)

        assert [r["id"] for r in first] == [r["id"] for r in second]
        assert len({r["id"] for r in first}) == len(first)


class TestBuildCiksToFetch:
    def test_primary_only_when_no_history(self):
        assert build_ciks_to_fetch("0001652044", []) == ["0001652044"]

    def test_historical_ciks_appended_after_primary(self):
        result = build_ciks_to_fetch("0001652044", ["0001288776"])

        assert result == ["0001652044", "0001288776"]

    def test_duplicates_of_primary_and_history_are_dropped(self):
        result = build_ciks_to_fetch(
            "0001652044", ["0001652044", "0001288776", "0001288776"]
        )

        assert result == ["0001652044", "0001288776"]


class TestFetchFilingsAcrossCiks:
    def test_fetching_spans_multiple_ciks(self):
        """Acceptance criterion: with mocked CIK history, filings are
        fetched for the current AND predecessor CIKs (Alphabet case)."""
        historical_ciks_by_symbol = {"GOOGL": ["0001288776"]}
        ciks = build_ciks_to_fetch(
            "0001652044", historical_ciks_by_symbol.get("GOOGL", [])
        )
        sec_edgar = Mock()
        sec_edgar.get_10k_10q_filings.side_effect = lambda cik, years_back, context: (
            _filings_frame(cik)
        )

        frames, backfills, incrementals = fetch_filings_across_ciks(
            sec_edgar, "GOOGL", ciks, latest_by_cik={}, context=Mock()
        )

        fetched_ciks = [
            call.args[0] for call in sec_edgar.get_10k_10q_filings.call_args_list
        ]
        assert fetched_ciks == ["0001652044", "0001288776"]
        assert len(frames) == 2
        assert backfills == 2
        assert incrementals == 0

    def test_incremental_fetch_used_when_latest_date_known(self):
        sec_edgar = Mock()
        sec_edgar.get_company_filings.return_value = _filings_frame("0001652044")
        sec_edgar.get_10k_10q_filings.return_value = _filings_frame("0001288776")

        frames, backfills, incrementals = fetch_filings_across_ciks(
            sec_edgar,
            "GOOGL",
            ["0001652044", "0001288776"],
            latest_by_cik={"0001652044": "2026-05-01"},
            context=Mock(),
        )

        sec_edgar.get_company_filings.assert_called_once()
        assert sec_edgar.get_company_filings.call_args.kwargs["cik"] == "0001652044"
        assert (
            sec_edgar.get_company_filings.call_args.kwargs["start_date"] == "2026-05-01"
        )
        sec_edgar.get_10k_10q_filings.assert_called_once()
        assert len(frames) == 2
        assert backfills == 1
        assert incrementals == 1

    def test_empty_results_are_not_collected(self):
        sec_edgar = Mock()
        sec_edgar.get_10k_10q_filings.return_value = pl.DataFrame()

        frames, backfills, incrementals = fetch_filings_across_ciks(
            sec_edgar, "GOOGL", ["0001652044"], latest_by_cik={}, context=Mock()
        )

        assert frames == []
        assert backfills == 1
