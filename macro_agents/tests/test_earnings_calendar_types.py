"""Regression tests for earnings_calendar EPS/revenue type coercion (issue #140).

The BigQuery ``earnings_calendar`` table types EPS and revenue columns as
FLOAT64. Yahoo's source feed returns EPS fields as strings, floats, or None, so
the values must be coerced to float (and the columns pinned to Float64) before
the frame is loaded into a staging table and MERGEd — otherwise BigQuery rejects
the MERGE with ``Value of type STRING cannot be assigned to ... FLOAT64``.
"""

from unittest.mock import MagicMock

import dagster as dg
import polars as pl

from macro_agents.defs.domains.calendars import (
    EARNINGS_NUMERIC_COLUMN_TYPES,
    _to_float,
    earnings_calendar,
)


class TestToFloat:
    def test_string_number_is_parsed(self):
        assert _to_float("1.23") == 1.23

    def test_float_passthrough(self):
        assert _to_float(2.5) == 2.5

    def test_integer_is_coerced(self):
        assert _to_float(3) == 3.0

    def test_none_maps_to_none(self):
        assert _to_float(None) is None

    def test_empty_string_maps_to_none(self):
        assert _to_float("") is None

    def test_non_numeric_string_maps_to_none(self):
        assert _to_float("N/A") is None


class TestNumericColumnDtypes:
    """The staging frame must expose FLOAT64 columns even for all-None batches."""

    NUMERIC_COLUMNS = [
        "eps_estimated",
        "eps_actual",
        "revenue_estimated",
        "revenue_actual",
    ]

    def _pin(self, records: list[dict]) -> pl.DataFrame:
        # Mirror the cast applied in earnings_calendar before upsert_data.
        df = pl.DataFrame(records)
        return df.with_columns(
            pl.col(col).cast(pl.Float64, strict=False) for col in self.NUMERIC_COLUMNS
        )

    def test_all_none_batch_yields_float64(self):
        records = [
            {c: None for c in self.NUMERIC_COLUMNS},
            {c: None for c in self.NUMERIC_COLUMNS},
        ]
        df = self._pin(records)
        for col in self.NUMERIC_COLUMNS:
            assert df.schema[col] == pl.Float64

    def test_coerced_values_yield_float64(self):
        records = [
            {
                "eps_estimated": _to_float("1.1"),
                "eps_actual": _to_float("1.3"),
                "revenue_estimated": None,
                "revenue_actual": None,
            }
        ]
        df = self._pin(records)
        for col in self.NUMERIC_COLUMNS:
            assert df.schema[col] == pl.Float64
        assert df["eps_actual"][0] == 1.3


class TestTargetNormalization:
    """The asset must normalize drifted STRING target columns before the MERGE.

    Existing deployments can hold eps_*/revenue_* as STRING (inferred by the
    old code's first load). The FLOAT64 staging columns would then fail the
    MERGE into a STRING target, so normalize_column_types must run first.
    """

    def test_normalize_runs_before_upsert_with_float64_types(self):
        source_df = pl.DataFrame(
            [
                {
                    "symbol": "AAPL",
                    "company_name": "Apple",
                    "report_date": "2026-07-15",
                    "eps_estimated": "1.10",
                    "eps_actual": None,
                    "report_time": "amc",
                    "timing": "after_market",
                }
            ]
        )
        yahoo = MagicMock()
        yahoo.get_earnings_range.return_value = source_df

        bq = MagicMock()
        call_order: list[str] = []
        bq.normalize_column_types.side_effect = lambda *a, **k: call_order.append(
            "normalize"
        )
        bq.upsert_data.side_effect = lambda *a, **k: call_order.append("upsert")

        ctx = dg.build_asset_context()
        earnings_calendar(ctx, yahoo, bq)

        # normalize must run, with FLOAT64 types, before the upsert.
        assert call_order == ["normalize", "upsert"]
        norm_args = bq.normalize_column_types.call_args
        assert norm_args.args[0] == "earnings_calendar"
        assert norm_args.args[1] == EARNINGS_NUMERIC_COLUMN_TYPES
        assert set(EARNINGS_NUMERIC_COLUMN_TYPES.values()) == {"FLOAT64"}

        # The frame handed to upsert_data has FLOAT64 numeric columns.
        upsert_df = bq.upsert_data.call_args.args[1]
        for col in EARNINGS_NUMERIC_COLUMN_TYPES:
            assert upsert_df.schema[col] == pl.Float64
