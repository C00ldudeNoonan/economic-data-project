"""Regression tests for earnings_calendar EPS/revenue type coercion (issue #140).

The BigQuery ``earnings_calendar`` table types EPS and revenue columns as
FLOAT64. Yahoo's source feed returns EPS fields as strings, floats, or None, so
the values must be coerced to float (and the columns pinned to Float64) before
the frame is loaded into a staging table and MERGEd — otherwise BigQuery rejects
the MERGE with ``Value of type STRING cannot be assigned to ... FLOAT64``.
"""

import polars as pl

from macro_agents.defs.domains.calendars import _to_float


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
