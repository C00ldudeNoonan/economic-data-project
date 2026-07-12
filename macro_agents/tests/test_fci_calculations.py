"""Tests for the pure FCI scoring math extracted from financial_condition_index.py (#136)."""

import polars as pl
import pytest

from macro_agents.defs.transformation.fci_calculations import (
    calculate_fci_scores,
    calculate_weighted_score,
)


class TestCalculateWeightedScore:
    def test_dot_product(self):
        assert calculate_weighted_score([1, 2, 3], [4, 5, 6]) == 32.0

    def test_length_mismatch_raises(self):
        with pytest.raises(ValueError):
            calculate_weighted_score([1, 2], [1, 2, 3])


class TestCalculateFciScores:
    def test_rolling_window_needs_full_window(self):
        # 12-row window: the first 11 rows have no score, the 12th does.
        weights = {"equity": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]}
        merged = pl.DataFrame({"equity": [1.0] * 12})

        out = calculate_fci_scores(merged, weights)

        scores = out["equity_score"].to_list()
        assert scores[:11] == [None] * 11
        # weights are applied reversed; all values are 1.0 so score == sum(weights) = 78.
        assert scores[11] == 78.0
        # FCI is the horizontal sum of component scores.
        assert out["FCI"].to_list()[11] == 78.0

    def test_only_present_components_are_scored(self):
        weights = {
            "equity": [1] * 12,
            "dollar": [1] * 12,
        }
        merged = pl.DataFrame({"equity": [1.0] * 12})  # no 'dollar' column

        out = calculate_fci_scores(merged, weights)

        assert "equity_score" in out.columns
        assert "dollar_score" not in out.columns
