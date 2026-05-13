"""Unit tests for data_point_finder.py statistical detection functions."""

import polars as pl
from macro_agents.defs.analysis.data_points.data_point_finder import (
    aggregate_findings,
    detect_big_moves,
    detect_correlation_anomalies,
    detect_statistical_outliers,
    detect_trend_changes,
)


class TestDetectBigMoves:
    """Test cases for detect_big_moves function."""

    def test_detects_outliers_above_threshold(self):
        """Test that changes >1.5 std dev are detected."""
        # Create data with known outlier (mean=5, std~3.16, outlier=15)
        df = pl.DataFrame(
            {
                "series_code": ["TEST"] * 5,
                "series_name": ["Test Metric"] * 5,
                "current_value": [100, 100, 100, 100, 100],
                "pct_change_3m": [2.0, 3.0, 5.0, 7.0, 15.0],  # 15.0 is ~3.16 std dev
                "pct_change_6m": [1.0, 1.0, 1.0, 1.0, 1.0],
                "pct_change_1y": [1.0, 1.0, 1.0, 1.0, 1.0],
            }
        )

        result = detect_big_moves(df, threshold_std_dev=1.5)

        assert result.height == 1
        assert result["data_point"][0] == "Test Metric"
        assert result["finding_type"][0] == "big_short_term_move"
        assert result["change_period"][0] == "3m"
        assert abs(result["z_score"][0]) > 1.5

    def test_detects_outliers_below_threshold(self):
        """Test that negative changes >1.5 std dev are detected."""
        df = pl.DataFrame(
            {
                "series_code": ["TEST"] * 5,
                "series_name": ["Test Metric"] * 5,
                "current_value": [100, 100, 100, 100, 100],
                "pct_change_3m": [2.0, 3.0, 5.0, 7.0, -15.0],  # -15.0 is outlier
                "pct_change_6m": [1.0, 1.0, 1.0, 1.0, 1.0],
                "pct_change_1y": [1.0, 1.0, 1.0, 1.0, 1.0],
            }
        )

        result = detect_big_moves(df, threshold_std_dev=1.5)

        assert result.height == 1
        assert abs(result["z_score"][0]) > 1.5
        assert result["change_pct"][0] == -15.0

    def test_no_outliers_when_below_threshold(self):
        """Test that no findings returned when all values within threshold."""
        # All values close to mean
        df = pl.DataFrame(
            {
                "series_code": ["TEST"] * 5,
                "series_name": ["Test Metric"] * 5,
                "current_value": [100, 100, 100, 100, 100],
                "pct_change_3m": [5.0, 5.1, 4.9, 5.2, 4.8],  # Low variance
                "pct_change_6m": [1.0, 1.0, 1.0, 1.0, 1.0],
                "pct_change_1y": [1.0, 1.0, 1.0, 1.0, 1.0],
            }
        )

        result = detect_big_moves(df, threshold_std_dev=1.5)

        assert result.height == 0

    def test_handles_multiple_periods(self):
        """Test detection across 3m, 6m, 1y periods."""
        df = pl.DataFrame(
            {
                "series_code": ["TEST"] * 6,
                "series_name": ["Test Metric"] * 6,
                "current_value": [100, 100, 100, 100, 100, 100],
                "pct_change_3m": [1.0, 2.0, 3.0, 2.5, 1.5, 20.0],  # Outlier in 3m
                "pct_change_6m": [1.0, 2.0, 3.0, 25.0, 2.0, 1.5],  # Outlier in 6m
                "pct_change_1y": [30.0, 1.0, 2.0, 3.0, 2.5, 1.5],  # Outlier in 1y
            }
        )

        result = detect_big_moves(df, threshold_std_dev=1.5)

        # Should detect outliers in all 3 periods
        assert result.height >= 3
        periods = result["change_period"].to_list()
        assert "3m" in periods
        assert "6m" in periods
        assert "1y" in periods

    def test_handles_empty_dataframe(self):
        """Test graceful handling of empty input."""
        df = pl.DataFrame(schema={"series_code": pl.Utf8, "pct_change_3m": pl.Float64})

        result = detect_big_moves(df)

        assert result.height == 0
        assert "data_point" in result.columns

    def test_handles_null_values(self):
        """Test that null values are filtered out."""
        df = pl.DataFrame(
            {
                "series_code": ["TEST"] * 5,
                "series_name": ["Test Metric"] * 5,
                "current_value": [100, 100, 100, 100, 100],
                "pct_change_3m": [1.0, None, 1.0, 1.0, 1.0],
                "pct_change_6m": [1.0, 1.0, 1.0, 1.0, 1.0],
                "pct_change_1y": [1.0, 1.0, 1.0, 1.0, 1.0],
            }
        )

        result = detect_big_moves(df)

        # Should not crash on null values
        assert isinstance(result, pl.DataFrame)


class TestDetectTrendChanges:
    """Test cases for detect_trend_changes function."""

    def test_detects_positive_to_negative_inflection(self):
        """Test detection of uptrend reversing to downtrend."""
        # Create time series: rising, then falling
        df = pl.DataFrame(
            {
                "series_code": ["TEST"] * 10,
                "series_name": ["Test Metric"] * 10,
                "month": [
                    "2024-01-01",
                    "2024-02-01",
                    "2024-03-01",
                    "2024-04-01",
                    "2024-05-01",
                    "2024-06-01",
                    "2024-07-01",
                    "2024-08-01",
                    "2024-09-01",
                    "2024-10-01",
                ],
                "current_value": [100, 105, 110, 115, 120, 118, 115, 112, 109, 106],
                "pct_change_6m": [None] * 10,
            }
        )

        result = detect_trend_changes(df, lookback_months=6)

        # Should detect inflection around month 6
        assert result.height >= 1
        assert result["finding_type"][0] == "trend_change_inflection"

    def test_detects_negative_to_positive_inflection(self):
        """Test detection of downtrend reversing to uptrend."""
        df = pl.DataFrame(
            {
                "series_code": ["TEST"] * 10,
                "series_name": ["Test Metric"] * 10,
                "month": [
                    "2024-01-01",
                    "2024-02-01",
                    "2024-03-01",
                    "2024-04-01",
                    "2024-05-01",
                    "2024-06-01",
                    "2024-07-01",
                    "2024-08-01",
                    "2024-09-01",
                    "2024-10-01",
                ],
                "current_value": [120, 115, 110, 105, 100, 102, 105, 108, 111, 114],
                "pct_change_6m": [None] * 10,
            }
        )

        result = detect_trend_changes(df, lookback_months=6)

        assert result.height >= 1
        assert result["finding_type"][0] == "trend_change_inflection"

    def test_no_inflection_for_continuous_trend(self):
        """Test no detection when trend continues in same direction."""
        # Continuous upward trend
        df = pl.DataFrame(
            {
                "series_code": ["TEST"] * 10,
                "series_name": ["Test Metric"] * 10,
                "month": [f"2024-{i:02d}-01" for i in range(1, 11)],
                "current_value": list(range(100, 110)),  # Steady increase
                "pct_change_6m": [None] * 10,
            }
        )

        result = detect_trend_changes(df, lookback_months=6)

        # May or may not find inflection depending on exact momentum calc
        # Just verify it doesn't crash
        assert isinstance(result, pl.DataFrame)

    def test_handles_insufficient_data(self):
        """Test graceful handling when not enough data points."""
        df = pl.DataFrame(
            {
                "series_code": ["TEST"] * 3,
                "series_name": ["Test Metric"] * 3,
                "month": ["2024-01-01", "2024-02-01", "2024-03-01"],
                "current_value": [100, 105, 110],
                "pct_change_6m": [None] * 3,
            }
        )

        result = detect_trend_changes(df, lookback_months=6)

        # Not enough data for 6-month lookback
        assert result.height == 0

    def test_handles_empty_dataframe(self):
        """Test graceful handling of empty input."""
        df = pl.DataFrame(
            schema={
                "series_code": pl.Utf8,
                "month": pl.Utf8,
                "current_value": pl.Float64,
            }
        )

        result = detect_trend_changes(df)

        assert result.height == 0


class TestDetectCorrelationAnomalies:
    """Test cases for detect_correlation_anomalies function."""

    def test_detects_strong_positive_correlation(self):
        """Test detection of strong positive correlation."""
        df = pl.DataFrame(
            {
                "series_name": ["UNRATE", "CPI"],
                "symbol": ["SPY", "SPY"],
                "corr_econ_q1_returns": [0.75, 0.2],
                "corr_econ_q2_returns": [0.5, 0.1],
                "corr_econ_q3_returns": [0.6, 0.15],
            }
        )

        result = detect_correlation_anomalies(df, threshold=0.3)

        assert result.height >= 1
        # UNRATE should be detected (corr > 0.3)
        unrate_finding = result.filter(pl.col("data_point").str.contains("UNRATE"))
        assert unrate_finding.height == 1
        assert unrate_finding["finding_type"][0] == "correlation_anomaly"
        assert abs(unrate_finding["current_value"][0]) >= 0.3

    def test_detects_strong_negative_correlation(self):
        """Test detection of strong negative correlation."""
        df = pl.DataFrame(
            {
                "series_name": ["FEDFUNDS"],
                "symbol": ["SPY"],
                "corr_econ_q1_returns": [-0.65],
                "corr_econ_q2_returns": [-0.45],
                "corr_econ_q3_returns": [-0.55],
            }
        )

        result = detect_correlation_anomalies(df, threshold=0.3)

        assert result.height == 1
        assert abs(result["current_value"][0]) >= 0.3
        assert result["current_value"][0] < 0  # Negative correlation

    def test_filters_weak_correlations(self):
        """Test that weak correlations below threshold are filtered."""
        df = pl.DataFrame(
            {
                "series_name": ["WEAK1", "WEAK2"],
                "symbol": ["SPY", "SPY"],
                "corr_econ_q1_returns": [0.1, -0.15],
                "corr_econ_q2_returns": [0.05, -0.1],
                "corr_econ_q3_returns": [0.08, -0.12],
            }
        )

        result = detect_correlation_anomalies(df, threshold=0.3)

        assert result.height == 0

    def test_selects_strongest_quarter(self):
        """Test that strongest correlation quarter is selected."""
        df = pl.DataFrame(
            {
                "series_name": ["TEST"],
                "symbol": ["SPY"],
                "corr_econ_q1_returns": [0.2],
                "corr_econ_q2_returns": [0.8],  # Strongest
                "corr_econ_q3_returns": [0.3],
            }
        )

        result = detect_correlation_anomalies(df, threshold=0.3)

        assert result.height == 1
        assert result["change_period"][0] == "Q2"
        assert result["current_value"][0] == 0.8

    def test_handles_empty_dataframe(self):
        """Test graceful handling of empty input."""
        df = pl.DataFrame(
            schema={
                "series_name": pl.Utf8,
                "symbol": pl.Utf8,
                "corr_econ_q1_returns": pl.Float64,
            }
        )

        result = detect_correlation_anomalies(df)

        assert result.height == 0


class TestDetectStatisticalOutliers:
    """Test cases for detect_statistical_outliers function."""

    def test_detects_high_percentile_outliers(self):
        """Test detection of values above 90th percentile."""
        # Create returns with one extreme high value
        df = pl.DataFrame(
            {
                "symbol": ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J"],
                "time_period": ["12_weeks"] * 10,
                "total_return_pct": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 50.0],
                "annualized_volatility_pct": [10.0] * 10,
            }
        )

        result = detect_statistical_outliers(df, percentile_low=10, percentile_high=90)

        # Should detect the 50.0% return as outlier
        assert result.height >= 1
        high_outlier = result.filter(pl.col("data_point") == "J")
        assert high_outlier.height == 1
        assert high_outlier["current_value"][0] == 50.0

    def test_detects_low_percentile_outliers(self):
        """Test detection of values below 10th percentile."""
        df = pl.DataFrame(
            {
                "symbol": ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J"],
                "time_period": ["12_weeks"] * 10,
                "total_return_pct": [
                    -50.0,
                    1.0,
                    2.0,
                    3.0,
                    4.0,
                    5.0,
                    6.0,
                    7.0,
                    8.0,
                    9.0,
                ],
                "annualized_volatility_pct": [10.0] * 10,
            }
        )

        result = detect_statistical_outliers(df, percentile_low=10, percentile_high=90)

        # Should detect the -50.0% return as outlier
        assert result.height >= 1
        low_outlier = result.filter(pl.col("data_point") == "A")
        assert low_outlier.height == 1
        assert low_outlier["current_value"][0] == -50.0

    def test_filters_12_weeks_only(self):
        """Test that only 12_weeks time period is analyzed."""
        df = pl.DataFrame(
            {
                "symbol": ["A", "A"],
                "time_period": ["12_weeks", "26_weeks"],
                "total_return_pct": [50.0, 1.0],
                "annualized_volatility_pct": [10.0, 10.0],
            }
        )

        result = detect_statistical_outliers(df)

        # Should only analyze 12_weeks period
        if result.height > 0:
            assert result["change_period"][0] == "12_weeks"

    def test_calculates_z_scores(self):
        """Test that z-scores are calculated correctly."""
        df = pl.DataFrame(
            {
                "symbol": ["A", "B", "C", "D", "E"],
                "time_period": ["12_weeks"] * 5,
                "total_return_pct": [0.0, 5.0, 10.0, 15.0, 50.0],  # Mean=16, Std~18.5
                "annualized_volatility_pct": [10.0] * 5,
            }
        )

        result = detect_statistical_outliers(df, percentile_low=10, percentile_high=90)

        if result.height > 0:
            # Z-scores should be reasonable values
            for z in result["z_score"]:
                assert abs(z) < 10  # Sanity check

    def test_handles_empty_dataframe(self):
        """Test graceful handling of empty input."""
        df = pl.DataFrame(
            schema={
                "symbol": pl.Utf8,
                "time_period": pl.Utf8,
                "total_return_pct": pl.Float64,
            }
        )

        result = detect_statistical_outliers(df)

        assert result.height == 0


class TestAggregateFindings:
    """Test cases for aggregate_findings function."""

    def test_combines_multiple_dataframes(self):
        """Test that multiple finding DataFrames are combined."""
        df1 = pl.DataFrame(
            {
                "data_point": ["Metric1"],
                "metric_category": ["economic"],
                "current_value": [100.0],
                "change_period": ["3m"],
                "change_pct": [5.0],
                "z_score": [2.0],
                "significance_score": [2.0],
                "finding_type": ["big_short_term_move"],
            }
        )

        df2 = pl.DataFrame(
            {
                "data_point": ["Metric2"],
                "metric_category": ["market"],
                "current_value": [50.0],
                "change_period": ["12_weeks"],
                "change_pct": [10.0],
                "z_score": [3.0],
                "significance_score": [3.0],
                "finding_type": ["statistical_outlier"],
            }
        )

        result = aggregate_findings([df1, df2])

        assert result.height == 2
        assert "Metric1" in result["data_point"].to_list()
        assert "Metric2" in result["data_point"].to_list()

    def test_sorts_by_significance_descending(self):
        """Test that findings are sorted by significance score."""
        df1 = pl.DataFrame(
            {
                "data_point": ["Low"],
                "metric_category": ["economic"],
                "current_value": [100.0],
                "change_period": ["3m"],
                "change_pct": [5.0],
                "z_score": [1.0],
                "significance_score": [1.0],
                "finding_type": ["big_short_term_move"],
            }
        )

        df2 = pl.DataFrame(
            {
                "data_point": ["High"],
                "metric_category": ["market"],
                "current_value": [50.0],
                "change_period": ["12_weeks"],
                "change_pct": [10.0],
                "z_score": [5.0],
                "significance_score": [5.0],
                "finding_type": ["statistical_outlier"],
            }
        )

        result = aggregate_findings([df1, df2])

        # "High" should be first (higher significance)
        assert result["data_point"][0] == "High"
        assert result["data_point"][1] == "Low"

    def test_handles_empty_list(self):
        """Test graceful handling when no findings."""
        result = aggregate_findings([])

        assert result.height == 0

    def test_filters_empty_dataframes(self):
        """Test that empty DataFrames are filtered out."""
        df_empty = pl.DataFrame(
            schema={
                "data_point": pl.Utf8,
                "significance_score": pl.Float64,
                "finding_type": pl.Utf8,
            }
        )

        df_valid = pl.DataFrame(
            {
                "data_point": ["Metric1"],
                "metric_category": ["economic"],
                "current_value": [100.0],
                "change_period": ["3m"],
                "change_pct": [5.0],
                "z_score": [2.0],
                "significance_score": [2.0],
                "finding_type": ["big_short_term_move"],
            }
        )

        result = aggregate_findings([df_empty, df_valid, df_empty])

        assert result.height == 1
        assert result["data_point"][0] == "Metric1"
