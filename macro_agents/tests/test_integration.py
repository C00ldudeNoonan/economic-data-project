"""
Integration tests for the macro_agents project.
"""

import polars as pl
import tempfile
import os
from unittest.mock import Mock, patch
from macro_agents.definitions import defs
from macro_agents.defs.resources.motherduck import MotherDuckResource
from macro_agents.defs.agents.analysis_agent import EconomicAnalyzer


class TestDagsterDefinitions:
    """Test Dagster definitions integration."""

    def test_definitions_load(self):
        """Test that definitions load without errors."""
        assert defs is not None
        assert len(defs.assets) > 0
        assert len(defs.resources) > 0

    def test_all_assets_have_descriptions(self):
        """Test that all assets have descriptions."""
        for asset_key, asset in defs.assets.items():
            assert asset.description is not None
            assert asset.description.strip() != ""

    def test_all_resources_are_configurable(self):
        """Test that all resources are configurable."""
        for resource_key, resource in defs.resources.items():
            assert hasattr(resource, "model_config")
            assert hasattr(resource, "model_fields")


class TestResourceIntegration:
    """Test resource integration."""

    def test_motherduck_resource_integration(self):
        """Test MotherDuck resource integration with test database."""
        with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as tmp_file:
            resource = MotherDuckResource(
                md_token="test_token", environment="dev", local_path=tmp_file.name
            )

            # Test full workflow
            test_df = pl.DataFrame(
                {
                    "id": [1, 2, 3],
                    "category": ["A", "B", "A"],
                    "value": [10.0, 20.0, 30.0],
                }
            )

            # Create table
            resource.drop_create_duck_db_table("test_table", test_df)

            # Query data
            csv_data = resource.query_sampled_data(
                "test_table", filters={"category": "A"}, sample_size=2
            )

            assert csv_data is not None
            assert "id" in csv_data

            # Test upsert
            new_df = pl.DataFrame(
                {"id": [4, 5], "category": ["C", "C"], "value": [40.0, 50.0]}
            )

            resource.upsert_data("test_table", new_df, ["id"])

            # Verify data
            data = resource.read_data("test_table")
            assert len(data) == 5

            # Clean up
            os.unlink(tmp_file.name)

    @patch("dspy.LM")
    @patch("dspy.settings.configure")
    def test_analyzer_integration(self, mock_configure, mock_lm):
        """Test analyzer integration with MotherDuck resource."""
        with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as tmp_file:
            # Setup resources
            md_resource = MotherDuckResource(
                md_token="test_token", environment="dev", local_path=tmp_file.name
            )

            # Create test data
            test_df = pl.DataFrame(
                {
                    "category": ["Technology", "Healthcare"],
                    "correlation_econ_vs_q1_returns": [0.5, 0.3],
                    "correlation_econ_vs_q2_returns": [0.4, 0.2],
                }
            )

            md_resource.drop_create_duck_db_table(
                "leading_econ_return_indicator", test_df
            )

            # Test analysis workflow
            categories = md_resource.get_unique_categories(
                "leading_econ_return_indicator", "category"
            )
            assert len(categories) == 2
            assert "Technology" in categories
            assert "Healthcare" in categories

            # Test sampled data query
            csv_data = md_resource.query_sampled_data(
                "leading_econ_return_indicator",
                sample_size=1,
                sampling_strategy="top_correlations",
            )

            assert csv_data is not None
            assert "category" in csv_data

            # Clean up
            os.unlink(tmp_file.name)


class TestAssetExecution:
    """Test asset execution integration."""

    @patch("dspy.LM")
    @patch("dspy.settings.configure")
    def test_economic_cycle_analysis_asset(self, mock_configure, mock_lm):
        """Test economic cycle analysis asset execution."""
        with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as tmp_file:
            # Mock DSPy responses
            mock_cycle_result = Mock()
            mock_cycle_result.analysis = "Economic cycle analysis result"

            mock_trend_result = Mock()
            mock_trend_result.trend_analysis = "Market trend analysis result"

            # Setup mocks
            mock_cycle_analyzer = Mock()
            mock_cycle_analyzer.analyze_economic_cycle.return_value = {
                "economic_cycle_analysis": "Cycle analysis",
                "market_trend_analysis": "Trend analysis",
                "analysis_timestamp": "2024-01-01T00:00:00",
                "model_name": "gpt-4-turbo-preview",
            }

            # Create test data
            md_resource = MotherDuckResource(
                md_token="test_token", environment="dev", local_path=tmp_file.name
            )

            # Create test tables
            economic_df = pl.DataFrame(
                {
                    "series_code": ["GDP", "CPI"],
                    "series_name": ["GDP", "CPI"],
                    "current_value": [100.0, 2.0],
                    "pct_change_3m": [0.01, 0.02],
                    "pct_change_6m": [0.02, 0.04],
                    "pct_change_1y": [0.03, 0.06],
                    "date_grain": ["Monthly", "Monthly"],
                }
            )

            market_df = pl.DataFrame(
                {
                    "symbol": ["AAPL", "GOOGL"],
                    "asset_type": ["stock", "stock"],
                    "time_period": ["12_weeks", "12_weeks"],
                    "total_return_pct": [10.0, 15.0],
                    "volatility_pct": [20.0, 25.0],
                    "win_rate_pct": [60.0, 65.0],
                }
            )

            md_resource.drop_create_duck_db_table(
                "fred_series_latest_aggregates", economic_df
            )
            md_resource.drop_create_duck_db_table("us_sector_summary", market_df)

            # Test that data is accessible
            economic_data = md_resource.execute_query(
                "SELECT * FROM fred_series_latest_aggregates", read_only=True
            )
            market_data = md_resource.execute_query(
                "SELECT * FROM us_sector_summary", read_only=True
            )

            assert not economic_data.is_empty()
            assert not market_data.is_empty()

            # Clean up
            os.unlink(tmp_file.name)


class TestDataValidation:
    """Test data validation and integrity."""

    def test_economic_data_schema_validation(self):
        """Test that economic data has expected schema."""
        with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as tmp_file:
            resource = MotherDuckResource(
                md_token="test_token", environment="dev", local_path=tmp_file.name
            )

            # Create test data with expected schema
            test_df = pl.DataFrame(
                {
                    "series_code": ["GDP", "CPI"],
                    "series_name": ["GDP", "CPI"],
                    "month": ["2024-01-01", "2024-01-01"],
                    "current_value": [100.0, 2.0],
                    "pct_change_3m": [0.01, 0.02],
                    "pct_change_6m": [0.02, 0.04],
                    "pct_change_1y": [0.03, 0.06],
                    "date_grain": ["Monthly", "Monthly"],
                }
            )

            resource.drop_create_duck_db_table("fred_series_latest_aggregates", test_df)

            # Validate schema
            result_df = resource.execute_query(
                "SELECT * FROM fred_series_latest_aggregates", read_only=True
            )

            expected_columns = {
                "series_code",
                "series_name",
                "month",
                "current_value",
                "pct_change_3m",
                "pct_change_6m",
                "pct_change_1y",
                "date_grain",
            }

            assert set(result_df.columns) == expected_columns
            assert len(result_df) == 2

            # Clean up
            os.unlink(tmp_file.name)

    def test_market_data_schema_validation(self):
        """Test that market data has expected schema."""
        with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as tmp_file:
            resource = MotherDuckResource(
                md_token="test_token", environment="dev", local_path=tmp_file.name
            )

            # Create test data with expected schema
            test_df = pl.DataFrame(
                {
                    "symbol": ["AAPL", "GOOGL"],
                    "asset_type": ["stock", "stock"],
                    "time_period": ["12_weeks", "6_months"],
                    "exchange": ["NASDAQ", "NASDAQ"],
                    "name": ["Apple", "Google"],
                    "period_start_date": ["2024-01-01", "2024-01-01"],
                    "period_end_date": ["2024-03-31", "2024-06-30"],
                    "trading_days": [65, 130],
                    "total_return_pct": [10.0, 15.0],
                    "avg_daily_return_pct": [0.15, 0.12],
                    "volatility_pct": [20.0, 25.0],
                    "win_rate_pct": [60.0, 65.0],
                    "total_price_change": [15.0, 20.0],
                    "avg_daily_price_change": [0.23, 0.15],
                    "worst_day_change": [-5.0, -8.0],
                    "best_day_change": [4.0, 6.0],
                    "positive_days": [40, 85],
                    "negative_days": [25, 45],
                    "neutral_days": [0, 0],
                    "period_start_price": [150.0, 200.0],
                    "period_end_price": [165.0, 230.0],
                }
            )

            resource.drop_create_duck_db_table("us_sector_summary", test_df)

            # Validate schema
            result_df = resource.execute_query(
                "SELECT * FROM us_sector_summary", read_only=True
            )

            expected_columns = {
                "symbol",
                "asset_type",
                "time_period",
                "exchange",
                "name",
                "period_start_date",
                "period_end_date",
                "trading_days",
                "total_return_pct",
                "avg_daily_return_pct",
                "volatility_pct",
                "win_rate_pct",
                "total_price_change",
                "avg_daily_price_change",
                "worst_day_change",
                "best_day_change",
                "positive_days",
                "negative_days",
                "neutral_days",
                "period_start_price",
                "period_end_price",
            }

            assert set(result_df.columns) == expected_columns
            assert len(result_df) == 2

            # Clean up
            os.unlink(tmp_file.name)
