"""
Integration tests for the macro_agents project.
"""

import polars as pl
import tempfile
import os
from macro_agents.definitions import defs
from macro_agents.defs.resources.motherduck import MotherDuckResource


class TestDagsterDefinitions:
    """Test Dagster definitions integration."""

    def test_definitions_load(self):
        """Test that definitions load without errors."""
        assert defs is not None
        assert len(defs.assets) > 0
        assert len(defs.resources) > 0

    def test_all_resources_are_configurable(self):
        """Test that all resources are configurable."""
        for resource_key, resource in defs.resources.items():
            assert hasattr(resource, "model_config")
            assert hasattr(type(resource), "model_fields")


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
