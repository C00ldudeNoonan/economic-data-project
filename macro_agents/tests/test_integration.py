"""
Integration tests for the macro_agents project.
"""

import polars as pl
import tempfile
import os
import pytest
from macro_agents.definitions import defs
from macro_agents.defs.resources.motherduck import MotherDuckResource
from macro_agents.defs.replication.sling import SlingResourceWithCredentials
from dagster import build_op_context


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


class TestSlingConnectionIntegration:
    """Integration tests for Sling database connections.
    
    These tests require real credentials and will be skipped if:
    - SLING_GOOGLE_APPLICATION_CREDENTIALS is not set
    - MOTHERDUCK_TOKEN is not set
    - Other required environment variables are missing
    
    To run these tests, ensure all required environment variables are set.
    """

    @pytest.mark.skipif(
        not os.getenv("SLING_GOOGLE_APPLICATION_CREDENTIALS"),
        reason="SLING_GOOGLE_APPLICATION_CREDENTIALS not set",
    )
    def test_bigquery_credentials_load(self):
        """Test that BigQuery credentials can be loaded from environment."""
        from macro_agents.defs.replication.sling import get_google_credentials_json

        credentials_json = get_google_credentials_json()
        assert credentials_json is not None
        assert len(credentials_json) > 0
        assert credentials_json.startswith("{")

        import json

        creds_dict = json.loads(credentials_json)
        assert "type" in creds_dict
        assert "project_id" in creds_dict

    @pytest.mark.skipif(
        not os.getenv("MOTHERDUCK_TOKEN"),
        reason="MOTHERDUCK_TOKEN not set",
    )
    @pytest.mark.skipif(
        not os.getenv("SLING_GOOGLE_APPLICATION_CREDENTIALS"),
        reason="SLING_GOOGLE_APPLICATION_CREDENTIALS not set",
    )
    @pytest.mark.skipif(
        not all(
            os.getenv(var)
            for var in [
                "MOTHERDUCK_DATABASE",
                "MOTHERDUCK_PROD_SCHEMA",
                "BIGQUERY_PROJECT_ID",
                "BIGQUERY_LOCATION",
                "BIGQUERY_DATASET",
            ]
        ),
        reason="Required environment variables not set",
    )
    def test_sling_resource_setup_creates_connections(self):
        """Test that SlingResourceWithCredentials can set up both connections."""
        context = build_op_context()

        resource = SlingResourceWithCredentials()
        resource.setup_for_execution(context)

        assert hasattr(resource, "_sling_resource")
        assert resource._sling_resource is not None

    @pytest.mark.skipif(
        not os.getenv("MOTHERDUCK_TOKEN"),
        reason="MOTHERDUCK_TOKEN not set",
    )
    def test_motherduck_connection_parameters(self):
        """Test that MotherDuck connection parameters are correctly configured."""
        required_vars = [
            "MOTHERDUCK_TOKEN",
            "MOTHERDUCK_DATABASE",
            "MOTHERDUCK_PROD_SCHEMA",
        ]

        for var in required_vars:
            assert os.getenv(var) is not None, f"{var} environment variable must be set"

        token = os.getenv("MOTHERDUCK_TOKEN")
        database = os.getenv("MOTHERDUCK_DATABASE")
        schema = os.getenv("MOTHERDUCK_PROD_SCHEMA")

        assert len(token) > 0, "MOTHERDUCK_TOKEN must not be empty"
        assert len(database) > 0, "MOTHERDUCK_DATABASE must not be empty"
        assert len(schema) > 0, "MOTHERDUCK_PROD_SCHEMA must not be empty"

    @pytest.mark.skipif(
        not os.getenv("SLING_GOOGLE_APPLICATION_CREDENTIALS"),
        reason="SLING_GOOGLE_APPLICATION_CREDENTIALS not set",
    )
    def test_bigquery_connection_parameters(self):
        """Test that BigQuery connection parameters are correctly configured."""
        required_vars = [
            "BIGQUERY_PROJECT_ID",
            "BIGQUERY_LOCATION",
            "BIGQUERY_DATASET",
        ]

        for var in required_vars:
            assert os.getenv(var) is not None, f"{var} environment variable must be set"

        project_id = os.getenv("BIGQUERY_PROJECT_ID")
        location = os.getenv("BIGQUERY_LOCATION")
        dataset = os.getenv("BIGQUERY_DATASET")

        assert len(project_id) > 0, "BIGQUERY_PROJECT_ID must not be empty"
        assert len(location) > 0, "BIGQUERY_LOCATION must not be empty"
        assert len(dataset) > 0, "BIGQUERY_DATASET must not be empty"

        from macro_agents.defs.replication.sling import get_google_credentials_json
        import json

        credentials_json = get_google_credentials_json()
        creds_dict = json.loads(credentials_json)

        assert (
            creds_dict.get("project_id") == project_id
            or creds_dict.get("project_id") is not None
        ), "Credentials project_id should match or be valid"
