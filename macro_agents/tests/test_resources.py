"""
Unit tests for Dagster resources.
"""

import polars as pl
import tempfile
import os
from unittest.mock import Mock, patch
from macro_agents.defs.resources.motherduck import MotherDuckResource
from macro_agents.defs.resources.fred import FredResource
from macro_agents.defs.resources.market_stack import MarketStackResource


class TestMotherDuckResource:
    """Test cases for MotherDuckResource."""

    def test_initialization_dev_environment(self):
        """Test resource initialization in dev environment."""
        resource = MotherDuckResource(
            md_token="test_token", environment="dev", local_path="test.duckdb"
        )

        assert resource.environment == "dev"
        assert resource.local_path == "test.duckdb"
        assert resource.db_connection == "test.duckdb"

    def test_initialization_prod_environment(self):
        """Test resource initialization in prod environment."""
        resource = MotherDuckResource(
            md_token="test_token", environment="prod", md_database="test_db"
        )

        assert resource.environment == "prod"
        assert resource.md_token == "test_token"
        assert resource.db_connection == "md:?motherduck_token=test_token"

    def test_map_dtype(self):
        """Test data type mapping functionality."""
        # Test various data types
        assert MotherDuckResource.map_dtype(pl.Int32) == "INTEGER"
        assert MotherDuckResource.map_dtype(pl.Int64) == "INTEGER"
        assert MotherDuckResource.map_dtype(pl.Float32) == "DOUBLE"
        assert MotherDuckResource.map_dtype(pl.Float64) == "DOUBLE"
        assert MotherDuckResource.map_dtype(pl.Boolean) == "BOOLEAN"
        assert MotherDuckResource.map_dtype(pl.Date) == "DATE"
        assert MotherDuckResource.map_dtype(pl.Datetime) == "TIMESTAMP"
        assert MotherDuckResource.map_dtype(pl.Utf8) == "VARCHAR"

    def test_get_connection_dev(self):
        """Test database connection in dev environment."""
        with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as tmp_file:
            resource = MotherDuckResource(
                md_token="test_token", environment="dev", local_path=tmp_file.name
            )

            conn = resource.get_connection(read_only=True)
            assert conn is not None
            conn.close()

            # Clean up
            os.unlink(tmp_file.name)

    def test_drop_create_duck_db_table(self):
        """Test table drop and create functionality."""
        with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as tmp_file:
            resource = MotherDuckResource(
                md_token="test_token", environment="dev", local_path=tmp_file.name
            )

            # Create test data
            test_df = pl.DataFrame(
                {"id": [1, 2, 3], "name": ["A", "B", "C"], "value": [10.5, 20.3, 30.1]}
            )

            # Test drop and create
            result = resource.drop_create_duck_db_table("test_table", test_df)
            assert result == tmp_file.name

            # Verify table was created
            conn = resource.get_connection(read_only=True)
            result_df = conn.execute("SELECT * FROM test_table").pl()
            assert len(result_df) == 3
            conn.close()

            # Clean up
            os.unlink(tmp_file.name)

    def test_upsert_data(self):
        """Test data upsert functionality."""
        with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as tmp_file:
            resource = MotherDuckResource(
                md_token="test_token", environment="dev", local_path=tmp_file.name
            )

            # Create test data
            test_df = pl.DataFrame(
                {"id": [1, 2, 3], "name": ["A", "B", "C"], "value": [10.5, 20.3, 30.1]}
            )

            # Test upsert
            resource.upsert_data("test_table", test_df, ["id"])

            # Verify data was inserted
            conn = resource.get_connection(read_only=True)
            result_df = conn.execute("SELECT * FROM test_table").pl()
            assert len(result_df) == 3
            conn.close()

            # Clean up
            os.unlink(tmp_file.name)

    def test_read_data(self):
        """Test data reading functionality."""
        with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as tmp_file:
            resource = MotherDuckResource(
                md_token="test_token", environment="dev", local_path=tmp_file.name
            )

            # Create test data
            test_df = pl.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})

            # Insert data
            resource.drop_create_duck_db_table("test_table", test_df)

            # Read data
            data = resource.read_data("test_table")
            assert len(data) == 3
            assert data[0]["id"] == 1
            assert data[0]["name"] == "A"

            # Clean up
            os.unlink(tmp_file.name)

    def test_query_sampled_data(self):
        """Test query sampled data functionality."""
        with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as tmp_file:
            resource = MotherDuckResource(
                md_token="test_token", environment="dev", local_path=tmp_file.name
            )

            # Create test data
            test_df = pl.DataFrame(
                {
                    "category": ["A", "A", "B", "B", "C"],
                    "value": [1, 2, 3, 4, 5],
                    "correlation_econ_vs_q1_returns": [0.1, 0.2, 0.3, 0.4, 0.5],
                }
            )

            # Insert data
            resource.drop_create_duck_db_table("test_table", test_df)

            # Test random sampling
            csv_data = resource.query_sampled_data(
                "test_table", sample_size=3, sampling_strategy="random"
            )
            assert csv_data is not None
            assert "category" in csv_data

            # Test top correlations sampling
            csv_data = resource.query_sampled_data(
                "test_table", sample_size=2, sampling_strategy="top_correlations"
            )
            assert csv_data is not None

            # Clean up
            os.unlink(tmp_file.name)

    def test_write_results_to_table(self):
        """Test writing results to table functionality."""
        with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as tmp_file:
            resource = MotherDuckResource(
                md_token="test_token", environment="dev", local_path=tmp_file.name
            )

            # Create test JSON results
            json_results = [
                {
                    "category": "test",
                    "analysis": "test analysis",
                    "timestamp": "2024-01-01T00:00:00",
                }
            ]

            # Test write
            resource.write_results_to_table(
                json_results, "test_output_table", if_exists="replace"
            )

            # Verify data was written
            conn = resource.get_connection(read_only=True)
            result_df = conn.execute("SELECT * FROM test_output_table").pl()
            assert len(result_df) == 1
            conn.close()

            # Clean up
            os.unlink(tmp_file.name)

    def test_table_exists(self):
        """Test table existence check."""
        with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as tmp_file:
            resource = MotherDuckResource(
                md_token="test_token", environment="dev", local_path=tmp_file.name
            )

            # Test non-existent table
            assert not resource.table_exists("non_existent_table")

            # Create table and test
            test_df = pl.DataFrame({"id": [1, 2, 3]})
            resource.drop_create_duck_db_table("test_table", test_df)
            assert resource.table_exists("test_table")

            # Clean up
            os.unlink(tmp_file.name)


class TestFredResource:
    """Test cases for FredResource."""

    def test_initialization(self):
        """Test FredResource initialization."""
        resource = FredResource(api_key="test_key")
        assert resource.api_key == "test_key"

    @patch("requests.get")
    def test_get_series_data(self, mock_get):
        """Test getting series data from FRED API."""
        # Mock API response
        mock_response = Mock()
        mock_response.json.return_value = {
            "observations": [
                {"date": "2024-01-01", "value": "100.0"},
                {"date": "2024-01-02", "value": "101.0"},
            ]
        }
        mock_get.return_value = mock_response

        resource = FredResource(api_key="test_key")
        data = resource.get_series_data("GDP", "2024-01-01", "2024-01-02")

        assert len(data) == 2
        assert data[0]["date"] == "2024-01-01"
        assert data[0]["value"] == "100.0"


class TestMarketStackResource:
    """Test cases for MarketStackResource."""

    def test_initialization(self):
        """Test MarketStackResource initialization."""
        resource = MarketStackResource(api_key="test_key")
        assert resource.api_key == "test_key"

    @patch("requests.get")
    def test_get_eod_data(self, mock_get):
        """Test getting end-of-day data from MarketStack API."""
        # Mock API response
        mock_response = Mock()
        mock_response.json.return_value = {
            "data": [
                {
                    "symbol": "AAPL",
                    "date": "2024-01-01",
                    "open": 100.0,
                    "high": 105.0,
                    "low": 99.0,
                    "close": 104.0,
                    "volume": 1000000,
                }
            ]
        }
        mock_get.return_value = mock_response

        resource = MarketStackResource(api_key="test_key")
        data = resource.get_eod_data("AAPL", "2024-01-01", "2024-01-01")

        assert len(data) == 1
        assert data[0]["symbol"] == "AAPL"
        assert data[0]["close"] == 104.0
