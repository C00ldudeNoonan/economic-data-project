"""
Unit tests for resources.
"""

import polars as pl
import tempfile
import os
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


class TestMarketStackResource:
    """Test cases for MarketStackResource."""

    def test_initialization(self):
        """Test MarketStackResource initialization."""
        resource = MarketStackResource(api_key="test_key")
        assert resource.api_key == "test_key"
