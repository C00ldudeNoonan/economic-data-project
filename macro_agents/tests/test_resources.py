"""
Unit tests for resources.
"""

import os
import tempfile
from unittest.mock import Mock, patch

import polars as pl
from macro_agents.defs.resources.fred import FredResource
from macro_agents.defs.resources.market_stack import MarketStackResource
from macro_agents.defs.resources.motherduck import MotherDuckResource


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
        # Create temp file and close it immediately so DuckDB can open it
        with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as tmp_file:
            tmp_path = tmp_file.name

        try:
            resource = MotherDuckResource(
                md_token="test_token", environment="dev", local_path=tmp_path
            )

            # Test non-existent table
            assert not resource.table_exists("non_existent_table")

            # Create table and test
            test_df = pl.DataFrame({"id": [1, 2, 3]})
            resource.drop_create_duck_db_table("test_table", test_df)
            assert resource.table_exists("test_table")
        finally:
            # Clean up
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    def test_write_results_to_table_aligns_columns(self):
        """Ensure append inserts align to existing table schema."""
        with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as tmp_file:
            tmp_path = tmp_file.name

        try:
            resource = MotherDuckResource(
                md_token="test_token", environment="dev", local_path=tmp_path
            )
            resource.execute_query(
                "CREATE TABLE test_table (col_a VARCHAR, col_b VARCHAR)",
                read_only=False,
            )

            resource.write_results_to_table(
                [{"col_a": "a1", "col_b": "b1", "col_c": "extra"}],
                output_table="test_table",
                if_exists="append",
            )

            df = resource.execute_query(
                "SELECT col_a, col_b FROM test_table",
                read_only=True,
            )
            assert df.to_dicts() == [{"col_a": "a1", "col_b": "b1"}]
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    def test_write_results_to_table_fills_missing_columns(self):
        """Ensure missing columns are filled with nulls when appending."""
        with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as tmp_file:
            tmp_path = tmp_file.name

        try:
            resource = MotherDuckResource(
                md_token="test_token", environment="dev", local_path=tmp_path
            )
            resource.execute_query(
                "CREATE TABLE test_table (col_a VARCHAR, col_b VARCHAR, col_c VARCHAR)",
                read_only=False,
            )

            resource.write_results_to_table(
                [{"col_a": "a2", "col_b": "b2"}],
                output_table="test_table",
                if_exists="append",
            )

            df = resource.execute_query(
                "SELECT col_a, col_b, col_c FROM test_table",
                read_only=True,
            )
            assert df.to_dicts() == [{"col_a": "a2", "col_b": "b2", "col_c": None}]
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)


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

    @patch("macro_agents.defs.resources.market_stack.requests.get")
    def test_get_cik_code(self, mock_get):
        """Test get_cik_code method."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": {"cik": "0000320193", "company_name": "Apple Inc."}
        }
        mock_get.return_value = mock_response

        resource = MarketStackResource(api_key="test_key")
        result = resource.get_cik_code("AAPL")

        assert result["data"]["cik"] == "0000320193"
        assert result["data"]["company_name"] == "Apple Inc."
        mock_get.assert_called_once()
        assert "cikcode" in mock_get.call_args[0][0]
        assert "company_name=AAPL" in mock_get.call_args[0][0]

    @patch("macro_agents.defs.resources.market_stack.requests.get")
    def test_get_company_by_cik(self, mock_get):
        """Test get_company_by_cik method."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": {"company_name": "Apple Inc.", "ticker": "AAPL"}
        }
        mock_get.return_value = mock_response

        resource = MarketStackResource(api_key="test_key")
        result = resource.get_company_by_cik("0000320193")

        assert result["data"]["company_name"] == "Apple Inc."
        mock_get.assert_called_once()
        assert "companyname" in mock_get.call_args[0][0]
        assert "cik=0000320193" in mock_get.call_args[0][0]

    @patch("macro_agents.defs.resources.market_stack.requests.get")
    def test_get_sec_submissions(self, mock_get):
        """Test get_sec_submissions method."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": {
                "filings": [
                    {"form_type": "10-K", "filing_date": "2024-01-15"},
                    {"form_type": "10-Q", "filing_date": "2024-04-15"},
                ]
            }
        }
        mock_get.return_value = mock_response

        resource = MarketStackResource(api_key="test_key")
        result = resource.get_sec_submissions("0000320193")

        assert len(result["data"]["filings"]) == 2
        mock_get.assert_called_once()
        assert "submissions" in mock_get.call_args[0][0]

    @patch("macro_agents.defs.resources.market_stack.requests.get")
    def test_get_sec_submissions_with_form_type_filter(self, mock_get):
        """Test get_sec_submissions method with form type filter."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": {"filings": [{"form_type": "10-K", "filing_date": "2024-01-15"}]}
        }
        mock_get.return_value = mock_response

        resource = MarketStackResource(api_key="test_key")
        result = resource.get_sec_submissions("0000320193", form_type="10-K")

        assert len(result["data"]["filings"]) == 1
        mock_get.assert_called_once()
        assert "form_type=10-K" in mock_get.call_args[0][0]

    @patch("macro_agents.defs.resources.market_stack.requests.get")
    def test_get_company_facts(self, mock_get):
        """Test get_company_facts method."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": {
                "cik": "0000320193",
                "facts": {"revenue": 394328000000, "net_income": 99803000000},
            }
        }
        mock_get.return_value = mock_response

        resource = MarketStackResource(api_key="test_key")
        result = resource.get_company_facts("0000320193")

        assert result["data"]["cik"] == "0000320193"
        assert "facts" in result["data"]
        mock_get.assert_called_once()
        assert "companyfacts" in mock_get.call_args[0][0]

    @patch("macro_agents.defs.resources.market_stack.time.sleep")
    @patch("macro_agents.defs.resources.market_stack.requests.get")
    def test_get_cik_codes_batch(self, mock_get, mock_sleep):
        """Test get_cik_codes_batch method."""
        # Mock responses for each ticker
        mock_responses = [
            Mock(
                status_code=200,
                json=Mock(
                    return_value={
                        "data": {"cik": "0000320193", "company_name": "Apple Inc."}
                    }
                ),
            ),
            Mock(
                status_code=200,
                json=Mock(
                    return_value={
                        "data": {"cik": "0000789019", "company_name": "Microsoft Corp"}
                    }
                ),
            ),
        ]
        mock_get.side_effect = mock_responses

        resource = MarketStackResource(api_key="test_key")
        result = resource.get_cik_codes_batch(["AAPL", "MSFT"])

        assert isinstance(result, pl.DataFrame)
        assert len(result) == 2
        assert "ticker" in result.columns
        assert "cik" in result.columns
        assert "status" in result.columns
        assert result["ticker"].to_list() == ["AAPL", "MSFT"]

    @patch("macro_agents.defs.resources.market_stack.time.sleep")
    @patch("macro_agents.defs.resources.market_stack.requests.get")
    def test_get_cik_codes_batch_handles_errors(self, mock_get, mock_sleep):
        """Test get_cik_codes_batch handles errors gracefully."""
        mock_get.side_effect = [
            Mock(
                status_code=200,
                json=Mock(
                    return_value={
                        "data": {"cik": "0000320193", "company_name": "Apple Inc."}
                    }
                ),
            ),
            Exception("API Error"),
        ]

        resource = MarketStackResource(api_key="test_key")
        result = resource.get_cik_codes_batch(["AAPL", "INVALID"])

        assert isinstance(result, pl.DataFrame)
        assert len(result) == 2
        # First should succeed
        assert result.row(0, named=True)["status"] == "success"
        # Second should have error status
        assert "error" in result.row(1, named=True)["status"]
