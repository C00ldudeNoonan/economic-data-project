"""
Unit tests for resources.
"""

from unittest.mock import Mock, patch

import polars as pl
from macro_agents.defs.resources.fred import FredResource
from macro_agents.defs.resources.market_stack import MarketStackResource


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
        assert result.row(0, named=True)["status"] == "success"
        assert "error" in result.row(1, named=True)["status"]
