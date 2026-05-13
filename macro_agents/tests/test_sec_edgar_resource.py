"""Unit tests for SECEdgarResource."""

from unittest.mock import Mock, patch

import polars as pl

from macro_agents.defs.resources.sec_edgar import SECEdgarResource


class TestSECEdgarResource:
    """Test cases for SECEdgarResource."""

    def test_initialization(self):
        """Test resource initialization with default values."""
        resource = SECEdgarResource()

        assert resource.min_delay == 0.15
        assert resource.max_delay == 0.3
        assert resource.company_name == "EconomicDataProject"
        assert resource.contact_email == "admin@example.com"

    def test_custom_initialization(self):
        """Test resource initialization with custom values."""
        resource = SECEdgarResource(
            min_delay=0.2,
            max_delay=0.5,
            company_name="TestCompany",
            contact_email="test@test.com",
        )

        assert resource.min_delay == 0.2
        assert resource.max_delay == 0.5
        assert resource.company_name == "TestCompany"
        assert resource.contact_email == "test@test.com"

    def test_get_headers(self):
        """Test headers generation with User-Agent."""
        resource = SECEdgarResource(
            company_name="TestCo", contact_email="admin@testco.com"
        )

        headers = resource._get_headers()

        assert "User-Agent" in headers
        assert "TestCo" in headers["User-Agent"]
        assert "admin@testco.com" in headers["User-Agent"]
        assert "Accept" in headers

    def test_pad_cik(self):
        """Test CIK padding to 10 digits."""
        resource = SECEdgarResource()

        # Test various CIK formats
        assert resource._pad_cik("320193") == "0000320193"
        assert resource._pad_cik("0000320193") == "0000320193"
        assert resource._pad_cik("123") == "0000000123"
        assert resource._pad_cik("1234567890") == "1234567890"

    def test_format_accession_number(self):
        """Test accession number formatting."""
        resource = SECEdgarResource()

        # Test with dashes
        assert (
            resource._format_accession_number("0000320193-24-000008", with_dashes=True)
            == "0000320193-24-000008"
        )
        assert (
            resource._format_accession_number("000032019324000008", with_dashes=True)
            == "0000320193-24-000008"
        )

        # Test without dashes
        assert (
            resource._format_accession_number("0000320193-24-000008", with_dashes=False)
            == "000032019324000008"
        )

    @patch("macro_agents.defs.resources.sec_edgar.requests.get")
    @patch("macro_agents.defs.resources.sec_edgar.time.sleep")
    def test_get_company_submissions(self, mock_sleep, mock_get):
        """Test fetching company submissions."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "cik": "0000320193",
            "name": "Apple Inc.",
            "filings": {
                "recent": {
                    "accessionNumber": ["0000320193-24-000008"],
                    "filingDate": ["2024-01-15"],
                    "form": ["10-K"],
                }
            },
        }
        mock_get.return_value = mock_response

        resource = SECEdgarResource()
        result = resource.get_company_submissions("320193")

        assert result["cik"] == "0000320193"
        assert result["name"] == "Apple Inc."
        mock_get.assert_called_once()
        assert "CIK0000320193.json" in mock_get.call_args[0][0]

    @patch("macro_agents.defs.resources.sec_edgar.requests.get")
    @patch("macro_agents.defs.resources.sec_edgar.time.sleep")
    def test_get_company_filings(self, mock_sleep, mock_get):
        """Test getting filtered company filings as DataFrame."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "cik": "0000320193",
            "name": "Apple Inc.",
            "filings": {
                "recent": {
                    "accessionNumber": [
                        "0000320193-24-000008",
                        "0000320193-24-000005",
                        "0000320193-23-000010",
                    ],
                    "filingDate": ["2024-01-15", "2024-04-15", "2023-10-15"],
                    "reportDate": ["2023-12-31", "2024-03-31", "2023-09-30"],
                    "form": ["10-K", "10-Q", "10-Q"],
                    "primaryDocument": ["aapl-10k.htm", "aapl-10q.htm", "aapl-10q.htm"],
                    "primaryDocDescription": ["10-K", "10-Q", "10-Q"],
                    "fileNumber": ["001-36743", "001-36743", "001-36743"],
                    "filmNumber": ["", "", ""],
                    "items": ["", "", ""],
                    "size": [1000000, 500000, 500000],
                    "isXBRL": [True, True, True],
                    "isInlineXBRL": [True, True, True],
                    "acceptanceDateTime": [
                        "2024-01-15T16:30:00",
                        "2024-04-15T16:30:00",
                        "2023-10-15T16:30:00",
                    ],
                }
            },
        }
        mock_get.return_value = mock_response

        resource = SECEdgarResource()
        result = resource.get_company_filings("320193", form_types=["10-K"])

        assert isinstance(result, pl.DataFrame)
        assert len(result) == 1  # Only 10-K filtered
        assert result["form_type"][0] == "10-K"

    @patch("macro_agents.defs.resources.sec_edgar.requests.get")
    @patch("macro_agents.defs.resources.sec_edgar.time.sleep")
    def test_get_company_filings_date_filter(self, mock_sleep, mock_get):
        """Test getting filings with date filter."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "cik": "0000320193",
            "name": "Apple Inc.",
            "filings": {
                "recent": {
                    "accessionNumber": [
                        "0000320193-24-000008",
                        "0000320193-23-000010",
                    ],
                    "filingDate": ["2024-01-15", "2023-06-15"],
                    "reportDate": ["2023-12-31", "2023-06-30"],
                    "form": ["10-K", "10-Q"],
                    "primaryDocument": ["aapl-10k.htm", "aapl-10q.htm"],
                    "primaryDocDescription": ["10-K", "10-Q"],
                    "fileNumber": ["001-36743", "001-36743"],
                    "filmNumber": ["", ""],
                    "items": ["", ""],
                    "size": [1000000, 500000],
                    "isXBRL": [True, True],
                    "isInlineXBRL": [True, True],
                    "acceptanceDateTime": [
                        "2024-01-15T16:30:00",
                        "2023-06-15T16:30:00",
                    ],
                }
            },
        }
        mock_get.return_value = mock_response

        resource = SECEdgarResource()
        result = resource.get_company_filings(
            "320193", start_date="2024-01-01", end_date="2024-12-31"
        )

        assert isinstance(result, pl.DataFrame)
        assert len(result) == 1  # Only 2024 filing
        assert result["filing_date"][0] == "2024-01-15"

    @patch("macro_agents.defs.resources.sec_edgar.requests.get")
    @patch("macro_agents.defs.resources.sec_edgar.time.sleep")
    def test_get_filing_index(self, mock_sleep, mock_get):
        """Test fetching filing document index."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "directory": {
                "item": [
                    {"name": "aapl-10k.htm", "type": "10-K"},
                    {"name": "aapl-ex21.htm", "type": "EX-21"},
                ]
            }
        }
        mock_get.return_value = mock_response

        resource = SECEdgarResource()
        result = resource.get_filing_index("320193", "0000320193-24-000008")

        assert "directory" in result
        mock_get.assert_called_once()
        assert "index.json" in mock_get.call_args[0][0]

    @patch("macro_agents.defs.resources.sec_edgar.requests.get")
    @patch("macro_agents.defs.resources.sec_edgar.time.sleep")
    def test_download_filing_document(self, mock_sleep, mock_get):
        """Test downloading a filing document."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = "<html><body>10-K Content</body></html>"
        mock_get.return_value = mock_response

        resource = SECEdgarResource()
        result = resource.download_filing_document(
            "320193", "0000320193-24-000008", "aapl-10k.htm"
        )

        assert "10-K Content" in result
        mock_get.assert_called_once()
        assert "aapl-10k.htm" in mock_get.call_args[0][0]

    def test_build_document_url(self):
        """Test building document URL."""
        resource = SECEdgarResource()

        url = resource.build_document_url(
            "320193", "0000320193-24-000008", "aapl-10k.htm"
        )

        assert "0000320193" in url
        assert "000032019324000008" in url
        assert "aapl-10k.htm" in url
        assert url.startswith("https://www.sec.gov/Archives/edgar/data/")

    @patch("macro_agents.defs.resources.sec_edgar.requests.get")
    @patch("macro_agents.defs.resources.sec_edgar.time.sleep")
    def test_get_10k_10q_filings(self, mock_sleep, mock_get):
        """Test getting 10-K and 10-Q filings."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "cik": "0000320193",
            "name": "Apple Inc.",
            "filings": {
                "recent": {
                    "accessionNumber": [
                        "0000320193-24-000008",
                        "0000320193-24-000005",
                        "0000320193-24-000001",
                    ],
                    "filingDate": ["2024-01-15", "2024-04-15", "2024-07-15"],
                    "reportDate": ["2023-12-31", "2024-03-31", "2024-06-30"],
                    "form": ["10-K", "10-Q", "8-K"],
                    "primaryDocument": [
                        "aapl-10k.htm",
                        "aapl-10q.htm",
                        "aapl-8k.htm",
                    ],
                    "primaryDocDescription": ["10-K", "10-Q", "8-K"],
                    "fileNumber": ["001-36743", "001-36743", "001-36743"],
                    "filmNumber": ["", "", ""],
                    "items": ["", "", ""],
                    "size": [1000000, 500000, 100000],
                    "isXBRL": [True, True, False],
                    "isInlineXBRL": [True, True, False],
                    "acceptanceDateTime": [
                        "2024-01-15T16:30:00",
                        "2024-04-15T16:30:00",
                        "2024-07-15T16:30:00",
                    ],
                }
            },
        }
        mock_get.return_value = mock_response

        resource = SECEdgarResource()
        result = resource.get_10k_10q_filings("320193", years_back=5)

        assert isinstance(result, pl.DataFrame)
        # Should only include 10-K and 10-Q, not 8-K
        assert len(result) == 2
        form_types = result["form_type"].to_list()
        assert "10-K" in form_types
        assert "10-Q" in form_types
        assert "8-K" not in form_types
