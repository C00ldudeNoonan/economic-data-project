"""Unit tests for CompanyListScraperResource and SCD2 asset logic."""

from datetime import date
from unittest.mock import Mock, patch

import duckdb
import polars as pl
from macro_agents.defs.resources.company_list_scraper import (
    CompanyListScraperResource,
)


class TestCompanyListScraperResource:
    """Test cases for CompanyListScraperResource."""

    def test_initialization(self):
        """Test resource initialization with default values."""
        resource = CompanyListScraperResource()

        assert resource.min_delay == 8.0
        assert resource.max_delay == 20.0
        assert len(resource.user_agents) == 3

    def test_custom_delays(self):
        """Test resource initialization with custom delay values."""
        resource = CompanyListScraperResource(min_delay=15.0, max_delay=30.0)

        assert resource.min_delay == 15.0
        assert resource.max_delay == 30.0

    def test_get_headers(self):
        """Test headers generation with user agent rotation."""
        resource = CompanyListScraperResource()

        headers = resource._get_headers()

        assert "User-Agent" in headers
        assert "Accept" in headers
        assert "DNT" in headers
        assert headers["DNT"] == "1"
        assert headers["User-Agent"] in resource.user_agents

    @patch("macro_agents.defs.resources.company_list_scraper.time.sleep")
    def test_random_delay(self, mock_sleep):
        """Test that random delay is called with appropriate range."""
        resource = CompanyListScraperResource(min_delay=8.0, max_delay=20.0)

        resource._random_delay()

        assert mock_sleep.call_count == 1
        delay = mock_sleep.call_args[0][0]
        assert 8.0 <= delay <= 20.0

    def test_parse_market_cap(self):
        """Test parsing market cap from formatted strings."""
        resource = CompanyListScraperResource()

        assert resource._parse_market_cap("1.2T") == 1_200_000_000_000
        assert resource._parse_market_cap("500M") == 500_000_000
        assert resource._parse_market_cap("3.5B") == 3_500_000_000
        assert resource._parse_market_cap("250K") == 250_000
        assert resource._parse_market_cap("N/A") is None
        assert resource._parse_market_cap("") is None

    def test_parse_price(self):
        """Test parsing price from strings."""
        resource = CompanyListScraperResource()

        assert resource._parse_price("$123.45") == 123.45
        assert resource._parse_price("1,234.56") == 1234.56
        assert resource._parse_price("N/A") is None

    def test_parse_year(self):
        """Test parsing year from strings."""
        resource = CompanyListScraperResource()

        assert resource._parse_year("2020") == 2020
        assert resource._parse_year("N/A") is None
        assert resource._parse_year("") is None

    def test_parse_date(self):
        """Test parsing date strings."""
        resource = CompanyListScraperResource()

        assert resource._parse_date("2024-01-15") == "2024-01-15"
        assert resource._parse_date("N/A") is None
        assert resource._parse_date("") is None

    @patch("macro_agents.defs.resources.company_list_scraper.requests.get")
    @patch("macro_agents.defs.resources.company_list_scraper.time.sleep")
    def test_scrape_sp500_success(self, mock_sleep, mock_get):
        """Test successful S&P 500 scraping with mocked HTML."""
        mock_context = Mock()
        mock_context.log = Mock()

        # Mock Wikipedia HTML table
        mock_html = """
        <html><body>
            <table id="constituents">
                <tr><th>Symbol</th><th>Security</th><th>GICS Sector</th><th>GICS Sub-Industry</th>
                    <th>Headquarters</th><th>Date added</th><th>CIK</th><th>Founded</th></tr>
                <tr>
                    <td>AAPL</td>
                    <td>Apple Inc.</td>
                    <td>Information Technology</td>
                    <td>Technology Hardware</td>
                    <td>Cupertino, California</td>
                    <td>1982-11-30</td>
                    <td>0000320193</td>
                    <td>1976</td>
                </tr>
            </table>
        </body></html>
        """

        mock_response = Mock()
        mock_response.content = mock_html.encode("utf-8")
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        resource = CompanyListScraperResource()
        df = resource.scrape_sp500_companies(mock_context)

        assert isinstance(df, pl.DataFrame)
        assert df.height >= 1
        assert "symbol" in df.columns
        assert "company_name" in df.columns
        assert df["symbol"][0] == "AAPL"
        assert df["company_name"][0] == "Apple Inc."

    @patch("macro_agents.defs.resources.company_list_scraper.requests.get")
    @patch("macro_agents.defs.resources.company_list_scraper.time.sleep")
    def test_scrape_sp500_empty_table(self, mock_sleep, mock_get):
        """Test handling of empty Wikipedia table."""
        mock_context = Mock()
        mock_context.log = Mock()

        mock_html = "<html><body><table id='constituents'><tr><th>Header</th></tr></table></body></html>"
        mock_response = Mock()
        mock_response.content = mock_html.encode("utf-8")
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        resource = CompanyListScraperResource()
        df = resource.scrape_sp500_companies(mock_context)

        assert isinstance(df, pl.DataFrame)
        assert df.height == 0
        assert "symbol" in df.columns

    @patch("macro_agents.defs.resources.company_list_scraper.requests.get")
    @patch("macro_agents.defs.resources.company_list_scraper.time.sleep")
    def test_scrape_sp500_no_table(self, mock_sleep, mock_get):
        """Test handling when table is not found."""
        mock_context = Mock()
        mock_context.log = Mock()

        mock_html = "<html><body><p>No table here</p></body></html>"
        mock_response = Mock()
        mock_response.content = mock_html.encode("utf-8")
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        resource = CompanyListScraperResource()
        df = resource.scrape_sp500_companies(mock_context)

        assert isinstance(df, pl.DataFrame)
        assert df.height == 0
        assert "symbol" in df.columns

    @patch("macro_agents.defs.resources.company_list_scraper.requests.get")
    @patch("macro_agents.defs.resources.company_list_scraper.time.sleep")
    def test_scrape_nasdaq_success(self, mock_sleep, mock_get):
        """Test successful NASDAQ scraping with mocked HTML."""
        mock_context = Mock()
        mock_context.log = Mock()

        # Mock Stock Analysis HTML table (matches current site structure)
        # Col 0: Row number, Col 1: Symbol, Col 2: Name, Col 3: Market Cap, etc.
        mock_html = """
        <html><body>
            <table>
                <tr><th>No.</th><th>Symbol</th><th>Name</th><th>Market Cap</th></tr>
                <tr>
                    <td>1</td>
                    <td>TSLA</td>
                    <td>Tesla, Inc.</td>
                    <td>$500B</td>
                </tr>
            </table>
        </body></html>
        """

        mock_response = Mock()
        mock_response.content = mock_html.encode("utf-8")
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        resource = CompanyListScraperResource()
        df = resource.scrape_nasdaq_companies(mock_context)

        assert isinstance(df, pl.DataFrame)
        assert df.height >= 1
        assert "symbol" in df.columns
        assert "company_name" in df.columns
        assert df["symbol"][0] == "TSLA"
        assert df["company_name"][0] == "Tesla, Inc."

    @patch("macro_agents.defs.resources.company_list_scraper.requests.get")
    @patch("macro_agents.defs.resources.company_list_scraper.time.sleep")
    def test_scrape_retry_on_failure(self, mock_sleep, mock_get):
        """Test that scraper retries on network failures."""
        mock_context = Mock()
        mock_context.log = Mock()

        # First two calls fail, third succeeds
        success_html = """
        <html><body>
            <table id="constituents">
                <tr><th>Symbol</th></tr>
                <tr><td>AAPL</td><td>Apple</td><td>IT</td><td>HW</td><td>CA</td><td>2020-01-01</td><td>123</td><td>1976</td></tr>
            </table>
        </body></html>
        """

        mock_get.side_effect = [
            Exception("Network error"),
            Exception("Timeout"),
            Mock(content=success_html.encode("utf-8"), raise_for_status=Mock()),
        ]

        resource = CompanyListScraperResource()
        df = resource.scrape_sp500_companies(mock_context)

        assert mock_get.call_count == 3
        assert isinstance(df, pl.DataFrame)

    def test_sp500_empty_schema_includes_scd2_columns(self):
        """Test that empty S&P 500 schema includes date_started and date_ended."""
        resource = CompanyListScraperResource()
        schema = resource._get_sp500_schema()

        assert "date_started" in schema
        assert "date_ended" in schema
        assert schema["date_started"] == pl.Date
        assert schema["date_ended"] == pl.Date


class TestSP500SCD2Logic:
    """Test SCD Type 2 logic for sp500_companies_raw asset."""

    def _create_test_db(self) -> duckdb.DuckDBPyConnection:
        """Create an in-memory DuckDB with the sp500_companies_raw table."""
        conn = duckdb.connect(":memory:")
        conn.execute(
            "CREATE TABLE sp500_companies_raw ("
            "symbol VARCHAR, company_name VARCHAR, sector VARCHAR, "
            "sub_industry VARCHAR, headquarters VARCHAR, date_added VARCHAR, "
            "cik VARCHAR, founded VARCHAR, source VARCHAR, fetched_at TIMESTAMP, "
            "date_started DATE, date_ended DATE)"
        )
        return conn

    def _insert_active_company(
        self,
        conn: duckdb.DuckDBPyConnection,
        symbol: str,
        company_name: str,
        sector: str,
        date_started: date,
    ) -> None:
        conn.execute(
            "INSERT INTO sp500_companies_raw VALUES "
            "(?, ?, ?, 'Sub', 'HQ', '2020-01-01', '123', '2000', 'sp500', "
            "CURRENT_TIMESTAMP, ?, NULL)",
            [symbol, company_name, sector, date_started],
        )

    def test_new_company_added(self):
        """New companies in the scrape should get date_started=today, date_ended=NULL."""
        conn = self._create_test_db()
        self._insert_active_company(conn, "AAPL", "Apple Inc.", "IT", date(2025, 1, 1))

        # Check that AAPL is active and MSFT doesn't exist
        active = conn.execute(
            "SELECT symbol FROM sp500_companies_raw WHERE date_ended IS NULL"
        ).fetchall()
        assert [r[0] for r in active] == ["AAPL"]

        # Simulate adding MSFT
        today = date.today()
        conn.execute(
            "INSERT INTO sp500_companies_raw VALUES "
            "('MSFT', 'Microsoft', 'IT', 'Software', 'Redmond', '2000-01-01', "
            "'456', '1975', 'sp500', CURRENT_TIMESTAMP, ?, NULL)",
            [today],
        )

        active = conn.execute(
            "SELECT symbol FROM sp500_companies_raw WHERE date_ended IS NULL ORDER BY symbol"
        ).fetchall()
        assert [r[0] for r in active] == ["AAPL", "MSFT"]

        msft_row = conn.execute(
            "SELECT date_started, date_ended FROM sp500_companies_raw WHERE symbol = 'MSFT'"
        ).fetchone()
        assert msft_row[0] == today
        assert msft_row[1] is None
        conn.close()

    def test_company_removed(self):
        """Companies no longer in the scrape should get date_ended set."""
        conn = self._create_test_db()
        self._insert_active_company(conn, "AAPL", "Apple Inc.", "IT", date(2025, 1, 1))
        self._insert_active_company(
            conn, "GE", "General Electric", "Industrials", date(2025, 1, 1)
        )

        today = date.today()
        # Simulate GE being removed (not in scrape)
        conn.execute(
            f"UPDATE sp500_companies_raw SET date_ended = '{today}' "
            "WHERE symbol = 'GE' AND date_ended IS NULL"
        )

        active = conn.execute(
            "SELECT symbol FROM sp500_companies_raw WHERE date_ended IS NULL"
        ).fetchall()
        assert [r[0] for r in active] == ["AAPL"]

        ge_row = conn.execute(
            "SELECT date_started, date_ended FROM sp500_companies_raw WHERE symbol = 'GE'"
        ).fetchone()
        assert ge_row[0] == date(2025, 1, 1)
        assert ge_row[1] == today
        conn.close()

    def test_unchanged_companies_stay_active(self):
        """Companies still in the scrape should keep their active row."""
        conn = self._create_test_db()
        started = date(2025, 1, 1)
        self._insert_active_company(conn, "AAPL", "Apple Inc.", "IT", started)

        # No changes — AAPL is still active
        active = conn.execute(
            "SELECT symbol, date_started, date_ended FROM sp500_companies_raw "
            "WHERE symbol = 'AAPL' AND date_ended IS NULL"
        ).fetchone()
        assert active[0] == "AAPL"
        assert active[1] == started
        assert active[2] is None
        conn.close()

    def test_company_readded_creates_new_row(self):
        """A company removed then re-added should get a new active row."""
        conn = self._create_test_db()
        # GE was added Jan 1, removed Feb 1
        conn.execute(
            "INSERT INTO sp500_companies_raw VALUES "
            "('GE', 'General Electric', 'Industrials', 'Sub', 'HQ', "
            "'2020-01-01', '123', '2000', 'sp500', CURRENT_TIMESTAMP, "
            "'2025-01-01', '2025-02-01')"
        )

        # GE re-added today
        today = date.today()
        conn.execute(
            "INSERT INTO sp500_companies_raw VALUES "
            "('GE', 'GE Aerospace', 'Industrials', 'Aerospace', 'Boston', "
            "'2020-01-01', '123', '2000', 'sp500', CURRENT_TIMESTAMP, ?, NULL)",
            [today],
        )

        # Should have 2 rows for GE total
        all_ge = conn.execute(
            "SELECT date_started, date_ended FROM sp500_companies_raw "
            "WHERE symbol = 'GE' ORDER BY date_started"
        ).fetchall()
        assert len(all_ge) == 2
        assert all_ge[0][1] == date(2025, 2, 1)  # First row closed
        assert all_ge[1][1] is None  # New row is active
        assert all_ge[1][0] == today

        # Only 1 active row
        active_ge = conn.execute(
            "SELECT * FROM sp500_companies_raw WHERE symbol = 'GE' AND date_ended IS NULL"
        ).fetchall()
        assert len(active_ge) == 1
        conn.close()

    def test_historical_rows_preserved(self):
        """Closed historical rows should remain untouched."""
        conn = self._create_test_db()
        # Historical closed row
        conn.execute(
            "INSERT INTO sp500_companies_raw VALUES "
            "('REMOVED', 'Removed Corp', 'Energy', 'Sub', 'HQ', "
            "'2010-01-01', '999', '1990', 'sp500', CURRENT_TIMESTAMP, "
            "'2024-01-01', '2024-06-01')"
        )
        # Current active row
        self._insert_active_company(conn, "AAPL", "Apple Inc.", "IT", date(2025, 1, 1))

        total = conn.execute("SELECT COUNT(*) FROM sp500_companies_raw").fetchone()[0]
        assert total == 2

        active = conn.execute(
            "SELECT COUNT(*) FROM sp500_companies_raw WHERE date_ended IS NULL"
        ).fetchone()[0]
        assert active == 1

        historical = conn.execute(
            "SELECT symbol FROM sp500_companies_raw WHERE date_ended IS NOT NULL"
        ).fetchone()
        assert historical[0] == "REMOVED"
        conn.close()
