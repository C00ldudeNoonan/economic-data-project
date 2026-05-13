"""Unit tests for FederalReserveResource."""

from datetime import datetime
from unittest.mock import Mock, patch

from macro_agents.defs.resources.federal_reserve import FederalReserveResource


class TestFederalReserveResource:
    """Test cases for FederalReserveResource."""

    def test_initialization(self):
        """Test resource initialization with default values."""
        resource = FederalReserveResource()

        assert resource.min_delay == 5.0
        assert resource.max_delay == 15.0
        assert len(resource.user_agents) == 3

    def test_custom_delays(self):
        """Test resource initialization with custom delay values."""
        resource = FederalReserveResource(min_delay=10.0, max_delay=30.0)

        assert resource.min_delay == 10.0
        assert resource.max_delay == 30.0

    def test_get_headers(self):
        """Test headers generation with user agent rotation."""
        resource = FederalReserveResource()

        headers1 = resource._get_headers()

        # Check required headers are present
        assert "User-Agent" in headers1
        assert "Accept" in headers1
        assert "DNT" in headers1
        assert headers1["DNT"] == "1"

        # User agent should be one of the configured ones
        assert headers1["User-Agent"] in resource.user_agents

    @patch("macro_agents.defs.resources.federal_reserve.time.sleep")
    def test_random_delay(self, mock_sleep):
        """Test that random delay is called with appropriate range."""
        resource = FederalReserveResource(min_delay=5.0, max_delay=15.0)

        resource._random_delay()

        # Check sleep was called once
        assert mock_sleep.call_count == 1

        # Check delay is in expected range
        delay = mock_sleep.call_args[0][0]
        assert 5.0 <= delay <= 15.0

    def test_format_date(self):
        """Test date formatting from YYYYMMDD to YYYY-MM-DD."""
        resource = FederalReserveResource()

        assert resource._format_date("20241106") == "2024-11-06"
        assert resource._format_date("20230315") == "2023-03-15"

    @patch("macro_agents.defs.resources.federal_reserve.requests.get")
    @patch("macro_agents.defs.resources.federal_reserve.time.sleep")
    def test_get_meeting_calendar(self, mock_sleep, mock_get):
        """Test fetching meeting calendar for a year."""
        mock_context = Mock()
        mock_context.log = Mock()

        # Mock HTML response with meeting links
        mock_html = """
        <html>
            <body>
                <a href="/monetarypolicy/fomcminutes20241106.htm">November 2024</a>
                <a href="/monetarypolicy/fomcminutes20240918.htm">September 2024</a>
                <a href="/monetarypolicy/fomcminutes20230315.htm">March 2023</a>
            </body>
        </html>
        """

        mock_response = Mock()
        mock_response.content = mock_html.encode("utf-8")
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        resource = FederalReserveResource()
        meeting_dates = resource.get_meeting_calendar(2024, mock_context)

        # Should find 2 meetings for 2024
        assert len(meeting_dates) == 2
        assert "20240918" in meeting_dates
        assert "20241106" in meeting_dates
        assert "20230315" not in meeting_dates  # Different year

        # Verify request was made
        assert mock_get.call_count == 1

    @patch("macro_agents.defs.resources.federal_reserve.requests.get")
    @patch("macro_agents.defs.resources.federal_reserve.time.sleep")
    def test_get_meeting_minutes(self, mock_sleep, mock_get):
        """Test fetching meeting minutes for a specific date."""
        mock_context = Mock()
        mock_context.log = Mock()

        # Mock HTML response with FOMC minutes
        mock_html = """
        <html>
            <head><title>Minutes of the Federal Open Market Committee</title></head>
            <body>
                <div id="article">
                    <h3 class="article__title">FOMC Meeting Minutes - November 2024</h3>
                    <h4>Participants' Views on Current Conditions</h4>
                    <p>The Committee discussed economic conditions and monetary policy.</p>
                    <h4>Policy Decision</h4>
                    <p>The Committee decided to maintain the target range.</p>
                </div>
            </body>
        </html>
        """

        mock_response = Mock()
        mock_response.content = mock_html.encode("utf-8")
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        resource = FederalReserveResource()
        result = resource.get_meeting_minutes("20241106", mock_context)

        # Check result structure
        assert result["meeting_date"] == "2024-11-06"
        assert result["meeting_type"] == "FOMC"
        assert "November 2024" in result["title"]
        assert "economic conditions" in result["content"].lower()
        assert result["year"] == 2024
        assert "fomcminutes20241106" in result["source_url"]
        assert isinstance(result["fetched_at"], datetime)
        assert isinstance(result["sections"], list)

        # Verify sections were extracted
        assert len(result["sections"]) > 0

    @patch("macro_agents.defs.resources.federal_reserve.requests.get")
    @patch("macro_agents.defs.resources.federal_reserve.time.sleep")
    def test_get_meeting_minutes_retry_on_failure(self, mock_sleep, mock_get):
        """Test that get_meeting_minutes retries on failure."""
        mock_context = Mock()
        mock_context.log = Mock()

        # First two calls fail, third succeeds
        mock_get.side_effect = [
            Exception("Network error"),
            Exception("Network error"),
            Mock(
                content=b"<html><body><div id='article'>Content</div></body></html>",
                raise_for_status=Mock(),
            ),
        ]

        resource = FederalReserveResource()
        result = resource.get_meeting_minutes("20241106", mock_context)

        # Should have retried and eventually succeeded
        assert mock_get.call_count == 3
        assert result["content"]

    def test_extract_title_fallback(self):
        """Test title extraction with fallback logic."""
        from bs4 import BeautifulSoup

        resource = FederalReserveResource()

        # Test with title tag only
        html = "<html><head><title>Test Title</title></head><body></body></html>"
        soup = BeautifulSoup(html, "lxml")
        title = resource._extract_title(soup)
        assert title == "Test Title"

        # Test with no title
        html = "<html><body></body></html>"
        soup = BeautifulSoup(html, "lxml")
        title = resource._extract_title(soup)
        assert title == "FOMC Minutes"
