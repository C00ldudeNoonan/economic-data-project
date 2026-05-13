"""Federal Reserve FOMC minutes scraping resource with aggressive rate limiting."""

import random
import re
import time
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup, Tag
from dagster import ConfigurableResource
from pydantic import Field
from tenacity import retry, stop_after_attempt, wait_exponential


class FederalReserveResource(ConfigurableResource):
    """Resource for scraping FOMC minutes with aggressive rate limiting to stay under the radar."""

    # Conservative delays to appear human
    min_delay: float = Field(
        default=5.0, description="Minimum delay between requests (seconds)"
    )
    max_delay: float = Field(
        default=15.0, description="Maximum delay between requests (seconds)"
    )

    # User agent rotation to appear as different browsers
    user_agents: list[str] = Field(
        default_factory=lambda: [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        ]
    )

    def _random_delay(self):
        """Introduce random delay between min_delay and max_delay to appear human."""
        delay = random.uniform(self.min_delay, self.max_delay)
        time.sleep(delay)

    def _get_headers(self):
        """Get realistic headers with rotated user agent to appear human."""
        return {
            "User-Agent": random.choice(self.user_agents),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
        }

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=30, max=300))
    def get_meeting_calendar(self, year: int, context) -> list[str]:
        """
        Fetch meeting dates for a given year from FOMC calendar.

        Returns list of meeting dates in YYYYMMDD format.
        """
        # Random delay before request
        self._random_delay()

        calendar_url = "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"
        context.log.debug(f"Fetching FOMC calendar for {year} from {calendar_url}")

        response = requests.get(calendar_url, headers=self._get_headers(), timeout=30)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, "lxml")
        meeting_dates = []

        # Find all meeting dates for the specified year
        # The calendar page lists meetings by year with links to minutes
        for link in soup.find_all("a", href=True):
            if not isinstance(link, Tag):
                continue
            href = str(link.get("href", ""))
            # Match links like: /monetarypolicy/fomcminutes20241106.htm
            match = re.search(r"fomcminutes(\d{8})\.htm", href)
            if match:
                date_str = match.group(1)
                # Extract year from date (first 4 digits)
                if date_str[:4] == str(year):
                    meeting_dates.append(date_str)

        context.log.debug(f"Found {len(meeting_dates)} meetings for {year}")
        return sorted(meeting_dates)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=30, max=300))
    def get_meeting_minutes(self, meeting_date: str, context) -> dict:
        """
        Fetch meeting minutes for a specific date with rate limiting and retry logic.

        Args:
            meeting_date: Date in YYYYMMDD format
            context: Dagster execution context

        Returns:
            Dictionary with meeting data including content, metadata
        """
        url = f"https://www.federalreserve.gov/monetarypolicy/fomcminutes{meeting_date}.htm"

        # Random delay before request
        self._random_delay()

        context.log.debug(f"Fetching FOMC minutes from {url}")

        response = requests.get(url, headers=self._get_headers(), timeout=30)
        response.raise_for_status()

        # Parse HTML with lxml parser (faster)
        soup = BeautifulSoup(response.content, "lxml")

        # Extract content
        content = self._extract_content(soup)
        title = self._extract_title(soup)
        sections = self._extract_sections(soup)

        return {
            "meeting_date": self._format_date(meeting_date),
            "meeting_type": "FOMC",
            "title": title,
            "content": content,
            "sections": sections,
            "fetched_at": datetime.now(timezone.utc),
            "year": int(meeting_date[:4]),
            "source_url": url,
        }

    def _extract_content(self, soup: BeautifulSoup) -> str:
        """Extract main content text from FOMC minutes page."""
        # The main content is typically in a div with id 'article' or class 'col-xs-12'
        content_div = soup.find("div", {"id": "article"}) or soup.find(
            "div", {"class": "col-xs-12 col-sm-8 col-md-8"}
        )

        if content_div:
            # Extract all text, clean up whitespace
            text = content_div.get_text(separator="\n", strip=True)
            # Remove excessive newlines
            text = re.sub(r"\n{3,}", "\n\n", text)
            return text

        # Fallback: get all text from body
        return soup.get_text(separator="\n", strip=True)

    def _extract_title(self, soup: BeautifulSoup) -> str:
        """Extract title from FOMC minutes page."""
        # Try h3 first (common for FOMC minutes)
        title_elem = soup.find("h3", {"class": "article__title"}) or soup.find("h3")

        if title_elem:
            return title_elem.get_text(strip=True)

        # Fallback to page title
        title_tag = soup.find("title")
        if title_tag:
            return title_tag.get_text(strip=True)

        return "FOMC Minutes"

    def _extract_sections(self, soup: BeautifulSoup) -> list[dict]:
        """Extract structured sections from FOMC minutes."""
        sections = []

        # Find all section headers (usually h4 or strong tags)
        for header in soup.find_all(["h4", "strong"]):
            if not isinstance(header, Tag):
                continue
            header_text = header.get_text(strip=True)

            # Get content until next header
            content_parts = []
            for sibling in header.find_next_siblings():
                if not isinstance(sibling, Tag):
                    continue
                if sibling.name in ["h4", "strong"]:
                    break
                if sibling.name == "p":
                    content_parts.append(sibling.get_text(strip=True))

            if content_parts:
                sections.append(
                    {"heading": header_text, "content": "\n".join(content_parts)}
                )

        return sections

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=30, max=300))
    def get_transcript_links(self, year: int, context) -> list[dict]:
        """
        Fetch transcript PDF links for a given year from the FOMC historical page.

        Returns list of dicts with meeting_date (YYYYMMDD) and pdf_url.
        """
        self._random_delay()

        url = f"https://www.federalreserve.gov/monetarypolicy/fomchistorical{year}.htm"
        context.log.info(f"Fetching FOMC transcript links for {year} from {url}")

        response = requests.get(url, headers=self._get_headers(), timeout=30)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, "lxml")
        transcripts = []

        for link in soup.find_all("a", href=True):
            if not isinstance(link, Tag):
                continue
            href = str(link.get("href", ""))
            # Match: /monetarypolicy/files/FOMC20150128meeting.pdf
            match = re.search(r"FOMC(\d{8})meeting\.pdf", href)
            if match:
                date_str = match.group(1)
                pdf_url = (
                    f"https://www.federalreserve.gov{href}"
                    if href.startswith("/")
                    else href
                )
                transcripts.append({"meeting_date": date_str, "pdf_url": pdf_url})

        context.log.info(f"Found {len(transcripts)} transcript PDFs for {year}")
        return sorted(transcripts, key=lambda t: t["meeting_date"])

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=30, max=300))
    def download_transcript_pdf(self, pdf_url: str, context) -> bytes:
        """
        Download a transcript PDF and return raw bytes.

        Args:
            pdf_url: Full URL to the transcript PDF
            context: Dagster execution context
        """
        self._random_delay()

        context.log.info(f"Downloading transcript PDF from {pdf_url}")

        response = requests.get(pdf_url, headers=self._get_headers(), timeout=120)
        response.raise_for_status()

        context.log.info(f"Downloaded {len(response.content)} bytes")
        return response.content

    def _format_date(self, date_str: str) -> str:
        """Convert YYYYMMDD to YYYY-MM-DD format."""
        if len(date_str) == 8:
            return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        return date_str
