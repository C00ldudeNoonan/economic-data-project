"""Yahoo Finance web scraper resource for earnings calendar data."""

import random
import time
from datetime import date, timedelta

import polars as pl
import requests
from bs4 import BeautifulSoup, Tag
from dagster import ConfigurableResource, get_dagster_logger
from pydantic import Field
from tenacity import retry, stop_after_attempt, wait_exponential


class YahooFinanceResource(ConfigurableResource):
    """Yahoo Finance web scraper for earnings calendar data."""

    base_url: str = Field(
        default="https://finance.yahoo.com/calendar/earnings",
        description="Yahoo Finance earnings calendar URL",
    )
    request_delay: float = Field(
        default=1.8,
        description="Delay between requests in seconds (2000 req/hour limit = 1.8s)",
    )
    page_size: int = Field(
        default=100,
        description="Number of records per page",
    )
    user_agents: list[str] = Field(
        default_factory=lambda: [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
        ],
        description="User agents for rotation",
    )

    @property
    def logger(self):
        return get_dagster_logger()

    def _get_headers(self) -> dict[str, str]:
        """Get request headers with rotated user agent."""
        return {
            "User-Agent": random.choice(self.user_agents),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Cache-Control": "max-age=0",
        }

    def _delay(self) -> None:
        """Add delay between requests with jitter."""
        jitter = random.uniform(0, 0.5)
        time.sleep(self.request_delay + jitter)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=5, max=60))
    def _fetch_page(self, target_date: date, offset: int = 0) -> list[dict]:
        """
        Fetch a single page of earnings data for a specific date.

        Args:
            target_date: The date to fetch earnings for
            offset: Pagination offset

        Returns:
            List of earnings records
        """
        params = {
            "day": target_date.isoformat(),
            "offset": offset,
            "size": self.page_size,
        }

        self._delay()

        response = requests.get(
            self.base_url,
            params=params,
            headers=self._get_headers(),
            timeout=30,
        )
        response.raise_for_status()

        return self._parse_html_table(response.text, target_date)

    def _parse_html_table(self, html: str, target_date: date) -> list[dict]:
        """
        Parse earnings data from Yahoo Finance HTML table.

        Table columns:
        - Symbol
        - Company
        - Event Name
        - Earnings Call Time (TAS/BMO/AMC)
        - EPS Estimate
        - Reported EPS
        - Surprise (%)
        - Market Cap
        """
        soup = BeautifulSoup(html, "lxml")
        records = []

        table = soup.find("table")
        if not table:
            self.logger.warning("No earnings table found on page")
            return []

        if not isinstance(table, Tag):
            return []

        rows = table.find_all("tr")
        if len(rows) <= 1:
            return []

        # Skip header row
        for row in rows[1:]:
            if not isinstance(row, Tag):
                continue
            cells = row.find_all("td")
            if len(cells) < 6:
                continue

            try:
                record = self._parse_row(cells, target_date)
                if record:
                    records.append(record)
            except Exception as e:
                self.logger.warning(f"Failed to parse row: {e}")
                continue

        return records

    def _parse_row(self, cells: list, target_date: date) -> dict | None:
        """Parse a single table row into an earnings record."""
        # Extract cell text
        symbol = cells[0].get_text(strip=True)
        if not symbol:
            return None

        company_name = cells[1].get_text(strip=True)
        # cells[2] is Event Name (usually "-")
        timing_raw = cells[3].get_text(strip=True)
        eps_estimate_str = cells[4].get_text(strip=True)
        eps_actual_str = cells[5].get_text(strip=True)
        eps_surprise_str = cells[6].get_text(strip=True) if len(cells) > 6 else ""

        # Parse timing
        timing = self._parse_timing(timing_raw)

        # Parse EPS values
        eps_estimated = self._parse_float(eps_estimate_str)
        eps_actual = self._parse_float(eps_actual_str)
        eps_surprise_pct = self._parse_percentage(eps_surprise_str)

        return {
            "symbol": symbol,
            "company_name": company_name,
            "report_date": target_date.isoformat(),
            "report_time": "",
            "timing": timing,
            "eps_estimated": eps_estimated,
            "eps_actual": eps_actual,
            "eps_surprise_pct": eps_surprise_pct,
        }

    def _parse_timing(self, timing_raw: str) -> str:
        """Parse timing string to BMO/AMC/during_market/unknown."""
        if not timing_raw:
            return "unknown"

        timing_lower = timing_raw.lower().strip()

        # TAS = Time After Session (After Market Close)
        # TNS = Time Not Supplied
        # BMO = Before Market Open
        # AMC = After Market Close
        if timing_lower in ("tas", "amc", "after market close", "after close"):
            return "amc"
        if timing_lower in ("bmo", "before market open", "before open"):
            return "bmo"
        if timing_lower in ("tns", "-", ""):
            return "unknown"

        return "unknown"

    def _parse_float(self, value: str) -> float | None:
        """Parse a string value to float, returning None if not possible."""
        if not value or value in ("-", "N/A", "n/a", ""):
            return None
        try:
            # Remove any currency symbols or commas
            cleaned = value.replace(",", "").replace("$", "").strip()
            return float(cleaned)
        except (ValueError, TypeError):
            return None

    def _parse_percentage(self, value: str) -> float | None:
        """Parse a percentage string like '+0.34' or '-1.2%'."""
        if not value or value in ("-", "N/A", "n/a", ""):
            return None
        try:
            cleaned = value.replace("%", "").replace("+", "").strip()
            return float(cleaned)
        except (ValueError, TypeError):
            return None

    def get_earnings_on_date(self, target_date: date) -> list[dict]:
        """
        Get all earnings announcements for a specific date.

        Handles pagination automatically.

        Args:
            target_date: The date to fetch earnings for

        Returns:
            List of earnings records
        """
        all_records = []
        offset = 0

        while True:
            records = self._fetch_page(target_date, offset)

            if not records:
                break

            all_records.extend(records)

            # If we got fewer than page_size, we've reached the end
            if len(records) < self.page_size:
                break

            offset += self.page_size

        self.logger.debug(f"Fetched {len(all_records)} earnings for {target_date}")
        return all_records

    def get_earnings_range(
        self,
        from_date: date,
        to_date: date,
    ) -> pl.DataFrame:
        """
        Get earnings calendar for a date range.

        Args:
            from_date: Start date (inclusive)
            to_date: End date (inclusive)

        Returns:
            Polars DataFrame with earnings data
        """
        all_records = []
        current_date = from_date

        while current_date <= to_date:
            try:
                records = self.get_earnings_on_date(current_date)
                all_records.extend(records)
            except Exception as e:
                self.logger.warning(f"Failed to fetch earnings for {current_date}: {e}")

            current_date += timedelta(days=1)

        if not all_records:
            return pl.DataFrame(schema=self._get_schema())

        return pl.DataFrame(all_records)

    def _get_schema(self) -> dict:
        """Get schema for empty DataFrame."""
        return {
            "symbol": pl.Utf8,
            "company_name": pl.Utf8,
            "report_date": pl.Utf8,
            "report_time": pl.Utf8,
            "timing": pl.Utf8,
            "eps_estimated": pl.Float64,
            "eps_actual": pl.Float64,
            "eps_surprise_pct": pl.Float64,
        }


yahoo_finance_resource = YahooFinanceResource()
