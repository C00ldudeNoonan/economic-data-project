"""Resource for scraping company lists with aggressive rate limiting."""

import random
import time
from datetime import datetime, timezone
from typing import cast

import polars as pl
import requests
from bs4 import BeautifulSoup, Tag
from dagster import ConfigurableResource
from pydantic import Field
from tenacity import retry, stop_after_attempt, wait_exponential


class CompanyListScraperResource(ConfigurableResource):
    """Resource for scraping company lists with aggressive rate limiting."""

    # Conservative delays (8-20s like Reddit, Stock Analysis may be aggressive)
    min_delay: float = Field(
        default=8.0, description="Minimum delay between requests (seconds)"
    )
    max_delay: float = Field(
        default=20.0, description="Maximum delay between requests (seconds)"
    )

    # User agent rotation to appear as different browsers
    user_agents: list[str] = Field(
        default_factory=lambda: [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        ]
    )

    def _random_delay(self) -> None:
        """Introduce random delay to appear human."""
        delay = random.uniform(self.min_delay, self.max_delay)
        time.sleep(delay)

    def _get_headers(self) -> dict:
        """Get realistic headers with rotated user agent."""
        return {
            "User-Agent": random.choice(self.user_agents),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        }

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=30, max=300))
    def scrape_sp500_companies(self, context) -> pl.DataFrame:
        """
        Scrape S&P 500 companies from Wikipedia.

        Returns DataFrame with columns:
        - symbol: VARCHAR (primary key)
        - company_name: VARCHAR
        - sector: VARCHAR (GICS Sector)
        - sub_industry: VARCHAR (GICS Sub-Industry)
        - headquarters: VARCHAR
        - date_added: DATE (when added to S&P 500)
        - cik: VARCHAR (SEC Central Index Key)
        - founded: VARCHAR
        - source: VARCHAR ('sp500')
        - fetched_at: TIMESTAMP
        """
        self._random_delay()

        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        context.log.debug(f"Scraping S&P 500 from {url}")

        response = requests.get(url, headers=self._get_headers(), timeout=30)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, "lxml")

        # Find the constituents table (first table with id="constituents")
        table = soup.find("table", {"id": "constituents"})
        if not table:
            context.log.error("Could not find constituents table")
            return pl.DataFrame(schema=self._get_sp500_schema())

        companies = []
        table_tag = cast(Tag, table)
        for row in table_tag.find_all("tr")[1:]:  # Skip header row
            if not isinstance(row, Tag):
                continue
            try:
                cols = row.find_all("td")
                if len(cols) < 8:
                    continue

                company_data = {
                    "symbol": cols[0].text.strip(),
                    "company_name": cols[1].text.strip(),
                    "sector": cols[2].text.strip(),
                    "sub_industry": cols[3].text.strip(),
                    "headquarters": cols[4].text.strip(),
                    "date_added": self._parse_date(cols[5].text.strip()),
                    "cik": cols[6].text.strip(),
                    "founded": cols[7].text.strip(),
                    "source": "sp500",
                    "fetched_at": datetime.now(timezone.utc),
                }
                companies.append(company_data)
            except Exception as e:
                context.log.warning(f"Failed to parse row: {e}")
                continue

        context.log.debug(f"Scraped {len(companies)} S&P 500 companies")

        if not companies:
            return pl.DataFrame(schema=self._get_sp500_schema())

        return pl.DataFrame(companies)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=30, max=300))
    def scrape_nasdaq_companies(self, context) -> pl.DataFrame:
        """
        Scrape NASDAQ companies from Stock Analysis.

        Returns DataFrame with columns:
        - symbol: VARCHAR (primary key)
        - company_name: VARCHAR
        - market_cap: FLOAT
        - price: FLOAT
        - change_pct: FLOAT (percent change)
        - revenue: FLOAT
        - source: VARCHAR ('nasdaq')
        - fetched_at: TIMESTAMP
        """
        self._random_delay()

        url = "https://stockanalysis.com/list/nasdaq-stocks/"
        context.log.debug(f"Scraping NASDAQ from {url}")

        response = requests.get(url, headers=self._get_headers(), timeout=30)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, "lxml")

        # Find the main table (structure may vary)
        table = soup.find("table")
        if not table:
            context.log.error("Could not find NASDAQ companies table")
            return pl.DataFrame(schema=self._get_nasdaq_schema())

        companies = []
        table_tag = cast(Tag, table)
        for row in table_tag.find_all("tr")[1:]:  # Skip header row
            if not isinstance(row, Tag):
                continue
            try:
                cols = row.find_all("td")
                if len(cols) < 3:
                    continue

                # Updated table structure (as of 2025):
                # Col 0: Row number (No.)
                # Col 1: Symbol
                # Col 2: Company Name
                # Col 3: Market Cap
                # Col 4: Stock Price
                # Col 5: % Change
                # Col 6: Revenue
                company_data = {
                    "symbol": cols[1].text.strip(),
                    "company_name": cols[2].text.strip(),
                    "market_cap": self._parse_market_cap(cols[3].text.strip())
                    if len(cols) > 3
                    else None,
                    "price": self._parse_price(cols[4].text.strip())
                    if len(cols) > 4
                    else None,
                    "change_pct": self._parse_percentage(cols[5].text.strip())
                    if len(cols) > 5
                    else None,
                    "revenue": self._parse_market_cap(cols[6].text.strip())
                    if len(cols) > 6
                    else None,
                    "source": "nasdaq",
                    "fetched_at": datetime.now(timezone.utc),
                }
                companies.append(company_data)
            except Exception as e:
                context.log.warning(f"Failed to parse row: {e}")
                continue

        context.log.debug(f"Scraped {len(companies)} NASDAQ companies")

        if not companies:
            return pl.DataFrame(schema=self._get_nasdaq_schema())

        return pl.DataFrame(companies)

    def _parse_date(self, date_str: str) -> str | None:
        """Parse date from various formats to YYYY-MM-DD."""
        if not date_str or date_str.lower() in ["n/a", "na", ""]:
            return None
        try:
            # Wikipedia uses YYYY-MM-DD format
            return date_str
        except Exception:
            return None

    def _parse_market_cap(self, cap_str: str) -> float | None:
        """Parse market cap from formatted strings like '1.2T', '500M', '3.5B'."""
        if not cap_str or cap_str.lower() in ["n/a", "na", ""]:
            return None

        cap_str = cap_str.strip().upper().replace("$", "").replace(",", "")

        try:
            multipliers = {
                "T": 1_000_000_000_000,
                "B": 1_000_000_000,
                "M": 1_000_000,
                "K": 1_000,
            }

            for suffix, multiplier in multipliers.items():
                if suffix in cap_str:
                    number = float(cap_str.replace(suffix, ""))
                    return number * multiplier

            # No suffix, just parse as float
            return float(cap_str)
        except Exception:
            return None

    def _parse_price(self, price_str: str) -> float | None:
        """Parse price from string."""
        if not price_str or price_str.lower() in ["n/a", "na", ""]:
            return None
        try:
            return float(price_str.strip().replace("$", "").replace(",", ""))
        except Exception:
            return None

    def _parse_year(self, year_str: str) -> int | None:
        """Parse year from string."""
        if not year_str or year_str.lower() in ["n/a", "na", ""]:
            return None
        try:
            return int(year_str.strip())
        except Exception:
            return None

    def _parse_percentage(self, pct_str: str) -> float | None:
        """Parse percentage from string like '1.59%' or '-0.5%'."""
        if not pct_str or pct_str.lower() in ["n/a", "na", ""]:
            return None
        try:
            return float(pct_str.strip().replace("%", "").replace(",", ""))
        except Exception:
            return None

    def _get_sp500_schema(self) -> dict:
        """Get schema for empty S&P 500 DataFrame."""
        return {
            "symbol": pl.Utf8,
            "company_name": pl.Utf8,
            "sector": pl.Utf8,
            "sub_industry": pl.Utf8,
            "headquarters": pl.Utf8,
            "date_added": pl.Utf8,
            "cik": pl.Utf8,
            "founded": pl.Utf8,
            "source": pl.Utf8,
            "fetched_at": pl.Datetime,
            "date_started": pl.Date,
            "date_ended": pl.Date,
        }

    def _get_nasdaq_schema(self) -> dict:
        """Get schema for empty NASDAQ DataFrame."""
        return {
            "symbol": pl.Utf8,
            "company_name": pl.Utf8,
            "market_cap": pl.Float64,
            "price": pl.Float64,
            "change_pct": pl.Float64,
            "revenue": pl.Float64,
            "source": pl.Utf8,
            "fetched_at": pl.Datetime,
        }
