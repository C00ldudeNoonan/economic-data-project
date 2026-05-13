import logging
import time
import urllib.parse
from typing import Any

import dagster as dg
import polars as pl
import requests
from pydantic import Field

from macro_agents.defs.resources._url_secrets import raise_for_status_safe


class MarketStackResource(dg.ConfigurableResource):
    """MarketStack API client as a Dagster configurable resource"""

    api_key: str = Field(description="MarketStack API key")

    @property
    def logger(self) -> logging.Logger:
        return dg.get_dagster_logger()

    @staticmethod
    def _redact_url(url: str) -> str:
        parsed = urllib.parse.urlsplit(url)
        return urllib.parse.urlunsplit(
            (parsed.scheme, parsed.netloc, parsed.path, "", "")
        )

    def _make_request(self, url: str) -> dict[str, Any]:
        """
        Make API request with retry logic and rate limit handling.

        Handles 429 errors specifically with longer waits and respects Retry-After headers.
        """
        max_attempts = 10
        attempt = 0

        redacted_url = self._redact_url(url)

        while attempt < max_attempts:
            try:
                response = requests.get(url)

                # Handle 404 (not found) and 406 (not acceptable) gracefully
                # These indicate missing data rather than errors worth retrying
                if response.status_code in (404, 406):
                    self.logger.warning(
                        f"No data available (HTTP {response.status_code}): {redacted_url}"
                    )
                    return {"data": [], "pagination": {"total": 0}}

                if response.status_code == 429:
                    retry_after = response.headers.get("Retry-After")
                    if retry_after:
                        try:
                            wait_seconds = int(retry_after)
                            self.logger.warning(
                                f"Rate limited (429). Retry-After header: {wait_seconds}s. "
                                f"Waiting before retry (attempt {attempt + 1}/{max_attempts})"
                            )
                            time.sleep(wait_seconds)
                        except ValueError:
                            self.logger.warning(
                                f"Rate limited (429). Invalid Retry-After header: {retry_after}. "
                                f"Using default wait time (attempt {attempt + 1}/{max_attempts})"
                            )
                            time.sleep(60 * (attempt + 1))
                    else:
                        wait_seconds = 60 * (attempt + 1)
                        self.logger.warning(
                            f"Rate limited (429). No Retry-After header. "
                            f"Waiting {wait_seconds}s before retry (attempt {attempt + 1}/{max_attempts})"
                        )
                        time.sleep(wait_seconds)

                    attempt += 1
                    if attempt >= max_attempts:
                        self.logger.error(
                            f"Rate limit exceeded after {max_attempts} attempts. "
                            f"URL: {redacted_url}"
                        )
                        raise_for_status_safe(response)
                    continue

                raise_for_status_safe(response)
                return response.json()

            except requests.exceptions.HTTPError as e:
                if e.response and e.response.status_code == 429:
                    retry_after = e.response.headers.get("Retry-After")
                    if retry_after:
                        try:
                            wait_seconds = int(retry_after)
                            self.logger.warning(
                                f"Rate limited (429). Retry-After header: {wait_seconds}s. "
                                f"Waiting before retry (attempt {attempt + 1}/{max_attempts})"
                            )
                            time.sleep(wait_seconds)
                        except ValueError:
                            wait_seconds = 60 * (attempt + 1)
                            self.logger.warning(
                                f"Rate limited (429). Invalid Retry-After header: {retry_after}. "
                                f"Waiting {wait_seconds}s before retry (attempt {attempt + 1}/{max_attempts})"
                            )
                            time.sleep(wait_seconds)
                    else:
                        wait_seconds = 60 * (attempt + 1)
                        self.logger.warning(
                            f"Rate limited (429). No Retry-After header. "
                            f"Waiting {wait_seconds}s before retry (attempt {attempt + 1}/{max_attempts})"
                        )
                        time.sleep(wait_seconds)

                    attempt += 1
                    if attempt >= max_attempts:
                        self.logger.error(
                            f"Rate limit exceeded after {max_attempts} attempts. "
                            f"URL: {redacted_url}"
                        )
                        raise
                    continue
                else:
                    raise
            except requests.exceptions.RequestException:
                if attempt < max_attempts - 1:
                    wait_seconds = min(5 * (2**attempt), 30)
                    self.logger.warning(
                        f"Request failed. Retrying in {wait_seconds}s "
                        f"(attempt {attempt + 1}/{max_attempts})"
                    )
                    time.sleep(wait_seconds)
                    attempt += 1
                    continue
                else:
                    raise

        raise requests.exceptions.HTTPError(
            f"Failed after {max_attempts} attempts. URL: {redacted_url}"
        )

    def _filter_usd_prices(self, df: pl.DataFrame) -> pl.DataFrame:
        """Filter DataFrame to only USD-denominated prices.

        MarketStack returns data from all exchanges where a ticker trades,
        each in the local currency. This filters to USD rows only.
        Rows with NULL currency are kept as they represent USD data
        from earlier API responses before the currency field was added.
        """
        if "price_currency" not in df.columns:
            return df

        # When all values are null, Polars assigns Null dtype instead of String.
        # In that case every row is USD (null = no currency info), so return as-is.
        if df["price_currency"].dtype == pl.Null:
            return df

        # Log distinct currency values for debugging filter issues
        distinct_currencies = df["price_currency"].unique().to_list()
        self.logger.info(f"Distinct price_currency values: {distinct_currencies}")

        before_count = len(df)
        filtered = df.filter(
            pl.col("price_currency").is_null()
            | pl.col("price_currency").str.to_lowercase().eq("usd")
        )
        removed = before_count - len(filtered)
        if removed > 0:
            self.logger.info(
                f"Filtered {removed} non-USD rows "
                f"(kept {len(filtered)} of {before_count})"
            )
        if len(filtered) == 0 and before_count > 0:
            self.logger.warning(
                f"USD filter removed ALL {before_count} rows! "
                f"Currencies found: {distinct_currencies}"
            )
        return filtered

    def find_stock(self, ticker: str) -> dict[str, Any]:
        """Find stock information by ticker"""
        url = (
            f"http://api.marketstack.com/v1/tickers/{ticker}?access_key={self.api_key}"
        )
        return self._make_request(url)

    def find_etf_holdings(self, ticker: str) -> dict[str, Any]:
        """Find ETF holdings by ticker"""
        url = f"http://api.marketstack.com/v1/etflist/?access_key={self.api_key}&list={ticker}"
        return self._make_request(url)

    def find_currencies(self) -> dict[str, Any]:
        """Find available currencies"""
        url = f"http://api.marketstack.com/v1/currencies/?access_key={self.api_key}"
        return self._make_request(url)

    @staticmethod
    def _normalize_ticker(ticker: str) -> str:
        """Normalize ticker for MarketStack API.

        MarketStack uses dots to separate symbol from exchange code
        (e.g. AAPL.XNAS). Share-class tickers like BF.B or BRK.B must
        use a hyphen instead (BF-B, BRK-B) to avoid misinterpretation.
        """
        return ticker.replace(".", "-")

    def get_ticker_historical_data(
        self, ticker: str, start_date: str, end_date: str
    ) -> pl.DataFrame:
        """
        Get all historical data for a ticker with pagination and rate limiting.
        start_date and end_date should be in YYYY-MM-DD format
        """
        all_data = []
        offset = 0
        limit = 100
        api_ticker = self._normalize_ticker(ticker)

        while True:
            url = f"http://api.marketstack.com/v2/eod?access_key={self.api_key}&symbols={api_ticker}&date_from={start_date}&date_to={end_date}&offset={offset}&limit={limit}"

            json_data = self._make_request(url)
            pagination = json_data["pagination"]
            data = json_data["data"]

            self.logger.info(f"Fetching offset {offset}: got {len(data)} records")

            all_data.extend(data)

            if offset + len(data) >= pagination["total"]:
                break

            offset += limit

            time.sleep(0.5)

        self.logger.info(f"Total records fetched: {len(all_data)}")

        if all_data:
            return pl.DataFrame(all_data)
        else:
            return pl.DataFrame()

    def get_commodity_historical_data(
        self, commodity_name: str, start_date: str, end_date: str
    ) -> pl.DataFrame:
        """
        Get historical commodity data with pagination and rate limiting.

        Includes delays between pagination requests to avoid hitting rate limits.
        """
        all_data = []
        offset = 0

        while True:
            url = f"http://api.marketstack.com/v2/commoditieshistory?access_key={self.api_key}&commodity_name={commodity_name}&date_from={start_date}&date_to={end_date}&frequency=daily&offset={offset}&limit=100"

            self.logger.info(
                f"Fetching commodity data for: {commodity_name}, offset: {offset}"
            )

            json_data = self._make_request(url)

            self.logger.debug(f"Response keys: {list(json_data.keys())}")

            if "error" in json_data:
                error_info = json_data.get("error", {})
                self.logger.error(f"API Error for {commodity_name}: {error_info}")
                return pl.DataFrame()

            if "result" not in json_data:
                self.logger.error(
                    f"'result' key not found in response for {commodity_name}"
                )
                self.logger.debug(f"Available keys: {list(json_data.keys())}")
                return pl.DataFrame()

            result = json_data["result"]

            if "data" not in result:
                self.logger.error(
                    f"'data' key not found in result for {commodity_name}"
                )
                self.logger.debug(f"Result keys: {list(result.keys())}")
                return pl.DataFrame()

            data = result["data"]
            self.logger.info(f"Got {len(data)} commodity records at offset {offset}")

            if not data:
                self.logger.info("No more data available")
                break

            records_before = len(all_data)
            for commodity_item in data:
                commodity_name_val = commodity_item.get(
                    "commodity_name", commodity_name
                )
                commodity_unit = commodity_item.get("commodity_unit", "")

                prices = commodity_item.get("commodity_prices", [])
                for price_point in prices:
                    flattened_record = {
                        "commodity_name": commodity_name_val,
                        "commodity_unit": commodity_unit,
                        "date": price_point.get("date"),
                        "commodity_price": price_point.get("commodity_price"),
                    }
                    all_data.append(flattened_record)

            records_added = len(all_data) - records_before
            self.logger.debug(
                f"Flattened to {records_added} price records (total: {len(all_data)})"
            )

            if records_added == 0:
                self.logger.info("No new records added, breaking")
                break

            if "pagination" not in result and "pagination" not in json_data:
                self.logger.info(
                    "No pagination info found, assuming single page response"
                )
                break

            pagination = result.get("pagination") or json_data.get("pagination")
            if pagination:
                total = pagination.get("total", 0)
                if len(all_data) >= total or offset + len(data) >= total:
                    self.logger.info(f"Fetched all {total} records")
                    break

            offset += 100

            time.sleep(0.5)

            if offset > 1000:
                self.logger.warning("Reached safety limit of 1000 offset")
                break

        self.logger.info(f"Total records fetched for {commodity_name}: {len(all_data)}")
        return pl.DataFrame(all_data) if all_data else pl.DataFrame()

    def get_splits_data(self, ticker: str) -> pl.DataFrame:
        """
        Get full stock split history for a ticker from the /v2/splits endpoint.

        Returns a Polars DataFrame with columns from the API response.
        Paginates through all results automatically.
        """
        all_data = []
        offset = 0
        limit = 100
        api_ticker = self._normalize_ticker(ticker)

        while True:
            url = (
                f"http://api.marketstack.com/v2/splits"
                f"?access_key={self.api_key}"
                f"&symbols={api_ticker}"
                f"&offset={offset}&limit={limit}"
            )

            json_data = self._make_request(url)
            pagination = json_data.get("pagination", {})
            data = json_data.get("data", [])

            self.logger.info(
                f"Fetching splits for {ticker}, offset {offset}: got {len(data)} records"
            )

            all_data.extend(data)

            total = pagination.get("total", 0)
            if offset + len(data) >= total or not data:
                break

            offset += limit
            time.sleep(0.5)

        self.logger.info(f"Total split records fetched for {ticker}: {len(all_data)}")

        if all_data:
            df = pl.DataFrame(all_data)
            return self._filter_us_splits(df, ticker)
        else:
            return pl.DataFrame()

    def _filter_us_splits(self, df: pl.DataFrame, ticker: str) -> pl.DataFrame:
        """Filter splits to US exchange listings only.

        The splits API can return data for the same symbol on multiple exchanges.
        Without filtering, non-US split records could produce incorrect adjustment
        factors when applied to US OHLC series.
        """
        if "exchange" not in df.columns:
            return df

        us_exchanges = {"XNYS", "XNAS", "XASE", "ARCX", "BATS"}
        before_count = len(df)
        filtered = df.filter(
            pl.col("exchange").is_null() | pl.col("exchange").is_in(us_exchanges)
        )
        removed = before_count - len(filtered)
        if removed > 0:
            self.logger.info(
                f"Filtered {removed} non-US split records for {ticker} "
                f"(kept {len(filtered)} of {before_count})"
            )
        return filtered

    # ==================== SEC EDGAR Functions ====================

    def get_cik_code(self, ticker: str) -> dict[str, Any]:
        """
        Get SEC CIK (Central Index Key) code for a company by ticker symbol.

        Args:
            ticker: Stock ticker symbol (e.g., "AAPL", "MSFT")

        Returns:
            Dict containing CIK code and company information
        """
        url = f"http://api.marketstack.com/v2/cikcode?access_key={self.api_key}&company_name={ticker}"
        return self._make_request(url)

    def get_company_by_cik(self, cik: str) -> dict[str, Any]:
        """
        Get company information by SEC CIK code.

        Args:
            cik: SEC Central Index Key (e.g., "0000320193" for Apple)

        Returns:
            Dict containing company name and details
        """
        url = f"http://api.marketstack.com/v2/companyname?access_key={self.api_key}&cik={cik}"
        return self._make_request(url)

    def get_sec_submissions(
        self, cik: str, form_type: str | None = None
    ) -> dict[str, Any]:
        """
        Get SEC filing submissions for a company.

        Args:
            cik: SEC Central Index Key
            form_type: Optional filter for form type (e.g., "10-K", "10-Q", "8-K")

        Returns:
            Dict containing list of SEC filings/submissions
        """
        url = f"http://api.marketstack.com/v2/submissions?access_key={self.api_key}&cik={cik}"
        if form_type:
            url += f"&form_type={form_type}"
        return self._make_request(url)

    def get_company_facts(self, cik: str) -> dict[str, Any]:
        """
        Get company facts from SEC EDGAR (financial data, metrics).

        Args:
            cik: SEC Central Index Key

        Returns:
            Dict containing company facts and financial data from SEC filings
        """
        url = f"http://api.marketstack.com/v2/companyfacts?access_key={self.api_key}&cik={cik}"
        return self._make_request(url)

    def get_cik_codes_batch(self, tickers: list[str]) -> pl.DataFrame:
        """
        Get CIK codes for multiple tickers with rate limiting.

        Args:
            tickers: List of stock ticker symbols

        Returns:
            Polars DataFrame with columns: ticker, cik, company_name, status
        """
        results = []

        for i, ticker in enumerate(tickers):
            try:
                self.logger.info(f"Fetching CIK for {ticker} ({i + 1}/{len(tickers)})")
                response = self.get_cik_code(ticker)

                # Extract CIK from response (structure may vary)
                if "data" in response:
                    data = response["data"]
                    results.append(
                        {
                            "ticker": ticker,
                            "cik": data.get("cik", ""),
                            "company_name": data.get("company_name", ""),
                            "status": "success",
                        }
                    )
                elif "cik" in response:
                    results.append(
                        {
                            "ticker": ticker,
                            "cik": response.get("cik", ""),
                            "company_name": response.get("company_name", ""),
                            "status": "success",
                        }
                    )
                else:
                    results.append(
                        {
                            "ticker": ticker,
                            "cik": "",
                            "company_name": "",
                            "status": "no_data",
                        }
                    )

                # Rate limiting between requests
                time.sleep(0.5)

            except Exception as e:
                self.logger.warning(f"Failed to get CIK for {ticker}: {e}")
                results.append(
                    {
                        "ticker": ticker,
                        "cik": "",
                        "company_name": "",
                        "status": f"error: {str(e)[:50]}",
                    }
                )

        return pl.DataFrame(results) if results else pl.DataFrame()


marketstack_resource = MarketStackResource(api_key=dg.EnvVar("MARKETSTACK_API_KEY"))
