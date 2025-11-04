import requests
import polars as pl
from typing import Dict, Any
import dagster as dg
from pydantic import Field
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)


class MarketStackResource(dg.ConfigurableResource):
    """MarketStack API client as a Dagster configurable resource"""

    api_key: str = Field(description="MarketStack API key")

    @property
    def logger(self):
        """Get Dagster logger"""
        return dg.get_dagster_logger()

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(
            (requests.exceptions.RequestException, requests.exceptions.HTTPError)
        ),
    )
    def _make_request(self, url: str) -> Dict[str, Any]:
        """Make API request with retry logic"""
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    def find_stock(self, ticker: str) -> Dict[str, Any]:
        """Find stock information by ticker"""
        url = (
            f"http://api.marketstack.com/v1/tickers/{ticker}?access_key={self.api_key}"
        )
        return self._make_request(url)

    def find_etf_holdings(self, ticker: str) -> Dict[str, Any]:
        """Find ETF holdings by ticker"""
        url = f"http://api.marketstack.com/v1/etflist/?access_key={self.api_key}&list={ticker}"
        return self._make_request(url)

    def find_currencies(self) -> Dict[str, Any]:
        """Find available currencies"""
        url = f"http://api.marketstack.com/v1/currencies/?access_key={self.api_key}"
        return self._make_request(url)

    def get_ticker_historical_data(
        self, ticker: str, start_date: str, end_date: str
    ) -> pl.DataFrame:
        """
        Get all historical data for a ticker with pagination
        start_date and end_date should be in YYYY-MM-DD format
        """
        all_data = []
        offset = 0
        limit = 100

        while True:
            url = f"http://api.marketstack.com/v2/eod?access_key={self.api_key}&symbols={ticker}&date_from={start_date}&date_to={end_date}&offset={offset}&limit={limit}"

            json_data = self._make_request(url)
            pagination = json_data["pagination"]
            data = json_data["data"]

            self.logger.info(f"Fetching offset {offset}: got {len(data)} records")

            all_data.extend(data)

            if offset + len(data) >= pagination["total"]:
                break

            offset += limit

        self.logger.info(f"Total records fetched: {len(all_data)}")

        if all_data:
            return pl.DataFrame(all_data)
        else:
            return pl.DataFrame()

    def get_commodity_historical_data(
        self, commodity_name: str, start_date: str, end_date: str
    ) -> pl.DataFrame:
        """Get historical commodity data with pagination"""
        all_data = []
        offset = 0

        while True:
            url = f"http://api.marketstack.com/v2/commoditieshistory?access_key={self.api_key}&commodity_name={commodity_name}&date_from={start_date}&date_to={end_date}&frequency=daily&offset={offset}&limit=100"
            
            self.logger.info(f"Fetching commodity data for: {commodity_name}, offset: {offset}")
            
            json_data = self._make_request(url)
            
            # Log the response structure for debugging
            self.logger.debug(f"Response keys: {list(json_data.keys())}")
            
            # Check for error response
            if "error" in json_data:
                error_info = json_data.get("error", {})
                self.logger.error(f"API Error for {commodity_name}: {error_info}")
                # Return empty DataFrame on error
                return pl.DataFrame()
            
            # The API returns data under "result" -> "data"
            if "result" not in json_data:
                self.logger.error(f"'result' key not found in response for {commodity_name}")
                self.logger.debug(f"Available keys: {list(json_data.keys())}")
                return pl.DataFrame()

            result = json_data["result"]
            
            if "data" not in result:
                self.logger.error(f"'data' key not found in result for {commodity_name}")
                self.logger.debug(f"Result keys: {list(result.keys())}")
                return pl.DataFrame()

            data = result["data"]
            self.logger.info(f"Got {len(data)} commodity records at offset {offset}")
            
            if not data:
                self.logger.info("No more data available")
                break
            
            # Flatten the nested structure
            # Each item in data has: commodity_name, commodity_unit, commodity_prices (array)
            records_before = len(all_data)
            for commodity_item in data:
                commodity_name_val = commodity_item.get("commodity_name", commodity_name)
                commodity_unit = commodity_item.get("commodity_unit", "")
                
                # Extract each price point
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
            self.logger.debug(f"Flattened to {records_added} price records (total: {len(all_data)})")

            if records_added == 0:
                self.logger.info("No new records added, breaking")
                break
            
            if "pagination" not in result and "pagination" not in json_data:
                self.logger.info("No pagination info found, assuming single page response")
                break
            
            pagination = result.get("pagination") or json_data.get("pagination")
            if pagination:
                total = pagination.get("total", 0)
                if len(all_data) >= total or offset + len(data) >= total:
                    self.logger.info(f"Fetched all {total} records")
                    break
            
            offset += 100
            
            if offset > 1000:  
                self.logger.warning("Reached safety limit of 1000 offset")
                break

        self.logger.info(f"Total records fetched for {commodity_name}: {len(all_data)}")
        return pl.DataFrame(all_data) if all_data else pl.DataFrame()


marketstack_resource = MarketStackResource(api_key=dg.EnvVar("MARKETSTACK_API_KEY"))
