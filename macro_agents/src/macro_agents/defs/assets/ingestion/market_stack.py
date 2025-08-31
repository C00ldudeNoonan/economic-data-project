import requests
import json
import polars as pl
from typing import Dict, Any
import dagster as dg
from pydantic import Field
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from macro_agents.defs.resources.motherduck import MotherDuckResource
from macro_agents.defs.assets.constants.market_stack_constants import ALL_TICKER_GROUPS

class MarketStackResource(dg.ConfigurableResource):
    """MarketStack API client as a Dagster configurable resource"""
    
    api_key: str = Field(description="MarketStack API key")
    
    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=1, max=10), 
           retry=retry_if_exception_type((requests.exceptions.RequestException, requests.exceptions.HTTPError)))
    def _make_request(self, url: str) -> Dict[str, Any]:
        """Make API request with retry logic"""
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    
    def find_stock(self, ticker: str) -> Dict[str, Any]:
        """Find stock information by ticker"""
        url = f"http://api.marketstack.com/v1/tickers/{ticker}?access_key={self.api_key}"
        return self._make_request(url)

    def find_etf_holdings(self, ticker: str) -> Dict[str, Any]:
        """Find ETF holdings by ticker"""
        url = f"http://api.marketstack.com/v1/etflist/?access_key={self.api_key}&list={ticker}"
        return self._make_request(url)

    def find_currencies(self) -> Dict[str, Any]:
        """Find available currencies"""
        url = f"http://api.marketstack.com/v1/currencies/?access_key={self.api_key}"
        return self._make_request(url)

    def get_ticker_historical_data(self, ticker: str, start_date: str, end_date: str) -> pl.DataFrame:
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
            pagination = json_data['pagination']
            data = json_data['data']
            
            print(f"Fetching offset {offset}: got {len(data)} records")
            
            all_data.extend(data)
            
            if offset + len(data) >= pagination['total']:
                break
                
            offset += limit
        
        print(f"Total records fetched: {len(all_data)}")
        
        if all_data:
            return pl.DataFrame(all_data)
        else:
            return pl.DataFrame()

    def get_commodity_historical_data(self, commodity_name: str, start_date: str, end_date: str) -> pl.DataFrame:
        """Get historical commodity data with pagination"""
        all_data = []
        offset = 0
        
        while True:
            url = f"http://api.marketstack.com/v2/commoditieshistory?access_key={self.api_key}&commodity_name={commodity_name}&date_from={start_date}&date_to={end_date}&frequency=day&offset={offset}&limit=100"
            json_data = self._make_request(url)
            
            data = json_data['data']
            all_data.extend(data)
            
            if len(data) < 100 or offset + len(data) >= json_data['pagination']['total']:
                break
                
            offset += 100
        
        return pl.DataFrame(all_data) if all_data else pl.DataFrame()


# Create partition definitions
ticker_partitions = dg.StaticPartitionsDefinition(ALL_TICKER_GROUPS)

weekly_partitions = dg.WeeklyPartitionsDefinition(start_date="2020-01-01")

# Multi-dimensional partitions
ticker_weekly_partitions = dg.MultiPartitionsDefinition({
    "ticker": ticker_partitions,
    "date": weekly_partitions
})

