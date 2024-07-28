import polars as pl
from dotenv import load_dotenv 
import os
from utils import load_data_to_duckdb
import json
from datetime import datetime
# Load environment variables from .env file
load_dotenv()

fred_api_key = os.getenv('FRED_API_KEY')

series = [("BAMLH0A0HYM2", "High Yield Bond Rate"), ("DJIA", "Dow Jones Industrial Average"), ("DFF", "Federal Funds Effective Rate"),
          ("MORTGAGE30US", "30 year Mortgage Rate"), ("USAUCSFRCONDOSMSAMID", "Zillow Housing Index"), ("DTWEXBGS", "Dollar Index"),
          ("DGS10", "10 year Treasury Rate"), ("USCONS", "All Employees: Construction"), ("LFWA64TTUSM647S", "Working-Age Population Total: From 15 to 64 Years"),
          ("EXHOSLUSM495S", "Existing Home Sales") , ("MDSP", "Mortgage Debt Service Payments as a Percent of Disposable Personal Income"),
          ("CDSP", "Consumer Debt Service Payments as a Percent of Disposable Personal Income"), ("MEDDAYONMARUS", "Median Days on The Market")
          ,("MEDLISPRIPERSQUFEEUS", "Median Listing Price per Square Feet in the United States"), ("WPUIP2311102", "roducer Price Index by Commodity: Inputs to Industries: Net Inputs to Single Family Residential Construction, Services"),
          ("TTLHH", "Total Households"), ("TTLFHH", "Total Family Households"), ("TTLHHM156N", "Household Estimates"), ("T4232MM157NCEN", " Merchant Wholesalers, Except Manufacturers' Sales Branches and Offices: Durable Goods: Furniture and Home Furnishings Inventories")]



