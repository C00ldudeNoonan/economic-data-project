import polars as pl
from dotenv import load_dotenv 
import os
import requests
from utils import upsert_data

load_dotenv()

fred_api_key = os.getenv('FRED_API_KEY')

series = [("BAMLH0A0HYM2", "High Yield Bond Rate"), ("DJIA", "Dow Jones Industrial Average"), ("DFF", "Federal Funds Effective Rate"),
          ("MORTGAGE30US", "30 year Mortgage Rate"), ("USAUCSFRCONDOSMSAMID", "Zillow Housing Index"), ("DTWEXBGS", "Dollar Index"),
          ("DGS10", "10 year Treasury Rate"), ("USCONS", "All Employees: Construction"), ("LFWA64TTUSM647S", "Working-Age Population Total: From 15 to 64 Years"),
          ("EXHOSLUSM495S", "Existing Home Sales") , ("MDSP", "Mortgage Debt Service Payments as a Percent of Disposable Personal Income"),
          ("CDSP", "Consumer Debt Service Payments as a Percent of Disposable Personal Income"), ("MEDDAYONMARUS", "Median Days on The Market")
          ,("MEDLISPRIPERSQUFEEUS", "Median Listing Price per Square Feet in the United States"), ("WPUIP2311102", "roducer Price Index by Commodity: Inputs to Industries: Net Inputs to Single Family Residential Construction, Services"),
          ("TTLHH", "Total Households"), ("TTLFHH", "Total Family Households"), ("TTLHHM156N", "Household Estimates"), ("T4232MM157NCEN", " Merchant Wholesalers, Except Manufacturers' Sales Branches and Offices: Durable Goods: Furniture and Home Furnishings Inventories")]



def get_fred_data(series_code, series_name, key):

    url = f"https://api.stlouisfed.org/fred/series/observations?series_id={series_code}&api_key={key}&file_type=json&"

    response = requests.get(url)
    data = response.json()

    df = pl.DataFrame(data['observations'])
    df = df.with_columns(
        series_name = pl.lit(series_name),
        series_code = pl.lit(series_code)
    )
    df = df.drop(['realtime_start', 'realtime_end'])
    df = df.with_columns(
        pl.col('date').str.strptime(pl.Date),
        pl.when(pl.col('value') == '.').then(None).otherwise(pl.col('value')).cast(pl.Float64)
    )
    df = df.drop_nulls('value')

    print(len(df))


    return df



for series_code, series_name in series:
    data = get_fred_data(series_code, series_name, fred_api_key)
    upsert_data('evidence_project/sources/econ/econ_db.duckdb', 'fred_data', data, ['date', 'series_code'])
    print(f"Updated table: {series_name}")
