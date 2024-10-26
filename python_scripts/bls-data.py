import requests
import polars as pl
from dotenv import load_dotenv 
import os
from utils import drop_create_duck_db_table
from datetime import datetime

# Load environment variables from .env file
load_dotenv()


def get_housing_inventory(census_api_key):
    # Get the data from the Census API

    # need to paginate the calls and then 
    year_list = list(range(1999, 2025))
    main_df = pl.DataFrame()
    mapping_dict = {
    'RENT': 'Vacant Housing Units For Rent',
    'URE': 'Vacant Housing Units Held off the Market and Usual Residence Elsewhere',
    'RVR': 'Rental Vacancy Rate',
    'E_RVR': 'Error Rental Vacancy Rate',
    'OWNOCC': 'Owner Occupied Units',
    'VACANT': 'Total Vacant Housing Units',
    'YRVAC': 'Year-Round Vacant Housing Units',
    'HOR': 'Homeownership Rate',
    'E_HOR': 'Error Homeownership Rate',
    'HVR': 'Homeowner Vacancy Rate',
    'E_HVR': 'Error Homeowner Vacancy Rate',
    'SAHOR': 'Seasonal Adjusted Home Owner Rate',
    'OCC': 'Total Occupied housing Units',
    'OCCUSE': 'Held Off the Market and for Occasional Use',
    'OFFMAR': 'Held Off the Market Vacant Housing Units',
    'OTH': 'Held off the Market and Vacant for Other Reasons Vacant Housing Units',
    'RNTOCC': 'Renter Occupied Units',
    'RNTSLD': 'Rented or Sold, Not Yet Occupied Vacant Housing Units',
    'SALE': 'Vacant Housing Units For Sale',
    'SEASON': 'Seasonal Vacant Housing Units',
    'TOTAL': 'Total Housing Units'
    }
# Define the second mapping dictionary for 'series_name' to 'Plot groupings'
    series_name_to_plot_groupings = {
        'Error Homeowner Vacancy Rate': 'Error',
        'Error Homeownership Rate': 'Error',
        'Error Rental Vacancy Rate': 'Error',
        'Owner Occupied Units': 'Occupied Inventory',
        'Renter Occupied Units': 'Occupied Inventory',
        'Total Housing Units': 'Total Housing Units',
        'Total Occupied housing Units': 'Total Housing Units',
        'Total Vacant Housing Units': 'Total Housing Units',
        'Held Off the Market and for Occasional Use': 'Vacant Inventory',
        'Held off the Market and Vacant for Other Reasons Vacant Housing Units': 'Vacant Inventory',
        'Held Off the Market Vacant Housing Units': 'Vacant Inventory',
        'Rented or Sold, Not Yet Occupied Vacant Housing Units': 'Vacant Inventory',
        'Seasonal Vacant Housing Units': 'Vacant Inventory',
        'Vacant Housing Units For Rent': 'Vacant Inventory',
        'Vacant Housing Units For Sale': 'Vacant Inventory',
        'Vacant Housing Units Held off the Market and Usual Residence Elsewhere': 'Vacant Inventory',
        'Year-Round Vacant Housing Units': 'Vacant Inventory',
        'Homeowner Vacancy Rate': 'Rates',
        'Homeownership Rate': 'Rates',
        'Rental Vacancy Rate': 'Rates',
        'Seasonal Adjusted Home Owner Rate': 'Rates'
    }
    for year in year_list:

    
        url = f'https://api.census.gov/data/timeseries/eits/hv?get=data_type_code,time_slot_id,seasonally_adj,category_code,cell_value,error_data&for=us:*&time={year}&key={census_api_key}'
        response = requests.get(url)
  
        # need to convert the json to a dataframe
        columns = response.json()[0]
        rows = response.json()[1:]

        # Create DataFrame
        df = pl.DataFrame(rows, schema=columns)
        # appending the df to the main_df
        main_df = pl.concat([main_df, df])

    # Create the new column by mapping 'data_type_code' to 'Series Name'
    main_df = main_df.with_columns(
        pl.col('data_type_code').replace(mapping_dict).alias('series_name')
    )

    main_df = main_df.with_columns(
        pl.col('series_name').replace(series_name_to_plot_groupings).alias('plot_groupings')
    )

    # This dataframe is also saved as a duckdb table in the sources folder as economic_data.duckdb
    drop_create_duck_db_table('housing_inventory', main_df)

    return 'housing_inventory added to database'


def get_household_pulse(census_api_key):
    main_df = pl.DataFrame()
    iterator = True
    while iterator:
        for cycle in list(range(1, datetime.now().month)):
            try:
                url = f'https://api.census.gov/data/timeseries/hhpulse?get=SURVEY_YEAR,NAME,MEASURE_NAME,COL_START_DATE,COL_END_DATE,RATE,TOTAL,MEASURE_DESCRIPTION&for=state:*&time=2024&CYCLE=0{str(cycle)}&key={census_api_key}'
                response = requests.get(url)
                columns = response.json()[0]
                rows = response.json()[1:]
                data = [dict(zip(columns, row)) for row in rows]
                df = pl.DataFrame(data)
                main_df = pl.concat([main_df, df])
            except Exception as e:
                print("series doesnt exist")
                print(cycle)
                print(e)
                break
            
        break       
    drop_create_duck_db_table('housing_pulse', main_df)

    return "housing pulse added to database"

def main():
    census_api_key = os.getenv("CENSUS_API_KEY") # Replace with your actual API key
    print(get_housing_inventory(census_api_key))
    print(get_household_pulse(census_api_key))

if __name__ == "__main__":
    main()