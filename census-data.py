import requests
import duckdb
import pandas as pd
from dotenv import load_dotenv 
import os

# Load environment variables from .env file
load_dotenv()

census_api_key = os.getenv('CENSUS_API_KEY')

# Load the data into a DuckDB database
def load_data_to_duckdb(table_name, df):
    # Define the database path
    db_path = 'evidence_project/sources/econ/econ_db.duckdb'

    try:
    # Connect to the DuckDB database
        con = duckdb.connect(db_path, read_only=False)
        # need
        # Replace the table in the DuckDB database with the data from the DataFrame
        con.execute(f'DROP TABLE IF EXISTS {table_name}')
        con.execute(f'CREATE TABLE {table_name} AS SELECT * FROM df')
        # Save the DuckDB file
        con.commit()
    except Exception as e:
        print(e)
    finally:


        con.close()

    return db_path

def get_housing_inventory(census_api_key):
    # Get the data from the Census API

    # need to paginate the calls and then 
    year_list = list(range(1999, 2025))
    main_df = pd.DataFrame()
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
        df = pd.DataFrame(rows, columns=columns)
        # appending the df to the main_df
        main_df = pd.concat([main_df, df], axis=0, ignore_index=True)



    # save df to csv in the data folder as the name housing_inventory.csv

    # Create the new column by mapping 'data_type_code' to 'Series Name'
    main_df['series_name'] = main_df['data_type_code'].map(mapping_dict)

    main_df['plot_groupings'] = main_df['series_name'].map(series_name_to_plot_groupings)

    main_df.to_csv('data/housing_inventory.csv', index=False)
    # this dataframe is also saved as a duckdb table in the sources folder as economic_data.duckdb
    load_data_to_duckdb('housing_inventory', main_df)

    return 'housing_inventory added to database'


print(get_housing_inventory(census_api_key))



