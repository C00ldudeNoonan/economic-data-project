import os
from utils import load_csv_data_to_duck_db
import re

#find all the csvs in the data folder that start with RDC and add them as a table to the duckdb database

for file in os.listdir('data'):
    if file.startswith('RDC') and file.endswith('.csv'):
        # for the table name extract the word before _History.csv using regular expression
        match = re.search(r'(.+)_History.csv', file)
        if match:
            table_name = match.group(1)
            print("Updated table: ", table_name)
            load_csv_data_to_duck_db(table_name, os.path.join('data', file))