import duckdb


def drop_create_duck_db_table(table_name, df):
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

def load_csv_data_to_duck_db(table_name, csv_file_path):
    # Define the database path
    db_path = 'evidence_project/sources/econ/econ_db.duckdb'
    csv_file_path = csv_file_path.replace('\\', '/')


    try:
    # Connect to the DuckDB database
        con = duckdb.connect(db_path, read_only=False)
        # Replace the table in the DuckDB database with the data from the DataFrame
        con.execute(f'DROP TABLE IF EXISTS {table_name}')
        con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{csv_file_path}')")
        # Save the DuckDB file
        con.commit()
    except Exception as e:
        print(e)
    finally:


        con.close()

    return db_path