import duckdb
import polars as pl

db_path = 'evidence_project/sources/econ/econ_db.duckdb'


def drop_create_duck_db_table(table_name, df):
    # Define the database path

    try:
    # Connect to the DuckDB database
        conn = duckdb.connect(db_path, read_only=False)
        # need
        # Replace the table in the DuckDB database with the data from the DataFrame
        conn.execute(f'DROP TABLE IF EXISTS {table_name}')
        conn.execute(f'CREATE TABLE {table_name} AS SELECT * FROM df')
        # Save the DuckDB file
        conn.commit()
    except Exception as e:
        print(e)
    finally:


        conn.close()

    return db_path


def load_csv_data_to_duck_db(table_name, csv_file_path):
    # Define the database path
    csv_file_path = csv_file_path.replace('\\', '/')

    try:
    # Connect to the DuckDB database
        conn = duckdb.connect(db_path, read_only=False)
        # Replace the table in the DuckDB database with the data from the DataFrame
        conn.execute(f'DROP TABLE IF EXISTS {table_name}')
        conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{csv_file_path}')")
        # Save the DuckDB file
        conn.commit()
    except Exception as e:
        print(e)
    finally:


        conn.close()

    return db_path


def map_dtype(dtype):
    if dtype == pl.Int32 or dtype == pl.Int64:
        return 'INTEGER'
    elif dtype == pl.Float32 or dtype == pl.Float64:
        return 'DOUBLE'
    elif dtype == pl.Boolean:
        return 'BOOLEAN'
    elif dtype == pl.Date or dtype == pl.Datetime:
        return 'TIMESTAMP'
    else:
        return 'VARCHAR'
    

def upsert_data(table_name, data, key_columns):
    # Connect to DuckDB
    conn = duckdb.connect(database=db_path, read_only=False)
    
    # Ensure the table exists
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {', '.join([f'{col} {map_dtype(dtype)}' for col, dtype in zip(data.columns, data.dtypes)])}
    )
    """
    conn.execute(create_table_query)
    
    # Create a temporary table for the new data
    conn.execute(f"CREATE TEMPORARY TABLE temp_{table_name} AS SELECT * FROM data")
    
    # Update existing rows
    update_query = f"""
    UPDATE {table_name}
    SET {', '.join([f'{col} = temp_{table_name}.{col}' for col in data.columns if col not in key_columns])}
    FROM temp_{table_name}
    WHERE { ' AND '.join([f'{table_name}.{col} = temp_{table_name}.{col}' for col in key_columns]) }
    """
    conn.execute(update_query)
    
    # Insert new rows
    insert_query = f"""
    INSERT INTO {table_name}
    SELECT * FROM temp_{table_name}
    WHERE NOT EXISTS (
        SELECT 1 FROM {table_name}
        WHERE { ' AND '.join([f'{table_name}.{col} = temp_{table_name}.{col}' for col in key_columns]) }
    )
    """
    conn.execute(insert_query)
    
    # Drop the temporary table
    conn.execute(f"DROP TABLE temp_{table_name}")
    
    # Close the connection
    conn.close()