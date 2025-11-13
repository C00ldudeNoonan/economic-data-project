import duckdb
import polars as pl
import dagster as dg
from typing import List, Union, Optional, Dict, Any
from pydantic import Field
import os
import io


class MotherDuckResource(dg.ConfigurableResource):
    """Enhanced Dagster resource for managing MotherDuck database connections and operations."""

    md_token: str = Field(
        description="MotherDuck token for authentication",
    )
    md_database: str = Field(description="MotherDuck database name", default="local")
    md_schema: str = Field(description="MotherDuck schema name", default="public")
    local_path: str = Field(
        description="Local DuckDB file path", default="local.duckdb"
    )
    environment: str = Field(description="Environment (dev or prod)", default="LOCAL")

    @property
    def db_connection(self) -> str:
        """Get the database connection string based on environment."""
        if self.environment == "dev":
            return self.local_path
        return f"md:?motherduck_token={self.md_token}"

    def get_connection(self) -> duckdb.DuckDBPyConnection:
        """Create a database connection."""
        conn = None
        try:
            # If it's a local file path and the file exists but is empty/invalid, delete it
            if (
                self.environment == "dev"
                and self.local_path
                and os.path.exists(self.local_path)
            ):
                try:
                    # Try to connect - if it fails with IO error, delete the file
                    conn = duckdb.connect(self.db_connection)
                except duckdb.IOException:
                    # File exists but is not a valid DuckDB file, delete it
                    os.remove(self.local_path)
                    conn = duckdb.connect(self.db_connection)
                except Exception:
                    # For other errors, try connecting normally
                    conn = duckdb.connect(self.db_connection)
            else:
                conn = duckdb.connect(self.db_connection)

            if self.environment != "dev":
                # Create database if it doesn't exist
                try:
                    conn.execute(f"CREATE DATABASE IF NOT EXISTS {self.md_database}")
                except Exception:
                    # Database might already exist or we might not have permissions
                    pass
                # Use the database
                conn.execute(f"USE {self.md_database}")
                try:
                    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self.md_schema}")
                except Exception:
                    pass
                conn.execute(f"USE {self.md_database}.{self.md_schema}")

            conn.commit()
            return conn
        except duckdb.ConnectionException as e:
            if "different configuration" in str(e):
                # Wait for connections to close
                import time

                time.sleep(0.5)
                # Retry once
                try:
                    conn = duckdb.connect(self.db_connection)
                    conn.commit()
                    return conn
                except Exception:
                    raise e
            raise

    def drop_create_duck_db_table(
        self, table_name: str, df: Union[pl.DataFrame, duckdb.DuckDBPyRelation]
    ):
        """Drop and recreate a table with the provided DataFrame data."""
        conn = None
        try:
            conn = self.get_connection()
            conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
            conn.commit()
        finally:
            if conn:
                conn.close()
        return self.db_connection

    @staticmethod
    def map_dtype(dtype: pl.DataType) -> str:
        """Map Polars data types to DuckDB data types."""
        type_mapping = {
            pl.Int32: "INTEGER",
            pl.Int64: "INTEGER",
            pl.Float32: "DOUBLE",
            pl.Float64: "DOUBLE",
            pl.Boolean: "BOOLEAN",
            pl.Date: "DATE",
            pl.Datetime: "TIMESTAMP",
        }
        return type_mapping.get(type(dtype), "VARCHAR")

    def upsert_data(
        self,
        table_name: str,
        data: pl.DataFrame,
        key_columns: List[str],
        context: Optional[dg.AssetExecutionContext] = None,
    ):
        """Upsert data into a table based on key columns."""
        log = context.log if context else None

        # Enhanced debugging: Log DataFrame state
        def log_info(msg: str):
            if log:
                log.info(msg)
            else:
                print(msg)

        def log_error(msg: str):
            if log:
                log.error(msg)
            else:
                print(f"ERROR: {msg}")

        def log_warning(msg: str):
            if log:
                log.warning(msg)
            else:
                print(f"WARNING: {msg}")

        log_info(f"Starting upsert_data for table: {table_name}")
        log_info(f"DataFrame shape: {data.shape}")
        log_info(f"DataFrame columns: {data.columns}")
        log_info(f"DataFrame column count: {len(data.columns)}")
        log_info(f"DataFrame row count: {len(data)}")
        log_info(f"Key columns: {key_columns}")

        # Validate DataFrame has columns
        if len(data.columns) == 0:
            error_msg = (
                f"DataFrame for table '{table_name}' has no columns! "
                f"This will cause a 'Table must have at least one column' error. "
                f"DataFrame shape: {data.shape}, DataFrame info: {data}"
            )
            log_error(error_msg)
            raise ValueError(error_msg)

        # Validate key columns exist in DataFrame
        missing_key_columns = [col for col in key_columns if col not in data.columns]
        if missing_key_columns:
            error_msg = (
                f"Key columns {missing_key_columns} not found in DataFrame columns {data.columns} "
                f"for table '{table_name}'"
            )
            log_error(error_msg)
            raise ValueError(error_msg)

        # Log data types for debugging
        columns_and_types = list(zip(data.columns, data.dtypes))
        log_info(f"DataFrame columns and types: {columns_and_types}")

        conn = None
        try:
            conn = self.get_connection()
            log_info(f"Connected to database: {self.db_connection}")

            # Create table if it doesn't exist
            column_definitions = [
                f"{col} {self.map_dtype(dtype)}"
                for col, dtype in zip(data.columns, data.dtypes)
            ]
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {", ".join(column_definitions)}
            )
            """
            log_info(f"Create table query: {create_table_query}")

            try:
                conn.execute(create_table_query)
                log_info(f"Successfully created/verified table: {table_name}")
            except Exception as e:
                error_msg = (
                    f"Failed to create table '{table_name}'. "
                    f"Query: {create_table_query}. "
                    f"DataFrame columns: {data.columns}, "
                    f"DataFrame dtypes: {data.dtypes}, "
                    f"DataFrame shape: {data.shape}"
                )
                log_error(error_msg)
                raise RuntimeError(error_msg) from e

            # Handle empty DataFrame case
            if len(data) == 0:
                log_warning(
                    f"DataFrame for table '{table_name}' is empty (0 rows). "
                    f"Skipping upsert operations but table structure is created."
                )
                conn.commit()
                return

            # Create temporary table for new data
            log_info(
                f"Creating temporary table temp_{table_name} with {len(data)} rows"
            )
            try:
                conn.execute(
                    f"CREATE TEMPORARY TABLE temp_{table_name} AS SELECT * FROM data"
                )
                log_info(f"Successfully created temporary table temp_{table_name}")
            except Exception as e:
                error_msg = (
                    f"Failed to create temporary table temp_{table_name}. "
                    f"DataFrame columns: {data.columns}, "
                    f"DataFrame shape: {data.shape}, "
                    f"DataFrame sample: {data.head(3) if len(data) > 0 else 'empty'}"
                )
                log_error(error_msg)
                raise RuntimeError(error_msg) from e

            # Update existing rows
            non_key_columns = [col for col in data.columns if col not in key_columns]
            if non_key_columns:
                update_query = f"""
                UPDATE {table_name}
                SET {", ".join([f"{col} = temp_{table_name}.{col}" for col in non_key_columns])}
                FROM temp_{table_name}
                WHERE {" AND ".join([f"{table_name}.{col} = temp_{table_name}.{col}" for col in key_columns])}
                """
                log_info(
                    f"Executing update query for {len(non_key_columns)} non-key columns"
                )
                try:
                    conn.execute(update_query)
                    log_info("Update query executed successfully")
                except Exception as e:
                    error_msg = (
                        f"Failed to execute update query for table '{table_name}'. "
                        f"Query: {update_query}. "
                        f"Key columns: {key_columns}, "
                        f"Non-key columns: {non_key_columns}"
                    )
                    log_error(error_msg)
                    raise RuntimeError(error_msg) from e

            # Insert new rows
            insert_query = f"""
            INSERT INTO {table_name}
            SELECT * FROM temp_{table_name}
            WHERE NOT EXISTS (
                SELECT 1 FROM {table_name}
                WHERE {" AND ".join([f"{table_name}.{col} = temp_{table_name}.{col}" for col in key_columns])}
            )
            """
            log_info("Executing insert query")
            try:
                conn.execute(insert_query)
                log_info("Insert query executed successfully")
            except Exception as e:
                error_msg = (
                    f"Failed to execute insert query for table '{table_name}'. "
                    f"Query: {insert_query}. "
                    f"Key columns: {key_columns}"
                )
                log_error(error_msg)
                raise RuntimeError(error_msg) from e

            # Clean up
            log_info(f"Dropping temporary table temp_{table_name}")
            conn.execute(f"DROP TABLE temp_{table_name}")
            conn.commit()
            log_info(f"Successfully completed upsert_data for table: {table_name}")
        except Exception as e:
            error_msg = (
                f"Error in upsert_data for table '{table_name}': {str(e)}. "
                f"DataFrame shape: {data.shape}, "
                f"DataFrame columns: {data.columns}, "
                f"Key columns: {key_columns}"
            )
            log_error(error_msg)
            raise
        finally:
            if conn:
                conn.close()
                log_info("Closed database connection")

    def read_data(self, table_name: str) -> List[dict]:
        """Read data from a table."""
        conn = None
        try:
            conn = self.get_connection()
            df = conn.execute(f"SELECT * FROM {table_name}")
            data_dict = df.to_dicts()
            return data_dict
        finally:
            if conn:
                conn.close()

    # Enhanced methods for querying and data analysis
    def get_unique_categories(self, table_name: str, column: str) -> List[str]:
        """Get unique values from a specified column."""
        query = f"SELECT DISTINCT {column} FROM {table_name} WHERE {column} IS NOT NULL ORDER BY {column}"
        conn = None
        try:
            conn = self.get_connection()
            result = conn.execute(query).pl()
            return result[column].to_list()
        finally:
            if conn:
                conn.close()

    def query_sampled_data(
        self,
        table_name: str,
        filters: Optional[Dict[str, Any]] = None,
        sample_size: int = 50,
        sampling_strategy: str = "top_correlations",
    ) -> str:
        """Query sampled data from DuckDB with various sampling strategies."""
        # Build base WHERE clause
        where_conditions = []
        if filters:
            for column, value in filters.items():
                if isinstance(value, str):
                    where_conditions.append(f"{column} = '{value}'")
                elif isinstance(value, list):
                    value_str = "', '".join(str(v) for v in value)
                    where_conditions.append(f"{column} IN ('{value_str}')")
                else:
                    where_conditions.append(f"{column} = {value}")

        where_clause = (
            " WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        )

        # Build query based on sampling strategy
        if sampling_strategy == "top_correlations":
            query = f"""
                SELECT * FROM {table_name}
                {where_clause}
                ORDER BY GREATEST(
                    ABS(COALESCE(correlation_econ_vs_q1_returns, 0)),
                    ABS(COALESCE(correlation_econ_vs_q2_returns, 0)),
                    ABS(COALESCE(correlation_econ_vs_q3_returns, 0))
                ) DESC
                LIMIT {sample_size}
            """
        elif sampling_strategy == "random":
            query = f"""
                SELECT * FROM {table_name}
                {where_clause}
                ORDER BY RANDOM()
                LIMIT {sample_size}
            """
        elif sampling_strategy == "mixed":
            half_size = sample_size // 2
            query = f"""
                (
                    SELECT * FROM {table_name}
                    {where_clause}
                    ORDER BY GREATEST(
                        ABS(COALESCE(correlation_econ_vs_q1_returns, 0)),
                        ABS(COALESCE(correlation_econ_vs_q2_returns, 0)),
                        ABS(COALESCE(correlation_econ_vs_q3_returns, 0))
                    ) DESC
                    LIMIT {half_size}
                )
                UNION ALL
                (
                    SELECT * FROM {table_name}
                    {where_clause}
                    ORDER BY RANDOM()
                    LIMIT {sample_size - half_size}
                )
            """
        else:
            raise ValueError(f"Unknown sampling strategy: {sampling_strategy}")

        # Execute query and convert to CSV
        conn = None
        try:
            conn = self.get_connection()
            df = conn.execute(query).pl()
            csv_buffer = io.StringIO()
            df.write_csv(csv_buffer)
            return csv_buffer.getvalue()
        finally:
            if conn:
                conn.close()

    def write_results_to_table(
        self,
        json_results: List[Dict[str, Any]],
        output_table: str,
        if_exists: str = "append",
        context: Optional[dg.AssetExecutionContext] = None,
    ) -> None:
        """Write JSON results to a MotherDuck table."""
        # Convert to Polars DataFrame
        df = pl.DataFrame(json_results)

        conn = None
        try:
            conn = self.get_connection()

            # Check if table exists
            try:
                conn.execute(f"SELECT 1 FROM {output_table} LIMIT 1")
                table_exists = True
            except Exception:
                table_exists = False

            if table_exists and if_exists == "fail":
                raise ValueError(f"Table {output_table} already exists")

            # Write to table
            if if_exists == "replace" or not table_exists:
                # Create or replace table
                conn.execute(
                    f"CREATE OR REPLACE TABLE {output_table} AS SELECT * FROM df"
                )
                if context:
                    context.log.info(
                        f"Created table {output_table} with {len(df)} records"
                    )
            else:  # append
                # Insert into existing table
                conn.execute(f"INSERT INTO {output_table} SELECT * FROM df")
                if context:
                    context.log.info(f"Appended {len(df)} records to {output_table}")

            conn.commit()
        finally:
            if conn:
                conn.close()

    def execute_query(
        self, query: str, read_only: bool = True, params: Optional[List[Any]] = None
    ) -> pl.DataFrame:
        """Execute a SQL query and return results as Polars DataFrame."""
        conn = None
        try:
            conn = self.get_connection()
            if params:
                result = conn.execute(query, params)
            else:
                result = conn.execute(query)
            return result.pl()
        finally:
            if conn:
                conn.close()

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database."""
        conn = None
        try:
            conn = self.get_connection()
            conn.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
            return True
        except Exception:
            return False
        finally:
            if conn:
                conn.close()


environment = os.getenv("ENVIRONMENT", "dev")

motherduck_resource = MotherDuckResource(
    environment=environment,
    md_token=dg.EnvVar("MOTHERDUCK_TOKEN") if environment != "dev" else "",
    md_database=dg.EnvVar("MOTHERDUCK_DATABASE") if environment != "dev" else "dev",
    md_schema=dg.EnvVar("MOTHERDUCK_PROD_SCHEMA") if environment != "dev" else "public",
    local_path="local.duckdb",
)
