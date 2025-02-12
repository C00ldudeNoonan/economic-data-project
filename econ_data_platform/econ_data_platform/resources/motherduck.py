import duckdb
import polars as pl
import dagster as dg
from typing import List, Union
from pydantic import Field
import os

class MotherDuckResource(dg.ConfigurableResource):
    """A Dagster resource for managing MotherDuck database connections and operations."""

    # Define the configuration schema

    md_token: str = Field(
        description="MotherDuck token for authentication",
    )
    md_database: str = Field(
        description="MotherDuck database name",
        default="local"
    )
    md_schema: str = Field(
        description="MotherDuck schema name",
        default="public"
    )
    local_path: str = Field(
        description="Local DuckDB file path",
        default="local.duckdb"
    )
    environment: str = Field(
        description="Environment (LOCAL or PROD)",
        default="LOCAL"
    )


    @property
    def db_connection(self) -> str:
        """Get the database connection string based on environment."""
        if self.environment == "LOCAL":
            return self.local_path
        return f"md:?motherduck_token={self.md_token}"

    def get_connection(self, read_only: bool = False) -> duckdb.DuckDBPyConnection:
        """Create a database connection."""
        conn = duckdb.connect(self.db_connection, read_only=read_only)
        if self.environment != "LOCAL":
            conn.execute(f"USE {self.md_database}.{self.md_schema}")
        conn.commit()
        return conn

    def drop_create_duck_db_table(
        self, table_name: str, df: Union[pl.DataFrame, duckdb.DuckDBPyRelation]
    ):
        """Drop and recreate a table with the provided DataFrame data."""
        conn = None
        try:
            conn = self.get_connection(read_only=False)
            conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
            conn.commit()
        finally:
            if conn:
                conn.close()
        return self.db_connection
    
    def drop_create_partitioned_table(
        self, table_name: str, df: Union[pl.DataFrame, duckdb.DuckDBPyRelation], parition_key, paritition_column
    ):
        """Drop and recreate a table with the provided DataFrame data."""
        conn = None
        try:
            conn = self.get_connection(read_only=False)
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
            pl.Date: "TIMESTAMP",
            pl.Datetime: "TIMESTAMP",
        }
        return type_mapping.get(type(dtype), "VARCHAR")

    def upsert_data(self, table_name: str, data: pl.DataFrame, key_columns: List[str]):
        """Upsert data into a table based on key columns."""
        conn = None
        try:
            conn = self.get_connection(read_only=False)

            # Create table if it doesn't exist
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {", ".join([f"{col} {self.map_dtype(dtype)}" for col, dtype in zip(data.columns, data.dtypes)])}
            )
            """
            conn.execute(create_table_query)

            # Create temporary table for new data
            conn.execute(
                f"CREATE TEMPORARY TABLE temp_{table_name} AS SELECT * FROM data"
            )

            # Update existing rows
            non_key_columns = [col for col in data.columns if col not in key_columns]
            if non_key_columns:
                update_query = f"""
                UPDATE {table_name}
                SET {", ".join([f"{col} = temp_{table_name}.{col}" for col in non_key_columns])}
                FROM temp_{table_name}
                WHERE {" AND ".join([f"{table_name}.{col} = temp_{table_name}.{col}" for col in key_columns])}
                """
                conn.execute(update_query)

            # Insert new rows
            insert_query = f"""
            INSERT INTO {table_name}
            SELECT * FROM temp_{table_name}
            WHERE NOT EXISTS (
                SELECT 1 FROM {table_name}
                WHERE {" AND ".join([f"{table_name}.{col} = temp_{table_name}.{col}" for col in key_columns])}
            )
            """
            conn.execute(insert_query)

            # Clean up
            conn.execute(f"DROP TABLE temp_{table_name}")
            conn.commit()
        finally:
            if conn:
                conn.close()

environment = os.getenv("ENVIRONMENT", "LOCAL")

motherduck_resource = MotherDuckResource(
    environment=environment,
    md_token=dg.EnvVar("MOTHERDUCK_TOKEN") if environment != "LOCAL" else "",
    md_database=dg.EnvVar("MOTHERDUCK_DATABASE") if environment != "LOCAL" else "local",
    md_schema=dg.EnvVar("MOTHERDUCK_PROD_SCHEMA") if environment != "LOCAL" else "public",
    local_path="local.duckdb"
)


