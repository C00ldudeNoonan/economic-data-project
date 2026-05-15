import io
import logging
import os
import re
from typing import Any

import dagster as dg
import duckdb
import polars as pl
from pydantic import Field

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$")


def _validate_identifier(name: str, kind: str = "identifier") -> str:
    """Reject any string that isn't a plain SQL identifier or schema.table.

    Why: identifiers can't be parameterized in DuckDB. Callers that interpolate
    table or column names need a strict allowlist to prevent SQL injection.
    """
    if not isinstance(name, str) or not _IDENTIFIER_RE.match(name):
        raise ValueError(f"Invalid {kind}: {name!r}")
    return name


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
    def _db_connection(self) -> str:
        """Internal: connection string, may contain the MotherDuck token.

        Why: returning this from a public method risks leaking the token into
        Dagster logs, exception traces, and asset metadata. Keep it private and
        only pass it directly to ``duckdb.connect``.
        """
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
                    conn = duckdb.connect(self._db_connection)
                except duckdb.IOException:
                    # File exists but is not a valid DuckDB file, delete it
                    os.remove(self.local_path)
                    conn = duckdb.connect(self._db_connection)
                except Exception:
                    # For other errors, try connecting normally
                    conn = duckdb.connect(self._db_connection)
            else:
                conn = duckdb.connect(self._db_connection)

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
                    conn = duckdb.connect(self._db_connection)
                    conn.commit()
                    return conn
                except Exception:
                    raise e
            raise

    def drop_create_duck_db_table(
        self, table_name: str, df: pl.DataFrame | duckdb.DuckDBPyRelation
    ) -> str:
        """
        Drop and recreate a table with the provided DataFrame data.

        Returns the table name. Previously returned the connection string, which
        could leak the MotherDuck token into Dagster logs and asset metadata.

        Column names are sanitized for DuckDB compatibility (spaces and special
        characters are quoted, reserved keywords are handled).
        """
        conn = None
        try:
            conn = self.get_connection()
            conn.execute(f"DROP TABLE IF EXISTS {table_name}")

            if isinstance(df, pl.DataFrame):
                column_definitions = [
                    f"{self.sanitize_column_name(col)} {self.map_dtype(dtype)}"
                    for col, dtype in zip(df.columns, df.dtypes)
                ]
                create_query = f"""
                CREATE TABLE {table_name} (
                    {", ".join(column_definitions)}
                )
                """
                conn.execute(create_query)

                if len(df) > 0:
                    conn.register("temp_df", df)
                    select_columns = []
                    insert_columns = []
                    for col in df.columns:
                        sanitized = self.sanitize_column_name(col)
                        insert_columns.append(sanitized)
                        select_columns.append(f"temp_df.{sanitized}")
                    column_list = ", ".join(select_columns)
                    insert_column_list = ", ".join(insert_columns)
                    insert_query = f"INSERT INTO {table_name} ({insert_column_list}) SELECT {column_list} FROM temp_df"
                    conn.execute(insert_query)
            else:
                conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")

            conn.commit()
        finally:
            if conn:
                conn.close()
        return table_name

    @staticmethod
    def sanitize_column_name(column_name: str) -> str:
        """
        Sanitize column name for DuckDB compatibility.

        DuckDB column names:
        - Can contain letters, digits, underscores without quoting
        - Must be quoted if they contain spaces, special characters, or are reserved keywords
        - Case-insensitive when unquoted

        This function quotes column names that contain spaces, special characters,
        or are reserved keywords to ensure DuckDB compatibility.
        """
        reserved_keywords = {
            "select",
            "from",
            "where",
            "group",
            "order",
            "by",
            "having",
            "limit",
            "offset",
            "join",
            "inner",
            "left",
            "right",
            "outer",
            "on",
            "as",
            "and",
            "or",
            "not",
            "in",
            "exists",
            "like",
            "ilike",
            "between",
            "is",
            "null",
            "true",
            "false",
            "case",
            "when",
            "then",
            "else",
            "end",
            "cast",
            "create",
            "table",
            "drop",
            "alter",
            "insert",
            "update",
            "delete",
            "index",
            "view",
            "union",
            "all",
            "distinct",
            "count",
            "sum",
            "avg",
            "min",
            "max",
            "date",
            "timestamp",
            "interval",
            "varchar",
            "integer",
            "double",
            "boolean",
        }

        original = column_name.strip()

        if not original:
            return '"_empty_"'

        needs_quoting = (
            " " in original
            or "-" in original
            or "." in original
            or original.lower() in reserved_keywords
            or not original[0].isalpha()
            and original[0] != "_"
            or any(not (c.isalnum() or c == "_") for c in original)
        )

        if needs_quoting:
            return f'"{original}"'

        return original

    @staticmethod
    def map_dtype(dtype: pl.DataType) -> str:
        """Map Polars data types to DuckDB data types."""
        type_mapping: dict[type, str] = {
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
        key_columns: list[str],
        context: dg.AssetExecutionContext | None = None,
    ) -> None:
        """Upsert data into a table based on key columns."""
        log = context.log if context else None
        fallback_logger = logging.getLogger(__name__)

        def log_debug(msg: str):
            if log:
                log.debug(msg)
            else:
                fallback_logger.debug(msg)

        def log_info(msg: str):
            log_debug(msg)

        def log_error(msg: str):
            if log:
                log.error(msg)
            else:
                fallback_logger.error(msg)

        def log_warning(msg: str):
            if log:
                log.warning(msg)
            else:
                fallback_logger.warning(msg)

        log_debug(f"Starting upsert_data for table: {table_name}")
        log_debug(f"DataFrame shape: {data.shape}")
        log_debug(f"DataFrame columns: {data.columns}")
        log_debug(f"DataFrame column count: {len(data.columns)}")
        log_debug(f"DataFrame row count: {len(data)}")
        log_debug(f"Key columns: {key_columns}")

        if len(data.columns) == 0:
            error_msg = (
                f"DataFrame for table '{table_name}' has no columns! "
                f"This will cause a 'Table must have at least one column' error. "
                f"DataFrame shape: {data.shape}, DataFrame info: {data}"
            )
            log_error(error_msg)
            raise ValueError(error_msg)

        missing_key_columns = [col for col in key_columns if col not in data.columns]
        if missing_key_columns:
            error_msg = (
                f"Key columns {missing_key_columns} not found in DataFrame columns {data.columns} "
                f"for table '{table_name}'"
            )
            log_error(error_msg)
            raise ValueError(error_msg)

        columns_and_types = list(zip(data.columns, data.dtypes))
        log_info(f"DataFrame columns and types: {columns_and_types}")

        conn = None
        try:
            conn = self.get_connection()
            log_info("Connected to database")

            column_definitions = [
                f"{self.sanitize_column_name(col)} {self.map_dtype(dtype)}"
                for col, dtype in zip(data.columns, data.dtypes)
            ]

            # Check if existing table schema matches the DataFrame.
            # Fail loudly on mismatch — never silently drop production data.
            # Compare using plain (unquoted, lowercased) names so that
            # sanitize_column_name quoting (e.g. "date") doesn't cause
            # false positives against duckdb_columns() output.
            def _normalize(col: str) -> str:
                return col.strip('"').lower()

            expected_columns = {_normalize(col) for col in data.columns}
            try:
                existing = conn.execute(
                    f"SELECT column_name FROM duckdb_columns() "
                    f"WHERE table_name = '{table_name}' "
                    f"AND database_name = current_database() "
                    f"AND schema_name = current_schema()"
                ).fetchall()
                existing_columns = {_normalize(row[0]) for row in existing}
            except Exception:
                existing_columns = set()

            if existing_columns and existing_columns != expected_columns:
                added = expected_columns - existing_columns
                removed = existing_columns - expected_columns
                raise RuntimeError(
                    f"Schema mismatch for '{table_name}': "
                    f"columns in DataFrame but not in table: {added}, "
                    f"columns in table but not in DataFrame: {removed}. "
                    f"Refusing to drop and recreate — fix the schema manually "
                    f"or update the asset to produce the expected columns."
                )

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
                # Register Polars DataFrame with DuckDB
                temp_df_name = f"temp_df_{table_name}"
                conn.register(temp_df_name, data)
                log_info(f"Registered DataFrame as {temp_df_name}")

                # Create temporary table from registered DataFrame
                column_definitions = [
                    f"{self.sanitize_column_name(col)} {self.map_dtype(dtype)}"
                    for col, dtype in zip(data.columns, data.dtypes)
                ]
                create_temp_query = f"""
                CREATE TEMPORARY TABLE temp_{table_name} (
                    {", ".join(column_definitions)}
                )
                """
                conn.execute(create_temp_query)
                log_info(f"Created temporary table structure temp_{table_name}")

                # Insert data from registered DataFrame into temporary table
                select_columns = []
                insert_columns = []
                for col in data.columns:
                    sanitized = self.sanitize_column_name(col)
                    insert_columns.append(sanitized)
                    select_columns.append(f"{temp_df_name}.{sanitized}")
                column_list = ", ".join(select_columns)
                insert_column_list = ", ".join(insert_columns)
                insert_temp_query = (
                    f"INSERT INTO temp_{table_name} ({insert_column_list}) "
                    f"SELECT {column_list} FROM {temp_df_name}"
                )
                conn.execute(insert_temp_query)
                log_info(f"Successfully populated temporary table temp_{table_name}")
            except Exception as e:
                error_msg = (
                    f"Failed to create temporary table temp_{table_name}. "
                    f"DataFrame columns: {data.columns}, "
                    f"DataFrame shape: {data.shape}, "
                    f"DataFrame sample: {data.head(3) if len(data) > 0 else 'empty'}"
                )
                log_error(error_msg)
                raise RuntimeError(error_msg) from e

            # Prepare column lists for deduplication and merge
            sanitized_key_columns = [
                self.sanitize_column_name(col) for col in key_columns
            ]
            insert_columns = [self.sanitize_column_name(col) for col in data.columns]

            # Deduplicate temp table by key columns to prevent constraint violations
            # Use ROW_NUMBER window function (DuckDB compatible)
            dedup_query = f"""
            CREATE OR REPLACE TEMPORARY TABLE temp_{table_name}_dedup AS
            SELECT * EXCLUDE (rn) FROM (
                SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY {", ".join(sanitized_key_columns)} ORDER BY {", ".join(sanitized_key_columns)}) as rn
                FROM temp_{table_name}
            ) WHERE rn = 1
            """
            try:
                conn.execute(dedup_query)
                log_info("Deduplicated temp table using ROW_NUMBER")
            except Exception as dedup_error:
                # Fallback: try without EXCLUDE (older DuckDB versions)
                try:
                    dedup_query = f"""
                    CREATE OR REPLACE TEMPORARY TABLE temp_{table_name}_dedup AS
                    SELECT {", ".join([col for col in insert_columns if col != "rn"])} FROM (
                        SELECT *,
                            ROW_NUMBER() OVER (PARTITION BY {", ".join(sanitized_key_columns)} ORDER BY {", ".join(sanitized_key_columns)}) as rn
                        FROM temp_{table_name}
                    ) WHERE rn = 1
                    """
                    conn.execute(dedup_query)
                    log_info("Deduplicated temp table using ROW_NUMBER (fallback)")
                except Exception as fallback_error:
                    log_warning(
                        f"Could not deduplicate temp table: {dedup_error}, {fallback_error}. "
                        f"Proceeding with original temp table (may cause constraint violations if duplicates exist)"
                    )
                    # Use original temp table if deduplication fails
                    conn.execute(
                        f"CREATE OR REPLACE TEMPORARY TABLE temp_{table_name}_dedup AS SELECT * FROM temp_{table_name}"
                    )

            # Use DELETE + INSERT pattern for upsert (more reliable than ON CONFLICT)
            # This approach works without requiring a unique index
            insert_column_list = ", ".join(insert_columns)
            select_column_list = ", ".join(
                [f"temp_{table_name}_dedup.{col}" for col in insert_columns]
            )

            # Build WHERE clause for matching keys
            key_match_conditions = " AND ".join(
                [
                    f"{table_name}.{col} = temp_{table_name}_dedup.{col}"
                    for col in sanitized_key_columns
                ]
            )

            # Step 1: Delete existing rows that match incoming keys
            delete_query = f"""
            DELETE FROM {table_name}
            WHERE EXISTS (
                SELECT 1 FROM temp_{table_name}_dedup
                WHERE {key_match_conditions}
            )
            """
            log_info(f"Executing delete for matching keys: {sanitized_key_columns}")
            try:
                conn.execute(delete_query)
                log_info("Delete query executed successfully")
            except Exception as del_error:
                log_warning(f"Delete query failed (table may be empty): {del_error}")

            # Step 2: Insert all rows from temp table
            insert_query = f"""
            INSERT INTO {table_name} ({insert_column_list})
            SELECT {select_column_list} FROM temp_{table_name}_dedup
            """
            log_info("Executing insert query")
            log_info(f"Insert columns: {insert_column_list}")
            log_info(f"Key columns: {sanitized_key_columns}")
            try:
                conn.execute(insert_query)
                log_info("Insert query executed successfully")
            except Exception as e:
                error_msg = (
                    f"Failed to execute insert query for table '{table_name}'. "
                    f"Query: {insert_query}. "
                    f"Key columns: {key_columns}, "
                    f"DataFrame columns: {data.columns}"
                )
                log_error(error_msg)
                raise RuntimeError(error_msg) from e

            # Clean up
            log_info("Dropping temporary tables")
            conn.execute(f"DROP TABLE IF EXISTS temp_{table_name}")
            conn.execute(f"DROP TABLE IF EXISTS temp_{table_name}_dedup")
            try:
                conn.unregister(temp_df_name)
                log_info(f"Unregistered DataFrame {temp_df_name}")
            except Exception:
                pass
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

    def read_data(self, table_name: str) -> list[dict]:
        """Read data from a table."""
        conn = None
        try:
            conn = self.get_connection()
            df = pl.read_database(f"SELECT * FROM {table_name}", connection=conn)
            return df.to_dicts()
        finally:
            if conn:
                conn.close()

    # Enhanced methods for querying and data analysis
    def get_unique_categories(self, table_name: str, column: str) -> list[str]:
        """Get unique values from a specified column."""
        _validate_identifier(table_name, "table name")
        _validate_identifier(column, "column name")
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
        filters: dict[str, Any] | None = None,
        sample_size: int = 50,
        sampling_strategy: str = "top_correlations",
    ) -> str:
        """Query sampled data from DuckDB with various sampling strategies."""
        _validate_identifier(table_name, "table name")
        if not isinstance(sample_size, int) or sample_size <= 0:
            raise ValueError(f"Invalid sample_size: {sample_size!r}")
        if sampling_strategy not in {"top_correlations", "random", "mixed"}:
            raise ValueError(f"Unknown sampling strategy: {sampling_strategy}")

        where_conditions: list[str] = []
        query_params: list[Any] = []
        if filters:
            for column, value in filters.items():
                _validate_identifier(column, "filter column")
                if isinstance(value, list):
                    if not value:
                        continue
                    placeholders = ", ".join("?" for _ in value)
                    where_conditions.append(f"{column} IN ({placeholders})")
                    query_params.extend(value)
                else:
                    where_conditions.append(f"{column} = ?")
                    query_params.append(value)

        where_clause = (
            " WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        )

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
            # The mixed query references where_clause twice; duplicate the bound params to match.
            query_params = query_params + query_params

        conn = None
        try:
            conn = self.get_connection()
            df = conn.execute(query, query_params).pl()
            csv_buffer = io.StringIO()
            df.write_csv(csv_buffer)
            return csv_buffer.getvalue()
        finally:
            if conn:
                conn.close()

    def write_results_to_table(
        self,
        json_results: list[dict[str, Any]],
        output_table: str,
        if_exists: str = "append",
        context: dg.AssetExecutionContext | None = None,
    ) -> None:
        """Write JSON results to a MotherDuck table."""
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
                # Insert into existing table with explicit column alignment.
                # This avoids schema mismatch when new columns are added to payloads.
                table_info = conn.execute(
                    f"PRAGMA table_info('{output_table}')"
                ).fetchall()
                table_columns = [row[1] for row in table_info]
                if not table_columns:
                    raise ValueError(
                        f"Unable to determine columns for table {output_table}"
                    )

                missing_columns = [
                    col for col in table_columns if col not in df.columns
                ]
                for col in missing_columns:
                    df = df.with_columns(pl.lit(None).alias(col))

                df = df.select(table_columns)
                insert_columns = ", ".join(
                    self.sanitize_column_name(col) for col in table_columns
                )
                select_columns = ", ".join(
                    f"df.{self.sanitize_column_name(col)}" for col in table_columns
                )
                conn.execute(
                    f"INSERT INTO {output_table} ({insert_columns}) "
                    f"SELECT {select_columns} FROM df"
                )
                if context:
                    context.log.info(f"Appended {len(df)} records to {output_table}")

            conn.commit()
        finally:
            if conn:
                conn.close()

    def execute_query(
        self, query: str, read_only: bool = True, params: list[Any] | None = None
    ) -> pl.DataFrame:
        """Execute a SQL query and return results as Polars DataFrame.

        For DDL statements (CREATE, DROP, ALTER) that don't return data,
        returns an empty DataFrame.
        """
        conn = None
        try:
            conn = self.get_connection()
            if params:
                result = conn.execute(query, params)
            else:
                result = conn.execute(query)

            try:
                return result.pl()
            except Exception:
                return pl.DataFrame()
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

# Use os.getenv() instead of dg.EnvVar() to resolve values at import time
# This is needed because some assets (e.g., SEC filings) query the database
# at definition time to create dynamic assets
motherduck_resource = MotherDuckResource(
    environment=environment,
    md_token=os.getenv("MOTHERDUCK_TOKEN", "") if environment != "dev" else "",
    md_database=os.getenv("MOTHERDUCK_DATABASE", "local")
    if environment != "dev"
    else "dev",
    md_schema=os.getenv("MOTHERDUCK_PROD_SCHEMA", "main")
    if environment != "dev"
    else "public",
    local_path="local.duckdb",
)
