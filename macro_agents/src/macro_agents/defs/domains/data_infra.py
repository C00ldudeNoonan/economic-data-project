"""Data infrastructure assets."""

from pathlib import Path
from typing import Any

import dagster as dg
import polars as pl
import yaml

from macro_agents.defs.resources.bigquery_warehouse import BigQueryWarehouseResource


class DataDictionaryBuilder:
    """Parses DBT schema YAML files and builds data dictionary metadata catalog."""

    # Column naming patterns that indicate additive metrics (can be summed)
    ADDITIVE_PATTERNS = [
        "_days",
        "_count",
        "trading_days",
        "positive_days",
        "negative_days",
        "neutral_days",
        "period_diff",
    ]

    # Column naming patterns that indicate non-additive metrics (percentages, rates, etc.)
    NON_ADDITIVE_PATTERNS = [
        "_pct",
        "_rate",
        "volatility",
        "correlation",
        "_price",
        "win_rate",
        "avg_",
        "total_return",
    ]

    # Tables to exclude from data dictionary (backtesting snapshot tables)
    EXCLUDED_TABLES = [
        "us_sector_summary_snapshot",
        "major_indicies_summary_snapshot",
        "fred_series_latest_aggregates_snapshot",
        "energy_commodities_summary_snapshot",
        "input_commodities_summary_snapshot",
        "agriculture_commodities_summary_snapshot",
        "leading_econ_return_indicator_snapshot",
    ]

    def classify_additivity(self, column_name: str, data_type: str) -> str:
        """
        Classify whether a column is ADDITIVE, NON_ADDITIVE, or SEMI_ADDITIVE.

        Args:
            column_name: Name of the column
            data_type: Data type of the column

        Returns:
            'ADDITIVE', 'NON_ADDITIVE', or 'SEMI_ADDITIVE'
        """
        column_lower = column_name.lower()

        # Dates and timestamps are semi-additive (meaningful in time-based aggregations)
        if data_type in ["DATE", "TIMESTAMP", "DATETIME"]:
            return "SEMI_ADDITIVE"

        # Check for additive patterns
        for pattern in self.ADDITIVE_PATTERNS:
            if pattern in column_lower:
                return "ADDITIVE"

        # Check for non-additive patterns
        for pattern in self.NON_ADDITIVE_PATTERNS:
            if pattern in column_lower:
                return "NON_ADDITIVE"

        # Default to NON_ADDITIVE for safety (better to not sum incorrectly)
        return "NON_ADDITIVE"

    def parse_schema_file(self, yaml_path: Path, schema_category: str) -> list[dict]:
        """
        Parse a single DBT schema.yml file and extract table/column metadata.

        Args:
            yaml_path: Path to schema.yml file
            schema_category: Category of schema (staging, government, markets, etc.)

        Returns:
            List of dictionaries containing table/column metadata
        """
        with open(yaml_path, "r", encoding="utf-8") as f:
            schema_data = yaml.safe_load(f)

        if not schema_data or "models" not in schema_data:
            return []

        metadata = []

        for model in schema_data["models"]:
            table_name = model.get("name")
            table_description = model.get("description", "")

            # Skip excluded tables
            if table_name in self.EXCLUDED_TABLES:
                continue

            # Get columns if they exist
            columns = model.get("columns", [])

            if not columns:
                # If no columns defined, create a basic entry for the table
                metadata.append(
                    {
                        "table_name": table_name,
                        "column_name": None,
                        "data_type": None,
                        "description": table_description,
                        "is_nullable": True,
                        "is_primary_key": False,
                        "additivity_type": None,
                        "example_values": None,
                        "data_source": None,
                        "update_frequency": None,
                        "schema_category": schema_category,
                    }
                )
            else:
                # Process each column
                for column in columns:
                    column_name = column.get("name")
                    column_description = column.get("description", "")
                    tests = column.get("tests", [])

                    # Infer data type from column name (basic heuristic)
                    data_type = self._infer_data_type(column_name)

                    # Check if column is nullable or primary key based on tests
                    is_nullable = "not_null" not in tests
                    is_primary_key = "unique" in tests and "not_null" in tests

                    # Classify additivity
                    additivity_type = self.classify_additivity(column_name, data_type)

                    metadata.append(
                        {
                            "table_name": table_name,
                            "column_name": column_name,
                            "data_type": data_type,
                            "description": column_description or table_description,
                            "is_nullable": is_nullable,
                            "is_primary_key": is_primary_key,
                            "additivity_type": additivity_type,
                            "example_values": None,  # Will be populated later
                            "data_source": self._infer_data_source(schema_category),
                            "update_frequency": self._infer_update_frequency(
                                schema_category
                            ),
                            "schema_category": schema_category,
                        }
                    )

        return metadata

    def _infer_data_type(self, column_name: str) -> str:
        """Infer data type from column name (basic heuristic)."""
        column_lower = column_name.lower()

        if "date" in column_lower or "time" in column_lower:
            if "timestamp" in column_lower:
                return "TIMESTAMP"
            return "DATE"
        elif (
            "_pct" in column_lower
            or "rate" in column_lower
            or "volatility" in column_lower
            or "price" in column_lower
            or "value" in column_lower
            or "correlation" in column_lower
        ):
            return "FLOAT"
        elif (
            "_days" in column_lower
            or "_count" in column_lower
            or "number" in column_lower
        ):
            return "INTEGER"
        elif (
            "code" in column_lower or "name" in column_lower or "symbol" in column_lower
        ):
            return "VARCHAR"
        else:
            return "VARCHAR"  # Default to VARCHAR

    def _infer_data_source(self, schema_category: str) -> str | None:
        """Infer data source from schema category."""
        source_map = {
            "staging": "Various APIs",
            "government": "FRED, BLS, Treasury",
            "markets": "MarketStack",
            "commodities": "Commodity APIs",
            "analysis": "Calculated",
        }
        return source_map.get(schema_category)

    def _infer_update_frequency(self, schema_category: str) -> str | None:
        """Infer update frequency from schema category."""
        frequency_map = {
            "staging": "daily",
            "government": "monthly",
            "markets": "daily",
            "commodities": "daily",
            "analysis": "monthly",
        }
        return frequency_map.get(schema_category)

    def sample_column_values(
        self, table_name: str, column_name: str, md_resource: BigQueryWarehouseResource
    ) -> list[Any]:
        """
        Sample 3-5 example values from a column.

        Args:
            table_name: Name of the table
            column_name: Name of the column
            md_resource: BigQuery resource for querying

        Returns:
            List of sample values (max 5)
        """
        try:
            query = f"""
            SELECT DISTINCT {column_name}
            FROM {table_name}
            WHERE {column_name} IS NOT NULL
            LIMIT 5
            """
            df = md_resource.execute_query(query, read_only=True)
            if df.is_empty():
                return []
            # Convert to list
            return df[column_name].to_list()
        except Exception:
            # If query fails, return empty list
            return []

    def build_dictionary(
        self, dbt_project_path: Path, md_resource: BigQueryWarehouseResource | None = None
    ) -> pl.DataFrame:
        """
        Build complete data dictionary by parsing all DBT schema files.

        Args:
            dbt_project_path: Path to dbt_project/models directory
            md_resource: Optional BigQuery resource for sampling values

        Returns:
            Polars DataFrame with data dictionary metadata
        """
        all_metadata = []

        # Define schema categories to process
        schema_categories = [
            "staging",
            "government",
            "markets",
            "commodities",
            "analysis",
        ]

        for category in schema_categories:
            category_path = dbt_project_path / category
            if not category_path.exists():
                continue

            # Find schema.yml file in this category
            schema_file = category_path / "schema.yml"
            if schema_file.exists():
                metadata = self.parse_schema_file(schema_file, category)
                all_metadata.extend(metadata)

        # Convert to Polars DataFrame
        if not all_metadata:
            # Return empty DataFrame with correct schema
            return pl.DataFrame(
                schema={
                    "table_name": pl.Utf8,
                    "column_name": pl.Utf8,
                    "data_type": pl.Utf8,
                    "description": pl.Utf8,
                    "is_nullable": pl.Boolean,
                    "is_primary_key": pl.Boolean,
                    "additivity_type": pl.Utf8,
                    "example_values": pl.Utf8,
                    "data_source": pl.Utf8,
                    "update_frequency": pl.Utf8,
                    "schema_category": pl.Utf8,
                }
            )

        df = pl.DataFrame(all_metadata)

        # Optionally sample values if BigQuery resource provided
        if md_resource is not None:
            # TODO: Implement value sampling (can be added later for performance)
            pass

        return df


@dg.asset(
    group_name="data_infrastructure",
    description="Build data dictionary from DBT schema files",
)
def build_data_dictionary(
    context: dg.AssetExecutionContext, bq: BigQueryWarehouseResource
) -> dg.MaterializeResult:
    """
    Dagster asset that populates the data_dictionary table from DBT schema files.

    This asset:
    1. Parses all DBT schema.yml files from dbt_project/models/
    2. Extracts table and column metadata
    3. Classifies metric additivity (ADDITIVE vs NON_ADDITIVE)
    4. Writes metadata to data_dictionary table in BigQuery

    Args:
        context: Dagster execution context
        md: BigQuery resource for database operations

    Returns:
        MaterializeResult with row count metadata
    """
    context.log.info("Starting data dictionary build...")

    # Initialize builder
    builder = DataDictionaryBuilder()

    # Determine DBT project path (5 levels up from this file)
    dbt_path = (
        Path(__file__).parent.parent.parent.parent.parent / "dbt_project" / "models"
    )

    context.log.info(f"Parsing DBT schemas from: {dbt_path}")

    # Build dictionary
    df = builder.build_dictionary(dbt_path, bq)

    if df.is_empty():
        context.log.warning("No metadata found in DBT schema files")
        return dg.MaterializeResult(metadata={"row_count": 0})

    context.log.info(f"Parsed {len(df)} column definitions from DBT schemas")

    # Create data_dictionary table if it doesn't exist
    create_table_query = """
    CREATE TABLE IF NOT EXISTS data_dictionary (
        table_name VARCHAR NOT NULL,
        column_name VARCHAR,
        data_type VARCHAR,
        description TEXT,
        is_nullable BOOLEAN DEFAULT TRUE,
        is_primary_key BOOLEAN DEFAULT FALSE,
        additivity_type VARCHAR CHECK (additivity_type IN ('ADDITIVE', 'NON_ADDITIVE', 'SEMI_ADDITIVE')),
        example_values JSON,
        data_source VARCHAR,
        update_frequency VARCHAR,
        schema_category VARCHAR,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (table_name, COALESCE(column_name, 'TABLE_METADATA'))
    )
    """

    context.log.info("Creating data_dictionary table if not exists...")
    bq.execute_query(create_table_query)

    # Clear existing data (full refresh)
    context.log.info("Clearing existing data dictionary...")
    bq.execute_query("DELETE FROM data_dictionary")

    # Write new data
    context.log.info(f"Writing {len(df)} rows to data_dictionary table...")
    bq.write_results_to_table(
        df.to_dicts(), "data_dictionary", if_exists="append", context=context
    )

    context.log.info("Data dictionary build complete!")

    return dg.MaterializeResult(
        metadata={
            "row_count": len(df),
            "tables_count": df["table_name"].n_unique(),
            "schema_categories": df["schema_category"].unique().to_list(),
        }
    )


defs = dg.Definitions(assets=[build_data_dictionary])
