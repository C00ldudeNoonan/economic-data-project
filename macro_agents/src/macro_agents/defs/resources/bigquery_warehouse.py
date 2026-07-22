import io
import json
import logging
import os
import re
import tempfile
import uuid
from typing import Any

import dagster as dg
import polars as pl
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from pydantic import Field, PrivateAttr

from macro_agents.defs.resources.bigquery_query import (
    QueryParameters,
    prepare_query_parameters,
)

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*){0,2}$")
_BIGQUERY_TYPE_ALIASES = {
    "BOOL": "BOOL",
    "BOOLEAN": "BOOL",
    "DOUBLE": "FLOAT64",
    "FLOAT": "FLOAT64",
    "FLOAT64": "FLOAT64",
    "INT64": "INT64",
    "INTEGER": "INT64",
}


def _validate_identifier(name: str, kind: str = "identifier") -> str:
    """Reject strings that aren't plain SQL identifiers to prevent injection."""
    if not isinstance(name, str) or not _IDENTIFIER_RE.match(name):
        raise ValueError(f"Invalid {kind}: {name!r}")
    return name


def _canonical_bigquery_type(type_name: str) -> str:
    normalized = type_name.upper()
    return _BIGQUERY_TYPE_ALIASES.get(normalized, normalized)


class BigQueryWarehouseResource(dg.ConfigurableResource):
    """Dagster resource for BigQuery.

    Exposes the same public API as the retired MotherDuckResource so assets
    written against it work unchanged (see the get_connection and
    drop_create_duck_db_table aliases).
    """

    project: str = Field(description="GCP project ID")
    dataset: str = Field(
        description="Default BigQuery dataset", default="economics_raw"
    )
    location: str = Field(description="BigQuery location", default="US")

    _client: bigquery.Client | None = PrivateAttr(default=None)

    def _prepare_google_application_credentials(self) -> None:
        """Support either a credentials file path or inline service-account JSON."""
        credentials = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
        if not credentials.startswith("{"):
            return

        try:
            json.loads(credentials)
        except json.JSONDecodeError as error:
            raise ValueError(
                "GOOGLE_APPLICATION_CREDENTIALS contains invalid JSON"
            ) from error

        credentials_path = os.path.join(
            tempfile.gettempdir(), "gcp_service_account_credentials.json"
        )
        with open(credentials_path, "w", encoding="utf-8") as credentials_file:
            credentials_file.write(credentials)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

    def get_client(self) -> bigquery.Client:
        """Return a cached BigQuery client. On GCE the VM service account is used via ADC.

        Client construction resolves ADC credentials, so the client is
        created once per resource instance and reused across calls.
        """
        if self._client is None:
            self._prepare_google_application_credentials()
            self._client = bigquery.Client(project=self.project, location=self.location)
        return self._client

    def get_connection(self) -> bigquery.Client:
        """Return a NEW client owned by the caller — drop-in replacement for MotherDuckResource.get_connection().

        Many callers close() the returned object like a DuckDB connection,
        so this must NOT hand out the shared cached client from
        get_client(): closing that would break every subsequent query on
        this resource within the same process.
        """
        self._prepare_google_application_credentials()
        return bigquery.Client(project=self.project, location=self.location)

    def _table_ref(self, table_name: str) -> str:
        parts = table_name.split(".")
        if len(parts) == 3:
            return table_name
        if len(parts) == 2:
            return f"{self.project}.{table_name}"
        return f"{self.project}.{self.dataset}.{table_name}"

    def write_table(self, table_name: str, df: pl.DataFrame) -> str:
        """Drop-and-replace a BigQuery table with the contents of df."""
        client = self.get_client()
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        client.load_table_from_dataframe(
            df.to_pandas(), self._table_ref(table_name), job_config=job_config
        ).result()
        return table_name

    def drop_create_duck_db_table(self, table_name: str, df: pl.DataFrame) -> str:
        """Backwards-compatible alias for write_table."""
        return self.write_table(table_name, df)

    def upsert_data(
        self,
        table_name: str,
        data: pl.DataFrame,
        key_columns: list[str],
        context: dg.AssetExecutionContext | None = None,
    ) -> None:
        """Upsert data via a staging table + MERGE statement."""
        log = context.log if context else logging.getLogger(__name__)

        if len(data.columns) == 0:
            raise ValueError(f"DataFrame for '{table_name}' has no columns")
        missing = [c for c in key_columns if c not in data.columns]
        if missing:
            raise ValueError(
                f"Key columns {missing} not found in DataFrame for '{table_name}'"
            )
        if len(data) == 0:
            log.warning(f"Empty DataFrame for '{table_name}' — skipping upsert")
            return

        client = self.get_client()
        table_ref = self._table_ref(table_name)
        staging_ref = f"{table_ref}_staging_{uuid.uuid4().hex[:12]}"

        client.load_table_from_dataframe(
            data.to_pandas(),
            staging_ref,
            job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
        ).result()
        log.info(f"Loaded {len(data)} rows to staging table {staging_ref}")

        if not self.table_exists(table_name):
            # First run: promote staging to target directly
            client.copy_table(
                staging_ref,
                table_ref,
                job_config=bigquery.CopyJobConfig(write_disposition="WRITE_TRUNCATE"),
            ).result()
        else:
            non_key_cols = [c for c in data.columns if c not in key_columns]
            key_match = " AND ".join(f"T.`{c}` = S.`{c}`" for c in key_columns)
            update_set = ", ".join(f"T.`{c}` = S.`{c}`" for c in non_key_cols)
            insert_cols = ", ".join(f"`{c}`" for c in data.columns)
            insert_vals = ", ".join(f"S.`{c}`" for c in data.columns)
            merge_sql = f"""
                MERGE `{table_ref}` AS T
                USING `{staging_ref}` AS S
                ON {key_match}
                WHEN MATCHED THEN UPDATE SET {update_set}
                WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
            """
            client.query(merge_sql).result()

        client.delete_table(staging_ref, not_found_ok=True)
        log.info(f"Upsert complete for '{table_name}'")

    def normalize_column_types(
        self,
        table_name: str,
        column_types: dict[str, str],
        context: dg.AssetExecutionContext | None = None,
    ) -> list[str]:
        """Rewrite an existing table when selected columns have drifted types.

        BigQuery MERGE requires matching assignment types. Older raw tables can
        retain schemas inferred from sparse early loads, so this method repairs
        only explicitly requested columns and leaves all other columns unchanged.
        """
        log = context.log if context else logging.getLogger(__name__)
        client = self.get_client()
        table_ref = self._table_ref(table_name)

        try:
            table = client.get_table(table_ref)
        except NotFound:
            return []

        desired_types = {
            column: _canonical_bigquery_type(column_type)
            for column, column_type in column_types.items()
        }
        mismatched_columns = [
            field.name
            for field in table.schema
            if field.name in desired_types
            and _canonical_bigquery_type(field.field_type) != desired_types[field.name]
        ]
        if not mismatched_columns:
            return []

        select_exprs = []
        for field in table.schema:
            if field.name in desired_types:
                select_exprs.append(
                    f"SAFE_CAST(`{field.name}` AS {desired_types[field.name]}) "
                    f"AS `{field.name}`"
                )
            else:
                select_exprs.append(f"`{field.name}`")

        staging_ref = f"{table_ref}_schema_fix_{uuid.uuid4().hex[:12]}"
        create_sql = f"""
            CREATE OR REPLACE TABLE `{staging_ref}` AS
            SELECT {", ".join(select_exprs)}
            FROM `{table_ref}`
        """
        client.query(create_sql).result()
        client.copy_table(
            staging_ref,
            table_ref,
            job_config=bigquery.CopyJobConfig(write_disposition="WRITE_TRUNCATE"),
        ).result()
        client.delete_table(staging_ref, not_found_ok=True)
        log.info(
            "Normalized column types for '%s': %s",
            table_name,
            ", ".join(mismatched_columns),
        )
        return mismatched_columns

    def fetchone(self, sql: str) -> tuple | None:
        """Run a SELECT and return the first row as a tuple (None if empty).

        Drop-in replacement for DuckDB's conn.execute(sql).fetchone().
        """
        df = self.execute_query(sql)
        return df.row(0) if len(df) > 0 else None

    def fetchall(self, sql: str) -> list[tuple]:
        """Run a SELECT and return all rows as a list of tuples.

        Drop-in replacement for DuckDB's conn.execute(sql).fetchall().
        """
        return self.execute_query(sql).rows()

    def read_data(self, table_name: str) -> list[dict]:
        """Read all rows from a table as a list of dicts."""
        client = self.get_client()
        ref = self._table_ref(table_name)
        return client.query(f"SELECT * FROM `{ref}`").to_dataframe().to_dict("records")

    def execute_query(
        self,
        query: str,
        read_only: bool = True,
        params: QueryParameters | None = None,
    ) -> pl.DataFrame:
        """Execute a SQL query and return results as a Polars DataFrame.

        Bare table names are resolved against self.dataset so callers don't
        need to fully qualify every reference.

        Parameters use BigQuery named placeholders such as ``@asset_key``.
        Query failures (bad SQL, missing tables, permissions) raise so they
        surface as asset failures instead of masquerading as empty result
        sets. When ``read_only`` is true, only one SELECT-style query is
        accepted. DML/DDL statements must opt in with ``read_only=False`` and
        return an empty DataFrame because they have no result schema.
        """
        query_parameters = prepare_query_parameters(
            query,
            read_only=read_only,
            params=params,
        )
        client = self.get_client()
        job_config = bigquery.QueryJobConfig(
            default_dataset=bigquery.DatasetReference(self.project, self.dataset)
        )
        job_config.query_parameters = query_parameters
        job = client.query(query, job_config=job_config)
        result = job.result()
        if not result.schema:
            # DML/DDL statements have no result rows to convert
            return pl.DataFrame()
        return pl.DataFrame(result.to_arrow())

    def table_exists(self, table_name: str) -> bool:
        """Check whether a table exists in BigQuery."""
        client = self.get_client()
        try:
            client.get_table(self._table_ref(table_name))
            return True
        except NotFound:
            return False

    def write_results_to_table(
        self,
        json_results: list[dict[str, Any]],
        output_table: str,
        if_exists: str = "append",
        context: dg.AssetExecutionContext | None = None,
    ) -> None:
        """Write JSON results to a BigQuery table."""
        df = pl.DataFrame(json_results)
        table_ref = self._table_ref(output_table)
        client = self.get_client()
        log = context.log if context else logging.getLogger(__name__)

        if if_exists == "fail" and self.table_exists(output_table):
            raise ValueError(f"Table {output_table} already exists")

        if if_exists == "replace" or not self.table_exists(output_table):
            write_disposition = "WRITE_TRUNCATE"
        else:
            # Align columns with existing table schema via INFORMATION_SCHEMA
            dataset_id = self._table_ref(output_table).split(".")[1]
            table_id = output_table.split(".")[-1]
            rows = client.query(
                f"SELECT column_name FROM `{self.project}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS` "
                f"WHERE table_name = '{table_id}'"
            ).result()
            table_columns = [row.column_name for row in rows]
            if table_columns:
                for col in table_columns:
                    if col not in df.columns:
                        df = df.with_columns(pl.lit(None).alias(col))
                df = df.select(table_columns)
            write_disposition = "WRITE_APPEND"

        client.load_table_from_dataframe(
            df.to_pandas(),
            table_ref,
            job_config=bigquery.LoadJobConfig(write_disposition=write_disposition),
        ).result()
        log.info(f"Wrote {len(df)} records to {output_table}")

    def query_sampled_data(
        self,
        table_name: str,
        filters: dict[str, Any] | None = None,
        sample_size: int = 50,
        sampling_strategy: str = "top_correlations",
    ) -> str:
        """Query sampled data from BigQuery and return as CSV string."""
        _validate_identifier(table_name, "table name")
        if not isinstance(sample_size, int) or sample_size <= 0:
            raise ValueError(f"Invalid sample_size: {sample_size!r}")
        if sampling_strategy not in {"top_correlations", "random", "mixed"}:
            raise ValueError(f"Unknown sampling strategy: {sampling_strategy}")

        ref = self._table_ref(table_name)
        where_parts: list[str] = []
        if filters:
            for column, value in filters.items():
                _validate_identifier(column, "filter column")
                if isinstance(value, list) and value:
                    placeholders = ", ".join(repr(v) for v in value)
                    where_parts.append(f"`{column}` IN ({placeholders})")
                elif not isinstance(value, list):
                    where_parts.append(f"`{column}` = {repr(value)}")

        where_clause = (" WHERE " + " AND ".join(where_parts)) if where_parts else ""
        order_by_correlation = """ORDER BY GREATEST(
                    ABS(COALESCE(correlation_econ_vs_q1_returns, 0)),
                    ABS(COALESCE(correlation_econ_vs_q2_returns, 0)),
                    ABS(COALESCE(correlation_econ_vs_q3_returns, 0))
                ) DESC"""

        if sampling_strategy == "top_correlations":
            query = f"SELECT * FROM `{ref}` {where_clause} {order_by_correlation} LIMIT {sample_size}"
        elif sampling_strategy == "random":
            query = f"SELECT * FROM `{ref}` {where_clause} ORDER BY RAND() LIMIT {sample_size}"
        else:
            half = sample_size // 2
            query = (
                f"(SELECT * FROM `{ref}` {where_clause} {order_by_correlation} LIMIT {half})"
                f" UNION ALL "
                f"(SELECT * FROM `{ref}` {where_clause} ORDER BY RAND() LIMIT {sample_size - half})"
            )

        df = self.execute_query(query)
        buf = io.StringIO()
        df.write_csv(buf)
        return buf.getvalue()

    def get_unique_categories(self, table_name: str, column: str) -> list[str]:
        """Get distinct non-null values from a column."""
        _validate_identifier(table_name, "table name")
        _validate_identifier(column, "column name")
        ref = self._table_ref(table_name)
        df = self.execute_query(
            f"SELECT DISTINCT `{column}` FROM `{ref}` "
            f"WHERE `{column}` IS NOT NULL ORDER BY `{column}`"
        )
        return df[column].to_list() if column in df.columns else []


def default_dataset_for_schema(schema: str) -> str:
    """Environment-suffixed dataset name for a given dbt schema base name.

    Mirrors dbt's generate_schema_name macro (dbt_project/macros/
    generate_schema_name.sql): prod uses the schema name as-is, staging/dev
    append a suffix. Use this to reference dbt-managed models living in a
    schema other than a resource's own default dataset — e.g. staging-schema
    `stg_*` views queried by a job whose BigQueryWarehouseResource defaults
    to the raw dataset. Computed at call time (not module load) so env
    overrides in tests take effect.

    Reads DBT_TARGET first — that's what dbt's own profiles.yml
    (`target: "{{ env_var('DBT_TARGET', 'dev') }}"`) and generate_schema_name
    actually key off — falling back to ENVIRONMENT only when DBT_TARGET is
    unset. The two can diverge (e.g. ENVIRONMENT=prod with DBT_TARGET=staging
    for an ad hoc dbt run against the production deployment), and the
    dataset a dbt model is actually materialized into follows DBT_TARGET.
    """
    target = os.getenv("DBT_TARGET") or os.getenv("ENVIRONMENT", "dev")
    suffix = {"prod": "", "staging": "_staging"}.get(target, "_dev")
    return f"{schema}{suffix}"


def default_raw_dataset() -> str:
    """Return the environment-scoped raw dataset used by Dagster resources.

    This follows the deployment environment rather than ``DBT_TARGET`` because
    raw ingestion assets are not dbt-managed. The explicit
    BIGQUERY_DATASET overrides the derived name. Computed at call time so
    env overrides (tests, sandbox runs) take effect without reimporting.
    """
    environment = os.getenv("ENVIRONMENT", "dev")
    suffix = {"prod": "", "staging": "_staging"}.get(environment, "_dev")
    return os.getenv("BIGQUERY_DATASET", f"economics_raw{suffix}")


_default_dataset = default_raw_dataset()

bigquery_warehouse_resource = BigQueryWarehouseResource(
    project=os.getenv("BIGQUERY_PROJECT", "econ-data-project-478800"),
    dataset=_default_dataset,
    location=os.getenv("BIGQUERY_LOCATION", "US"),
)
