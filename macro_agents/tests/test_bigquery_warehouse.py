"""Tests for BigQueryWarehouseResource client caching and error surfacing (issue #115)."""

from datetime import date, datetime, timezone
from decimal import Decimal
from unittest.mock import Mock, patch

import polars as pl
import pyarrow as pa
import pytest
from google.cloud import bigquery

from macro_agents.defs.resources.bigquery_query import (
    QueryArrayParameter,
    QueryParameter,
    numeric_query_parameter,
)
from macro_agents.defs.resources.bigquery_warehouse import (
    BigQueryWarehouseResource,
    default_dataset_for_schema,
)


def _make_resource() -> BigQueryWarehouseResource:
    return BigQueryWarehouseResource(project="test-project", dataset="test_dataset")


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (0.1, Decimal("0.1")),
        (1, Decimal("1")),
        (None, None),
    ],
)
def test_numeric_query_parameter_uses_exact_decimal(value, expected) -> None:
    parameter = numeric_query_parameter(value)

    assert parameter.bigquery_type == "NUMERIC"
    assert parameter.value == expected


class TestGetClientCaching:
    @patch("macro_agents.defs.resources.bigquery_warehouse.bigquery.Client")
    def test_client_constructed_once_across_calls(self, mock_client_cls):
        resource = _make_resource()

        first = resource.get_client()
        second = resource.get_client()

        assert first is second
        mock_client_cls.assert_called_once_with(project="test-project", location="US")

    @patch("macro_agents.defs.resources.bigquery_warehouse.bigquery.Client")
    def test_location_config_is_passed_through(self, mock_client_cls):
        resource = BigQueryWarehouseResource(
            project="test-project", dataset="test_dataset", location="EU"
        )

        resource.get_client()

        mock_client_cls.assert_called_once_with(project="test-project", location="EU")

    @patch("macro_agents.defs.resources.bigquery_warehouse.bigquery.Client")
    def test_get_connection_returns_caller_owned_client(self, mock_client_cls):
        """get_connection() callers close() the result like a DuckDB
        connection, so it must never hand out the shared cached client."""
        mock_client_cls.side_effect = lambda **kwargs: Mock(name="client")
        resource = _make_resource()

        client = resource.get_client()
        connection = resource.get_connection()

        assert connection is not client

    @patch("macro_agents.defs.resources.bigquery_warehouse.bigquery.Client")
    def test_closing_connection_does_not_break_cached_client(self, mock_client_cls):
        mock_client_cls.side_effect = lambda **kwargs: Mock(name="client")
        resource = _make_resource()

        cached = resource.get_client()
        connection = resource.get_connection()
        connection.close()

        assert resource.get_client() is cached
        cached.close.assert_not_called()


class TestDefaultDatasetForSchema:
    """dbt's own schema resolution keys off DBT_TARGET (profiles.yml,
    generate_schema_name.sql), not ENVIRONMENT — these must stay in sync
    or cross-dataset references 404 or silently read the wrong dataset."""

    @pytest.mark.parametrize(
        ("dbt_target", "expected"),
        [
            ("prod", "economics_staging"),
            ("staging", "economics_staging_staging"),
            ("dev", "economics_staging_dev"),
            ("unrecognized", "economics_staging_dev"),
        ],
    )
    def test_follows_dbt_target(self, monkeypatch, dbt_target, expected):
        monkeypatch.setenv("DBT_TARGET", dbt_target)
        monkeypatch.setenv("ENVIRONMENT", "prod")  # must not win over DBT_TARGET
        assert default_dataset_for_schema("economics_staging") == expected

    def test_falls_back_to_environment_when_dbt_target_unset(self, monkeypatch):
        monkeypatch.delenv("DBT_TARGET", raising=False)
        monkeypatch.setenv("ENVIRONMENT", "staging")
        assert (
            default_dataset_for_schema("economics_staging")
            == "economics_staging_staging"
        )

    def test_defaults_to_dev_when_neither_is_set(self, monkeypatch):
        monkeypatch.delenv("DBT_TARGET", raising=False)
        monkeypatch.delenv("ENVIRONMENT", raising=False)
        assert (
            default_dataset_for_schema("economics_staging") == "economics_staging_dev"
        )


class TestExecuteQueryErrorSurfacing:
    @patch("macro_agents.defs.resources.bigquery_warehouse.bigquery.Client")
    def test_query_failure_raises_instead_of_returning_empty(self, mock_client_cls):
        mock_job = Mock()
        mock_job.result.side_effect = RuntimeError("Table not found: missing_table")
        mock_client_cls.return_value.query.return_value = mock_job
        resource = _make_resource()

        with pytest.raises(RuntimeError, match="missing_table"):
            resource.execute_query("SELECT * FROM missing_table")

    @patch("macro_agents.defs.resources.bigquery_warehouse.bigquery.Client")
    def test_select_returns_polars_dataframe(self, mock_client_cls):
        arrow_table = pa.table({"value": [1, 2, 3]})
        mock_result = Mock()
        mock_result.schema = ["value"]
        mock_result.to_arrow.return_value = arrow_table
        mock_job = Mock()
        mock_job.result.return_value = mock_result
        mock_client_cls.return_value.query.return_value = mock_job
        resource = _make_resource()

        df = resource.execute_query("SELECT value FROM some_table")

        assert isinstance(df, pl.DataFrame)
        assert df["value"].to_list() == [1, 2, 3]

    @patch("macro_agents.defs.resources.bigquery_warehouse.bigquery.Client")
    def test_read_only_query_rejects_dml(self, mock_client_cls):
        resource = _make_resource()

        with pytest.raises(ValueError, match="read_only=True"):
            resource.execute_query("UPDATE some_table SET x = 1 WHERE TRUE")

        mock_client_cls.assert_not_called()

    @patch("macro_agents.defs.resources.bigquery_warehouse.bigquery.Client")
    def test_explicit_dml_statement_returns_empty_dataframe(self, mock_client_cls):
        mock_result = Mock()
        mock_result.schema = []
        mock_job = Mock()
        mock_job.result.return_value = mock_result
        mock_client_cls.return_value.query.return_value = mock_job
        resource = _make_resource()

        df = resource.execute_query(
            "UPDATE some_table SET x = 1 WHERE TRUE",
            read_only=False,
        )

        assert isinstance(df, pl.DataFrame)
        assert df.is_empty()
        mock_result.to_arrow.assert_not_called()

    @patch("macro_agents.defs.resources.bigquery_warehouse.bigquery.Client")
    def test_read_only_query_rejects_multiple_statements(self, mock_client_cls):
        resource = _make_resource()

        with pytest.raises(ValueError, match="Exactly one SQL statement"):
            resource.execute_query("SELECT 1; SELECT 2")

        mock_client_cls.assert_not_called()

    @patch("macro_agents.defs.resources.bigquery_warehouse.bigquery.Client")
    def test_named_parameters_are_bound_with_inferred_types(self, mock_client_cls):
        mock_result = Mock()
        mock_result.schema = []
        mock_job = Mock()
        mock_job.result.return_value = mock_result
        mock_client_cls.return_value.query.return_value = mock_job
        resource = _make_resource()
        as_of = datetime(2026, 7, 10, 12, 30, tzinfo=timezone.utc)
        report_date = date(2026, 7, 10)

        resource.execute_query(
            """
            SELECT
                @series_name,
                @observation_count,
                @is_current,
                @score,
                @as_of,
                @report_date,
                @amount,
                @payload
            """,
            params={
                "series_name": "GDP",
                "observation_count": 42,
                "is_current": True,
                "score": 1.5,
                "as_of": as_of,
                "report_date": report_date,
                "amount": Decimal("12.34"),
                "payload": b"data",
            },
        )

        job_config = mock_client_cls.return_value.query.call_args.kwargs["job_config"]
        actual_parameters = {
            parameter.name: (parameter.type_, parameter.value)
            for parameter in job_config.query_parameters
        }
        assert actual_parameters == {
            "series_name": ("STRING", "GDP"),
            "observation_count": ("INT64", 42),
            "is_current": ("BOOL", True),
            "score": ("FLOAT64", 1.5),
            "as_of": ("TIMESTAMP", as_of),
            "report_date": ("DATE", report_date),
            "amount": ("NUMERIC", Decimal("12.34")),
            "payload": ("BYTES", b"data"),
        }

    @patch("macro_agents.defs.resources.bigquery_warehouse.bigquery.Client")
    def test_explicit_parameter_type_supports_null(self, mock_client_cls):
        mock_result = Mock()
        mock_result.schema = []
        mock_job = Mock()
        mock_job.result.return_value = mock_result
        mock_client_cls.return_value.query.return_value = mock_job
        resource = _make_resource()

        resource.execute_query(
            "SELECT @optional_value",
            params={
                "optional_value": QueryParameter(
                    value=None,
                    bigquery_type="string",
                )
            },
        )

        job_config = mock_client_cls.return_value.query.call_args.kwargs["job_config"]
        parameter = job_config.query_parameters[0]
        assert (parameter.name, parameter.type_, parameter.value) == (
            "optional_value",
            "STRING",
            None,
        )

    @patch("macro_agents.defs.resources.bigquery_warehouse.bigquery.Client")
    def test_explicit_array_parameter_supports_embeddings(self, mock_client_cls):
        mock_result = Mock()
        mock_result.schema = []
        mock_job = Mock()
        mock_job.result.return_value = mock_result
        mock_client_cls.return_value.query.return_value = mock_job
        resource = _make_resource()

        resource.execute_query(
            "SELECT @embedding",
            params={
                "embedding": QueryArrayParameter(
                    values=[0.25, 0.75],
                    bigquery_type="float64",
                )
            },
        )

        job_config = mock_client_cls.return_value.query.call_args.kwargs["job_config"]
        parameter = job_config.query_parameters[0]
        assert (parameter.name, parameter.array_type, parameter.values) == (
            "embedding",
            "FLOAT64",
            [0.25, 0.75],
        )

    @pytest.mark.parametrize(
        ("params", "message"),
        [
            ({}, "missing parameters: series_name"),
            (
                {"series_name": "GDP", "unused": "value"},
                "unexpected parameters: unused",
            ),
        ],
    )
    @patch("macro_agents.defs.resources.bigquery_warehouse.bigquery.Client")
    def test_parameter_mismatches_fail_before_client_creation(
        self,
        mock_client_cls,
        params,
        message,
    ):
        resource = _make_resource()

        with pytest.raises(ValueError, match=message):
            resource.execute_query("SELECT @series_name", params=params)

        mock_client_cls.assert_not_called()


class TestNormalizeColumnTypes:
    @patch("macro_agents.defs.resources.bigquery_warehouse.bigquery.Client")
    def test_skips_table_when_schema_already_matches(self, mock_client_cls):
        mock_client = mock_client_cls.return_value
        mock_client.get_table.return_value = Mock(
            schema=[
                bigquery.SchemaField("date", "STRING"),
                bigquery.SchemaField("bc_1month", "FLOAT"),
            ]
        )
        resource = _make_resource()

        normalized = resource.normalize_column_types(
            "treasury_yields_raw", {"bc_1month": "FLOAT64"}
        )

        assert normalized == []
        mock_client.query.assert_not_called()
        mock_client.copy_table.assert_not_called()
        mock_client.delete_table.assert_not_called()

    @patch("macro_agents.defs.resources.bigquery_warehouse.bigquery.Client")
    def test_rewrites_table_when_schema_has_drifted_types(self, mock_client_cls):
        mock_client = mock_client_cls.return_value
        mock_client.get_table.return_value = Mock(
            schema=[
                bigquery.SchemaField("date", "STRING"),
                bigquery.SchemaField("bc_1month", "STRING"),
                bigquery.SchemaField("bc_2year", "FLOAT64"),
            ]
        )
        mock_client.query.return_value = Mock()
        resource = _make_resource()

        normalized = resource.normalize_column_types(
            "treasury_yields_raw",
            {"bc_1month": "FLOAT64", "bc_2year": "FLOAT64"},
        )

        assert normalized == ["bc_1month"]
        create_sql = mock_client.query.call_args.args[0]
        assert "CREATE OR REPLACE TABLE" in create_sql
        assert "SAFE_CAST(`bc_1month` AS FLOAT64) AS `bc_1month`" in create_sql
        assert "SAFE_CAST(`bc_2year` AS FLOAT64) AS `bc_2year`" in create_sql
        assert "FROM `test-project.test_dataset.treasury_yields_raw`" in create_sql
        mock_client.query.return_value.result.assert_called_once()
        mock_client.copy_table.assert_called_once()
        mock_client.copy_table.return_value.result.assert_called_once()
        mock_client.delete_table.assert_called_once()
