"""Tests for BigQueryWarehouseResource client caching and error surfacing (issue #115)."""

from unittest.mock import Mock, patch

import polars as pl
import pyarrow as pa
import pytest

from macro_agents.defs.resources.bigquery_warehouse import BigQueryWarehouseResource


def _make_resource() -> BigQueryWarehouseResource:
    return BigQueryWarehouseResource(project="test-project", dataset="test_dataset")


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
    def test_get_connection_reuses_cached_client(self, mock_client_cls):
        resource = _make_resource()

        client = resource.get_client()
        connection = resource.get_connection()

        assert connection is client
        mock_client_cls.assert_called_once()


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
    def test_dml_statement_returns_empty_dataframe(self, mock_client_cls):
        mock_result = Mock()
        mock_result.schema = []
        mock_job = Mock()
        mock_job.result.return_value = mock_result
        mock_client_cls.return_value.query.return_value = mock_job
        resource = _make_resource()

        df = resource.execute_query("UPDATE some_table SET x = 1 WHERE TRUE")

        assert isinstance(df, pl.DataFrame)
        assert df.is_empty()
        mock_result.to_arrow.assert_not_called()
