"""
Pytest configuration and fixtures.
"""

import re
from unittest.mock import Mock

import duckdb
import polars as pl
import pytest

from macro_agents.defs.resources.bigquery_query import (
    QueryParameters,
    prepare_query_parameters,
)

_TIMESTAMP_CALL_RE = re.compile(
    r"TIMESTAMP\('(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) UTC'\)"
)
_NAMED_PARAMETER_RE = re.compile(r"@([A-Za-z_][A-Za-z0-9_]*)")


def _bq_to_duckdb(sql: str) -> str:
    """Translate BigQuery-specific SQL constructs to DuckDB equivalents."""
    sql = sql.replace("`", "")
    sql = re.sub(r"\bFLOAT64\b", "DOUBLE", sql)
    sql = re.sub(r"\bINT64\b", "BIGINT", sql)
    sql = re.sub(r"\bSTRING\b", "VARCHAR", sql)
    sql = sql.replace("CURRENT_TIMESTAMP()", "CURRENT_TIMESTAMP")
    sql = _TIMESTAMP_CALL_RE.sub(r"TIMESTAMPTZ '\1 UTC'", sql)
    return sql


def _bind_named_parameters(
    query: str,
    *,
    read_only: bool,
    params: QueryParameters | None,
) -> tuple[str, list[object]]:
    if params is None and "@" not in query:
        return query, []

    query_parameters = prepare_query_parameters(
        query,
        read_only=read_only,
        params=params,
    )
    values_by_name = {parameter.name: parameter.value for parameter in query_parameters}
    ordered_values: list[object] = []

    def replace_parameter(match: re.Match[str]) -> str:
        name = match.group(1)
        if name not in values_by_name:
            return match.group(0)
        ordered_values.append(values_by_name[name])
        return "?"

    return _NAMED_PARAMETER_RE.sub(replace_parameter, query), ordered_values


class DuckDBWarehouseStub:
    """DuckDB-backed stub implementing the BigQueryWarehouseResource interface for tests."""

    def __init__(self):
        self._conn = duckdb.connect()

    def execute_query(
        self,
        query: str,
        read_only: bool = True,
        params: QueryParameters | None = None,
    ) -> pl.DataFrame:
        bound_query, bound_values = _bind_named_parameters(
            query,
            read_only=read_only,
            params=params,
        )
        result = self._conn.execute(_bq_to_duckdb(bound_query), bound_values)
        if result.description is None:
            return pl.DataFrame()
        return result.pl()

    def close(self) -> None:
        self._conn.close()

    def get_connection(self):
        return self._conn

    def table_exists(self, table_name: str) -> bool:
        result = self._conn.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_name = ?",
            [table_name],
        ).fetchone()
        return bool(result and result[0] > 0)

    def drop_create_duck_db_table(self, table_name: str, df: pl.DataFrame) -> str:
        self._conn.register("_tmp_write", df)
        self._conn.execute(
            f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM _tmp_write"
        )
        return table_name

    def write_table(self, table_name: str, df: pl.DataFrame) -> str:
        return self.drop_create_duck_db_table(table_name, df)

    def fetchone(self, sql: str) -> tuple | None:
        result = self._conn.execute(sql).fetchone()
        return result

    def fetchall(self, sql: str) -> list[tuple]:
        return self._conn.execute(sql).fetchall()

    def write_results_to_table(
        self,
        json_results: list[dict],
        output_table: str,
        if_exists: str = "append",
        context=None,
    ) -> None:
        df = pl.DataFrame(json_results)
        self._conn.register("_tmp_results", df)
        if if_exists == "replace" or not self.table_exists(output_table):
            self._conn.execute(
                f"CREATE OR REPLACE TABLE {output_table} AS SELECT * FROM _tmp_results"
            )
        else:
            self._conn.execute(f"INSERT INTO {output_table} SELECT * FROM _tmp_results")


@pytest.fixture
def bq_test_resource():
    """Fresh DuckDB-backed warehouse stub for each test."""
    resource = DuckDBWarehouseStub()
    yield resource
    resource.close()


@pytest.fixture
def sample_economic_data():
    """Sample economic data for testing."""
    return {
        "series_code": ["GDP", "CPI", "UNRATE"],
        "series_name": ["GDP", "CPI", "Unemployment Rate"],
        "current_value": [100.0, 2.0, 3.5],
        "pct_change_3m": [0.01, 0.02, -0.01],
        "pct_change_6m": [0.02, 0.04, -0.02],
        "pct_change_1y": [0.03, 0.06, -0.05],
        "date_grain": ["Monthly", "Monthly", "Monthly"],
    }


@pytest.fixture
def sample_market_data():
    """Sample market data for testing."""
    return {
        "symbol": ["AAPL", "GOOGL", "MSFT"],
        "asset_type": ["stock", "stock", "stock"],
        "time_period": ["12_weeks", "12_weeks", "12_weeks"],
        "total_return_pct": [10.0, 15.0, 8.0],
        "volatility_pct": [20.0, 25.0, 18.0],
        "win_rate_pct": [60.0, 65.0, 55.0],
    }


@pytest.fixture
def mock_dspy_lm():
    """Mock DSPy LM for testing."""
    mock_lm = Mock()
    mock_lm.return_value = Mock()
    return mock_lm


@pytest.fixture
def mock_dspy_settings():
    """Mock DSPy settings for testing."""
    with pytest.MonkeyPatch().context() as m:
        m.setattr("dspy.settings.configure", Mock())
        yield
