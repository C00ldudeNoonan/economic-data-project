"""
Pytest configuration and fixtures.
"""

import re
from unittest.mock import Mock

import duckdb
import polars as pl
import pytest

_TIMESTAMP_CALL_RE = re.compile(
    r"TIMESTAMP\('(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) UTC'\)"
)


def _bq_to_duckdb(sql: str) -> str:
    """Translate BigQuery-specific SQL constructs to DuckDB equivalents."""
    sql = re.sub(r"\bFLOAT64\b", "DOUBLE", sql)
    sql = _TIMESTAMP_CALL_RE.sub(r"TIMESTAMPTZ '\1 UTC'", sql)
    return sql


class DuckDBWarehouseStub:
    """DuckDB-backed stub implementing the BigQueryWarehouseResource interface for tests."""

    def __init__(self):
        self._conn = duckdb.connect()

    def execute_query(
        self, query: str, read_only: bool = True, params=None
    ) -> pl.DataFrame:
        translated = _bq_to_duckdb(query)
        try:
            result = self._conn.execute(translated)
            try:
                return result.pl()
            except Exception:
                return pl.DataFrame()
        except Exception:
            return pl.DataFrame()

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
    return DuckDBWarehouseStub()


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
