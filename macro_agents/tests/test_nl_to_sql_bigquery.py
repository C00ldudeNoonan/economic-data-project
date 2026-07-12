"""Tests that NL-to-SQL emits and validates BigQuery SQL, not DuckDB (issue #130)."""

from macro_agents.defs.analysis.ai.nl_to_sql_module import (
    NaturalLanguageToSQLSignature,
    SQLValidator,
)


def test_validator_parses_bigquery_only_syntax() -> None:
    """A backtick-quoted qualified table parses under BigQuery but not DuckDB.

    This fails if the validator's sqlglot dialect regresses to ``duckdb``,
    which rejects backtick identifiers.
    """
    is_valid, error = SQLValidator.validate_query(
        "SELECT symbol, close FROM `proj.economics_raw.prices` WHERE close > 0"
    )
    assert is_valid, error


def test_validator_accepts_typical_bigquery_select() -> None:
    is_valid, error = SQLValidator.validate_query(
        "SELECT series_code, AVG(current_value) AS avg_value "
        "FROM fred_series WHERE month >= DATE '2023-01-01' "
        "GROUP BY series_code LIMIT 100"
    )
    assert is_valid, error


def test_validator_rejects_mutations_and_multiple_statements() -> None:
    assert SQLValidator.validate_query("DELETE FROM prices")[0] is False
    assert SQLValidator.validate_query("DROP TABLE prices")[0] is False
    assert SQLValidator.validate_query("SELECT 1; SELECT 2")[0] is False
    assert SQLValidator.validate_query("")[0] is False


def test_prompt_targets_bigquery_dialect() -> None:
    """The LLM-facing output description must ask for BigQuery, not DuckDB."""
    desc = NaturalLanguageToSQLSignature.model_fields["sql_query"].json_schema_extra[
        "desc"
    ]
    assert "BigQuery" in desc
    assert "DuckDB-compatible" not in desc
