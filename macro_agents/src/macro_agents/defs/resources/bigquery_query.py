from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, cast

from google.cloud import bigquery
from sqlglot import exp, parse
from sqlglot.errors import ParseError

QueryParameterScalar = str | int | float | bool | bytes | date | datetime | Decimal


@dataclass(frozen=True)
class QueryParameter:
    """A scalar BigQuery parameter with an explicit type.

    Use this wrapper when BigQuery cannot infer a type, most notably for a
    nullable parameter value.
    """

    value: QueryParameterScalar | None
    bigquery_type: str

    def __post_init__(self) -> None:
        normalized_type = self.bigquery_type.strip().upper()
        if not normalized_type:
            raise ValueError("bigquery_type must not be empty")
        object.__setattr__(self, "bigquery_type", normalized_type)


@dataclass(frozen=True)
class QueryArrayParameter:
    """An array BigQuery parameter with an explicit element type."""

    values: Sequence[QueryParameterScalar | None]
    bigquery_type: str

    def __post_init__(self) -> None:
        normalized_type = self.bigquery_type.strip().upper()
        if not normalized_type:
            raise ValueError("bigquery_type must not be empty")
        object.__setattr__(self, "values", tuple(self.values))
        object.__setattr__(self, "bigquery_type", normalized_type)


QueryParameterInput = QueryParameterScalar | QueryParameter | QueryArrayParameter
QueryParameters = Mapping[str, QueryParameterInput]


def numeric_query_parameter(value: Decimal | int | float | None) -> QueryParameter:
    """Build an exact BigQuery NUMERIC parameter from a Python number."""
    decimal_value = None if value is None else Decimal(str(value))
    return QueryParameter(decimal_value, "NUMERIC")


def prepare_query_parameters(
    query: str,
    *,
    read_only: bool,
    params: QueryParameters | None,
) -> list[bigquery.ScalarQueryParameter | bigquery.ArrayQueryParameter]:
    """Validate a query contract and build named BigQuery parameters."""
    statement = _parse_single_statement(query)

    if read_only and not isinstance(statement, exp.Query):
        raise ValueError("read_only=True requires a single SELECT-style query")

    required_names = {parameter.name for parameter in statement.find_all(exp.Parameter)}
    supplied_params = params or {}
    supplied_names = set(supplied_params)

    missing_names = required_names - supplied_names
    unexpected_names = supplied_names - required_names
    if missing_names or unexpected_names:
        details = []
        if missing_names:
            details.append(f"missing parameters: {', '.join(sorted(missing_names))}")
        if unexpected_names:
            details.append(
                f"unexpected parameters: {', '.join(sorted(unexpected_names))}"
            )
        raise ValueError("Query parameter mismatch (" + "; ".join(details) + ")")

    return [
        _to_bigquery_parameter(name, value) for name, value in supplied_params.items()
    ]


def _parse_single_statement(query: str) -> exp.Expression:
    if not query.strip():
        raise ValueError("Query must not be empty")

    try:
        statements = [
            statement for statement in parse(query, read="bigquery") if statement
        ]
    except ParseError as error:
        raise ValueError(f"Invalid BigQuery SQL: {error}") from error

    if len(statements) != 1:
        raise ValueError("Exactly one SQL statement is required")
    return statements[0]


def _to_bigquery_parameter(
    name: str, value: QueryParameterInput
) -> bigquery.ScalarQueryParameter | bigquery.ArrayQueryParameter:
    if isinstance(value, QueryArrayParameter):
        return bigquery.ArrayQueryParameter(
            name,
            value.bigquery_type,
            list(value.values),
        )
    if isinstance(value, QueryParameter):
        return bigquery.ScalarQueryParameter(
            name,
            value.bigquery_type,
            _bigquery_scalar_value(value.value),
        )
    return bigquery.ScalarQueryParameter(
        name,
        _infer_bigquery_type(value),
        _bigquery_scalar_value(value),
    )


def _bigquery_scalar_value(value: QueryParameterScalar | None) -> Any:
    """Bridge an upstream annotation that omits supported BYTES values."""
    return cast(Any, value)


def _infer_bigquery_type(value: QueryParameterScalar) -> str:
    if isinstance(value, bool):
        return "BOOL"
    if isinstance(value, int):
        return "INT64"
    if isinstance(value, float):
        return "FLOAT64"
    if isinstance(value, datetime):
        return "TIMESTAMP"
    if isinstance(value, date):
        return "DATE"
    if isinstance(value, Decimal):
        return "NUMERIC"
    if isinstance(value, bytes):
        return "BYTES"
    if isinstance(value, str):
        return "STRING"
    raise TypeError(f"Unsupported BigQuery parameter type: {type(value).__name__}")
