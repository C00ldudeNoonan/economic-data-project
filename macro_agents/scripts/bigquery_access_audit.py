from __future__ import annotations

import argparse
import atexit
import csv
import json
import os
import re
import sys
import tempfile
from collections.abc import Sequence
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from google.cloud import bigquery

_PROJECT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{4,61}[A-Za-z0-9]$")
_REGION_RE = re.compile(r"^[A-Za-z0-9-]+$")


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


def _validate_project(project: str) -> str:
    if not _PROJECT_RE.fullmatch(project):
        raise ValueError(f"Invalid BigQuery project id: {project!r}")
    return project


def _validate_region(region: str) -> str:
    normalized = region.lower().removeprefix("region-")
    if not _REGION_RE.fullmatch(normalized):
        raise ValueError(f"Invalid BigQuery region: {region!r}")
    return normalized


def _prepare_google_application_credentials() -> None:
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

    with tempfile.NamedTemporaryFile(
        "w", suffix=".json", delete=False, encoding="utf-8"
    ) as credentials_file:
        credentials_file.write(credentials)
        credentials_path = Path(credentials_file.name)

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(credentials_path)
    atexit.register(_remove_temp_credentials, credentials_path)


def _remove_temp_credentials(credentials_path: Path) -> None:
    if credentials_path.exists():
        credentials_path.unlink()


def _query_conditions(args: argparse.Namespace) -> tuple[list[str], list[Any]]:
    conditions = [
        "creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @days DAY)",
        "job_type = 'QUERY'",
        "statement_type != 'SCRIPT'",
    ]
    parameters: list[Any] = [
        bigquery.ScalarQueryParameter("days", "INT64", args.days),
        bigquery.ScalarQueryParameter("limit", "INT64", args.limit),
    ]

    if args.user_email:
        conditions.append("LOWER(user_email) = LOWER(@user_email)")
        parameters.append(
            bigquery.ScalarQueryParameter("user_email", "STRING", args.user_email)
        )

    if args.query_contains:
        conditions.append("LOWER(query) LIKE CONCAT('%', LOWER(@query_contains), '%')")
        parameters.append(
            bigquery.ScalarQueryParameter(
                "query_contains", "STRING", args.query_contains
            )
        )

    return conditions, parameters


def _summary_sql(project: str, region: str, conditions: Sequence[str]) -> str:
    where_clause = "\n        AND ".join(conditions)
    jobs_view = f"`{project}.region-{region}.INFORMATION_SCHEMA.JOBS_BY_PROJECT`"
    return f"""
    WITH jobs AS (
        SELECT
            creation_time,
            job_id,
            user_email,
            cache_hit,
            error_result,
            total_bytes_processed,
            total_bytes_billed,
            query,
            referenced_tables
        FROM {jobs_view}
        WHERE {where_clause}
    ),
    referenced AS (
        SELECT
            creation_time,
            job_id,
            user_email,
            cache_hit,
            error_result,
            total_bytes_processed,
            total_bytes_billed,
            query,
            CONCAT(ref.project_id, '.', ref.dataset_id, '.', ref.table_id)
                AS referenced_table
        FROM jobs, UNNEST(referenced_tables) AS ref
    )
    SELECT
        referenced_table,
        COUNT(DISTINCT job_id) AS query_count,
        COUNT(DISTINCT user_email) AS user_count,
        ARRAY_AGG(DISTINCT user_email IGNORE NULLS LIMIT 10) AS users,
        MAX(creation_time) AS last_accessed_at,
        SUM(total_bytes_processed) AS total_bytes_processed,
        SUM(total_bytes_billed) AS total_bytes_billed,
        AVG(total_bytes_processed) AS avg_bytes_processed,
        COUNTIF(cache_hit) AS cache_hit_count,
        COUNTIF(error_result IS NOT NULL) AS failed_job_count,
        ARRAY_AGG(
            STRUCT(
                job_id,
                creation_time,
                user_email,
                total_bytes_processed,
                SUBSTR(query, 1, 500) AS query_preview
            )
            ORDER BY creation_time DESC
            LIMIT 3
        ) AS recent_jobs
    FROM referenced
    GROUP BY referenced_table
    ORDER BY total_bytes_processed DESC
    LIMIT @limit
    """


def _jobs_sql(project: str, region: str, conditions: Sequence[str]) -> str:
    where_clause = "\n        AND ".join(conditions)
    jobs_view = f"`{project}.region-{region}.INFORMATION_SCHEMA.JOBS_BY_PROJECT`"
    return f"""
    SELECT
        creation_time,
        job_id,
        user_email,
        statement_type,
        cache_hit,
        error_result IS NOT NULL AS failed,
        total_bytes_processed,
        total_bytes_billed,
        ARRAY(
            SELECT CONCAT(ref.project_id, '.', ref.dataset_id, '.', ref.table_id)
            FROM UNNEST(referenced_tables) AS ref
        ) AS referenced_tables,
        SUBSTR(query, 1, 1000) AS query_preview
    FROM {jobs_view}
    WHERE {where_clause}
    ORDER BY creation_time DESC
    LIMIT @limit
    """


def _run_audit(args: argparse.Namespace) -> list[dict[str, Any]]:
    project = _validate_project(args.project)
    region = _validate_region(args.region)
    conditions, parameters = _query_conditions(args)
    sql = (
        _summary_sql(project, region, conditions)
        if args.mode == "summary"
        else _jobs_sql(project, region, conditions)
    )
    client = bigquery.Client(project=project, location=region.upper())
    job_config = bigquery.QueryJobConfig(query_parameters=parameters)
    return [
        _normalize_value(dict(row.items()))
        for row in client.query(sql, job_config=job_config)
    ]


def _normalize_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _normalize_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_normalize_value(item) for item in value]
    if hasattr(value, "items"):
        return _normalize_value(dict(value.items()))
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value


def _format_bytes(value: Any) -> str:
    if value in {None, ""}:
        return "0 B"
    size = float(value)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB", "PiB"):
        if abs(size) < 1024 or unit == "PiB":
            return f"{size:,.1f} {unit}" if unit != "B" else f"{int(size):,} {unit}"
        size /= 1024
    return f"{size:,.1f} PiB"


def _write_json(rows: Sequence[dict[str, Any]], output: Path | None) -> None:
    serialized = json.dumps(rows, indent=2, sort_keys=True)
    if output is None:
        print(serialized)
        return
    output.write_text(serialized + "\n", encoding="utf-8")


def _write_csv(rows: Sequence[dict[str, Any]], output: Path | None) -> None:
    if not rows:
        if output is None:
            return
        output.write_text("", encoding="utf-8")
        return

    fieldnames = list(rows[0])
    target = output.open("w", newline="", encoding="utf-8") if output else sys.stdout
    close_target = output is not None
    try:
        writer = csv.DictWriter(target, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    key: json.dumps(value) if isinstance(value, list | dict) else value
                    for key, value in row.items()
                }
            )
    finally:
        if close_target:
            target.close()


def _write_table(rows: Sequence[dict[str, Any]], mode: str) -> None:
    if not rows:
        print("No matching BigQuery query jobs found.")
        return

    columns = (
        [
            ("referenced_table", "table"),
            ("query_count", "queries"),
            ("user_count", "users"),
            ("last_accessed_at", "last_accessed"),
            ("total_bytes_processed", "processed"),
            ("total_bytes_billed", "billed"),
            ("failed_job_count", "failed"),
        ]
        if mode == "summary"
        else [
            ("creation_time", "created"),
            ("user_email", "user"),
            ("total_bytes_processed", "processed"),
            ("total_bytes_billed", "billed"),
            ("failed", "failed"),
            ("referenced_tables", "tables"),
            ("query_preview", "query_preview"),
        ]
    )

    formatted_rows = [
        {label: _format_table_value(row.get(key), key) for key, label in columns}
        for row in rows
    ]
    widths = {
        label: max(len(label), *(len(row[label]) for row in formatted_rows))
        for _, label in columns
    }
    print("  ".join(label.ljust(widths[label]) for _, label in columns))
    print("  ".join("-" * widths[label] for _, label in columns))
    for row in formatted_rows:
        print("  ".join(row[label].ljust(widths[label]) for _, label in columns))


def _format_table_value(value: Any, key: str) -> str:
    if key in {"total_bytes_processed", "total_bytes_billed", "avg_bytes_processed"}:
        return _format_bytes(value)
    if isinstance(value, list):
        if all(isinstance(item, str) for item in value):
            return ", ".join(value)
        return json.dumps(value)
    if value is None:
        return ""
    return str(value).replace("\n", " ")[:240]


def _write_output(rows: Sequence[dict[str, Any]], args: argparse.Namespace) -> None:
    output = Path(args.output) if args.output else None
    if args.format == "json":
        _write_json(rows, output)
    elif args.format == "csv":
        _write_csv(rows, output)
    else:
        _write_table(rows, args.mode)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Audit BigQuery query history to see which tables were accessed, "
            "by whom, and how many bytes were processed."
        )
    )
    parser.add_argument(
        "--project",
        default=os.getenv("BIGQUERY_PROJECT")
        or os.getenv("BIGQUERY_PROJECT_ID")
        or "econ-data-project-478800",
        help="BigQuery project id. Defaults to BIGQUERY_PROJECT or repo default.",
    )
    parser.add_argument(
        "--region",
        default=os.getenv("BIGQUERY_LOCATION", "US"),
        help="BigQuery job history region, for example US or us-central1.",
    )
    parser.add_argument(
        "--days",
        type=_positive_int,
        default=7,
        help="Number of days of job history to inspect.",
    )
    parser.add_argument(
        "--limit",
        type=_positive_int,
        default=100,
        help="Maximum rows to return.",
    )
    parser.add_argument(
        "--mode",
        choices=("summary", "jobs"),
        default="summary",
        help="summary aggregates by referenced table; jobs lists recent query jobs.",
    )
    parser.add_argument(
        "--user-email",
        help="Only include jobs run by this user or service account.",
    )
    parser.add_argument(
        "--query-contains",
        help="Only include jobs whose SQL contains this text.",
    )
    parser.add_argument(
        "--format",
        choices=("table", "json", "csv"),
        default="table",
        help="Output format.",
    )
    parser.add_argument(
        "--output",
        help="Optional output file for json or csv formats.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        _prepare_google_application_credentials()
        rows = _run_audit(args)
    except (ValueError, OSError) as error:
        parser.error(str(error))
    except Exception as error:
        print(f"BigQuery access audit failed: {error}", file=sys.stderr)
        return 1

    _write_output(rows, args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
