"""Parse SEC filing envelopes: HTML content -> body text, metadata -> columns.

The GCS objects are JSON envelopes written by macro_agents'
filing_downloader: {"content": "<html...>", "metadata": {filing_id, symbol,
form_type, filing_date, accession_number, ...}}. The registry model projects
`content` and `metadata`; this transform produces the analysis-ready grain.
"""

from __future__ import annotations

import json
from typing import Any

import polars as pl
from bs4 import BeautifulSoup

_METADATA_FIELDS = (
    "filing_id",
    "symbol",
    "form_type",
    "filing_date",
    "report_date",
    "accession_number",
    "company_name",
)

_LINEAGE_COLUMNS = (
    "document_id",
    "source_path",
    "source_uri",
    "content_hash",
    "extracted_at",
)


def _html_to_text(html: str | None) -> str | None:
    if not html:
        return None
    return BeautifulSoup(html, "html.parser").get_text(" ", strip=True)


def _metadata_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value:
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _is_symbol_layout(source_path: str | None) -> bool:
    """True when the path uses the target layout ({symbol}/ first segment)."""
    if not source_path:
        return False
    first = source_path.split("/", 1)[0]
    return not first.startswith("10-")


def run(deps: dict[str, pl.DataFrame]) -> pl.DataFrame:
    registry = deps["sec_document_registry"]

    # The GCS migration copies legacy-layout objects to symbol-layout paths
    # without deleting the originals, so migrated filings appear twice in
    # the registry with identical content. Keep one row per content_hash,
    # preferring the symbol-layout path (the target convention).
    registry = (
        registry.sort(
            pl.col("source_path")
            .map_elements(_is_symbol_layout, return_dtype=pl.Boolean)
            .fill_null(False),
            descending=True,
        )
        .unique(subset=["content_hash"], keep="first")
    )

    rows: list[dict[str, Any]] = []
    for row in registry.iter_rows(named=True):
        metadata = _metadata_dict(row.get("metadata"))
        out: dict[str, Any] = {
            column: row.get(column) for column in _LINEAGE_COLUMNS
        }
        out["text"] = _html_to_text(row.get("content"))
        for field in _METADATA_FIELDS:
            value = metadata.get(field)
            out[field] = str(value) if value is not None else None
        rows.append(out)

    if not rows:
        # An empty source (fresh/empty bucket) yields no rows; a bare
        # pl.DataFrame([]) would be 0-column and the downstream chunk step
        # and dbt-ml tests would fail on missing document_id/text/symbol/etc.
        # Materialize an empty frame carrying the transform's full schema
        # instead — lineage dtypes from the registry, text + metadata as
        # strings — matching the non-empty column set and order.
        registry_schema = dict(registry.schema)
        schema: dict[str, Any] = {
            column: registry_schema.get(column, pl.String)
            for column in _LINEAGE_COLUMNS
        }
        schema["text"] = pl.String
        for field in _METADATA_FIELDS:
            schema[field] = pl.String
        return pl.DataFrame(schema=schema)

    return pl.DataFrame(rows)
