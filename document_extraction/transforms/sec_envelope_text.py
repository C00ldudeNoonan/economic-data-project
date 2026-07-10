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


def run(deps: dict[str, pl.DataFrame]) -> pl.DataFrame:
    registry = deps["sec_document_registry"]

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

    return pl.DataFrame(rows)
