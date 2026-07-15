"""SEC filing Markdown conversion asset.

Converts SEC filing HTML documents to clean Markdown for AI agent consumption.
Processes filings in batches, storing both full-filing and per-section Markdown
files in GCS with tracking in the sec_filing_markdown table.
"""

from datetime import datetime, timezone

import dagster as dg
import polars as pl

from macro_agents.defs.domains.sec.helpers import (
    build_filing_markdown_gcs_path,
    build_section_markdown_gcs_path,
)
from macro_agents.defs.domains.sec.metadata import sec_filing_metadata
from macro_agents.defs.domains.sec.tables import ensure_sec_filing_markdown_table
from macro_agents.defs.resources.gcs import GCSResource
from macro_agents.defs.resources.bigquery_warehouse import BigQueryWarehouseResource
from macro_agents.defs.domains.sec.config import BATCH_SIZE_STANDARD, MAX_ERROR_DETAILS
from macro_agents.defs.utils.sec_markdown_converter import SECMarkdownConverter
from macro_agents.defs.utils.sec_text_extractor import SECTextExtractor


@dg.asset(
    group_name="transformation",
    kinds={"gcs", "bigquery"},
    deps=[sec_filing_metadata],
    description=(
        "Convert SEC filing HTML to Markdown for AI agent consumption. "
        "Stores full-filing and per-section Markdown in GCS."
    ),
)
def sec_filing_markdown(
    context: dg.AssetExecutionContext,
    gcs: GCSResource,
    bq: BigQueryWarehouseResource,
) -> dg.MaterializeResult:
    """
    Convert processed SEC filings from HTML to Markdown.

    Steps:
    1. Find processed filings that don't have markdown yet
    2. Download the HTML content from GCS
    3. Convert to Markdown using SECMarkdownConverter
    4. Extract individual sections and convert each
    5. Upload full-filing and section Markdown to GCS
    6. Track completion in sec_filing_markdown table
    """
    conn = None
    try:
        conn = bq.get_connection()
        ensure_sec_filing_markdown_table(conn)

        batch_size = BATCH_SIZE_STANDARD
        filings_to_process = bq.execute_query(
            f"""
            SELECT f.filing_id, f.cik, f.symbol, f.form_type,
                   f.filing_date, f.accession_number, f.gcs_path
            FROM sec_filings f
            WHERE f.processed = TRUE
            AND f.gcs_path IS NOT NULL
            AND NOT EXISTS (
                SELECT 1 FROM sec_filing_markdown m
                WHERE m.filing_id = f.filing_id
            )
            LIMIT {batch_size}
            """
        )

        if filings_to_process.is_empty():
            context.log.debug("No filings pending markdown conversion")
            return dg.MaterializeResult(
                metadata={"status": "up_to_date", "filings_converted": 0}
            )

        context.log.info(f"Converting {len(filings_to_process)} filings to Markdown")

        converter = SECMarkdownConverter()
        extractor = SECTextExtractor()
        records = []
        errors = []

        for row in filings_to_process.iter_rows(named=True):
            filing_id = row["filing_id"]
            symbol = row["symbol"]
            form_type = row["form_type"]
            gcs_path = row["gcs_path"]

            try:
                # Download the filing JSON envelope from GCS
                filing_json = gcs.download_json(gcs_path)
                html_content = filing_json.get("content", "")

                if not html_content:
                    context.log.warning(
                        f"No HTML content for {symbol} filing {filing_id}"
                    )
                    continue

                # Convert full filing to Markdown
                full_markdown = converter.convert(html_content, form_type)
                word_count = len(full_markdown.split())

                if not full_markdown.strip():
                    context.log.warning(
                        f"Empty markdown for {symbol} filing {filing_id}"
                    )
                    continue

                # Derive base path from gcs_path (remove the document filename)
                base_path = "/".join(gcs_path.split("/")[:-1])

                # Upload full markdown
                md_gcs_path = build_filing_markdown_gcs_path(base_path)
                gcs.upload_bytes(
                    md_gcs_path, full_markdown.encode("utf-8"), "text/markdown"
                )

                # Extract and convert individual sections
                sections = extractor.extract_sections(html_content, form_type)
                section_count = 0

                for section in sections:
                    if not section.content.strip():
                        continue

                    section_md = converter.convert_section(
                        section.content, section.name
                    )
                    if section_md.strip():
                        section_path = build_section_markdown_gcs_path(
                            base_path, section.name
                        )
                        gcs.upload_bytes(
                            section_path,
                            section_md.encode("utf-8"),
                            "text/markdown",
                        )
                        section_count += 1

                records.append(
                    {
                        "filing_id": filing_id,
                        "symbol": symbol,
                        "markdown_gcs_path": md_gcs_path,
                        "section_count": section_count,
                        "word_count": word_count,
                        "created_at": datetime.now(timezone.utc),
                    }
                )

                context.log.debug(
                    f"Converted {symbol} ({form_type}): "
                    f"{word_count} words, {section_count} sections"
                )

            except Exception as e:
                error_msg = f"Error converting {symbol} filing {filing_id}: {e}"
                context.log.warning(error_msg)
                errors.append(error_msg)
                continue

        if records:
            records_df = pl.DataFrame(records)
            bq.upsert_data(
                "sec_filing_markdown", records_df, ["filing_id"], context=context
            )
            context.log.info(f"Tracked {len(records)} markdown conversions")

        return dg.MaterializeResult(
            metadata={
                "status": "completed",
                "filings_converted": len(records),
                "errors": len(errors),
                "error_details": errors[:MAX_ERROR_DETAILS] if errors else [],
            }
        )

    finally:
        if conn:
            conn.close()
