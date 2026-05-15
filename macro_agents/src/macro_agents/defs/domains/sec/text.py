from datetime import datetime, timezone

import dagster as dg
import polars as pl
from metaxy.ext.dagster import MetaxyStoreFromConfigResource, metaxify

# Importing the lineage module registers SEC FeatureSpecs in Metaxy's global
# feature graph before @metaxify below tries to look them up.
from macro_agents.defs.domains.sec import lineage  # noqa: F401
from macro_agents.defs.domains.sec.metadata import sec_filing_metadata
from macro_agents.defs.domains.sec.tables import ensure_sec_filing_content_table
from macro_agents.defs.resources.gcs import GCSResource
from macro_agents.defs.resources.motherduck import MotherDuckResource
from macro_agents.defs.resources.sec_edgar import SECEdgarResource
from macro_agents.defs.domains.sec.config import BATCH_SIZE_STANDARD, MAX_ERROR_DETAILS
from macro_agents.defs.utils.sec_text_extractor import SECTextExtractor


@metaxify
@dg.asset(
    group_name="transformation",
    kinds={"gcs", "duckdb", "sec_filing_documents"},
    deps=[sec_filing_metadata],
    description="Extract text content from SEC filings stored in GCS",
    metadata={"metaxy/feature": "sec/extracted_text"},
)
def sec_filing_text_extracted(
    context: dg.AssetExecutionContext,
    sec_edgar: SECEdgarResource,
    gcs: GCSResource,
    md: MotherDuckResource,
    metaxy_store: MetaxyStoreFromConfigResource,
) -> dg.MaterializeResult:
    """
    Extract text content from SEC filings stored in GCS.

    Steps:
    1. Load processed filings with GCS paths from sec_filings table
    2. For each filing, download from GCS and extract text
    3. Parse sections (Business, Risk Factors, MD&A, etc.)
    4. Store extracted content in sec_filing_content table
    """
    conn = None
    metaxy_stale_count: int | None = None
    metaxy_divergence_count: int | None = None
    metaxy_shadow_error: str | None = None
    try:
        conn = md.get_connection()
        ensure_sec_filing_content_table(conn)

        # Load processed filings that haven't been text-extracted yet
        batch_size = BATCH_SIZE_STANDARD
        filings_to_process = pl.read_database(
            f"""
            SELECT f.filing_id, f.cik, f.symbol, f.form_type, f.gcs_path
            FROM sec_filings f
            WHERE f.processed = TRUE
            AND f.gcs_path IS NOT NULL
            AND NOT EXISTS (
                SELECT 1 FROM sec_filing_content c
                WHERE c.filing_id = f.filing_id
            )
            LIMIT {batch_size}
            """,
            connection=conn,
        )

        # Shadow mode (issue #46 Phase 1): compute Metaxy's stale set and log
        # divergence vs. the boolean-based query above. Behavior is unchanged —
        # only measurement happens here. Wrapped in try/except so a store
        # outage cannot block extraction.
        try:
            boolean_stale_ids = set(filings_to_process["filing_id"].to_list())
            with metaxy_store.get_store() as store:
                increment = store.resolve_update("sec/extracted_text")
                metaxy_stale_df = increment.to_native()
                metaxy_stale_ids = (
                    set(metaxy_stale_df["filing_id"].to_list())
                    if "filing_id" in metaxy_stale_df.columns
                    else set()
                )
            metaxy_stale_count = len(metaxy_stale_ids)
            metaxy_divergence_count = len(
                metaxy_stale_ids.symmetric_difference(boolean_stale_ids)
            )
        except Exception as e:  # noqa: BLE001 — shadow mode must never fail the asset
            metaxy_shadow_error = f"{type(e).__name__}: {e}"
            context.log.warning(f"Metaxy shadow comparison failed: {metaxy_shadow_error}")

        if filings_to_process.is_empty():
            context.log.debug("No filings to extract text from")
            return dg.MaterializeResult(
                metadata={
                    "status": "no_filings",
                    "filings_processed": 0,
                    "metaxy_stale_count": metaxy_stale_count,
                    "metaxy_divergence_count": metaxy_divergence_count,
                    "metaxy_shadow_error": metaxy_shadow_error,
                }
            )

        context.log.debug(f"Extracting text from {len(filings_to_process)} filings")

        extractor = SECTextExtractor()
        total_processed = 0
        total_sections = 0
        total_errors = 0
        errors = []
        all_content_records = []

        for row in filings_to_process.iter_rows(named=True):
            filing_id = row["filing_id"]
            symbol = row["symbol"]
            form_type = row["form_type"]
            gcs_path = row["gcs_path"]

            try:
                context.log.debug(f"Extracting text from {form_type} for {symbol}")

                # Download filing content from GCS
                filing_data = gcs.download_json(gcs_path, context=context)
                if not filing_data or "content" not in filing_data:
                    context.log.warning(f"No content found in GCS for {filing_id}")
                    continue

                html_content = filing_data["content"]

                # Extract full text and sections
                extraction_result = extractor.get_full_text_with_metadata(
                    html_content, form_type
                )

                # Store full text first
                full_text_id = sec_edgar.generate_content_id(filing_id, "full_text")
                full_text_gcs_path = (
                    f"{gcs_path.rsplit('/', 1)[0]}/extracted/full_text.json"
                )

                gcs.upload_json(
                    full_text_gcs_path,
                    {
                        "filing_id": filing_id,
                        "section_name": "full_text",
                        "content": extraction_result["full_text"],
                        "word_count": extraction_result["total_word_count"],
                        "extracted_at": datetime.now(timezone.utc).isoformat(),
                    },
                    context=context,
                )

                # Collect full text record for bulk insert
                all_content_records.append(
                    {
                        "content_id": full_text_id,
                        "filing_id": filing_id,
                        "section_name": "full_text",
                        "section_order": 0,
                        "word_count": extraction_result["total_word_count"],
                        "gcs_path": full_text_gcs_path,
                    }
                )

                # Store each extracted section
                for i, section in enumerate(extraction_result["sections"]):
                    section_id = sec_edgar.generate_content_id(
                        filing_id, section["name"]
                    )
                    section_gcs_path = (
                        f"{gcs_path.rsplit('/', 1)[0]}/extracted/"
                        f"section_{i + 1}_{section['item_number'].replace(' ', '_')}.json"
                    )

                    gcs.upload_json(
                        section_gcs_path,
                        {
                            "filing_id": filing_id,
                            "section_name": section["name"],
                            "item_number": section["item_number"],
                            "content": section["content"],
                            "word_count": section["word_count"],
                            "extracted_at": datetime.now(timezone.utc).isoformat(),
                        },
                        context=context,
                    )

                    # Collect section record for bulk insert
                    all_content_records.append(
                        {
                            "content_id": section_id,
                            "filing_id": filing_id,
                            "section_name": section["name"],
                            "section_order": i + 1,
                            "word_count": section["word_count"],
                            "gcs_path": section_gcs_path,
                        }
                    )

                    total_sections += 1

                total_processed += 1

                context.log.debug(
                    f"Extracted {len(extraction_result['sections'])} sections "
                    f"from {form_type} for {symbol}"
                )

            except Exception as e:
                error_msg = f"Error extracting text for {symbol} ({filing_id}): {e}"
                context.log.error(error_msg)
                errors.append(error_msg)
                total_errors += 1
                continue

        # Bulk upsert all content records
        if all_content_records:
            context.log.debug(
                f"Bulk upserting {len(all_content_records)} content records "
                f"({total_processed} filings, {total_sections} sections)"
            )
            content_df = pl.DataFrame(all_content_records)
            md.upsert_data(
                "sec_filing_content",
                content_df,
                ["content_id"],
                context=context,
            )
            context.log.debug(
                f"Successfully bulk upserted {len(all_content_records)} content records"
            )

        context.log.debug(
            f"Text extraction complete. "
            f"Filings: {total_processed}, Sections: {total_sections}, Errors: {total_errors}"
        )

        # Get count of remaining filings to process
        remaining_row = conn.execute(
            """
            SELECT COUNT(*)
            FROM sec_filings f
            WHERE f.processed = TRUE
            AND f.gcs_path IS NOT NULL
            AND NOT EXISTS (
                SELECT 1 FROM sec_filing_content c
                WHERE c.filing_id = f.filing_id
            )
            """
        ).fetchone()
        remaining = remaining_row[0] if remaining_row else 0

        return dg.MaterializeResult(
            metadata={
                "status": "completed",
                "filings_processed": total_processed,
                "sections_extracted": total_sections,
                "errors": total_errors,
                "remaining_to_process": remaining,
                "error_details": errors[:MAX_ERROR_DETAILS] if errors else [],
                "metaxy_stale_count": metaxy_stale_count,
                "metaxy_divergence_count": metaxy_divergence_count,
                "metaxy_shadow_error": metaxy_shadow_error,
            }
        )

    finally:
        if conn:
            conn.close()
