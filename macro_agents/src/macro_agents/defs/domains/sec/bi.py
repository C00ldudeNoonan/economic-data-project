import dagster as dg
import polars as pl

from macro_agents.defs.domains.sec.tables import ensure_sec_filing_search_terms_table
from macro_agents.defs.domains.sec.text import sec_filing_text_extracted
from macro_agents.defs.resources.gcs import GCSResource
from macro_agents.defs.resources.motherduck import MotherDuckResource
from macro_agents.defs.resources.sec_edgar import SECEdgarResource
from macro_agents.defs.domains.sec.config import (
    BATCH_SIZE_STANDARD,
    BI_CONTEXT_WINDOW,
    MAX_ERROR_DETAILS,
    SIGNAL_CONTEXT_MAX_LENGTH,
    SIGNAL_TERM_MAX_LENGTH,
)
from macro_agents.defs.utils.sec_bi_extractor import SECBIExtractor


@dg.asset(
    group_name="transformation",
    kinds={"gcs", "duckdb", "sec_filing_documents"},
    deps=[sec_filing_text_extracted],
    description="Extract business intelligence signals from SEC filing content",
)
def sec_filing_business_intelligence(
    context: dg.AssetExecutionContext,
    sec_edgar: SECEdgarResource,
    gcs: GCSResource,
    md: MotherDuckResource,
) -> dg.MaterializeResult:
    """
    Extract business intelligence signals from SEC filing content.

    Analyzes extracted text to identify:
    - Growth signals (revenue growth, expansion)
    - Hiring plans (workforce expansion)
    - Market expansion (geographic, new markets)
    - Capital expenditure signals
    - Product innovation indicators
    - Cost efficiency measures
    - Risk factors
    - Financial health indicators
    - Strategic initiatives

    Steps:
    1. Load filings with extracted content from sec_filing_content
    2. For key sections, run BI extraction
    3. Store signals in sec_filing_search_terms table
    """
    conn = None
    try:
        conn = md.get_connection()
        ensure_sec_filing_search_terms_table(conn)

        # Load filings with extracted content that haven't been BI-processed
        batch_size = BATCH_SIZE_STANDARD
        filings_to_process = pl.read_database(
            f"""
            SELECT DISTINCT
                c.filing_id,
                f.symbol,
                f.form_type,
                c.gcs_path,
                c.section_name
            FROM sec_filing_content c
            JOIN sec_filings f ON c.filing_id = f.filing_id
            WHERE c.section_name IN (
                'full_text', 'Business', 'Risk Factors',
                'Management Discussion and Analysis'
            )
            AND NOT EXISTS (
                SELECT 1 FROM sec_filing_search_terms t
                WHERE t.filing_id = c.filing_id
            )
            LIMIT {batch_size}
            """,
            connection=conn,
        )

        if filings_to_process.is_empty():
            context.log.debug("No filings to extract business intelligence from")
            return dg.MaterializeResult(
                metadata={"status": "no_filings", "filings_processed": 0}
            )

        # Group by filing_id to process all sections together
        filing_ids = filings_to_process["filing_id"].unique().to_list()
        context.log.debug(
            f"Extracting business intelligence from {len(filing_ids)} filings"
        )

        bi_extractor = SECBIExtractor(context_window=BI_CONTEXT_WINDOW)
        total_processed = 0
        total_signals = 0
        total_errors = 0
        errors = []
        category_counts: dict[str, int] = {}

        for filing_id in filing_ids:
            filing_rows = filings_to_process.filter(pl.col("filing_id") == filing_id)
            symbol = filing_rows["symbol"][0]
            form_type = filing_rows["form_type"][0]

            try:
                context.log.debug(
                    f"Extracting BI signals from {form_type} for {symbol}"
                )

                all_signals = []

                # Process each section for this filing
                for row in filing_rows.iter_rows(named=True):
                    section_name = row["section_name"]
                    gcs_path = row["gcs_path"]

                    # Download section content from GCS
                    section_data = gcs.download_json(gcs_path, context=context)
                    if not section_data or "content" not in section_data:
                        continue

                    content = section_data["content"]

                    # Extract signals from this section
                    signals = bi_extractor.extract_signals(
                        content, section_name=section_name
                    )
                    all_signals.extend(signals)

                # Insert signals into database
                for signal in all_signals:
                    term_id = sec_edgar.generate_term_id(
                        filing_id, signal.category, signal.position
                    )

                    conn.execute(
                        """
                        INSERT INTO sec_filing_search_terms
                        (term_id, filing_id, term_category, term_text,
                         context_text, section_name, confidence_score)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT (term_id) DO UPDATE SET
                        context_text = EXCLUDED.context_text,
                        confidence_score = EXCLUDED.confidence_score
                        """,
                        [
                            term_id,
                            filing_id,
                            signal.category,
                            signal.term[:SIGNAL_TERM_MAX_LENGTH],
                            signal.context[:SIGNAL_CONTEXT_MAX_LENGTH],
                            signal.section_name,
                            signal.confidence_score,
                        ],
                    )

                    # Track category counts
                    category_counts[signal.category] = (
                        category_counts.get(signal.category, 0) + 1
                    )
                    total_signals += 1

                conn.commit()
                total_processed += 1

                # Get summary stats for logging
                stats = bi_extractor.get_summary_stats(all_signals)
                context.log.debug(
                    f"Extracted {stats['total_signals']} signals from {symbol} "
                    f"(top categories: {stats['top_categories'][:3]})"
                )

            except Exception as e:
                error_msg = f"Error extracting BI for {symbol} ({filing_id}): {e}"
                context.log.error(error_msg)
                errors.append(error_msg)
                total_errors += 1
                continue

        context.log.debug(
            f"Business intelligence extraction complete. "
            f"Filings: {total_processed}, Signals: {total_signals}, "
            f"Errors: {total_errors}"
        )

        # Get remaining count
        try:
            remaining_row = conn.execute(
                """
                SELECT COUNT(DISTINCT c.filing_id)
                FROM sec_filing_content c
                WHERE c.section_name IN (
                    'full_text', 'Business', 'Risk Factors',
                    'Management Discussion and Analysis'
                )
                AND NOT EXISTS (
                    SELECT 1 FROM sec_filing_search_terms t
                    WHERE t.filing_id = c.filing_id
                )
                """
            ).fetchone()
            remaining = remaining_row[0] if remaining_row else 0
        except Exception:
            # Table might not exist yet
            remaining = 0

        return dg.MaterializeResult(
            metadata={
                "status": "completed",
                "filings_processed": total_processed,
                "signals_extracted": total_signals,
                "category_breakdown": category_counts,
                "errors": total_errors,
                "remaining_to_process": remaining,
                "error_details": errors[:MAX_ERROR_DETAILS] if errors else [],
            }
        )

    finally:
        if conn:
            conn.close()
