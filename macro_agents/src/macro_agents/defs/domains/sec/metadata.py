from datetime import datetime, timezone

import dagster as dg
import polars as pl

from macro_agents.defs.domains.sec.cik import sp500_cik_enriched
from macro_agents.defs.domains.sec.helpers import get_company_filing_partition_name
from macro_agents.defs.domains.sec.tables import ensure_sec_filings_table
from macro_agents.defs.resources.bigquery_warehouse import BigQueryWarehouseResource
from macro_agents.defs.domains.sec.config import (
    MAX_ERROR_DETAILS,
    METADATA_PROGRESS_LOG_INTERVAL,
    YEARS_BACK_FULL_BACKFILL,
)
from macro_agents.defs.resources.sec_edgar import SECEdgarResource

_FORM_TYPES = ["10-K", "10-Q", "10-K/A", "10-Q/A"]


@dg.asset(
    group_name="sec_ingestion",
    kinds={"api", "duckdb"},
    deps=[sp500_cik_enriched],
    description="Fetch SEC 10-K and 10-Q filing metadata for S&P 500 companies (incremental)",
)
def sec_filing_metadata(
    context: dg.AssetExecutionContext,
    sec_edgar: SECEdgarResource,
    md: BigQueryWarehouseResource,
) -> dg.MaterializeResult:
    """
    Fetch SEC filing metadata for all S&P 500 companies with CIK codes.

    Incremental: for companies with existing filings, only fetches filings
    newer than the latest known filing date. New companies get a full
    20-year backfill.

    Steps:
    1. Load companies with CIK codes from sec_company_cik
    2. Query latest filing date per CIK from sec_filings
    3. For each company, fetch only new filings from SEC EDGAR
    4. Upsert filing metadata into sec_filings table
    5. Sync dynamic partitions for document download
    """
    conn = None
    try:
        conn = md.get_connection()
        ensure_sec_filings_table(conn)

        # Load companies with CIK codes
        try:
            companies_df = md.execute_query("SELECT symbol, cik, cik_padded, company_name FROM sec_company_cik")
        except Exception as e:
            context.log.warning(
                f"Could not query sec_company_cik table (may not exist yet): {e}"
            )
            return dg.MaterializeResult(
                metadata={
                    "status": "table_not_found",
                    "error": str(e),
                    "filings_fetched": 0,
                }
            )

        if companies_df.is_empty():
            context.log.warning("No companies with CIK codes found")
            return dg.MaterializeResult(
                metadata={"status": "no_companies", "filings_fetched": 0}
            )

        context.log.debug(f"Fetching filings for {len(companies_df)} companies")

        # Query latest filing date per CIK for incremental fetching
        try:
            latest_filings = md.execute_query(
                "SELECT cik, MAX(filing_date) as latest_filing_date "
                "FROM sec_filings GROUP BY cik"
            )
            latest_by_cik = (
                {
                    row["cik"]: str(row["latest_filing_date"])
                    for row in latest_filings.iter_rows(named=True)
                }
                if not latest_filings.is_empty()
                else {}
            )
        except Exception as e:
            context.log.warning(
                f"Could not read existing filings (table may be empty): {e}"
            )
            latest_by_cik = {}

        # Load historical CIK mappings (if table exists)
        historical_ciks_by_symbol: dict[str, list[str]] = {}
        try:
            history_df = md.execute_query(
                "SELECT current_symbol, cik_padded "
                "FROM sec_company_cik_history "
                "WHERE relationship_type != 'current' "
                "AND cik_padded IS NOT NULL"
            )
            if not history_df.is_empty():
                for row in history_df.iter_rows(named=True):
                    sym = row["current_symbol"]
                    cik_pad = row["cik_padded"]
                    if sym not in historical_ciks_by_symbol:
                        historical_ciks_by_symbol[sym] = []
                    if cik_pad not in historical_ciks_by_symbol[sym]:
                        historical_ciks_by_symbol[sym].append(cik_pad)
                context.log.debug(
                    f"Loaded historical CIKs for "
                    f"{len(historical_ciks_by_symbol)} companies"
                )
        except Exception:
            context.log.debug(
                "No sec_company_cik_history table yet, using current CIKs only"
            )

        backfill_count = 0
        incremental_count = 0

        total_filings = 0
        total_companies = 0
        errors = []
        all_filings = []

        # Process all companies — incremental for existing, full backfill for new
        total_companies_count = len(companies_df)
        for company_index, row in enumerate(companies_df.iter_rows(named=True)):
            symbol = row["symbol"]
            cik = row["cik_padded"] or row["cik"]

            if (
                company_index > 0
                and company_index % METADATA_PROGRESS_LOG_INTERVAL == 0
            ):
                context.log.info(
                    f"Progress: {company_index}/{total_companies_count} companies "
                    f"({total_filings} filings fetched, {len(errors)} errors)"
                )

            # Build list of all CIKs to fetch for this company
            ciks_to_fetch = [cik]
            extra_ciks = historical_ciks_by_symbol.get(symbol, [])
            for extra_cik in extra_ciks:
                if extra_cik != cik and extra_cik not in ciks_to_fetch:
                    ciks_to_fetch.append(extra_cik)

            try:
                company_filings = []

                for fetch_cik in ciks_to_fetch:
                    latest_date = latest_by_cik.get(fetch_cik)

                    if latest_date:
                        context.log.debug(
                            f"Incremental fetch for {symbol} (CIK: {fetch_cik}) "
                            f"from {latest_date}"
                        )
                        filings_df = sec_edgar.get_company_filings(
                            cik=fetch_cik,
                            form_types=_FORM_TYPES,
                            start_date=latest_date,
                            context=context,
                        )
                        incremental_count += 1
                    else:
                        context.log.debug(
                            f"Full backfill for {symbol} (CIK: {fetch_cik})"
                        )
                        filings_df = sec_edgar.get_10k_10q_filings(
                            fetch_cik,
                            years_back=YEARS_BACK_FULL_BACKFILL,
                            context=context,
                        )
                        backfill_count += 1

                    if not filings_df.is_empty():
                        company_filings.append(filings_df)

                if not company_filings:
                    continue

                filings_df = pl.concat(company_filings)

                # Add symbol and generate filing IDs
                filings_df = filings_df.with_columns(
                    [
                        pl.lit(symbol).alias("symbol"),
                        pl.struct(["cik", "accession_number"])
                        .map_elements(
                            lambda x: sec_edgar.generate_filing_id(
                                str(x["cik"]), x["accession_number"]
                            ),
                            return_dtype=pl.Utf8,
                        )
                        .alias("filing_id"),
                    ]
                )

                # Select columns matching the full table schema
                filings_to_insert = filings_df.select(
                    [
                        "filing_id",
                        "cik",
                        "symbol",
                        pl.col("accession_number"),
                        "form_type",
                        pl.col("filing_date").cast(pl.Date),
                        pl.col("accepted_datetime").alias("accepted_datetime"),
                        pl.col("report_date").cast(pl.Date),
                        "file_number",
                        "film_number",
                        "items",
                        "size_bytes",
                        "is_xbrl",
                        "is_inline_xbrl",
                        "primary_document",
                        "primary_doc_description",
                        pl.lit(None).cast(pl.Utf8).alias("gcs_path"),
                        pl.lit(False).alias("processed"),
                        pl.lit(datetime.now(timezone.utc)).alias("created_at"),
                    ]
                )

                # Collect filings for bulk upsert
                all_filings.append(filings_to_insert)
                total_filings += len(filings_df)
                total_companies += 1

                context.log.debug(f"Collected {len(filings_df)} filings for {symbol}")

            except Exception as e:
                error_msg = f"Error fetching filings for {symbol}: {e}"
                context.log.error(error_msg)
                errors.append(error_msg)
                continue

        # Perform bulk upsert of all collected filings
        if all_filings:
            context.log.debug(
                f"Performing bulk upsert of {total_filings} filings "
                f"from {total_companies} companies"
            )
            combined_filings = pl.concat(all_filings)
            # Deduplicate by symbol and filing_id to prevent PRIMARY KEY violations
            # Keep the first occurrence of each (symbol, filing_id) combination
            combined_filings = combined_filings.unique(
                subset=["symbol", "filing_id"], keep="first"
            )
            context.log.debug(
                f"After deduplication: {len(combined_filings)} unique filings"
            )
            md.upsert_data(
                "sec_filings",
                combined_filings,
                ["symbol", "filing_id"],
                context=context,
            )
            context.log.debug(
                f"Successfully bulk upserted {len(combined_filings)} filings"
            )

        context.log.debug(
            f"Filing metadata complete. "
            f"Companies: {total_companies}, Filings: {total_filings}, "
            f"Errors: {len(errors)}, "
            f"Backfills: {backfill_count}, Incremental: {incremental_count}"
        )

        # Sync dynamic partitions for new filings
        # Create partitions only for filings with primary_document (required for document download)
        if total_filings > 0 and all_filings:
            filings_with_docs = combined_filings.filter(
                (pl.col("primary_document").is_not_null())
                & (pl.col("primary_document") != "")
                & (pl.col("symbol").is_not_null())
                & (pl.col("filing_id").is_not_null())
            )
            if not filings_with_docs.is_empty():
                sec_edgar.sync_filing_partitions_from_dataframe(
                    context,
                    filings_with_docs,
                    get_company_filing_partition_name,
                )

                # Also sync to the document download partition namespace
                filing_ids = filings_with_docs["filing_id"].unique().to_list()
                context.instance.add_dynamic_partitions(
                    "sec_filing_documents", filing_ids
                )

        return dg.MaterializeResult(
            metadata={
                "status": "completed",
                "companies_processed": total_companies,
                "filings_fetched": total_filings,
                "backfill_companies": backfill_count,
                "incremental_companies": incremental_count,
                "errors": len(errors),
                "error_details": errors[:MAX_ERROR_DETAILS] if errors else [],
            }
        )

    finally:
        if conn:
            conn.close()


@dg.asset(
    group_name="sec_ingestion",
    kinds={"database", "partitions"},
    description="Sync dynamic partitions for SEC filings from database",
)
def sec_filing_partitions_sync(
    context: dg.AssetExecutionContext,
    sec_edgar: SECEdgarResource,
    md: BigQueryWarehouseResource,
) -> dg.MaterializeResult:
    """
    Sync dynamic partitions for companies and filings from existing sec_filings table.

    This ensures partitions are available for company-specific filing document assets
    even if sec_filing_metadata hasn't run recently or partitions weren't created initially.
    """
    conn = None
    try:
        conn = md.get_connection()
        ensure_sec_filings_table(conn)

        # Get all filings with primary_document (required for document download)
        filings_df = md.execute_query("""
            SELECT DISTINCT symbol, filing_id
            FROM sec_filings
            WHERE primary_document IS NOT NULL
            AND primary_document != ''
            AND symbol IS NOT NULL
            AND filing_id IS NOT NULL
            """)

        if filings_df.is_empty():
            context.log.debug("No filings found in database to create partitions for")
            return dg.MaterializeResult(
                metadata={
                    "status": "no_filings",
                    "companies_synced": 0,
                    "filings_synced": 0,
                }
            )

        # Sync per-company partitions from database
        sec_edgar.sync_filing_partitions_from_dataframe(
            context,
            filings_df,
            get_company_filing_partition_name,
        )

        # Also sync to the document download partition namespace
        filing_ids = filings_df["filing_id"].unique().to_list()
        context.instance.add_dynamic_partitions("sec_filing_documents", filing_ids)

        # Get counts for metadata
        unique_companies = sorted(
            {symbol for symbol in filings_df["symbol"].to_list() if symbol}
        )
        instance = context.instance

        companies_synced = 0
        total_filings = 0

        for company in unique_companies:
            partition_name = get_company_filing_partition_name(company)
            try:
                filings_count = len(instance.get_dynamic_partitions(partition_name))
                if filings_count > 0:
                    companies_synced += 1
                    total_filings += filings_count
            except Exception:
                pass

        return dg.MaterializeResult(
            metadata={
                "status": "completed",
                "companies_synced": companies_synced,
                "total_filings_synced": total_filings,
            }
        )

    finally:
        if conn:
            conn.close()
