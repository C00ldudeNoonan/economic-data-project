from datetime import datetime, timezone

import dagster as dg
import polars as pl

from macro_agents.defs.domains.sec.tables import ensure_sec_company_cik_table
from macro_agents.defs.resources.bigquery_warehouse import BigQueryWarehouseResource
from macro_agents.defs.resources.sec_edgar import SECEdgarResource


@dg.asset(
    group_name="sec_ingestion",
    kinds={"api", "duckdb"},
    deps=[dg.AssetKey("sp500_companies_raw")],
    description="Enrich S&P 500 companies with validated CIK codes from SEC EDGAR",
)
def sp500_cik_enriched(
    context: dg.AssetExecutionContext,
    sec_edgar: SECEdgarResource,
    bq: BigQueryWarehouseResource,
) -> dg.MaterializeResult:
    """
    Validate and enrich S&P 500 CIK codes using SEC EDGAR's official ticker mapping.

    This asset upserts CIK records each run — existing rows are updated with fresh
    data, new companies are inserted, and removed companies' records are preserved
    for historical reference.

    Steps:
    1. Load S&P 500 companies from sp500_companies_raw
    2. Download SEC's official company_tickers.json mapping
    3. Look up CIK codes for all companies
    4. Fall back to Wikipedia CIK if SEC lookup fails
    5. Upsert into sec_company_cik table (preserves history)
    """
    conn = None
    try:
        conn = bq.get_connection()
        ensure_sec_company_cik_table(conn)

        # Load S&P 500 companies
        sp500_df = bq.execute_query("SELECT symbol, company_name, cik FROM sp500_companies_raw WHERE date_ended IS NULL")

        if sp500_df.is_empty():
            context.log.warning("No S&P 500 companies found in database")
            return dg.MaterializeResult(
                metadata={"status": "no_companies", "companies_processed": 0}
            )

        context.log.debug(f"Found {len(sp500_df)} S&P 500 companies to process")

        # Download SEC's official ticker-to-CIK mapping (single API call)
        context.log.debug("Downloading SEC company tickers mapping...")
        sec_ticker_mapping = sec_edgar.get_company_tickers_mapping(context)
        context.log.debug(f"Loaded {len(sec_ticker_mapping)} tickers from SEC")

        # Process all companies
        records = []
        total_success = 0
        total_failed = 0
        now = datetime.now(timezone.utc)

        for row in sp500_df.iter_rows(named=True):
            ticker = row["symbol"]
            wiki_cik = row["cik"] or ""
            wiki_company_name = row["company_name"] or ""

            # Look up in SEC mapping
            sec_data = sec_ticker_mapping.get(ticker.upper())

            if sec_data:
                cik = sec_data["cik"]
                company_name = sec_data["company_name"] or wiki_company_name
                source = "sec_edgar"
            elif wiki_cik:
                cik = wiki_cik
                company_name = wiki_company_name
                source = "wikipedia"
            else:
                context.log.warning(f"No CIK found for {ticker}")
                total_failed += 1
                continue

            # Pad CIK to 10 digits
            cik_clean = str(cik).strip()
            cik_padded = cik_clean.zfill(10) if cik_clean.isdigit() else cik_clean

            records.append(
                {
                    "symbol": ticker,
                    "cik": cik_clean,
                    "cik_padded": cik_padded,
                    "company_name": company_name,
                    "source": source,
                    "validated_at": now,
                    "created_at": now,
                }
            )
            total_success += 1

        # Upsert records (preserves existing rows, updates changed values)
        if records:
            records_df = pl.DataFrame(records)
            context.log.debug(
                f"Upserting {len(records)} CIK records into sec_company_cik"
            )
            bq.upsert_data("sec_company_cik", records_df, ["symbol"], context=context)
            context.log.debug(f"Successfully upserted {len(records)} CIK records")

        context.log.debug(
            f"CIK enrichment complete. Success: {total_success}, Failed: {total_failed}"
        )

        return dg.MaterializeResult(
            metadata={
                "status": "completed",
                "companies_processed": total_success + total_failed,
                "cik_found": total_success,
                "cik_not_found": total_failed,
                "total_companies": len(sp500_df),
            }
        )

    finally:
        if conn:
            conn.close()
