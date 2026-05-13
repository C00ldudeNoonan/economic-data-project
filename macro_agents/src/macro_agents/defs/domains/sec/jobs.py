import dagster as dg

from macro_agents.defs.domains.sec.config import (
    INGESTION_JOB_MAX_RUNTIME,
    INGESTION_JOB_PRIORITY,
    PROCESSING_JOB_MAX_RUNTIME,
    PROCESSING_JOB_PRIORITY,
)

sec_filings_ingestion_job = dg.define_asset_job(
    name="sec_filings_ingestion_job",
    tags={
        "dagster/priority": str(INGESTION_JOB_PRIORITY),
        "dagster/max_runtime": INGESTION_JOB_MAX_RUNTIME,
    },
    selection=dg.AssetSelection.assets(
        "sp500_cik_enriched",
        "sec_company_cik_history",
        "sec_filing_metadata",
    ),
    description=(
        "SEC EDGAR filings ingestion pipeline - fetches 10-K/10-Q filings for S&P 500 "
        "companies, extracts text, and generates business intelligence summaries"
    ),
)

sec_filing_processing_job = dg.define_asset_job(
    name="sec_filing_processing_job",
    tags={
        "dagster/priority": str(PROCESSING_JOB_PRIORITY),
        "dagster/max_runtime": PROCESSING_JOB_MAX_RUNTIME,
    },
    selection=dg.AssetSelection.assets(
        "sec_filing_documents_batch",
        "sec_filing_text_extracted",
        "sec_filing_business_intelligence",
        "sec_filing_markdown",
        "sec_filing_search_index",
        "sec_filing_fts_index",
        "sec_filing_hybrid_search_ready",
    ),
    description=(
        "SEC filing document processing pipeline - downloads documents, "
        "extracts text, generates BI signals, and builds search indexes"
    ),
)
