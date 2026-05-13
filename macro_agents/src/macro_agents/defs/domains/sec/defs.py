import dagster as dg

from macro_agents.defs.domains.sec import (
    bi,
    cik,
    cik_history,
    dashboard,
    fts,
    llm_analysis,
    manifest,
    markdown,
    metadata,
    migration,
    search,
    semantic_search,
    summary,
    text,
)
from macro_agents.defs.domains.sec.documents import (
    sec_filing_documents,
    sec_filing_documents_batch,
)
from macro_agents.defs.domains.sec.jobs import (
    sec_filing_processing_job,
    sec_filings_ingestion_job,
)
from macro_agents.defs.domains.sec.sensors import (
    sec_new_companies_sensor,
    sec_unprocessed_filings_sensor,
)
from macro_agents.defs.resources.ollama import ollama_resource
from macro_agents.defs.resources.sec_edgar import SECEdgarResource
from macro_agents.defs.domains.sec.checks import sec_checks


_static_assets = dg.load_assets_from_modules(
    [
        cik,
        cik_history,
        metadata,
        text,
        bi,
        dashboard,
        summary,
        llm_analysis,
        manifest,
        markdown,
        migration,
        search,
        fts,
        semantic_search,
    ]
)


defs = dg.Definitions(
    assets=[
        *_static_assets,
        sec_filing_documents_batch,
        sec_filing_documents,
    ],
    asset_checks=sec_checks,
    jobs=[sec_filings_ingestion_job, sec_filing_processing_job],
    sensors=[sec_unprocessed_filings_sensor, sec_new_companies_sensor],
    resources={
        "sec_edgar": SECEdgarResource(
            company_name="EconomicDataProject",
            contact_email=dg.EnvVar("SEC_EDGAR_CONTACT_EMAIL"),
        ),
        "ollama": ollama_resource,
    },
)
