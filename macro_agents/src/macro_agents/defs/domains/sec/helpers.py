from datetime import datetime, timezone

from macro_agents.defs.domains.sec.config import PIPELINE_VERSION

SEC_FILING_COMPANIES_PARTITION_NAME = "sec_filing_companies"


def build_filing_gcs_path(
    symbol: str, form_type: str, filing_date, cik: str, accession: str
) -> str:
    """Build a consistent GCS path for a filing document.

    Path format: sec_filings/{symbol}/{form_type}/{year}/{cik}/{accession}
    """
    year = str(filing_date)[:4] if filing_date else "unknown"
    form_type_clean = form_type.replace("/", "-")
    accession_clean = accession.replace("-", "")
    return f"sec_filings/{symbol}/{form_type_clean}/{year}/{cik}/{accession_clean}"


def build_filing_metadata_envelope(
    filing_id: str,
    cik: str,
    symbol: str,
    form_type: str,
    filing_date,
    accession: str,
    primary_doc: str,
    company_name: str | None = None,
    report_date=None,
    section_count: int = 0,
) -> dict:
    """Build a standard metadata envelope for filing JSON stored in GCS."""
    return {
        "filing_id": filing_id,
        "cik": cik,
        "symbol": symbol,
        "company_name": company_name,
        "form_type": form_type,
        "filing_date": str(filing_date),
        "report_date": str(report_date) if report_date else None,
        "accession_number": accession,
        "document_name": primary_doc,
        "section_count": section_count,
        "pipeline_version": PIPELINE_VERSION,
        "downloaded_at": datetime.now(timezone.utc).isoformat(),
    }


def build_filing_markdown_gcs_path(base_gcs_path: str) -> str:
    """Build GCS path for a filing's markdown conversion.

    Args:
        base_gcs_path: Base path from build_filing_gcs_path()

    Returns:
        Path like sec_filings/{symbol}/{form_type}/{year}/{cik}/{accession}/filing.md
    """
    return f"{base_gcs_path}/filing.md"


def build_section_markdown_gcs_path(base_gcs_path: str, section_name: str) -> str:
    """Build GCS path for a section's markdown file.

    Args:
        base_gcs_path: Base path from build_filing_gcs_path()
        section_name: Sanitized section name (e.g., 'business', 'risk_factors')

    Returns:
        Path like sec_filings/.../sections/{section_name}.md
    """
    safe_name = section_name.lower().replace(" ", "_").replace("/", "_")
    return f"{base_gcs_path}/sections/{safe_name}.md"


def get_company_filing_partition_name(company_symbol: str) -> str:
    """Get the partition name for filings of a specific company."""
    return f"sec_filing_filings_{company_symbol}"
