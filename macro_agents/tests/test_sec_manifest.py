"""Tests for SEC filing GCS manifest generation."""

from macro_agents.defs.domains.sec.helpers import PIPELINE_VERSION
from macro_agents.defs.domains.sec.manifest import build_company_manifest


def test_manifest_structure_with_filings():
    filings = [
        {
            "filing_id": "fid-001",
            "form_type": "10-K",
            "filing_date": "2024-12-31",
            "gcs_path": "sec_filings/AAPL/10-K/2024/0000320193/000032019324000008/aapl-10k.htm",
            "processed": True,
            "section_count": 5,
        },
        {
            "filing_id": "fid-002",
            "form_type": "10-Q",
            "filing_date": "2024-06-30",
            "gcs_path": "sec_filings/AAPL/10-Q/2024/0000320193/000032019324000099/aapl-10q.htm",
            "processed": True,
            "section_count": 3,
        },
    ]

    manifest = build_company_manifest("AAPL", "Apple Inc.", "0000320193", filings)

    assert manifest["symbol"] == "AAPL"
    assert manifest["company_name"] == "Apple Inc."
    assert manifest["cik"] == "0000320193"
    assert manifest["pipeline_version"] == PIPELINE_VERSION
    assert manifest["filing_count"] == 2
    assert len(manifest["filings"]) == 2
    assert manifest["filings"][0]["filing_id"] == "fid-001"
    assert manifest["filings"][0]["form_type"] == "10-K"
    assert manifest["filings"][0]["processed"] is True
    assert manifest["filings"][0]["section_count"] == 5
    assert (
        manifest["filings"][0]["base_gcs_path"]
        == "sec_filings/AAPL/10-K/2024/0000320193/000032019324000008"
    )
    assert manifest["filings"][1]["filing_id"] == "fid-002"
    assert manifest["filings"][1]["section_count"] == 3
    assert "generated_at" in manifest


def test_manifest_empty_filings():
    manifest = build_company_manifest("NEWCO", "New Company Inc.", "0001234567", [])

    assert manifest["symbol"] == "NEWCO"
    assert manifest["company_name"] == "New Company Inc."
    assert manifest["filing_count"] == 0
    assert manifest["filings"] == []
    assert manifest["pipeline_version"] == PIPELINE_VERSION


def test_manifest_null_filing_date():
    filings = [
        {
            "filing_id": "fid-003",
            "form_type": "10-K",
            "filing_date": None,
            "gcs_path": None,
            "processed": False,
            "section_count": 0,
        },
    ]

    manifest = build_company_manifest("TEST", "Test Corp", "0009999999", filings)

    assert manifest["filings"][0]["filing_date"] is None
    assert manifest["filings"][0]["processed"] is False
    assert manifest["filings"][0]["section_count"] == 0


def test_manifest_null_section_count():
    filings = [
        {
            "filing_id": "fid-004",
            "form_type": "10-Q",
            "filing_date": "2025-03-01",
            "gcs_path": "sec_filings/XYZ/10-Q/2025/123/456/doc.htm",
            "processed": True,
            "section_count": None,
        },
    ]

    manifest = build_company_manifest("XYZ", "XYZ Inc.", "0001111111", filings)

    assert manifest["filings"][0]["section_count"] == 0
