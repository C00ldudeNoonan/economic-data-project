"""Tests for SEC GCS path and metadata helpers."""

from datetime import date

from macro_agents.defs.domains.sec.helpers import (
    PIPELINE_VERSION,
    build_filing_gcs_path,
    build_filing_metadata_envelope,
)


class TestBuildFilingGcsPath:
    def test_standard_10k(self):
        result = build_filing_gcs_path(
            symbol="AAPL",
            form_type="10-K",
            filing_date=date(2024, 12, 31),
            cik="0000320193",
            accession="0000320193-24-000008",
        )
        assert result == "sec_filings/AAPL/10-K/2024/0000320193/000032019324000008"

    def test_form_type_with_slash(self):
        result = build_filing_gcs_path(
            symbol="MSFT",
            form_type="10-K/A",
            filing_date=date(2023, 6, 15),
            cik="0000789019",
            accession="0000789019-23-000042",
        )
        assert result == "sec_filings/MSFT/10-K-A/2023/0000789019/000078901923000042"

    def test_missing_filing_date(self):
        result = build_filing_gcs_path(
            symbol="GOOG",
            form_type="10-Q",
            filing_date=None,
            cik="0001652044",
            accession="0001652044-24-000001",
        )
        assert result == "sec_filings/GOOG/10-Q/unknown/0001652044/000165204424000001"

    def test_string_filing_date(self):
        result = build_filing_gcs_path(
            symbol="AMZN",
            form_type="10-K",
            filing_date="2025-01-15",
            cik="0001018724",
            accession="0001018724-25-000001",
        )
        assert result == "sec_filings/AMZN/10-K/2025/0001018724/000101872425000001"

    def test_symbol_appears_in_path(self):
        result = build_filing_gcs_path(
            symbol="TSLA",
            form_type="10-Q",
            filing_date=date(2024, 3, 31),
            cik="0001318605",
            accession="0001318605-24-000010",
        )
        assert result.startswith("sec_filings/TSLA/")


class TestBuildFilingMetadataEnvelope:
    def test_full_metadata(self):
        result = build_filing_metadata_envelope(
            filing_id="fid-001",
            cik="0000320193",
            symbol="AAPL",
            form_type="10-K",
            filing_date=date(2024, 12, 31),
            accession="0000320193-24-000008",
            primary_doc="aapl-10k.htm",
            company_name="Apple Inc.",
            report_date=date(2024, 9, 28),
            section_count=5,
        )
        assert result["filing_id"] == "fid-001"
        assert result["cik"] == "0000320193"
        assert result["symbol"] == "AAPL"
        assert result["company_name"] == "Apple Inc."
        assert result["form_type"] == "10-K"
        assert result["filing_date"] == "2024-12-31"
        assert result["report_date"] == "2024-09-28"
        assert result["accession_number"] == "0000320193-24-000008"
        assert result["document_name"] == "aapl-10k.htm"
        assert result["section_count"] == 5
        assert result["pipeline_version"] == PIPELINE_VERSION
        assert "downloaded_at" in result

    def test_minimal_metadata(self):
        result = build_filing_metadata_envelope(
            filing_id="fid-002",
            cik="0000789019",
            symbol="MSFT",
            form_type="10-Q",
            filing_date=date(2024, 6, 30),
            accession="0000789019-24-000100",
            primary_doc="msft-10q.htm",
        )
        assert result["company_name"] is None
        assert result["report_date"] is None
        assert result["section_count"] == 0
        assert result["pipeline_version"] == PIPELINE_VERSION

    def test_pipeline_version_is_2(self):
        assert PIPELINE_VERSION == "2"
