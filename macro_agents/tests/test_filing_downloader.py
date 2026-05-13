"""Tests for FilingDownloader helper class."""

from unittest.mock import MagicMock, patch

import polars as pl

from macro_agents.defs.domains.sec.filing_downloader import (
    BatchDownloadResult,
    FilingDownloader,
)


def _make_filing_row(**overrides):
    """Create a standard filing row dict for tests."""
    defaults = {
        "filing_id": "fid-001",
        "cik": "0000320193",
        "symbol": "AAPL",
        "accession_number": "0000320193-24-000008",
        "form_type": "10-K",
        "filing_date": "2024-12-31",
        "report_date": "2024-09-28",
        "primary_document": "aapl-10k.htm",
        "company_name": "Apple Inc.",
    }
    defaults.update(overrides)
    return defaults


def _make_downloader():
    """Create a FilingDownloader with mocked resources."""
    sec_edgar = MagicMock()
    gcs = MagicMock()
    log = MagicMock()
    return FilingDownloader(sec_edgar, gcs, log), sec_edgar, gcs, log


class TestDownloadFiling:
    def test_success(self):
        downloader, sec_edgar, gcs, _ = _make_downloader()
        sec_edgar.download_filing_document.return_value = "<html>content</html>"
        conn = MagicMock()

        result = downloader.download_filing(conn, _make_filing_row())

        assert result.status == "downloaded"
        assert result.filing_id == "fid-001"
        assert result.symbol == "AAPL"
        assert result.gcs_path is not None
        assert "AAPL" in result.gcs_path
        assert result.error is None

        sec_edgar.download_filing_document.assert_called_once()
        gcs.upload_json.assert_called_once()
        conn.execute.assert_called_once()
        conn.commit.assert_called_once()

    def test_no_primary_document_empty(self):
        downloader, _, _, _ = _make_downloader()
        conn = MagicMock()

        result = downloader.download_filing(conn, _make_filing_row(primary_document=""))

        assert result.status == "no_document"
        assert result.filing_id == "fid-001"
        conn.execute.assert_not_called()

    def test_no_primary_document_none(self):
        downloader, _, _, _ = _make_downloader()
        conn = MagicMock()

        result = downloader.download_filing(
            conn, _make_filing_row(primary_document=None)
        )

        assert result.status == "no_document"

    def test_sec_download_error(self):
        downloader, sec_edgar, _, _ = _make_downloader()
        sec_edgar.download_filing_document.side_effect = RuntimeError("SEC timeout")
        conn = MagicMock()

        result = downloader.download_filing(conn, _make_filing_row())

        assert result.status == "error"
        assert result.error is not None
        assert "SEC timeout" in result.error
        conn.commit.assert_not_called()

    def test_gcs_upload_error(self):
        downloader, sec_edgar, gcs, _ = _make_downloader()
        sec_edgar.download_filing_document.return_value = "<html>content</html>"
        gcs.upload_json.side_effect = RuntimeError("GCS permission denied")
        conn = MagicMock()

        result = downloader.download_filing(conn, _make_filing_row())

        assert result.status == "error"
        assert "GCS permission denied" in result.error

    def test_gcs_path_format(self):
        downloader, sec_edgar, gcs, _ = _make_downloader()
        sec_edgar.download_filing_document.return_value = "<html/>"
        conn = MagicMock()

        result = downloader.download_filing(conn, _make_filing_row())

        expected_prefix = "sec_filings/AAPL/10-K/2024/0000320193/000032019324000008"
        assert result.gcs_path == f"{expected_prefix}/aapl-10k.htm"


class TestDownloadBatch:
    def test_all_success(self):
        downloader, sec_edgar, _, _ = _make_downloader()
        sec_edgar.download_filing_document.return_value = "<html/>"
        conn = MagicMock()
        conn.execute.return_value.fetchone.return_value = (0,)

        filings = pl.DataFrame(
            [
                _make_filing_row(filing_id="fid-001", symbol="AAPL"),
                _make_filing_row(filing_id="fid-002", symbol="MSFT", cik="0000789019"),
            ]
        )

        result = downloader.download_batch(conn, filings)

        assert isinstance(result, BatchDownloadResult)
        assert result.total_downloaded == 2
        assert result.total_errors == 0
        assert result.error_details == []

    def test_mixed_results(self):
        downloader, sec_edgar, _, _ = _make_downloader()
        sec_edgar.download_filing_document.side_effect = [
            "<html/>",
            RuntimeError("timeout"),
        ]
        conn = MagicMock()
        conn.execute.return_value.fetchone.return_value = (5,)

        filings = pl.DataFrame(
            [
                _make_filing_row(filing_id="fid-001"),
                _make_filing_row(filing_id="fid-002"),
            ]
        )

        result = downloader.download_batch(conn, filings)

        assert result.total_downloaded == 1
        assert result.total_errors == 1
        assert len(result.error_details) == 1
        assert result.remaining == 5

    def test_empty_dataframe(self):
        downloader, _, _, _ = _make_downloader()
        conn = MagicMock()
        conn.execute.return_value.fetchone.return_value = (0,)

        filings = pl.DataFrame(
            schema={
                "filing_id": pl.Utf8,
                "cik": pl.Utf8,
                "symbol": pl.Utf8,
                "accession_number": pl.Utf8,
                "form_type": pl.Utf8,
                "filing_date": pl.Utf8,
                "report_date": pl.Utf8,
                "primary_document": pl.Utf8,
                "company_name": pl.Utf8,
            }
        )

        result = downloader.download_batch(conn, filings)

        assert result.total_downloaded == 0
        assert result.total_errors == 0


class TestQueryMethods:
    @patch("macro_agents.defs.domains.sec.filing_downloader.pl.read_database")
    @patch("macro_agents.defs.domains.sec.filing_downloader.ensure_sec_filings_table")
    def test_query_unprocessed_no_symbol(self, mock_ensure, mock_read_db):
        downloader, _, _, _ = _make_downloader()
        conn = MagicMock()
        mock_read_db.return_value = pl.DataFrame([_make_filing_row()])

        result = downloader.query_unprocessed_filings(conn)

        mock_ensure.assert_called_once_with(conn)
        call_args = mock_read_db.call_args
        query = call_args[0][0]
        assert "processed = FALSE" in query
        assert "f.symbol = ?" not in query
        assert len(result) == 1

    @patch("macro_agents.defs.domains.sec.filing_downloader.pl.read_database")
    @patch("macro_agents.defs.domains.sec.filing_downloader.ensure_sec_filings_table")
    def test_query_unprocessed_with_symbol(self, mock_ensure, mock_read_db):
        downloader, _, _, _ = _make_downloader()
        conn = MagicMock()
        mock_read_db.return_value = pl.DataFrame([_make_filing_row()])

        downloader.query_unprocessed_filings(conn, symbol="AAPL")

        call_args = mock_read_db.call_args
        query = call_args[0][0]
        assert "f.symbol = ?" in query
        params = call_args[1]["execute_options"]["parameters"]
        assert "AAPL" in params

    @patch("macro_agents.defs.domains.sec.filing_downloader.pl.read_database")
    @patch("macro_agents.defs.domains.sec.filing_downloader.ensure_sec_filings_table")
    def test_query_filing_by_id(self, mock_ensure, mock_read_db):
        downloader, _, _, _ = _make_downloader()
        conn = MagicMock()
        mock_read_db.return_value = pl.DataFrame([_make_filing_row()])

        result = downloader.query_filing_by_id(conn, "fid-001")

        call_args = mock_read_db.call_args
        query = call_args[0][0]
        assert "f.filing_id = ?" in query
        assert "f.symbol = ?" not in query
        assert len(result) == 1

    @patch("macro_agents.defs.domains.sec.filing_downloader.pl.read_database")
    @patch("macro_agents.defs.domains.sec.filing_downloader.ensure_sec_filings_table")
    def test_query_filing_by_id_with_symbol(self, mock_ensure, mock_read_db):
        downloader, _, _, _ = _make_downloader()
        conn = MagicMock()
        mock_read_db.return_value = pl.DataFrame([_make_filing_row()])

        downloader.query_filing_by_id(conn, "fid-001", symbol="AAPL")

        call_args = mock_read_db.call_args
        query = call_args[0][0]
        assert "f.filing_id = ?" in query
        assert "f.symbol = ?" in query


class TestIsFilingProcessed:
    def test_processed_true(self):
        downloader, _, _, _ = _make_downloader()
        conn = MagicMock()
        conn.execute.return_value.fetchone.return_value = (True,)

        assert downloader.is_filing_processed(conn, "fid-001", "AAPL") is True

    def test_processed_false(self):
        downloader, _, _, _ = _make_downloader()
        conn = MagicMock()
        conn.execute.return_value.fetchone.return_value = (False,)

        assert downloader.is_filing_processed(conn, "fid-001", "AAPL") is False

    def test_not_found(self):
        downloader, _, _, _ = _make_downloader()
        conn = MagicMock()
        conn.execute.return_value.fetchone.return_value = None

        assert downloader.is_filing_processed(conn, "fid-001", "AAPL") is False


class TestCountRemainingUnprocessed:
    def test_some_remaining(self):
        downloader, _, _, _ = _make_downloader()
        conn = MagicMock()
        conn.execute.return_value.fetchone.return_value = (42,)

        assert downloader.count_remaining_unprocessed(conn) == 42

    def test_none_remaining(self):
        downloader, _, _, _ = _make_downloader()
        conn = MagicMock()
        conn.execute.return_value.fetchone.return_value = (0,)

        assert downloader.count_remaining_unprocessed(conn) == 0
