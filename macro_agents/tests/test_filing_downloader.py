"""Tests for FilingDownloader helper class."""

from unittest.mock import MagicMock, patch

import polars as pl

from macro_agents.defs.domains.sec.filing_downloader import (
    BatchDownloadResult,
    FilingDownloader,
)


def _make_filing_row(**overrides):
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
    sec_edgar = MagicMock()
    gcs = MagicMock()
    bq = MagicMock()
    bq.dataset = "economics_raw_dev"
    log = MagicMock()
    return FilingDownloader(sec_edgar, gcs, bq, log), sec_edgar, gcs, bq, log


class TestDownloadFiling:
    def test_success_marks_filing_processed_with_named_parameters(self):
        downloader, sec_edgar, gcs, bq, _ = _make_downloader()
        sec_edgar.download_filing_document.return_value = "<html>content</html>"

        result = downloader.download_filing(_make_filing_row())

        assert result.status == "downloaded"
        assert result.filing_id == "fid-001"
        assert result.symbol == "AAPL"
        assert result.gcs_path is not None
        assert "AAPL" in result.gcs_path
        assert result.error is None
        sec_edgar.download_filing_document.assert_called_once()
        gcs.upload_json.assert_called_once()
        query = bq.execute_query.call_args.args[0]
        assert "gcs_path = @gcs_path" in query
        assert bq.execute_query.call_args.kwargs == {
            "read_only": False,
            "params": {
                "gcs_path": result.gcs_path,
                "symbol": "AAPL",
                "filing_id": "fid-001",
            },
        }

    def test_no_primary_document(self):
        downloader, _, _, bq, _ = _make_downloader()

        result = downloader.download_filing(_make_filing_row(primary_document=None))

        assert result.status == "no_document"
        bq.execute_query.assert_not_called()

    def test_sec_download_error(self):
        downloader, sec_edgar, _, bq, _ = _make_downloader()
        sec_edgar.download_filing_document.side_effect = RuntimeError("SEC timeout")

        result = downloader.download_filing(_make_filing_row())

        assert result.status == "error"
        assert result.error is not None
        assert "SEC timeout" in result.error
        bq.execute_query.assert_not_called()

    def test_gcs_upload_error(self):
        downloader, sec_edgar, gcs, bq, _ = _make_downloader()
        sec_edgar.download_filing_document.return_value = "<html>content</html>"
        gcs.upload_json.side_effect = RuntimeError("GCS permission denied")

        result = downloader.download_filing(_make_filing_row())

        assert result.status == "error"
        assert "GCS permission denied" in result.error
        bq.execute_query.assert_not_called()

    def test_gcs_path_format(self):
        downloader, sec_edgar, _, _, _ = _make_downloader()
        sec_edgar.download_filing_document.return_value = "<html/>"

        result = downloader.download_filing(_make_filing_row())

        expected_prefix = "sec_filings/AAPL/10-K/2024/0000320193/000032019324000008"
        assert result.gcs_path == f"{expected_prefix}/aapl-10k.htm"


class TestDownloadBatch:
    def test_all_success(self):
        downloader, sec_edgar, _, bq, _ = _make_downloader()
        sec_edgar.download_filing_document.return_value = "<html/>"
        bq.execute_query.side_effect = [
            pl.DataFrame(),
            pl.DataFrame(),
            pl.DataFrame({"count": [0]}),
        ]
        filings = pl.DataFrame(
            [
                _make_filing_row(filing_id="fid-001", symbol="AAPL"),
                _make_filing_row(filing_id="fid-002", symbol="MSFT", cik="0000789019"),
            ]
        )

        result = downloader.download_batch(filings)

        assert isinstance(result, BatchDownloadResult)
        assert result.total_downloaded == 2
        assert result.total_errors == 0
        assert result.error_details == []
        assert result.remaining == 0

    def test_mixed_results(self):
        downloader, sec_edgar, _, bq, _ = _make_downloader()
        sec_edgar.download_filing_document.side_effect = [
            "<html/>",
            RuntimeError("timeout"),
        ]
        bq.execute_query.side_effect = [pl.DataFrame(), pl.DataFrame({"count": [5]})]
        filings = pl.DataFrame(
            [
                _make_filing_row(filing_id="fid-001"),
                _make_filing_row(filing_id="fid-002"),
            ]
        )

        result = downloader.download_batch(filings)

        assert result.total_downloaded == 1
        assert result.total_errors == 1
        assert len(result.error_details) == 1
        assert result.remaining == 5


class TestQueryMethods:
    @patch("macro_agents.defs.domains.sec.filing_downloader.ensure_sec_filings_table")
    def test_query_unprocessed_uses_named_symbol_and_limit(self, mock_ensure):
        downloader, _, _, bq, _ = _make_downloader()
        conn = bq.get_connection.return_value
        bq.execute_query.return_value = pl.DataFrame([_make_filing_row()])

        result = downloader.query_unprocessed_filings(symbol="AAPL", limit=10)

        mock_ensure.assert_called_once_with(conn, "economics_raw_dev")
        conn.close.assert_called_once()
        query = bq.execute_query.call_args.args[0]
        assert "f.symbol = @symbol" in query
        assert "LIMIT @limit" in query
        assert bq.execute_query.call_args.kwargs["params"] == {
            "symbol": "AAPL",
            "limit": 10,
        }
        assert len(result) == 1

    @patch("macro_agents.defs.domains.sec.filing_downloader.ensure_sec_filings_table")
    def test_query_filing_by_id_uses_named_parameters(self, _mock_ensure):
        downloader, _, _, bq, _ = _make_downloader()
        bq.execute_query.return_value = pl.DataFrame([_make_filing_row()])

        result = downloader.query_filing_by_id("fid-001", symbol="AAPL")

        query = bq.execute_query.call_args.args[0]
        assert "f.filing_id = @filing_id" in query
        assert "f.symbol = @symbol" in query
        assert bq.execute_query.call_args.kwargs["params"] == {
            "filing_id": "fid-001",
            "symbol": "AAPL",
        }
        assert len(result) == 1


class TestStatusQueries:
    def test_is_filing_processed(self):
        downloader, _, _, bq, _ = _make_downloader()
        bq.execute_query.return_value = pl.DataFrame({"processed": [True]})

        assert downloader.is_filing_processed("fid-001", "AAPL") is True
        assert bq.execute_query.call_args.kwargs["params"] == {
            "filing_id": "fid-001",
            "symbol": "AAPL",
        }

    def test_is_filing_processed_when_not_found(self):
        downloader, _, _, bq, _ = _make_downloader()
        bq.execute_query.return_value = pl.DataFrame(
            {"processed": []}, schema={"processed": pl.Boolean}
        )

        assert downloader.is_filing_processed("fid-001", "AAPL") is False

    def test_count_remaining_unprocessed(self):
        downloader, _, _, bq, _ = _make_downloader()
        bq.execute_query.return_value = pl.DataFrame({"count": [42]})

        assert downloader.count_remaining_unprocessed() == 42
