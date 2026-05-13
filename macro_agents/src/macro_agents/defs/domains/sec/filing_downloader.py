"""Helper class for downloading SEC filing documents to GCS.

Encapsulates the shared download logic used by both the batch asset
and the dynamically-partitioned per-filing asset.
"""

from dataclasses import dataclass
from logging import Logger

import polars as pl

from macro_agents.defs.domains.sec.config import BATCH_SIZE_STANDARD
from macro_agents.defs.domains.sec.helpers import (
    build_filing_gcs_path,
    build_filing_metadata_envelope,
)
from macro_agents.defs.domains.sec.tables import ensure_sec_filings_table
from macro_agents.defs.resources.gcs import GCSResource
from macro_agents.defs.resources.sec_edgar import SECEdgarResource


@dataclass(frozen=True)
class FilingDownloadResult:
    """Result of downloading a single filing."""

    status: (
        str  # "downloaded", "error", "already_processed", "no_document", "not_found"
    )
    filing_id: str
    symbol: str
    gcs_path: str | None = None
    error: str | None = None


@dataclass(frozen=True)
class BatchDownloadResult:
    """Aggregated result of downloading multiple filings."""

    total_downloaded: int
    total_errors: int
    error_details: list[str]
    remaining: int


_FILINGS_QUERY = """
    SELECT f.filing_id, f.cik, f.symbol, f.accession_number, f.form_type,
           f.filing_date, f.report_date, f.primary_document,
           c.company_name
    FROM sec_filings f
    LEFT JOIN sec_company_cik c ON f.symbol = c.symbol
"""

_UNPROCESSED_FILTER = """
    WHERE f.processed = FALSE
    AND f.primary_document IS NOT NULL
    AND f.primary_document != ''
"""

_REMAINING_COUNT_QUERY = """
    SELECT COUNT(*)
    FROM sec_filings
    WHERE processed = FALSE
    AND primary_document IS NOT NULL
    AND primary_document != ''
"""


class FilingDownloader:
    """Downloads SEC filing documents to GCS and marks them processed in the database."""

    def __init__(self, sec_edgar: SECEdgarResource, gcs: GCSResource, log: Logger):
        self.sec_edgar = sec_edgar
        self.gcs = gcs
        self.log = log

    def download_filing(self, conn, filing_row: dict) -> FilingDownloadResult:
        """Download a single filing document to GCS and mark it processed.

        Args:
            conn: DuckDB connection
            filing_row: Dict with keys: filing_id, cik, symbol, accession_number,
                        form_type, filing_date, report_date, primary_document,
                        company_name

        Returns:
            FilingDownloadResult with status and details.
        """
        filing_id = filing_row["filing_id"]
        cik = filing_row["cik"]
        symbol = filing_row["symbol"]
        accession = filing_row["accession_number"]
        form_type = filing_row["form_type"]
        filing_date = filing_row["filing_date"]
        report_date = filing_row.get("report_date")
        primary_doc = filing_row["primary_document"]
        company_name = filing_row.get("company_name")

        if not primary_doc or primary_doc == "":
            self.log.debug(f"Filing {filing_id} has no primary document")
            return FilingDownloadResult(
                status="no_document", filing_id=filing_id, symbol=symbol
            )

        try:
            gcs_path = build_filing_gcs_path(
                symbol, form_type, filing_date, cik, accession
            )

            self.log.debug(f"Downloading {form_type} for {symbol}: {primary_doc}")

            document_content = self.sec_edgar.download_filing_document(
                cik, accession, primary_doc, context=None
            )

            doc_gcs_path = f"{gcs_path}/{primary_doc}"
            metadata_envelope = build_filing_metadata_envelope(
                filing_id=filing_id,
                cik=cik,
                symbol=symbol,
                form_type=form_type,
                filing_date=filing_date,
                accession=accession,
                primary_doc=primary_doc,
                company_name=company_name,
                report_date=report_date,
            )
            self.gcs.upload_json(
                doc_gcs_path,
                {"content": document_content, "metadata": metadata_envelope},
                context=None,
            )

            conn.execute(
                """
                UPDATE sec_filings
                SET gcs_path = ?, processed = TRUE
                WHERE symbol = ? AND filing_id = ?
                """,
                [doc_gcs_path, symbol, filing_id],
            )
            conn.commit()

            self.log.debug(f"Downloaded {form_type} for {symbol} ({filing_id})")
            return FilingDownloadResult(
                status="downloaded",
                filing_id=filing_id,
                symbol=symbol,
                gcs_path=doc_gcs_path,
            )

        except Exception as e:
            error_msg = f"Error downloading {form_type} for {symbol}: {e}"
            self.log.error(error_msg)
            return FilingDownloadResult(
                status="error",
                filing_id=filing_id,
                symbol=symbol,
                error=error_msg,
            )

    def download_batch(self, conn, filings_df: pl.DataFrame) -> BatchDownloadResult:
        """Download multiple filings, collecting results and errors.

        Args:
            conn: DuckDB connection
            filings_df: DataFrame with filing rows (same columns as query_unprocessed_filings)

        Returns:
            BatchDownloadResult with aggregated counts.
        """
        total_downloaded = 0
        total_errors = 0
        errors: list[str] = []

        for row in filings_df.iter_rows(named=True):
            result = self.download_filing(conn, row)
            if result.status == "downloaded":
                total_downloaded += 1
            elif result.status == "error":
                total_errors += 1
                if result.error:
                    errors.append(result.error)

        remaining = self.count_remaining_unprocessed(conn)

        return BatchDownloadResult(
            total_downloaded=total_downloaded,
            total_errors=total_errors,
            error_details=errors,
            remaining=remaining,
        )

    def query_unprocessed_filings(
        self, conn, *, symbol: str | None = None, limit: int = BATCH_SIZE_STANDARD
    ) -> pl.DataFrame:
        """Query sec_filings for unprocessed filings.

        Args:
            conn: DuckDB connection
            symbol: If provided, filter to this company symbol
            limit: Max number of filings to return

        Returns:
            DataFrame with filing rows ready for download_filing().
        """
        ensure_sec_filings_table(conn)

        query = _FILINGS_QUERY + _UNPROCESSED_FILTER
        params: list = []

        if symbol:
            query += " AND f.symbol = ?"
            params.append(symbol)

        query += f" LIMIT {limit}"

        return pl.read_database(
            query, connection=conn, execute_options={"parameters": params}
        )

    def query_filing_by_id(
        self, conn, filing_id: str, *, symbol: str | None = None
    ) -> pl.DataFrame:
        """Load a specific filing by ID.

        Args:
            conn: DuckDB connection
            filing_id: The filing ID to look up
            symbol: Optional symbol filter

        Returns:
            DataFrame with 0 or 1 rows.
        """
        ensure_sec_filings_table(conn)

        query = _FILINGS_QUERY + " WHERE f.filing_id = ?"
        params: list = [filing_id]

        if symbol:
            query += " AND f.symbol = ?"
            params.append(symbol)

        return pl.read_database(
            query, connection=conn, execute_options={"parameters": params}
        )

    def is_filing_processed(self, conn, filing_id: str, symbol: str) -> bool:
        """Check if a filing has already been processed."""
        result = conn.execute(
            "SELECT processed FROM sec_filings WHERE filing_id = ? AND symbol = ?",
            [filing_id, symbol],
        )
        row = result.fetchone()
        return bool(row and row[0] is True)

    def count_remaining_unprocessed(self, conn) -> int:
        """Count filings still awaiting processing."""
        row = conn.execute(_REMAINING_COUNT_QUERY).fetchone()
        return row[0] if row else 0
