"""SEC EDGAR direct API resource with rate limiting."""

import random
import re
import time
from collections.abc import Callable
from datetime import datetime, timezone

import dagster as dg
import polars as pl
import requests
from typing import Any
from pydantic import Field
from tenacity import retry, stop_after_attempt, wait_exponential

from macro_agents.defs.resources.bigquery_warehouse import BigQueryWarehouseResource


class SECEdgarResource(dg.ConfigurableResource):
    """
    Resource for accessing SEC EDGAR data directly.

    SEC EDGAR allows 10 requests per second. This resource implements
    conservative rate limiting to stay well under that limit.

    Requires a User-Agent header with company name and contact email
    as per SEC guidelines.
    """

    # Rate limiting - SEC allows 10 req/sec, we stay conservative
    min_delay: float = Field(
        default=0.15, description="Minimum delay between requests (seconds)"
    )
    max_delay: float = Field(
        default=0.3, description="Maximum delay between requests (seconds)"
    )

    # User agent identification (required by SEC)
    company_name: str = Field(
        default="EconomicDataProject",
        description="Company/project name for User-Agent header",
    )
    contact_email: str = Field(
        default="admin@example.com",
        description="Contact email for User-Agent header",
    )

    # Base URLs
    submissions_base_url: str = Field(
        default="https://data.sec.gov/submissions",
        description="Base URL for company submissions",
    )
    archives_base_url: str = Field(
        default="https://www.sec.gov/Archives/edgar/data",
        description="Base URL for filing archives",
    )

    def _random_delay(self) -> None:
        """Introduce random delay between requests."""
        delay = random.uniform(self.min_delay, self.max_delay)
        time.sleep(delay)

    def _get_headers(self) -> dict[str, str]:
        """Get headers with required User-Agent for SEC."""
        return {
            "User-Agent": f"{self.company_name} {self.contact_email}",
            "Accept": "application/json, text/html, */*",
            "Accept-Encoding": "gzip, deflate",
        }

    def _pad_cik(self, cik: str) -> str:
        """Pad CIK to 10 digits as required by SEC."""
        # Remove any non-numeric characters
        cik_clean = re.sub(r"\D", "", str(cik))
        return cik_clean.zfill(10)

    def _format_accession_number(self, accession: str, with_dashes: bool = True) -> str:
        """Format accession number with or without dashes."""
        # Remove existing dashes
        clean = accession.replace("-", "")
        if with_dashes:
            # Format as XXXXXXXXXX-XX-XXXXXX
            return f"{clean[:10]}-{clean[10:12]}-{clean[12:]}"
        return clean

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
    def get_company_submissions(
        self, cik: str, context: dg.AssetExecutionContext | None = None
    ) -> dict[str, Any]:
        """
        Get all SEC submissions for a company.

        Args:
            cik: SEC Central Index Key (will be padded to 10 digits)
            context: Optional Dagster context for logging

        Returns:
            Dict containing company info and all filings
        """
        self._random_delay()

        cik_padded = self._pad_cik(cik)
        url = f"{self.submissions_base_url}/CIK{cik_padded}.json"

        if context:
            context.log.debug(f"Fetching submissions for CIK {cik_padded}")

        response = requests.get(url, headers=self._get_headers(), timeout=30)
        response.raise_for_status()

        return response.json()

    def get_company_filings(
        self,
        cik: str,
        form_types: list[str] | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        context: dg.AssetExecutionContext | None = None,
    ) -> pl.DataFrame:
        """
        Get filtered filings for a company as a DataFrame.

        Args:
            cik: SEC Central Index Key
            form_types: Filter by form types (e.g., ["10-K", "10-Q"])
            start_date: Filter filings after this date (YYYY-MM-DD)
            end_date: Filter filings before this date (YYYY-MM-DD)
            context: Optional Dagster context for logging

        Returns:
            Polars DataFrame with filing metadata
        """
        submissions = self.get_company_submissions(cik, context)

        # Extract company info
        company_name = submissions.get("name", "")
        cik_padded = self._pad_cik(cik)

        # Get recent filings from the main response
        recent_filings = submissions.get("filings", {}).get("recent", {})

        if not recent_filings:
            if context:
                context.log.warning(f"No recent filings found for CIK {cik}")
            return pl.DataFrame()

        # Build records from parallel arrays
        accession_numbers = recent_filings.get("accessionNumber", [])
        filing_dates = recent_filings.get("filingDate", [])
        report_dates = recent_filings.get("reportDate", [])
        form_types_list = recent_filings.get("form", [])
        primary_documents = recent_filings.get("primaryDocument", [])
        primary_doc_descriptions = recent_filings.get("primaryDocDescription", [])
        file_numbers = recent_filings.get("fileNumber", [])
        film_numbers = recent_filings.get("filmNumber", [])
        items = recent_filings.get("items", [])
        sizes = recent_filings.get("size", [])
        is_xbrl_list = recent_filings.get("isXBRL", [])
        is_inline_xbrl_list = recent_filings.get("isInlineXBRL", [])
        accepted_datetimes = recent_filings.get("acceptanceDateTime", [])

        records = []
        for i in range(len(accession_numbers)):
            form_type = form_types_list[i] if i < len(form_types_list) else ""

            # Apply form type filter
            if form_types and form_type not in form_types:
                continue

            filing_date = filing_dates[i] if i < len(filing_dates) else ""

            # Apply date filters
            if start_date and filing_date < start_date:
                continue
            if end_date and filing_date > end_date:
                continue

            accession = accession_numbers[i]
            accession_no_dashes = self._format_accession_number(accession, False)

            records.append(
                {
                    "cik": cik_padded,
                    "company_name": company_name,
                    "accession_number": accession,
                    "accession_number_clean": accession_no_dashes,
                    "form_type": form_type,
                    "filing_date": filing_date,
                    "report_date": report_dates[i] if i < len(report_dates) else "",
                    "accepted_datetime": (
                        accepted_datetimes[i] if i < len(accepted_datetimes) else ""
                    ),
                    "primary_document": (
                        primary_documents[i] if i < len(primary_documents) else ""
                    ),
                    "primary_doc_description": (
                        primary_doc_descriptions[i]
                        if i < len(primary_doc_descriptions)
                        else ""
                    ),
                    "file_number": file_numbers[i] if i < len(file_numbers) else "",
                    "film_number": film_numbers[i] if i < len(film_numbers) else "",
                    "items": items[i] if i < len(items) else "",
                    "size_bytes": sizes[i] if i < len(sizes) else 0,
                    "is_xbrl": is_xbrl_list[i] if i < len(is_xbrl_list) else False,
                    "is_inline_xbrl": (
                        is_inline_xbrl_list[i]
                        if i < len(is_inline_xbrl_list)
                        else False
                    ),
                }
            )

        if context:
            context.log.debug(
                f"Found {len(records)} filings for CIK {cik_padded}"
                f"{f' (filtered to {form_types})' if form_types else ''}"
            )

        return pl.DataFrame(records) if records else pl.DataFrame()

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
    def get_filing_index(
        self,
        cik: str,
        accession_number: str,
        context: dg.AssetExecutionContext | None = None,
    ) -> dict[str, Any]:
        """
        Get the index of documents in a filing.

        Args:
            cik: SEC Central Index Key
            accession_number: Filing accession number
            context: Optional Dagster context for logging

        Returns:
            Dict containing list of documents in the filing
        """
        self._random_delay()

        cik_padded = self._pad_cik(cik)
        accession_clean = self._format_accession_number(accession_number, False)
        url = f"{self.archives_base_url}/{cik_padded}/{accession_clean}/index.json"

        if context:
            context.log.debug(f"Fetching filing index: {url}")

        response = requests.get(url, headers=self._get_headers(), timeout=30)
        response.raise_for_status()

        return response.json()

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
    def download_filing_document(
        self,
        cik: str,
        accession_number: str,
        document_name: str,
        context: dg.AssetExecutionContext | None = None,
    ) -> str:
        """
        Download a specific document from a filing.

        Args:
            cik: SEC Central Index Key
            accession_number: Filing accession number
            document_name: Name of the document to download
            context: Optional Dagster context for logging

        Returns:
            Document content as string
        """
        self._random_delay()

        cik_padded = self._pad_cik(cik)
        accession_clean = self._format_accession_number(accession_number, False)
        url = f"{self.archives_base_url}/{cik_padded}/{accession_clean}/{document_name}"

        if context:
            context.log.debug(f"Downloading document: {url}")

        response = requests.get(url, headers=self._get_headers(), timeout=60)
        response.raise_for_status()

        return response.text

    def get_10k_10q_filings(
        self,
        cik: str,
        years_back: int = 20,
        context: dg.AssetExecutionContext | None = None,
    ) -> pl.DataFrame:
        """
        Get 10-K and 10-Q filings for a company.

        Args:
            cik: SEC Central Index Key
            years_back: Number of years of filings to retrieve
            context: Optional Dagster context for logging

        Returns:
            Polars DataFrame with 10-K and 10-Q filing metadata
        """
        # Calculate date range
        end_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        start_year = datetime.now(timezone.utc).year - years_back
        start_date = f"{start_year}-01-01"

        return self.get_company_filings(
            cik=cik,
            form_types=["10-K", "10-Q", "10-K/A", "10-Q/A"],
            start_date=start_date,
            end_date=end_date,
            context=context,
        )

    def build_document_url(
        self, cik: str, accession_number: str, document_name: str
    ) -> str:
        """
        Build the full URL for a filing document.

        Args:
            cik: SEC Central Index Key
            accession_number: Filing accession number
            document_name: Name of the document

        Returns:
            Full URL to the document
        """
        cik_padded = self._pad_cik(cik)
        accession_clean = self._format_accession_number(accession_number, False)
        return (
            f"{self.archives_base_url}/{cik_padded}/{accession_clean}/{document_name}"
        )

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
    def get_company_tickers_mapping(
        self, context: dg.AssetExecutionContext | None = None
    ) -> dict[str, dict]:
        """
        Get the SEC's official ticker-to-CIK mapping.

        Downloads the company_tickers.json file from SEC which contains
        all registered company tickers and their CIK codes.

        Args:
            context: Optional Dagster context for logging

        Returns:
            Dict mapping ticker symbols to company info including CIK
        """
        self._random_delay()

        url = "https://www.sec.gov/files/company_tickers.json"

        if context:
            context.log.debug("Fetching SEC company tickers mapping")

        response = requests.get(url, headers=self._get_headers(), timeout=30)
        response.raise_for_status()

        data = response.json()

        # Convert from indexed format to ticker-keyed dict
        ticker_mapping = {}
        for entry in data.values():
            ticker = entry.get("ticker", "").upper()
            if ticker:
                ticker_mapping[ticker] = {
                    "cik": str(entry.get("cik_str", "")),
                    "company_name": entry.get("title", ""),
                }

        if context:
            context.log.debug(
                f"Loaded {len(ticker_mapping)} ticker-CIK mappings from SEC"
            )

        return ticker_mapping

    def lookup_cik_by_ticker(
        self,
        ticker: str,
        ticker_mapping: dict[str, dict] | None = None,
        context: dg.AssetExecutionContext | None = None,
    ) -> dict | None:
        """
        Look up CIK for a ticker symbol.

        Args:
            ticker: Stock ticker symbol
            ticker_mapping: Pre-loaded mapping (optional, will fetch if not provided)
            context: Optional Dagster context for logging

        Returns:
            Dict with cik and company_name, or None if not found
        """
        if ticker_mapping is None:
            ticker_mapping = self.get_company_tickers_mapping(context)

        ticker_upper = ticker.upper()
        return ticker_mapping.get(ticker_upper)

    def sync_filing_partitions_from_dataframe(
        self,
        context: dg.AssetExecutionContext,
        filings_df: pl.DataFrame,
        get_company_partition_name_fn: Callable[[str], str],
    ) -> None:
        """
        Sync dynamic partitions for filings grouped by company.

        Creates company-specific partition definitions and adds filing partitions
        to each company's partition set.
        """
        if filings_df.is_empty():
            context.log.debug("No filings to create partitions for")
            return

        instance = context.instance

        # Group filings by company
        companies = filings_df.group_by("symbol").agg(
            pl.col("filing_id").unique().alias("filing_ids")
        )

        total_companies = 0
        total_filings = 0

        for row in companies.iter_rows(named=True):
            company_symbol = row["symbol"]
            if not company_symbol:
                continue

            filing_ids = row["filing_ids"]
            if not filing_ids or len(filing_ids) == 0:
                continue

            partition_name = get_company_partition_name_fn(company_symbol)

            # Get existing partitions for this company
            try:
                existing_filings = set(instance.get_dynamic_partitions(partition_name))
            except Exception as exc:
                context.log.warning(
                    f"Could not read existing filing partitions for {company_symbol}: {exc}"
                )
                existing_filings = set()

            # Add new filing partitions for this company
            new_filings = [
                filing_id
                for filing_id in filing_ids
                if filing_id not in existing_filings
            ]

            if new_filings:
                instance.add_dynamic_partitions(partition_name, new_filings)
                context.log.debug(
                    f"Added {len(new_filings)} filing partitions for {company_symbol} "
                    f"(total: {len(existing_filings) + len(new_filings)})"
                )
                total_filings += len(new_filings)
                total_companies += 1

        context.log.debug(
            f"Synced partitions for {total_companies} companies, "
            f"added {total_filings} total filing partitions"
        )

    def sync_filing_partitions_from_database(
        self,
        context: dg.AssetExecutionContext,
        bq: BigQueryWarehouseResource,
        get_company_partition_name_fn: Callable[[str], str],
    ) -> None:
        """Sync dynamic partitions for companies and filings from sec_filings table."""
        conn = None
        try:
            conn = bq.get_connection()

            # Get all filings with primary_document
            filings_df = bq.execute_query("""
                SELECT DISTINCT symbol, filing_id
                FROM sec_filings
                WHERE primary_document IS NOT NULL
                AND primary_document != ''
                AND symbol IS NOT NULL
                AND filing_id IS NOT NULL
                """)

            if filings_df.is_empty():
                context.log.debug("No filings to create partitions for")
                return

            self.sync_filing_partitions_from_dataframe(
                context, filings_df, get_company_partition_name_fn
            )

        finally:
            if conn:
                conn.close()

    def extract_company_history(self, submissions: dict[str, Any]) -> dict[str, Any]:
        """Extract former names and tickers from SEC submissions response.

        Args:
            submissions: Response from get_company_submissions()

        Returns:
            Dict with company_name, cik, tickers, former_names (list of
            dicts with name, from_date, to_date)
        """
        former_names = submissions.get("formerNames", [])
        parsed_former_names = []
        for entry in former_names:
            parsed_former_names.append(
                {
                    "name": entry.get("name", ""),
                    "from_date": entry.get("from", ""),
                    "to_date": entry.get("to", ""),
                }
            )

        tickers = submissions.get("tickers", [])
        return {
            "company_name": submissions.get("name", ""),
            "cik": submissions.get("cik", ""),
            "tickers": tickers if isinstance(tickers, list) else [],
            "former_names": parsed_former_names,
        }

    def generate_filing_id(self, cik: str, accession_number: str) -> str:
        """Generate a unique filing ID from CIK and accession number."""
        import hashlib

        hash_input = f"{cik}_{accession_number}"
        return hashlib.sha256(hash_input.encode()).hexdigest()[:16]

    def generate_content_id(self, filing_id: str, section_name: str) -> str:
        """Generate a unique content ID from filing ID and section name."""
        import hashlib

        hash_input = f"{filing_id}_{section_name}"
        return hashlib.sha256(hash_input.encode()).hexdigest()[:16]

    def generate_term_id(self, filing_id: str, category: str, position: int) -> str:
        """Generate a unique term ID from filing ID, category, and position."""
        import hashlib

        hash_input = f"{filing_id}_{category}_{position}"
        return hashlib.sha256(hash_input.encode()).hexdigest()[:16]
