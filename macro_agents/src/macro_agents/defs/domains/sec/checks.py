from datetime import date, timedelta

import dagster as dg

from macro_agents.defs.resources.bigquery_warehouse import BigQueryWarehouseResource


@dg.asset_check(asset="sp500_cik_enriched")
def sec_cik_data_check(md: BigQueryWarehouseResource) -> dg.AssetCheckResult:
    """Validate SEC CIK mapping has companies with valid CIK codes."""
    if not md.table_exists("sec_company_cik"):
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.ERROR,
            metadata={"error": "sec_company_cik table does not exist"},
        )

    df = md.execute_query(
        """
        SELECT
            COUNT(*) AS row_count,
            SUM(CASE WHEN cik IS NULL OR cik = 0 THEN 1 ELSE 0 END) AS invalid_cik,
            COUNT(DISTINCT symbol) AS symbol_count
        FROM sec_company_cik
        """,
        read_only=True,
    )

    if df.is_empty() or df["row_count"][0] == 0:
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.ERROR,
            metadata={"error": "sec_company_cik table is empty"},
        )

    row_count = int(df["row_count"][0])
    invalid_cik = int(df["invalid_cik"][0])
    symbol_count = int(df["symbol_count"][0])

    return dg.AssetCheckResult(
        passed=invalid_cik == 0 and symbol_count > 400,
        severity=dg.AssetCheckSeverity.WARN,
        metadata={
            "row_count": row_count,
            "symbol_count": symbol_count,
            "invalid_cik_count": invalid_cik,
        },
    )


@dg.asset_check(asset="sec_filing_metadata")
def sec_filing_metadata_check(md: BigQueryWarehouseResource) -> dg.AssetCheckResult:
    """Validate SEC filing metadata has recent filings."""
    if not md.table_exists("sec_filings"):
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.ERROR,
            metadata={"error": "sec_filings table does not exist"},
        )

    cutoff = date.today() - timedelta(days=180)
    df = md.execute_query(
        f"""
        SELECT
            COUNT(*) AS total_filings,
            COUNT(DISTINCT cik) AS company_count,
            MAX(filing_date) AS latest_filing,
            SUM(CASE WHEN CAST(filing_date AS DATE) >= '{cutoff}' THEN 1 ELSE 0 END) AS recent_filings
        FROM sec_filings
        """,
        read_only=True,
    )

    if df.is_empty() or df["total_filings"][0] == 0:
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.ERROR,
            metadata={"error": "sec_filings table is empty"},
        )

    return dg.AssetCheckResult(
        passed=int(df["recent_filings"][0]) > 0,
        severity=dg.AssetCheckSeverity.WARN,
        metadata={
            "total_filings": int(df["total_filings"][0]),
            "company_count": int(df["company_count"][0]),
            "latest_filing": str(df["latest_filing"][0]),
            "recent_filings_6mo": int(df["recent_filings"][0]),
        },
    )


@dg.asset_check(asset="sec_filing_text_extracted")
def sec_text_extracted_check(md: BigQueryWarehouseResource) -> dg.AssetCheckResult:
    """Validate SEC filing text extraction has content."""
    if not md.table_exists("sec_filing_content"):
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.ERROR,
            metadata={"error": "sec_filing_content table does not exist"},
        )

    df = md.execute_query(
        """
        SELECT
            COUNT(*) AS row_count,
            SUM(CASE WHEN content IS NULL OR content = '' THEN 1 ELSE 0 END) AS empty_content
        FROM sec_filing_content
        """,
        read_only=True,
    )

    if df.is_empty() or df["row_count"][0] == 0:
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.WARN,
            metadata={"error": "sec_filing_content table is empty"},
        )

    row_count = int(df["row_count"][0])
    empty_content = int(df["empty_content"][0])

    return dg.AssetCheckResult(
        passed=empty_content < row_count * 0.1,
        severity=dg.AssetCheckSeverity.WARN,
        metadata={
            "row_count": row_count,
            "empty_content_count": empty_content,
        },
    )


@dg.asset_check(asset="sec_filing_business_intelligence")
def sec_bi_data_check(md: BigQueryWarehouseResource) -> dg.AssetCheckResult:
    """Validate SEC business intelligence search terms table."""
    if not md.table_exists("sec_filing_search_terms"):
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.ERROR,
            metadata={"error": "sec_filing_search_terms table does not exist"},
        )

    df = md.execute_query(
        "SELECT COUNT(*) AS row_count FROM sec_filing_search_terms",
        read_only=True,
    )

    row_count = int(df["row_count"][0]) if not df.is_empty() else 0
    return dg.AssetCheckResult(
        passed=row_count > 0,
        severity=dg.AssetCheckSeverity.WARN,
        metadata={"row_count": row_count},
    )


@dg.asset_check(asset="sec_filing_gcs_catalog")
def sec_gcs_catalog_check(md: BigQueryWarehouseResource) -> dg.AssetCheckResult:
    """Validate SEC GCS catalog is populated."""
    if not md.table_exists("sec_filing_gcs_catalog"):
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.ERROR,
            metadata={"error": "sec_filing_gcs_catalog table does not exist"},
        )

    df = md.execute_query(
        """
        SELECT
            COUNT(*) AS row_count,
            COUNT(DISTINCT company_symbol) AS company_count
        FROM sec_filing_gcs_catalog
        """,
        read_only=True,
    )

    row_count = int(df["row_count"][0]) if not df.is_empty() else 0
    return dg.AssetCheckResult(
        passed=row_count > 0,
        severity=dg.AssetCheckSeverity.WARN,
        metadata={
            "row_count": row_count,
            "company_count": int(df["company_count"][0]) if row_count > 0 else 0,
        },
    )


sec_checks = [
    sec_cik_data_check,
    sec_filing_metadata_check,
    sec_text_extracted_check,
    sec_bi_data_check,
    sec_gcs_catalog_check,
]
