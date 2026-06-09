"""Shared DDL helpers for SEC filing tables.

Each function creates a table if it doesn't exist. Called inline by the
asset that writes to the table, so there's no separate schema asset.
"""

from google.cloud import bigquery


def ensure_sec_company_cik_table(
    conn: bigquery.Client, dataset: str = "economics_raw"
) -> None:
    """Create sec_company_cik table if it doesn't exist."""
    project = conn.project
    conn.query(f"""
        CREATE TABLE IF NOT EXISTS `{project}.{dataset}.sec_company_cik` (
            symbol STRING,
            cik STRING NOT NULL,
            cik_padded STRING,
            company_name STRING,
            source STRING,
            validated_at TIMESTAMP,
            created_at TIMESTAMP
        )
    """).result()


def ensure_sec_filings_table(
    conn: bigquery.Client, dataset: str = "economics_raw"
) -> None:
    """Create sec_filings table if it doesn't exist."""
    project = conn.project
    conn.query(f"""
        CREATE TABLE IF NOT EXISTS `{project}.{dataset}.sec_filings` (
            filing_id STRING NOT NULL,
            cik STRING NOT NULL,
            symbol STRING NOT NULL,
            accession_number STRING NOT NULL,
            form_type STRING NOT NULL,
            filing_date DATE,
            accepted_datetime TIMESTAMP,
            report_date DATE,
            file_number STRING,
            film_number STRING,
            items STRING,
            size_bytes INT64,
            is_xbrl BOOL,
            is_inline_xbrl BOOL,
            primary_document STRING,
            primary_doc_description STRING,
            gcs_path STRING,
            processed BOOL,
            created_at TIMESTAMP
        )
    """).result()


def ensure_sec_company_cik_history_table(
    conn: bigquery.Client, dataset: str = "economics_raw"
) -> None:
    """Create sec_company_cik_history table for tracking historical CIK mappings."""
    project = conn.project
    conn.query(f"""
        CREATE TABLE IF NOT EXISTS `{project}.{dataset}.sec_company_cik_history` (
            id STRING,
            current_symbol STRING NOT NULL,
            cik STRING NOT NULL,
            cik_padded STRING,
            company_name STRING,
            former_names STRING,
            relationship_type STRING,
            effective_from DATE,
            effective_to DATE,
            source STRING,
            created_at TIMESTAMP
        )
    """).result()


def ensure_sec_filing_documents_table(
    conn: bigquery.Client, dataset: str = "economics_raw"
) -> None:
    """Create sec_filing_documents table if it doesn't exist."""
    project = conn.project
    conn.query(f"""
        CREATE TABLE IF NOT EXISTS `{project}.{dataset}.sec_filing_documents` (
            document_id STRING,
            filing_id STRING NOT NULL,
            sequence INT64,
            document_name STRING,
            document_type STRING,
            description STRING,
            size_bytes INT64,
            gcs_path STRING,
            created_at TIMESTAMP
        )
    """).result()


def ensure_sec_filing_content_table(
    conn: bigquery.Client, dataset: str = "economics_raw"
) -> None:
    """Create sec_filing_content table if it doesn't exist.

    Issue #70: this table used to have `content_text` and `created_at`.
    Section text moved to GCS; the legacy columns broke the bulk upsert.
    This migration drops them if present using INFORMATION_SCHEMA.
    """
    project = conn.project
    conn.query(f"""
        CREATE TABLE IF NOT EXISTS `{project}.{dataset}.sec_filing_content` (
            content_id STRING,
            filing_id STRING NOT NULL,
            section_name STRING,
            section_order INT64,
            word_count INT64,
            gcs_path STRING
        )
    """).result()

    legacy_row = (
        conn.query(f"""
            SELECT COUNT(*) AS cnt
            FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = 'sec_filing_content'
            AND column_name IN ('content_text', 'created_at')
        """)
        .result()
        .to_dataframe()
    )
    legacy_columns_present = (
        int(legacy_row["cnt"].iloc[0]) > 0 if len(legacy_row) else False
    )

    if legacy_columns_present:
        conn.query(
            f"ALTER TABLE `{project}.{dataset}.sec_filing_content` DROP COLUMN IF EXISTS content_text"
        ).result()
        conn.query(
            f"ALTER TABLE `{project}.{dataset}.sec_filing_content` DROP COLUMN IF EXISTS created_at"
        ).result()


def ensure_sec_filing_llm_metadata_table(
    conn: bigquery.Client, dataset: str = "economics_raw"
) -> None:
    """Create sec_filing_llm_metadata table if it doesn't exist."""
    project = conn.project
    conn.query(f"""
        CREATE TABLE IF NOT EXISTS `{project}.{dataset}.sec_filing_llm_metadata` (
            metadata_id STRING,
            filing_id STRING NOT NULL,
            symbol STRING NOT NULL,
            section_name STRING,
            executive_summary STRING,
            key_topics STRING,
            sentiment STRING,
            named_entities STRING,
            financial_metrics STRING,
            forward_looking_statements STRING,
            risk_factors STRING,
            embedding ARRAY<FLOAT64>,
            model_name STRING,
            created_at TIMESTAMP
        )
    """).result()


def ensure_sec_filing_search_terms_table(
    conn: bigquery.Client, dataset: str = "economics_raw"
) -> None:
    """Create sec_filing_search_terms table if it doesn't exist."""
    project = conn.project
    conn.query(f"""
        CREATE TABLE IF NOT EXISTS `{project}.{dataset}.sec_filing_search_terms` (
            term_id STRING,
            filing_id STRING NOT NULL,
            term_category STRING,
            term_text STRING,
            context_text STRING,
            section_name STRING,
            confidence_score NUMERIC,
            created_at TIMESTAMP
        )
    """).result()


def ensure_sec_filing_markdown_table(
    conn: bigquery.Client, dataset: str = "economics_raw"
) -> None:
    """Create sec_filing_markdown table for tracking markdown conversions."""
    project = conn.project
    conn.query(f"""
        CREATE TABLE IF NOT EXISTS `{project}.{dataset}.sec_filing_markdown` (
            filing_id STRING,
            symbol STRING NOT NULL,
            markdown_gcs_path STRING,
            section_count INT64,
            word_count INT64,
            created_at TIMESTAMP
        )
    """).result()


def ensure_sec_filing_chunks_table(
    conn: bigquery.Client, dataset: str = "economics_raw"
) -> None:
    """Create sec_filing_chunks table for chunked vector embeddings."""
    project = conn.project
    conn.query(f"""
        CREATE TABLE IF NOT EXISTS `{project}.{dataset}.sec_filing_chunks` (
            chunk_id STRING,
            filing_id STRING NOT NULL,
            symbol STRING NOT NULL,
            section_name STRING,
            chunk_index INT64,
            chunk_text STRING,
            word_count INT64,
            embedding ARRAY<FLOAT64>,
            model_name STRING,
            created_at TIMESTAMP
        )
    """).result()
