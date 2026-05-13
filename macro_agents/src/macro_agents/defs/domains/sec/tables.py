"""Shared DDL helpers for SEC filing tables.

Each function creates a table if it doesn't exist. Called inline by the
asset that writes to the table, so there's no separate schema asset.
"""

import duckdb


def ensure_sec_company_cik_table(conn: duckdb.DuckDBPyConnection) -> None:
    """Create sec_company_cik table if it doesn't exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sec_company_cik (
            symbol VARCHAR PRIMARY KEY,
            cik VARCHAR NOT NULL,
            cik_padded VARCHAR,
            company_name VARCHAR,
            source VARCHAR,
            validated_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)


def ensure_sec_filings_table(conn: duckdb.DuckDBPyConnection) -> None:
    """Create sec_filings table if it doesn't exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sec_filings (
            filing_id VARCHAR NOT NULL,
            cik VARCHAR NOT NULL,
            symbol VARCHAR NOT NULL,
            accession_number VARCHAR NOT NULL,
            form_type VARCHAR NOT NULL,
            filing_date DATE,
            accepted_datetime TIMESTAMP,
            report_date DATE,
            file_number VARCHAR,
            film_number VARCHAR,
            items VARCHAR,
            size_bytes INTEGER,
            is_xbrl BOOLEAN DEFAULT FALSE,
            is_inline_xbrl BOOLEAN DEFAULT FALSE,
            primary_document VARCHAR,
            primary_doc_description VARCHAR,
            gcs_path VARCHAR,
            processed BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (symbol, filing_id)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_sec_filings_cik
        ON sec_filings(cik)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_sec_filings_form_type
        ON sec_filings(form_type)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_sec_filings_filing_date
        ON sec_filings(filing_date)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_sec_filings_processed
        ON sec_filings(processed)
    """)


def ensure_sec_company_cik_history_table(conn: duckdb.DuckDBPyConnection) -> None:
    """Create sec_company_cik_history table for tracking historical CIK mappings."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sec_company_cik_history (
            id VARCHAR PRIMARY KEY,
            current_symbol VARCHAR NOT NULL,
            cik VARCHAR NOT NULL,
            cik_padded VARCHAR,
            company_name VARCHAR,
            former_names TEXT,
            relationship_type VARCHAR,
            effective_from DATE,
            effective_to DATE,
            source VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_sec_cik_history_symbol
        ON sec_company_cik_history(current_symbol)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_sec_cik_history_cik
        ON sec_company_cik_history(cik)
    """)


def ensure_sec_filing_documents_table(conn: duckdb.DuckDBPyConnection) -> None:
    """Create sec_filing_documents table if it doesn't exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sec_filing_documents (
            document_id VARCHAR PRIMARY KEY,
            filing_id VARCHAR NOT NULL,
            sequence INTEGER,
            document_name VARCHAR,
            document_type VARCHAR,
            description VARCHAR,
            size_bytes INTEGER,
            gcs_path VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)


def ensure_sec_filing_content_table(conn: duckdb.DuckDBPyConnection) -> None:
    """Create sec_filing_content table if it doesn't exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sec_filing_content (
            content_id VARCHAR PRIMARY KEY,
            filing_id VARCHAR NOT NULL,
            section_name VARCHAR,
            section_order INTEGER,
            content_text TEXT,
            word_count INTEGER,
            gcs_path VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_sec_filing_content_filing_id
        ON sec_filing_content(filing_id)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_sec_filing_content_filing_section
        ON sec_filing_content(filing_id, section_name)
    """)


def ensure_sec_filing_llm_metadata_table(conn: duckdb.DuckDBPyConnection) -> None:
    """Create sec_filing_llm_metadata table if it doesn't exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sec_filing_llm_metadata (
            metadata_id VARCHAR PRIMARY KEY,
            filing_id VARCHAR NOT NULL,
            symbol VARCHAR NOT NULL,
            section_name VARCHAR,
            executive_summary TEXT,
            key_topics TEXT,
            sentiment VARCHAR,
            named_entities TEXT,
            financial_metrics TEXT,
            forward_looking_statements TEXT,
            risk_factors TEXT,
            embedding FLOAT[768],
            model_name VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_sec_filing_llm_metadata_filing
        ON sec_filing_llm_metadata(filing_id)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_sec_filing_llm_metadata_symbol
        ON sec_filing_llm_metadata(symbol)
    """)


def ensure_sec_filing_search_terms_table(conn: duckdb.DuckDBPyConnection) -> None:
    """Create sec_filing_search_terms table if it doesn't exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sec_filing_search_terms (
            term_id VARCHAR PRIMARY KEY,
            filing_id VARCHAR NOT NULL,
            term_category VARCHAR,
            term_text VARCHAR,
            context_text TEXT,
            section_name VARCHAR,
            confidence_score DECIMAL(3,2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_sec_filing_search_terms_category
        ON sec_filing_search_terms(term_category)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_sec_filing_search_terms_filing_id
        ON sec_filing_search_terms(filing_id)
    """)


def ensure_sec_filing_markdown_table(conn: duckdb.DuckDBPyConnection) -> None:
    """Create sec_filing_markdown table for tracking markdown conversions."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sec_filing_markdown (
            filing_id VARCHAR PRIMARY KEY,
            symbol VARCHAR NOT NULL,
            markdown_gcs_path VARCHAR,
            section_count INTEGER DEFAULT 0,
            word_count INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_sec_filing_markdown_symbol
        ON sec_filing_markdown(symbol)
    """)


def ensure_sec_filing_chunks_table(conn: duckdb.DuckDBPyConnection) -> None:
    """Create sec_filing_chunks table for chunked vector embeddings."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sec_filing_chunks (
            chunk_id VARCHAR PRIMARY KEY,
            filing_id VARCHAR NOT NULL,
            symbol VARCHAR NOT NULL,
            section_name VARCHAR,
            chunk_index INTEGER,
            chunk_text TEXT,
            word_count INTEGER,
            embedding FLOAT[768],
            model_name VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_sec_filing_chunks_filing
        ON sec_filing_chunks(filing_id)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_sec_filing_chunks_symbol
        ON sec_filing_chunks(symbol)
    """)
