"""Schema migration tests for sec_filing_content (issue #70).

The asset writes 6 columns (content_id, filing_id, section_name,
section_order, word_count, gcs_path). The legacy schema also had
content_text TEXT and created_at TIMESTAMP, which made md.upsert_data
fail its strict schema-equality check. These tests verify that
ensure_sec_filing_content_table converges any legacy schema to the new
shape without losing the indexes.
"""

import duckdb

from macro_agents.defs.domains.sec.tables import ensure_sec_filing_content_table


EXPECTED_COLUMNS = {
    "content_id",
    "filing_id",
    "section_name",
    "section_order",
    "word_count",
    "gcs_path",
}
EXPECTED_INDEXES = {
    "idx_sec_filing_content_filing_id",
    "idx_sec_filing_content_filing_section",
}


def _columns(conn: duckdb.DuckDBPyConnection) -> set[str]:
    rows = conn.execute(
        "SELECT column_name FROM duckdb_columns() "
        "WHERE table_name = 'sec_filing_content'"
    ).fetchall()
    return {r[0] for r in rows}


def _indexes(conn: duckdb.DuckDBPyConnection) -> set[str]:
    rows = conn.execute(
        "SELECT index_name FROM duckdb_indexes() "
        "WHERE table_name = 'sec_filing_content'"
    ).fetchall()
    return {r[0] for r in rows}


def test_creates_fresh_table_with_expected_shape():
    conn = duckdb.connect(":memory:")
    ensure_sec_filing_content_table(conn)
    assert _columns(conn) == EXPECTED_COLUMNS
    assert _indexes(conn) == EXPECTED_INDEXES


def test_idempotent_on_already_migrated_table():
    conn = duckdb.connect(":memory:")
    ensure_sec_filing_content_table(conn)
    ensure_sec_filing_content_table(conn)
    assert _columns(conn) == EXPECTED_COLUMNS
    assert _indexes(conn) == EXPECTED_INDEXES


def test_migrates_legacy_schema_with_content_text_and_created_at():
    conn = duckdb.connect(":memory:")
    # Recreate the pre-#70 schema exactly.
    conn.execute("""
        CREATE TABLE sec_filing_content (
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
    conn.execute(
        "CREATE INDEX idx_sec_filing_content_filing_id ON sec_filing_content(filing_id)"
    )
    conn.execute(
        "CREATE INDEX idx_sec_filing_content_filing_section "
        "ON sec_filing_content(filing_id, section_name)"
    )

    ensure_sec_filing_content_table(conn)

    assert _columns(conn) == EXPECTED_COLUMNS
    assert _indexes(conn) == EXPECTED_INDEXES


def test_migrates_partial_legacy_schema_with_only_content_text():
    """Only `content_text` present (no `created_at`). Migration should
    still drop it and converge."""
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE sec_filing_content (
            content_id VARCHAR PRIMARY KEY,
            filing_id VARCHAR NOT NULL,
            section_name VARCHAR,
            section_order INTEGER,
            content_text TEXT,
            word_count INTEGER,
            gcs_path VARCHAR
        )
    """)
    ensure_sec_filing_content_table(conn)
    assert _columns(conn) == EXPECTED_COLUMNS
