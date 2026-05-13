"""Tests for SEC filing LLM metadata table DDL."""

import duckdb
import pytest

from macro_agents.defs.domains.sec.tables import ensure_sec_filing_llm_metadata_table


class TestSecFilingLLMMetadataTable:
    """Test cases for ensure_sec_filing_llm_metadata_table."""

    @pytest.fixture
    def conn(self):
        """Create an in-memory DuckDB connection."""
        conn = duckdb.connect(":memory:")
        yield conn
        conn.close()

    def test_table_created(self, conn):
        """Table should be created successfully."""
        ensure_sec_filing_llm_metadata_table(conn)

        tables = conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_name = 'sec_filing_llm_metadata'"
        ).fetchall()
        assert len(tables) == 1

    def test_table_columns(self, conn):
        """Table should have all expected columns."""
        ensure_sec_filing_llm_metadata_table(conn)

        columns = conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'sec_filing_llm_metadata' "
            "ORDER BY ordinal_position"
        ).fetchall()
        column_names = [c[0] for c in columns]

        expected = [
            "metadata_id",
            "filing_id",
            "symbol",
            "section_name",
            "executive_summary",
            "key_topics",
            "sentiment",
            "named_entities",
            "financial_metrics",
            "forward_looking_statements",
            "risk_factors",
            "embedding",
            "model_name",
            "created_at",
        ]
        assert column_names == expected

    def test_idempotent(self, conn):
        """Calling twice should not error."""
        ensure_sec_filing_llm_metadata_table(conn)
        ensure_sec_filing_llm_metadata_table(conn)

    def test_primary_key(self, conn):
        """metadata_id should be the primary key (rejects duplicates)."""
        ensure_sec_filing_llm_metadata_table(conn)

        conn.execute(
            "INSERT INTO sec_filing_llm_metadata "
            "(metadata_id, filing_id, symbol) VALUES ('id1', 'f1', 'AAPL')"
        )

        with pytest.raises(duckdb.ConstraintException):
            conn.execute(
                "INSERT INTO sec_filing_llm_metadata "
                "(metadata_id, filing_id, symbol) VALUES ('id1', 'f2', 'MSFT')"
            )

    def test_embedding_column_accepts_float_array(self, conn):
        """embedding column should accept a FLOAT[768] array."""
        ensure_sec_filing_llm_metadata_table(conn)

        embedding = [0.1] * 768
        conn.execute(
            "INSERT INTO sec_filing_llm_metadata "
            "(metadata_id, filing_id, symbol, embedding) "
            "VALUES ('id1', 'f1', 'AAPL', ?)",
            [embedding],
        )

        result = conn.execute(
            "SELECT embedding FROM sec_filing_llm_metadata WHERE metadata_id = 'id1'"
        ).fetchone()
        assert result is not None
        assert len(result[0]) == 768
