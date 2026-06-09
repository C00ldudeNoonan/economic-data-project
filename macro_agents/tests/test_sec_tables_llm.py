"""Tests for SEC filing LLM metadata table DDL."""

from unittest.mock import Mock

import pytest

from macro_agents.defs.domains.sec.tables import ensure_sec_filing_llm_metadata_table


@pytest.fixture
def mock_bq_client():
    """Mock BigQuery client that records query calls."""
    client = Mock()
    client.project = "test-project"
    job = Mock()
    client.query.return_value = job
    return client


class TestSecFilingLLMMetadataTable:
    """Test cases for ensure_sec_filing_llm_metadata_table."""

    def test_table_created(self, mock_bq_client):
        """Function should call client.query with CREATE TABLE statement."""
        ensure_sec_filing_llm_metadata_table(mock_bq_client)
        mock_bq_client.query.assert_called_once()
        sql = mock_bq_client.query.call_args[0][0]
        assert "sec_filing_llm_metadata" in sql
        assert "CREATE TABLE IF NOT EXISTS" in sql

    def test_table_columns(self, mock_bq_client):
        """CREATE TABLE statement should include all expected columns."""
        ensure_sec_filing_llm_metadata_table(mock_bq_client)
        sql = mock_bq_client.query.call_args[0][0]
        expected_columns = [
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
        for col in expected_columns:
            assert col in sql, f"Expected column '{col}' not found in SQL"

    def test_idempotent(self, mock_bq_client):
        """Calling twice should issue two CREATE TABLE IF NOT EXISTS queries."""
        ensure_sec_filing_llm_metadata_table(mock_bq_client)
        ensure_sec_filing_llm_metadata_table(mock_bq_client)
        assert mock_bq_client.query.call_count == 2

    def test_result_called(self, mock_bq_client):
        """Should call .result() on the returned job to wait for completion."""
        ensure_sec_filing_llm_metadata_table(mock_bq_client)
        mock_bq_client.query.return_value.result.assert_called_once()

    def test_uses_project_and_dataset(self, mock_bq_client):
        """Table reference should include project and dataset."""
        ensure_sec_filing_llm_metadata_table(mock_bq_client, dataset="my_dataset")
        sql = mock_bq_client.query.call_args[0][0]
        assert "test-project" in sql
        assert "my_dataset" in sql

    def test_embedding_column_is_float_array(self, mock_bq_client):
        """embedding column should be declared as ARRAY<FLOAT64>."""
        ensure_sec_filing_llm_metadata_table(mock_bq_client)
        sql = mock_bq_client.query.call_args[0][0]
        assert "ARRAY<FLOAT64>" in sql
