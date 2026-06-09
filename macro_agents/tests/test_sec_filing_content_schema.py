"""Schema tests for ensure_sec_filing_content_table DDL (issue #70).

After the BigQuery migration, ensure_sec_filing_content_table issues DDL
against a BigQuery client. These tests verify the SQL produced is correct
by inspecting calls to a mock client.
"""

from unittest.mock import MagicMock

import pytest

from macro_agents.defs.domains.sec.tables import ensure_sec_filing_content_table


EXPECTED_COLUMNS = {
    "content_id",
    "filing_id",
    "section_name",
    "section_order",
    "word_count",
    "gcs_path",
}


@pytest.fixture
def mock_bq():
    """Mock BigQuery client with no legacy columns present."""
    client = MagicMock()
    client.project = "test-project"
    job = MagicMock()
    # INFORMATION_SCHEMA query returns empty result (no legacy columns)
    job.result.return_value.to_dataframe.return_value.__len__ = lambda self: 0
    client.query.return_value = job
    return client


def test_creates_fresh_table_with_expected_shape(mock_bq):
    """CREATE TABLE IF NOT EXISTS should include all expected columns."""
    ensure_sec_filing_content_table(mock_bq)
    assert mock_bq.query.called
    create_sql = mock_bq.query.call_args_list[0][0][0]
    assert "CREATE TABLE IF NOT EXISTS" in create_sql
    assert "sec_filing_content" in create_sql
    for col in EXPECTED_COLUMNS:
        assert col in create_sql


def test_idempotent_on_already_migrated_table(mock_bq):
    """Calling twice should issue CREATE TABLE IF NOT EXISTS twice."""
    ensure_sec_filing_content_table(mock_bq)
    ensure_sec_filing_content_table(mock_bq)
    create_calls = [
        c
        for c in mock_bq.query.call_args_list
        if "CREATE TABLE IF NOT EXISTS" in c[0][0]
    ]
    assert len(create_calls) == 2


def test_migrates_legacy_schema_with_content_text_and_created_at(mock_bq):
    """When legacy columns are present, ALTER TABLE should drop them."""
    import pandas as pd

    legacy_df = pd.DataFrame({"cnt": [2]})
    job_with_legacy = MagicMock()
    job_with_legacy.result.return_value.to_dataframe.return_value = legacy_df

    drop_job = MagicMock()

    mock_bq.query.side_effect = [
        job_with_legacy,  # CREATE TABLE
        job_with_legacy,  # INFORMATION_SCHEMA query
        drop_job,  # DROP content_text
        drop_job,  # DROP created_at
    ]

    ensure_sec_filing_content_table(mock_bq)

    all_sql = [c[0][0] for c in mock_bq.query.call_args_list]
    drop_sqls = [s for s in all_sql if "DROP COLUMN" in s]
    assert any("content_text" in s for s in drop_sqls)
    assert any("created_at" in s for s in drop_sqls)


def test_migrates_partial_legacy_schema_with_only_content_text(mock_bq):
    """When only content_text is present (cnt=1), DROP calls are still issued."""
    import pandas as pd

    legacy_df = pd.DataFrame({"cnt": [1]})
    job_with_legacy = MagicMock()
    job_with_legacy.result.return_value.to_dataframe.return_value = legacy_df

    drop_job = MagicMock()

    mock_bq.query.side_effect = [
        job_with_legacy,
        job_with_legacy,
        drop_job,
        drop_job,
    ]

    ensure_sec_filing_content_table(mock_bq)

    all_sql = [c[0][0] for c in mock_bq.query.call_args_list]
    drop_sqls = [s for s in all_sql if "DROP COLUMN" in s]
    assert len(drop_sqls) >= 1
