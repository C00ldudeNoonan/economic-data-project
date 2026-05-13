"""Tests for SEC unprocessed filings sensor."""

import json
from unittest.mock import MagicMock

import dagster as dg

from macro_agents.defs.domains.sec.sensors import sec_unprocessed_filings_sensor


def _make_md_mock(unprocessed_count=0, db_error=False):
    """Create a mock MotherDuckResource."""
    md = MagicMock()
    if db_error:
        conn = MagicMock()
        conn.execute.side_effect = Exception("DB connection failed")
        md.get_connection.return_value = conn
    else:
        conn = MagicMock()
        conn.execute.return_value.fetchone.return_value = (unprocessed_count,)
        md.get_connection.return_value = conn
    return md


def _run_sensor(cursor=None, unprocessed_count=0, db_error=False):
    """Run the sensor's underlying function directly to avoid Dagster wrapping."""
    md = _make_md_mock(unprocessed_count=unprocessed_count, db_error=db_error)

    context = MagicMock(spec=dg.SensorEvaluationContext)
    context.cursor = cursor
    context.log = MagicMock()

    # Call the raw underlying function (unwrapped)
    raw_fn = sec_unprocessed_filings_sensor.__wrapped__
    gen = raw_fn(context, md)

    # Collect yielded RunRequests
    run_requests = list(gen) if gen is not None else []

    return run_requests, context


class TestSecUnprocessedFilingsSensor:
    """Test cases for sec_unprocessed_filings_sensor."""

    def test_no_unprocessed_filings(self):
        """Sensor should not yield any RunRequests when count is 0."""
        results, context = _run_sensor(unprocessed_count=0)
        assert len(results) == 0
        context.update_cursor.assert_called_once()
        cursor_data = json.loads(context.update_cursor.call_args[0][0])
        assert cursor_data["last_unprocessed_count"] == 0

    def test_unprocessed_filings_triggers_run(self):
        """Sensor should yield a RunRequest when unprocessed filings exist."""
        results, _ = _run_sensor(unprocessed_count=5)
        assert len(results) == 1
        run_request = results[0]
        assert isinstance(run_request, dg.RunRequest)
        assert run_request.tags["trigger"] == "unprocessed_filings_detected"
        assert run_request.tags["unprocessed_count"] == "5"

    def test_cursor_updated_after_run(self):
        """Cursor should be updated with last check info after trigger."""
        _, context = _run_sensor(unprocessed_count=3)
        context.update_cursor.assert_called_once()
        cursor_data = json.loads(context.update_cursor.call_args[0][0])
        assert "last_check" in cursor_data
        assert "last_run_key" in cursor_data
        assert cursor_data["last_unprocessed_count"] == 3

    def test_existing_cursor_parsed(self):
        """Sensor should parse an existing JSON cursor without errors."""
        existing_cursor = json.dumps(
            {
                "last_check": "2026-03-01T00:00:00+00:00",
                "last_run_key": "sec_processing_old",
                "last_unprocessed_count": 0,
            }
        )
        results, _ = _run_sensor(cursor=existing_cursor, unprocessed_count=2)
        assert len(results) == 1

    def test_invalid_cursor_handled(self):
        """Sensor should handle invalid cursor JSON gracefully."""
        results, context = _run_sensor(cursor="not-json", unprocessed_count=1)
        assert len(results) == 1
        context.log.warning.assert_called()

    def test_db_error_returns_no_runs(self):
        """Sensor should not trigger if the DB query fails."""
        results, context = _run_sensor(db_error=True)
        assert len(results) == 0
        context.log.warning.assert_called()

    def test_run_key_contains_timestamp(self):
        """RunRequest should have a run_key with sec_processing prefix."""
        results, _ = _run_sensor(unprocessed_count=1)
        assert results[0].run_key.startswith("sec_processing_")
