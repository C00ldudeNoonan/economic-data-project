"""Tests for the alert evaluation logic against a real DuckDB."""

from datetime import datetime, timezone
from unittest.mock import Mock

import polars as pl
import pytest

from macro_agents.defs.alerts.assets import (
    ALERT_EVENTS_TABLE,
    ALERT_INPUTS_TABLE,
    _ensure_events_table,
    evaluate_alerts,
)


@pytest.fixture
def md(motherduck_resource):
    """Return the test MotherDuck resource with the inputs table seeded."""
    return motherduck_resource


def _seed_inputs(md, rows: list[dict]) -> None:
    md.execute_query(
        f"""
        CREATE OR REPLACE TABLE {ALERT_INPUTS_TABLE} (
            date TIMESTAMP,
            cpi_yoy_pct DOUBLE,
            t10y2y_spread DOUBLE,
            unrate_change_3mo DOUBLE,
            fedfunds_change_1mo DOUBLE,
            hy_oas_pct DOUBLE
        )
        """,
        read_only=False,
    )
    df = pl.DataFrame(rows)
    conn = md.get_connection()
    try:
        conn.register("seed_df", df)
        conn.execute(f"INSERT INTO {ALERT_INPUTS_TABLE} SELECT * FROM seed_df")
        conn.commit()
    finally:
        conn.close()


def _mock_context(run_id: str = "test-run"):
    ctx = Mock()
    ctx.log = Mock()
    ctx.run_id = run_id
    return ctx


def test_new_breach_inserts_event(md):
    _seed_inputs(
        md,
        [
            {
                "date": datetime(2026, 1, 1, tzinfo=timezone.utc),
                "cpi_yoy_pct": 4.5,
                "t10y2y_spread": 0.4,
                "unrate_change_3mo": 0.1,
                "fedfunds_change_1mo": 0.05,
                "hy_oas_pct": 3.5,
            }
        ],
    )
    ctx = _mock_context()
    _ensure_events_table(md, ctx)
    counts = evaluate_alerts(md, ctx, now=datetime(2026, 1, 2, tzinfo=timezone.utc))

    assert counts["new_breaches"] == 1
    events = md.execute_query(f"SELECT * FROM {ALERT_EVENTS_TABLE}")
    rows = events.to_dicts()
    assert len(rows) == 1
    assert rows[0]["alert_id"] == "cpi_yoy_above_3pct"
    assert rows[0]["resolved_at"] is None


def test_open_breach_is_idempotent(md):
    _seed_inputs(
        md,
        [
            {
                "date": datetime(2026, 1, 1, tzinfo=timezone.utc),
                "cpi_yoy_pct": 4.5,
                "t10y2y_spread": 0.4,
                "unrate_change_3mo": 0.1,
                "fedfunds_change_1mo": 0.05,
                "hy_oas_pct": 3.5,
            }
        ],
    )
    ctx = _mock_context()
    _ensure_events_table(md, ctx)
    evaluate_alerts(md, ctx, now=datetime(2026, 1, 2, tzinfo=timezone.utc))
    counts = evaluate_alerts(md, ctx, now=datetime(2026, 1, 3, tzinfo=timezone.utc))

    assert counts["new_breaches"] == 0
    assert counts["still_open"] == 1
    events = md.execute_query(f"SELECT COUNT(*) AS n FROM {ALERT_EVENTS_TABLE}")
    assert events.to_dicts()[0]["n"] == 1


def test_resolution_sets_resolved_at(md):
    # Day 1: breach
    _seed_inputs(
        md,
        [
            {
                "date": datetime(2026, 1, 1, tzinfo=timezone.utc),
                "cpi_yoy_pct": 4.5,
                "t10y2y_spread": 0.4,
                "unrate_change_3mo": 0.1,
                "fedfunds_change_1mo": 0.05,
                "hy_oas_pct": 3.5,
            }
        ],
    )
    ctx = _mock_context()
    _ensure_events_table(md, ctx)
    evaluate_alerts(md, ctx, now=datetime(2026, 1, 2, tzinfo=timezone.utc))

    # Day 2: CPI cools below threshold
    md.execute_query(f"DROP TABLE {ALERT_INPUTS_TABLE}", read_only=False)
    _seed_inputs(
        md,
        [
            {
                "date": datetime(2026, 2, 1, tzinfo=timezone.utc),
                "cpi_yoy_pct": 2.0,
                "t10y2y_spread": 0.4,
                "unrate_change_3mo": 0.1,
                "fedfunds_change_1mo": 0.05,
                "hy_oas_pct": 3.5,
            }
        ],
    )
    counts = evaluate_alerts(md, ctx, now=datetime(2026, 2, 2, tzinfo=timezone.utc))

    assert counts["resolved"] == 1
    rows = md.execute_query(
        f"SELECT * FROM {ALERT_EVENTS_TABLE} WHERE alert_id = 'cpi_yoy_above_3pct'"
    ).to_dicts()
    assert len(rows) == 1
    assert rows[0]["resolved_at"] is not None


def test_picks_latest_non_null_row(md):
    # Latest row has NULL cpi, prior row has cpi above threshold.
    _seed_inputs(
        md,
        [
            {
                "date": datetime(2026, 1, 1, tzinfo=timezone.utc),
                "cpi_yoy_pct": 5.0,
                "t10y2y_spread": 0.4,
                "unrate_change_3mo": 0.1,
                "fedfunds_change_1mo": 0.05,
                "hy_oas_pct": 3.5,
            },
            {
                "date": datetime(2026, 2, 1, tzinfo=timezone.utc),
                "cpi_yoy_pct": None,
                "t10y2y_spread": -0.5,
                "unrate_change_3mo": 0.1,
                "fedfunds_change_1mo": 0.05,
                "hy_oas_pct": 3.5,
            },
        ],
    )
    ctx = _mock_context()
    _ensure_events_table(md, ctx)
    evaluate_alerts(md, ctx, now=datetime(2026, 2, 2, tzinfo=timezone.utc))

    rows = md.execute_query(
        f"SELECT alert_id FROM {ALERT_EVENTS_TABLE} ORDER BY alert_id"
    ).to_dicts()
    ids = [r["alert_id"] for r in rows]
    assert "cpi_yoy_above_3pct" in ids
    assert "yield_curve_inversion_10y2y" in ids


def test_empty_inputs_short_circuits(md):
    md.execute_query(
        f"""
        CREATE OR REPLACE TABLE {ALERT_INPUTS_TABLE} (
            date TIMESTAMP,
            cpi_yoy_pct DOUBLE,
            t10y2y_spread DOUBLE,
            unrate_change_3mo DOUBLE,
            fedfunds_change_1mo DOUBLE,
            hy_oas_pct DOUBLE
        )
        """,
        read_only=False,
    )
    ctx = _mock_context()
    _ensure_events_table(md, ctx)
    counts = evaluate_alerts(md, ctx)
    assert counts == {"evaluated": 0, "new_breaches": 0, "resolved": 0, "still_open": 0}
