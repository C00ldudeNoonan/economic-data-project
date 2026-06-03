"""Economic alert evaluation asset.

Reads the latest values from the `economic_alert_inputs` dbt model,
compares each indicator against the YAML alert definitions, and
records new breaches / resolutions in the `economic_alert_events`
table. The companion sensor handles email delivery.
"""

from datetime import datetime, timezone

import dagster as dg
import polars as pl

from macro_agents.defs.alerts.config import AlertDefinition, load_alert_config

ALERT_EVENTS_TABLE = "economic_alert_events"
ALERT_INPUTS_TABLE = "economic_alert_inputs"


def _ensure_events_table(md, context: dg.AssetExecutionContext) -> None:
    md.execute_query(
        f"""
        CREATE TABLE IF NOT EXISTS {ALERT_EVENTS_TABLE} (
            event_id BIGINT PRIMARY KEY,
            alert_id VARCHAR NOT NULL,
            indicator VARCHAR NOT NULL,
            comparator VARCHAR NOT NULL,
            threshold DOUBLE NOT NULL,
            observed_value DOUBLE NOT NULL,
            severity VARCHAR NOT NULL,
            title VARCHAR NOT NULL,
            description VARCHAR,
            observed_at TIMESTAMP NOT NULL,
            breached_at TIMESTAMP NOT NULL,
            resolved_at TIMESTAMP,
            notified_at TIMESTAMP,
            run_id VARCHAR
        )
        """,
        read_only=False,
    )
    context.log.debug(f"Ensured {ALERT_EVENTS_TABLE} exists")


def _latest_value(inputs: pl.DataFrame, column: str) -> tuple[float, datetime] | None:
    """Return (value, observation_date) for the most recent non-null row."""
    if column not in inputs.columns:
        return None
    series = inputs.select(["date", column]).drop_nulls()
    if series.is_empty():
        return None
    row = series.sort("date", descending=True).head(1).to_dicts()[0]
    obs_date = row["date"]
    if isinstance(obs_date, datetime):
        observed_at = (
            obs_date if obs_date.tzinfo else obs_date.replace(tzinfo=timezone.utc)
        )
    else:
        observed_at = datetime.combine(
            obs_date, datetime.min.time(), tzinfo=timezone.utc
        )
    return float(row[column]), observed_at


def _open_event(md, alert_id: str) -> dict | None:
    rows = md.execute_query(
        f"""
        SELECT *
        FROM {ALERT_EVENTS_TABLE}
        WHERE alert_id = ? AND resolved_at IS NULL
        ORDER BY breached_at DESC
        LIMIT 1
        """,
        read_only=True,
        params=[alert_id],
    )
    return rows.to_dicts()[0] if not rows.is_empty() else None


def _next_event_id(md) -> int:
    rows = md.execute_query(
        f"SELECT COALESCE(MAX(event_id), 0) AS max_id FROM {ALERT_EVENTS_TABLE}",
        read_only=True,
    )
    return int(rows.to_dicts()[0]["max_id"]) + 1


def _insert_breach(
    md,
    event_id: int,
    alert: AlertDefinition,
    value: float,
    observed_at: datetime,
    now: datetime,
    run_id: str | None,
) -> None:
    md.execute_query(
        f"""
        INSERT INTO {ALERT_EVENTS_TABLE} (
            event_id, alert_id, indicator, comparator, threshold,
            observed_value, severity, title, description,
            observed_at, breached_at, resolved_at, notified_at, run_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?)
        """,
        read_only=False,
        params=[
            event_id,
            alert.alert_id,
            alert.indicator,
            alert.comparator,
            float(alert.threshold),
            float(value),
            alert.severity,
            alert.title,
            alert.description,
            observed_at,
            now,
            run_id,
        ],
    )


def _resolve_event(md, event_id: int, now: datetime) -> None:
    md.execute_query(
        f"UPDATE {ALERT_EVENTS_TABLE} SET resolved_at = ? WHERE event_id = ?",
        read_only=False,
        params=[now, event_id],
    )


def evaluate_alerts(
    md,
    context: dg.AssetExecutionContext,
    now: datetime | None = None,
) -> dict[str, int]:
    """Evaluate all alert definitions against latest inputs.

    Pure-ish helper: takes the resource and context, returns counts.
    Extracted for testability — `economic_alert_evaluations` wraps it.
    """
    now = now or datetime.now(timezone.utc)
    config = load_alert_config()

    inputs = md.execute_query(
        f"SELECT * FROM {ALERT_INPUTS_TABLE}",
        read_only=True,
    )
    if inputs.is_empty():
        context.log.warning(
            f"{ALERT_INPUTS_TABLE} is empty — run dbt before evaluating alerts"
        )
        return {"evaluated": 0, "new_breaches": 0, "resolved": 0, "still_open": 0}

    new_breaches = 0
    resolved = 0
    still_open = 0
    run_id = context.run_id

    for alert in config.alerts:
        latest = _latest_value(inputs, alert.indicator)
        if latest is None:
            context.log.info(
                f"alert {alert.alert_id}: no data for indicator {alert.indicator}"
            )
            continue

        value, observed_at = latest
        breached = alert.is_breached(value)
        open_event = _open_event(md, alert.alert_id)

        if breached and open_event is None:
            event_id = _next_event_id(md)
            _insert_breach(md, event_id, alert, value, observed_at, now, run_id)
            context.log.info(
                f"alert {alert.alert_id} breached at value={value} "
                f"(threshold {alert.comparator} {alert.threshold})"
            )
            new_breaches += 1
        elif breached:
            still_open += 1
        elif not breached and open_event is not None:
            _resolve_event(md, int(open_event["event_id"]), now)
            context.log.info(f"alert {alert.alert_id} resolved at value={value}")
            resolved += 1

    return {
        "evaluated": len(config.alerts),
        "new_breaches": new_breaches,
        "resolved": resolved,
        "still_open": still_open,
    }


@dg.asset(
    name="economic_alert_evaluations",
    description=(
        "Evaluate economic indicator alerts against the latest "
        "`economic_alert_inputs` row and record breaches / resolutions "
        "in `economic_alert_events`."
    ),
    deps=[dg.AssetKey([ALERT_INPUTS_TABLE])],
    required_resource_keys={"bq"},
    automation_condition=dg.AutomationCondition.eager(),
    group_name="alerts",
)
def economic_alert_evaluations(context: dg.AssetExecutionContext):
    md = context.resources.md
    _ensure_events_table(md, context)
    counts = evaluate_alerts(md, context)
    context.add_output_metadata({k: dg.MetadataValue.int(v) for k, v in counts.items()})
