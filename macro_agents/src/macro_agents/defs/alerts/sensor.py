"""Sensor that emails unnotified alert breaches via Gmail."""

import os
from datetime import datetime, timedelta, timezone

import dagster as dg

from macro_agents.defs.alerts.assets import ALERT_EVENTS_TABLE
from macro_agents.defs.alerts.config import AlertDefinition, load_alert_config


def _build_email_body(event: dict, alert: AlertDefinition) -> tuple[str, str]:
    observed_at = event["observed_at"]
    breached_at = event["breached_at"]

    text = (
        f"{alert.title}\n"
        f"{'=' * len(alert.title)}\n\n"
        f"Severity:        {alert.severity.upper()}\n"
        f"Indicator:       {alert.indicator} ({alert.series_code})\n"
        f"Observed value:  {event['observed_value']}\n"
        f"Threshold:       {alert.comparator} {alert.threshold}\n"
        f"Observation date:{observed_at}\n"
        f"Breached at:     {breached_at}\n\n"
        f"{alert.description.strip()}\n"
    )

    html = (
        f"<h2>{alert.title}</h2>"
        f"<p><strong>Severity:</strong> {alert.severity.upper()}</p>"
        f"<table style='border-collapse: collapse;'>"
        f"<tr><td><b>Indicator</b></td><td>{alert.indicator} ({alert.series_code})</td></tr>"
        f"<tr><td><b>Observed value</b></td><td>{event['observed_value']}</td></tr>"
        f"<tr><td><b>Threshold</b></td><td>{alert.comparator} {alert.threshold}</td></tr>"
        f"<tr><td><b>Observation date</b></td><td>{observed_at}</td></tr>"
        f"<tr><td><b>Breached at</b></td><td>{breached_at}</td></tr>"
        f"</table>"
        f"<p>{alert.description.strip()}</p>"
    )
    return text, html


def _pending_events(
    md, alert_ids: list[str], cooldown_cutoff_by_id: dict[str, datetime]
):
    """Return unnotified events whose alert_id is still beyond its cooldown."""
    rows = md.execute_query(
        f"""
        SELECT *
        FROM {ALERT_EVENTS_TABLE}
        WHERE notified_at IS NULL
          AND alert_id IN ({", ".join(["?"] * len(alert_ids))})
        ORDER BY breached_at ASC
        """,
        read_only=True,
        params=alert_ids,
    )
    events = rows.to_dicts()
    pending = []
    for event in events:
        cutoff = cooldown_cutoff_by_id.get(event["alert_id"])
        if cutoff is None:
            pending.append(event)
            continue
        if event["breached_at"] >= cutoff:
            pending.append(event)
    return pending


def _most_recent_notification_by_id(md, alert_ids: list[str]) -> dict[str, datetime]:
    rows = md.execute_query(
        f"""
        SELECT alert_id, MAX(notified_at) AS last_notified
        FROM {ALERT_EVENTS_TABLE}
        WHERE notified_at IS NOT NULL
          AND alert_id IN ({", ".join(["?"] * len(alert_ids))})
        GROUP BY alert_id
        """,
        read_only=True,
        params=alert_ids,
    )
    return {row["alert_id"]: row["last_notified"] for row in rows.to_dicts()}


def _mark_notified(md, event_id: int, now: datetime) -> None:
    md.execute_query(
        f"UPDATE {ALERT_EVENTS_TABLE} SET notified_at = ? WHERE event_id = ?",
        read_only=False,
        params=[now, event_id],
    )


@dg.sensor(
    name="economic_alert_notification_sensor",
    description=(
        "Send Gmail notifications for unnotified rows in "
        "`economic_alert_events`, respecting per-alert cooldowns."
    ),
    minimum_interval_seconds=1800,
    default_status=dg.DefaultSensorStatus.STOPPED,
)
def economic_alert_notification_sensor(context: dg.SensorEvaluationContext):
    from macro_agents.defs.resources.gmail import gmail_notifier_resource
    from macro_agents.defs.resources.bigquery_warehouse import bigquery_warehouse_resource

    recipient = os.getenv("ALERT_RECIPIENT")
    if not recipient:
        context.log.warning("ALERT_RECIPIENT not set; skipping notification sweep")
        return

    md = bigquery_warehouse_resource
    config = load_alert_config()
    alerts_by_id = {a.alert_id: a for a in config.alerts}
    alert_ids = list(alerts_by_id.keys())
    if not alert_ids:
        return

    if not md.table_exists(ALERT_EVENTS_TABLE):
        context.log.debug(f"{ALERT_EVENTS_TABLE} does not exist yet")
        return

    now = datetime.now(timezone.utc)
    last_notified_by_id = _most_recent_notification_by_id(md, alert_ids)
    cooldown_cutoff: dict[str, datetime] = {}
    for alert_id, last in last_notified_by_id.items():
        alert = alerts_by_id[alert_id]
        cooldown_cutoff[alert_id] = last + timedelta(hours=alert.cooldown_hours)

    pending = _pending_events(md, alert_ids, cooldown_cutoff)
    if not pending:
        context.log.debug("No alert notifications pending")
        return

    sent_count = 0
    for event in pending:
        alert = alerts_by_id.get(event["alert_id"])
        if alert is None:
            continue
        # Cooldown applies relative to the most recent notification.
        last = last_notified_by_id.get(alert.alert_id)
        if last is not None and now < last + timedelta(hours=alert.cooldown_hours):
            continue

        text_body, html_body = _build_email_body(event, alert)
        gmail_notifier_resource.send_alert(
            to=recipient,
            subject=f"[{alert.severity.upper()}] {alert.title}",
            text_body=text_body,
            html_body=html_body,
        )
        _mark_notified(md, int(event["event_id"]), now)
        last_notified_by_id[alert.alert_id] = now
        sent_count += 1

    context.log.info(f"Sent {sent_count} alert notification(s)")
