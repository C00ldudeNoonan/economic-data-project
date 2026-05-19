"""Dagster definitions for the economic alerts subsystem."""

import dagster as dg

from macro_agents.defs.alerts.assets import economic_alert_evaluations
from macro_agents.defs.alerts.sensor import economic_alert_notification_sensor
from macro_agents.defs.resources.gmail import gmail_notifier_resource

defs = dg.Definitions(
    assets=[economic_alert_evaluations],
    sensors=[economic_alert_notification_sensor],
    resources={"gmail": gmail_notifier_resource},
)
