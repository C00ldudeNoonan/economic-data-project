"""Alert definition loading and validation."""

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field, field_validator

Comparator = Literal["gt", "lt", "gte", "lte"]
Severity = Literal["info", "warning", "critical"]


class AlertDefinition(BaseModel):
    """A single threshold-based alert."""

    alert_id: str
    indicator: str
    series_code: str
    comparator: Comparator
    threshold: float
    severity: Severity = "warning"
    cooldown_hours: int = Field(default=24, ge=0)
    title: str
    description: str = ""

    @field_validator("alert_id")
    @classmethod
    def _valid_id(cls, v: str) -> str:
        if not v or not all(c.isalnum() or c == "_" for c in v):
            raise ValueError(
                "alert_id must be non-empty and contain only alphanumerics or underscores"
            )
        return v

    def is_breached(self, value: float) -> bool:
        match self.comparator:
            case "gt":
                return value > self.threshold
            case "lt":
                return value < self.threshold
            case "gte":
                return value >= self.threshold
            case "lte":
                return value <= self.threshold


class AlertConfig(BaseModel):
    alerts: list[AlertDefinition]

    @field_validator("alerts")
    @classmethod
    def _unique_ids(cls, alerts: list[AlertDefinition]) -> list[AlertDefinition]:
        seen: set[str] = set()
        for a in alerts:
            if a.alert_id in seen:
                raise ValueError(f"Duplicate alert_id: {a.alert_id}")
            seen.add(a.alert_id)
        return alerts


DEFAULT_DEFINITIONS_PATH = Path(__file__).parent / "definitions.yaml"


def load_alert_config(path: Path | None = None) -> AlertConfig:
    """Load and validate the alert definitions YAML."""
    target = path or DEFAULT_DEFINITIONS_PATH
    with target.open() as f:
        raw = yaml.safe_load(f)
    return AlertConfig.model_validate(raw)
