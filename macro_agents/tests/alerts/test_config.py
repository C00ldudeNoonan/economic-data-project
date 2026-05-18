"""Tests for alert YAML loading and validation."""

from pathlib import Path

import pytest
import yaml

from macro_agents.defs.alerts.config import AlertConfig, load_alert_config


def test_default_definitions_load():
    config = load_alert_config()
    assert len(config.alerts) >= 5
    ids = {a.alert_id for a in config.alerts}
    assert {
        "cpi_yoy_above_3pct",
        "yield_curve_inversion_10y2y",
        "unemployment_3mo_rise",
        "fed_funds_step_change",
        "hy_oas_stress",
    }.issubset(ids)


def test_is_breached_comparators():
    config = load_alert_config()
    cpi = next(a for a in config.alerts if a.alert_id == "cpi_yoy_above_3pct")
    assert cpi.is_breached(3.5) is True
    assert cpi.is_breached(2.9) is False

    curve = next(
        a for a in config.alerts if a.alert_id == "yield_curve_inversion_10y2y"
    )
    assert curve.is_breached(-0.1) is True
    assert curve.is_breached(0.0) is False  # strict lt

    fed = next(a for a in config.alerts if a.alert_id == "fed_funds_step_change")
    assert fed.is_breached(0.25) is True  # gte
    assert fed.is_breached(0.24) is False


def test_duplicate_alert_ids_rejected(tmp_path: Path):
    payload = {
        "alerts": [
            {
                "alert_id": "dupe",
                "indicator": "x",
                "series_code": "X",
                "comparator": "gt",
                "threshold": 0.0,
                "title": "t",
            },
            {
                "alert_id": "dupe",
                "indicator": "y",
                "series_code": "Y",
                "comparator": "lt",
                "threshold": 1.0,
                "title": "t2",
            },
        ]
    }
    yaml_path = tmp_path / "bad.yaml"
    yaml_path.write_text(yaml.safe_dump(payload))
    with pytest.raises(ValueError, match="Duplicate alert_id"):
        load_alert_config(yaml_path)


def test_invalid_comparator_rejected():
    with pytest.raises(ValueError):
        AlertConfig.model_validate(
            {
                "alerts": [
                    {
                        "alert_id": "bad",
                        "indicator": "x",
                        "series_code": "X",
                        "comparator": "between",
                        "threshold": 0.0,
                        "title": "t",
                    }
                ]
            }
        )


def test_invalid_alert_id_rejected():
    with pytest.raises(ValueError):
        AlertConfig.model_validate(
            {
                "alerts": [
                    {
                        "alert_id": "has spaces",
                        "indicator": "x",
                        "series_code": "X",
                        "comparator": "gt",
                        "threshold": 0.0,
                        "title": "t",
                    }
                ]
            }
        )
