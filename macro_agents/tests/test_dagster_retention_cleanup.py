"""Guardrails for the Dagster retention/backup design (issue #132).

The cleanup job must never delete asset-automation state. These tests assert the
safety contract statically (there is no PostgreSQL in CI) so a regression that
reintroduces destructive deletes fails the build.
"""

from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
CLEANUP_SH = REPO_ROOT / "docker" / "cleanup_event_logs.sh"
BACKUP_SH = REPO_ROOT / "docker" / "backup_dagster_postgres.sh"
DAGSTER_YAML = REPO_ROOT / "dagster.yaml"
COMPOSE_YAML = REPO_ROOT / "docker-compose.yml"

# Tables that hold asset-automation / audit state — the cleanup job must never
# delete from any of them.
PROTECTED_TABLES = (
    "asset_materializations",
    "asset_observations",
    "asset_check_executions",
    "event_log_tags",
)


def test_cleanup_never_deletes_asset_state_tables() -> None:
    script = CLEANUP_SH.read_text(encoding="utf-8").lower()
    for table in PROTECTED_TABLES:
        assert f"delete from {table}" not in script, (
            f"cleanup script must not DELETE FROM {table} (issue #132)"
        )


def test_cleanup_only_deletes_null_type_log_rows() -> None:
    script = CLEANUP_SH.read_text(encoding="utf-8")
    # The single DELETE targets event_logs and is scoped to plain log rows.
    assert "DELETE FROM event_logs" in script
    assert "dagster_event_type IS NULL" in script
    # It must not fall back to the old "everything except errors" predicate.
    assert "dagster_event_type !~" not in script
    assert script.count("DELETE FROM") == 1


def test_cleanup_and_backup_scripts_are_valid_sh() -> None:
    import subprocess

    for script in (CLEANUP_SH, BACKUP_SH):
        result = subprocess.run(
            ["sh", "-n", str(script)], capture_output=True, text=True
        )
        assert result.returncode == 0, f"{script.name} sh syntax error: {result.stderr}"


def test_dagster_yaml_declares_supported_tick_retention() -> None:
    config = yaml.safe_load(DAGSTER_YAML.read_text(encoding="utf-8"))
    retention = config.get("retention", {})
    assert "schedule" in retention and "sensor" in retention, (
        "dagster.yaml must configure supported schedule/sensor tick retention"
    )


def test_compose_wires_backup_service_and_volume() -> None:
    compose = yaml.safe_load(COMPOSE_YAML.read_text(encoding="utf-8"))
    assert "dagster_db_backup" in compose["services"], "backup service missing"
    assert "dagster_backups" in compose["volumes"], "backups volume missing"
    backup = compose["services"]["dagster_db_backup"]
    assert any("/backups" in v for v in backup["volumes"]), (
        "backup service must mount the backups volume at /backups"
    )
