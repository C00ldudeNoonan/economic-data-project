import importlib
import sys

import dagster as dg
from dagster_dbt import DbtCloudWorkspace


def test_dbt_platform_observe_only_registers_specs_without_dbt_jobs(monkeypatch):
    monkeypatch.setenv("DBT_ORCHESTRATION_MODE", "dbt_platform")
    monkeypatch.setenv("DBT_PLATFORM_OBSERVE_ONLY", "true")
    monkeypatch.setenv("DBT_PLATFORM_ACCOUNT_ID", "1")
    monkeypatch.setenv("DBT_PLATFORM_PROJECT_ID", "2")
    monkeypatch.setenv("DBT_PLATFORM_ENVIRONMENT_ID", "3")
    monkeypatch.setenv("DBT_PLATFORM_TOKEN", "test-token")

    def fake_load_asset_specs(self, **kwargs):
        return [
            dg.AssetSpec(
                key=dg.AssetKey("observed_dbt_model"),
                group_name="staging",
                kinds={"dbtcloud"},
            )
        ]

    monkeypatch.setattr(DbtCloudWorkspace, "load_asset_specs", fake_load_asset_specs)

    for module_name in [
        "macro_agents.defs.transformation",
        "macro_agents.defs.transformation.dbt",
    ]:
        sys.modules.pop(module_name, None)

    transformation = importlib.import_module("macro_agents.defs.transformation")

    assert transformation.dbt_jobs == []
    assert transformation.dbt_asset_definitions[0].key == dg.AssetKey(
        "observed_dbt_model"
    )
    assert transformation.dbt_cloud_polling_sensor is not None
