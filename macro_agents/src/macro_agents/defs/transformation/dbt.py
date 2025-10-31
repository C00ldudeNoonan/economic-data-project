from pathlib import Path

from dagster_dbt import DbtProject, dbt_assets, DbtCliResource, DagsterDbtTranslator
import dagster as dg
import os
from typing import Any, Optional
from collections.abc import Mapping

import logging

logging.getLogger("dagster_dbt").setLevel(logging.DEBUG)


class CustomizedDagsterDbtTranslator(DagsterDbtTranslator):
    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> Optional[str]:
        asset_path = dbt_resource_props["fqn"][1:-1]
        if asset_path:
            return "_".join(asset_path)
        return "dbt_seeds"

    def get_asset_key(self, dbt_resource_props):
        resource_type = dbt_resource_props["resource_type"]
        name = dbt_resource_props["name"]
        if resource_type == "source":
            return dg.AssetKey(name)
        else:
            return super().get_asset_key(dbt_resource_props)

    def get_automation_condition(
        self, dbt_resource_props: Mapping[str, Any]
    ) -> Optional[dg.AutomationCondition]:
        return dg.AutomationCondition.eager()


environment = os.getenv("DBT_TARGET", "dev")

# Find the dbt_project directory
# First, check if explicitly set via environment variable
dbt_project_dir = os.getenv("DBT_PROJECT_DIR")
if dbt_project_dir:
    dbt_project_dir = Path(dbt_project_dir).resolve()
    if not dbt_project_dir.exists():
        raise FileNotFoundError(f"DBT_PROJECT_DIR environment variable points to non-existent path: {dbt_project_dir}")
else:
    # Use working directory (set by Dagster Cloud or current directory) to find repo root
    # For Dagster Cloud: working_directory is ./macro_agents, so repo root is parent
    # For local dev: depends on where script is run from
    current_dir = Path.cwd()
    possible_dbt_project_paths = [
        # Primary: For Dagster Cloud with working_directory: ./macro_agents
        # Current dir should be macro_agents/, so go up one level to repo root
        current_dir.parent / "dbt_project",
        # Alternative: if already at repo root
        current_dir / "dbt_project",
        # For local dev when running from macro_agents directory
        current_dir.parent.parent / "dbt_project",
    ]

    dbt_project_dir = None
    for path in possible_dbt_project_paths:
        abs_path = path.resolve()
        if abs_path.exists() and abs_path.is_dir():
            dbt_project_dir = abs_path
            break

    if dbt_project_dir is None:
        # Provide helpful error with actual paths tried
        tried_paths = [str(p.resolve()) for p in possible_dbt_project_paths]
        raise FileNotFoundError(
            f"Could not find dbt_project directory.\n"
            f"Current working directory: {current_dir.resolve()}\n"
            f"Tried paths: {tried_paths}\n"
            f"Please ensure dbt_project directory exists relative to the repository root, "
            f"or set DBT_PROJECT_DIR environment variable."
        )

dbt_project = DbtProject(
    project_dir=dbt_project_dir,
    target=environment,
)

dbt_project.prepare_if_dev()

dbt_cli_resource = DbtCliResource(project_dir=dbt_project)


@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=CustomizedDagsterDbtTranslator(),
)
def full_dbt_assets(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
