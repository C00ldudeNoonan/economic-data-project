from pathlib import Path
import os

# Set GIT_PYTHON_REFRESH to quiet to suppress git executable errors in environments where git is not available
# This is needed because dagster-dbt imports GitPython, which requires git to be in PATH
os.environ.setdefault("GIT_PYTHON_REFRESH", "quiet")

from dagster_dbt import DbtProject, dbt_assets, DbtCliResource, DagsterDbtTranslator
import dagster as dg
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


environment = os.getenv("DBT_TARGET", "local")

dbt_project_dir = os.getenv("DBT_PROJECT_DIR")
if dbt_project_dir:
    dbt_project_dir = Path(dbt_project_dir).resolve()
    if not dbt_project_dir.exists():
        raise FileNotFoundError(
            f"DBT_PROJECT_DIR environment variable points to non-existent path: {dbt_project_dir}"
        )
else:
    current_file = Path(__file__).resolve()
    # Try to find macro_agents package root (where dbt_project is copied during deployment)
    # Go up from: macro_agents/defs/transformation/dbt.py -> macro_agents/defs/transformation -> macro_agents/defs -> macro_agents
    macro_agents_root = current_file.parent.parent.parent
    repo_root = current_file.parent.parent.parent.parent.parent
    cwd = Path.cwd()
    possible_dbt_project_paths = [
        # Check in current working directory (Dagster Cloud runtime: working_directory/root)
        cwd / "dbt_project",
        # Check in macro_agents subdirectory (if working directory is root)
        cwd / "macro_agents" / "dbt_project",
        # Check relative to macro_agents package location
        macro_agents_root / "dbt_project",
        # Check in repo root
        repo_root / "dbt_project",
        # Check parent of working directory
        cwd.parent / "dbt_project",
        # Check if dbt_project is alongside macro_agents package
        macro_agents_root.parent / "dbt_project",
    ]

    dbt_project_dir = None
    for path in possible_dbt_project_paths:
        abs_path = path.resolve()
        if abs_path.exists() and abs_path.is_dir():
            dbt_project_dir = abs_path
            break

    if dbt_project_dir is None:
        tried_paths = [str(p.resolve()) for p in possible_dbt_project_paths]
        raise FileNotFoundError(
            f"Could not find dbt_project directory.\n"
            f"Current working directory: {Path.cwd().resolve()}\n"
            f"Module file location: {current_file}\n"
            f"Macro agents root (calculated): {macro_agents_root}\n"
            f"Repo root (calculated): {repo_root}\n"
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
