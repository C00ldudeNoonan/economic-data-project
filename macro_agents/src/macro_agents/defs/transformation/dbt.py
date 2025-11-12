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

    # In Dagster Cloud, the working_directory is set to ./macro_agents
    # The dbt_project should be copied there during deployment
    # The working directory at runtime is typically working_directory/root
    # So dbt_project should be at working_directory/root/dbt_project (i.e., cwd/dbt_project)
    possible_dbt_project_paths = [
        # First priority: current working directory (Dagster Cloud runtime: working_directory/root)
        # This is where the working_directory contents are extracted
        cwd / "dbt_project",
        # Second: if we're in a "root" subdirectory, check parent
        cwd.parent / "dbt_project" if cwd.name == "root" else None,
        # Third: check if dbt_project is in the parent of root (working_directory level)
        cwd.parent.parent / "dbt_project" if cwd.parent.name == "root" else None,
        # Fourth: check relative to macro_agents package location (if bundled in wheel)
        macro_agents_root / "dbt_project",
        # Fifth: check if dbt_project is alongside the package
        macro_agents_root.parent / "dbt_project",
        # Sixth: check in macro_agents subdirectory (if working directory is repo root)
        cwd / "macro_agents" / "dbt_project",
        # Seventh: check in repo root (fallback)
        repo_root / "dbt_project",
        # Eighth: check parent of working directory
        cwd.parent / "dbt_project",
    ]

    # Filter out None values
    possible_dbt_project_paths = [
        p for p in possible_dbt_project_paths if p is not None
    ]

    dbt_project_dir = None
    for path in possible_dbt_project_paths:
        try:
            abs_path = path.resolve()
            if abs_path.exists() and abs_path.is_dir():
                # Verify it's actually a dbt project by checking for dbt_project.yml
                if (abs_path / "dbt_project.yml").exists() or (
                    abs_path / "dbt_project.yaml"
                ).exists():
                    dbt_project_dir = abs_path
                    break
        except (OSError, RuntimeError):
            # Skip paths that can't be resolved (e.g., broken symlinks)
            continue

    if dbt_project_dir is None:
        tried_paths = []
        for p in possible_dbt_project_paths:
            try:
                tried_paths.append(str(p.resolve()))
            except (OSError, RuntimeError):
                tried_paths.append(str(p))

        # Also list what actually exists in the working directory for debugging
        cwd_contents = []
        try:
            cwd_contents = [str(p) for p in cwd.iterdir()] if cwd.exists() else []
        except (OSError, PermissionError):
            pass

        raise FileNotFoundError(
            f"Could not find dbt_project directory.\n"
            f"Current working directory: {Path.cwd().resolve()}\n"
            f"Working directory contents: {cwd_contents}\n"
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
