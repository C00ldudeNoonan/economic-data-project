import os
from pathlib import Path

# Set GIT_PYTHON_REFRESH to quiet to suppress git executable errors in environments where git is not available
# This is needed because dagster-dbt imports GitPython, which requires git to be in PATH
os.environ.setdefault("GIT_PYTHON_REFRESH", "quiet")

import logging
from collections.abc import Mapping
from typing import Any

import dagster as dg
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

logging.getLogger("dagster_dbt").setLevel(logging.DEBUG)

# When DBT_CLOUD_API_TOKEN is set, Dagster executes dbt via the dbt Cloud API
# (Fusion engine, artifact storage, CI). Without it, falls back to local DbtCliResource.
_DBT_CLOUD_MODE = bool(os.getenv("DBT_CLOUD_API_TOKEN"))


class CustomizedDagsterDbtTranslator(DagsterDbtTranslator):
    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> str | None:
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
    ) -> dg.AutomationCondition | None:
        return dg.AutomationCondition.eager()


if _DBT_CLOUD_MODE:
    from dagster_dbt.cloud_v2.asset_decorator import dbt_cloud_assets
    from dagster_dbt.cloud_v2.resources import DbtCloudCredentials, DbtCloudWorkspace

    dbt_cloud_workspace = DbtCloudWorkspace(
        credentials=DbtCloudCredentials(
            account_id=int(os.environ["DBT_CLOUD_ACCOUNT_ID"]),
            token=os.environ["DBT_CLOUD_API_TOKEN"],
            access_url=os.getenv("DBT_CLOUD_ACCESS_URL", "https://cloud.getdbt.com"),
        ),
        project_id=int(os.environ["DBT_CLOUD_PROJECT_ID"]),
        environment_id=int(os.environ["DBT_CLOUD_ENVIRONMENT_ID"]),
    )

    dbt_resource = dbt_cloud_workspace
    dbt_cli_resource = None  # not used in cloud mode

    @dbt_cloud_assets(
        workspace=dbt_cloud_workspace,
        dagster_dbt_translator=CustomizedDagsterDbtTranslator(),
    )
    def full_dbt_assets(context: dg.AssetExecutionContext, dbt: DbtCloudWorkspace):
        yield from dbt.cli(["build"], context=context).stream()

else:
    environment = os.getenv("DBT_TARGET", "dev")

    dbt_project_dir = os.getenv("DBT_PROJECT_DIR")
    if dbt_project_dir:
        dbt_project_dir = Path(dbt_project_dir).resolve()
        if not dbt_project_dir.exists():
            raise FileNotFoundError(
                "DBT_PROJECT_DIR environment variable points to a non-existent path."
            )
    else:
        current_file = Path(__file__).resolve()
        macro_agents_root = current_file.parent.parent.parent
        repo_root = current_file.parent.parent.parent.parent.parent
        cwd = Path.cwd()

        possible_dbt_project_paths = [
            cwd / "dbt_project",
            cwd.parent / "dbt_project" if cwd.name == "root" else None,
            cwd.parent.parent / "dbt_project" if cwd.parent.name == "root" else None,
            macro_agents_root / "dbt_project",
            macro_agents_root.parent / "dbt_project",
            cwd / "macro_agents" / "dbt_project",
            repo_root / "dbt_project",
            cwd.parent / "dbt_project",
        ]
        possible_dbt_project_paths = [p for p in possible_dbt_project_paths if p is not None]

        dbt_project_dir = None
        for path in possible_dbt_project_paths:
            try:
                abs_path = path.resolve()
                if abs_path.exists() and abs_path.is_dir():
                    if (abs_path / "dbt_project.yml").exists() or (abs_path / "dbt_project.yaml").exists():
                        dbt_project_dir = abs_path
                        break
            except (OSError, RuntimeError):
                continue

        if dbt_project_dir is None:
            raise FileNotFoundError(
                "Could not find dbt_project directory. "
                "Set DBT_PROJECT_DIR or ensure dbt_project/ exists relative to the repo root."
            )

    dbt_project_dir_path = dbt_project_dir

    dbt_packages_dir = dbt_project_dir_path / "dbt_packages"
    dbt_utils_dir = dbt_packages_dir / "dbt_utils" if dbt_packages_dir.exists() else None

    if not dbt_packages_dir.exists() or not dbt_utils_dir or not dbt_utils_dir.exists():
        import subprocess

        logger = logging.getLogger("dagster_dbt")
        logger.warning("dbt packages not found. Running 'dbt deps' (15s timeout)...")
        try:
            result = subprocess.run(
                ["dbt", "deps", "--target", environment],
                cwd=dbt_project_dir_path,
                capture_output=True,
                text=True,
                timeout=15,
            )
            if result.returncode != 0:
                logger.warning(f"dbt deps failed (non-fatal): {result.stderr or result.stdout}")
            else:
                logger.info("dbt packages installed successfully")
        except subprocess.TimeoutExpired:
            logger.warning("dbt deps timed out (network may be unavailable), continuing without packages")
        except Exception as e:
            logger.warning(f"Could not install dbt packages (non-fatal): {e}")

    dbt_project = DbtProject(project_dir=dbt_project_dir_path, target=environment)
    dbt_project.prepare_if_dev()

    dbt_cli_resource = DbtCliResource(project_dir=dbt_project_dir_path)
    dbt_resource = dbt_cli_resource

    @dbt_assets(
        manifest=dbt_project.manifest_path,
        dagster_dbt_translator=CustomizedDagsterDbtTranslator(),
    )
    def full_dbt_assets(context: dg.AssetExecutionContext, dbt: DbtCliResource):
        dbt_packages_dir = dbt_project_dir_path / "dbt_packages"
        dbt_utils_dir = dbt_packages_dir / "dbt_utils" if dbt_packages_dir.exists() else None

        if not dbt_packages_dir.exists() or not dbt_utils_dir or not dbt_utils_dir.exists():
            context.log.info("dbt packages not found. Running 'dbt deps' before build...")
            try:
                deps_result = dbt.cli(["deps"], context=context).wait()
                return_code = getattr(deps_result, "return_code", None)
                if return_code not in (None, 0):
                    raise RuntimeError("dbt packages installation failed. Please run 'dbt deps' manually")
                context.log.info("dbt packages installed successfully")
            except Exception as e:
                raise RuntimeError(
                    "dbt packages are required but could not be installed. Please run 'dbt deps' manually"
                ) from e

        yield from dbt.cli(["build"], context=context).stream()
