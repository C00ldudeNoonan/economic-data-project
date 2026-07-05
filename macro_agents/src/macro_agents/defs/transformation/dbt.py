import os
from pathlib import Path

# Set GIT_PYTHON_REFRESH to quiet to suppress git executable errors in environments where git is not available
# This is needed because dagster-dbt imports GitPython, which requires git to be in PATH
os.environ.setdefault("GIT_PYTHON_REFRESH", "quiet")

import logging
from collections.abc import Mapping
from typing import Any

import dagster as dg
from dagster_dbt import (
    DagsterDbtTranslator,
    DbtCliResource,
    DbtCloudCredentials,
    DbtCloudWorkspace,
    DbtProject,
    build_dbt_cloud_polling_sensor,
    dbt_assets,
    dbt_cloud_assets,
)

logging.getLogger("dagster_dbt").setLevel(logging.DEBUG)
logger = logging.getLogger("dagster_dbt")

DBT_NASDAQ_EXCLUDE = (
    "stg_nasdaq_companies_prices "
    "nasdaq_companies_analysis_return "
    "nasdaq_companies_summary "
    "source:staging.nasdaq_companies_prices_raw"
)


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


environment = os.getenv("DBT_TARGET", "dev")
orchestration_mode = os.getenv("DBT_ORCHESTRATION_MODE", "local").lower()


def _first_env(*names: str) -> str | None:
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return None


def _truthy_env(*names: str) -> bool:
    value = _first_env(*names)
    return value is not None and value.lower() in {"1", "true", "t", "yes", "y", "on"}


def _required_int_env(*names: str) -> int:
    value = _first_env(*names)
    if value is None:
        joined_names = " or ".join(names)
        raise RuntimeError(f"Set {joined_names} to enable dbt Platform orchestration.")
    try:
        return int(value)
    except ValueError as error:
        joined_names = " or ".join(names)
        raise RuntimeError(f"{joined_names} must be an integer.") from error


def _required_env_var(*names: str) -> dg.EnvVar:
    for name in names:
        if os.getenv(name):
            return dg.EnvVar(name)
    else:
        joined_names = " or ".join(names)
        raise RuntimeError(f"Set {joined_names} to enable dbt Platform orchestration.")


dbt_platform_mode = orchestration_mode in {
    "dbt_platform",
    "dbt_cloud",
    "platform",
    "cloud",
}
dbt_platform_observe_only = dbt_platform_mode and _truthy_env(
    "DBT_PLATFORM_OBSERVE_ONLY", "DBT_CLOUD_OBSERVE_ONLY"
)


if dbt_platform_mode:
    dbt_cloud_credentials = DbtCloudCredentials(
        account_id=_required_int_env("DBT_PLATFORM_ACCOUNT_ID", "DBT_CLOUD_ACCOUNT_ID"),
        token=_required_env_var("DBT_PLATFORM_TOKEN", "DBT_CLOUD_API_TOKEN"),
        access_url=_first_env("DBT_PLATFORM_ACCESS_URL", "DBT_CLOUD_ACCESS_URL")
        or "https://cloud.getdbt.com",
    )

    dbt_resource = DbtCloudWorkspace(
        credentials=dbt_cloud_credentials,
        project_id=_required_int_env("DBT_PLATFORM_PROJECT_ID", "DBT_CLOUD_PROJECT_ID"),
        environment_id=_required_int_env(
            "DBT_PLATFORM_ENVIRONMENT_ID", "DBT_CLOUD_ENVIRONMENT_ID"
        ),
        adhoc_job_name=_first_env(
            "DBT_PLATFORM_ADHOC_JOB_NAME", "DBT_CLOUD_ADHOC_JOB_NAME"
        ),
    )

    dbt_platform_translator = CustomizedDagsterDbtTranslator()

    if dbt_platform_observe_only:
        full_dbt_assets = list(
            dbt_resource.load_asset_specs(
                select="fqn:*",
                exclude=DBT_NASDAQ_EXCLUDE,
                selector="",
                dagster_dbt_translator=dbt_platform_translator,
            )
        )
    else:

        @dbt_cloud_assets(
            workspace=dbt_resource,
            exclude=DBT_NASDAQ_EXCLUDE,
            dagster_dbt_translator=dbt_platform_translator,
        )
        def full_dbt_assets(context: dg.AssetExecutionContext, dbt: DbtCloudWorkspace):
            yield from dbt.cli(
                ["build"],
                dagster_dbt_translator=dbt_platform_translator,
                context=context,
            ).wait()

    dbt_cloud_polling_sensor = build_dbt_cloud_polling_sensor(
        workspace=dbt_resource,
        dagster_dbt_translator=dbt_platform_translator,
        minimum_interval_seconds=int(
            os.getenv("DBT_PLATFORM_POLL_INTERVAL_SECONDS", "60")
        ),
    )
else:
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

        possible_dbt_project_paths = [
            path for path in possible_dbt_project_paths if path is not None
        ]

        dbt_project_dir = None
        for path in possible_dbt_project_paths:
            try:
                abs_path = path.resolve()
                if abs_path.exists() and abs_path.is_dir():
                    if (abs_path / "dbt_project.yml").exists() or (
                        abs_path / "dbt_project.yaml"
                    ).exists():
                        dbt_project_dir = abs_path
                        break
            except (OSError, RuntimeError):
                continue

        if dbt_project_dir is None:
            raise FileNotFoundError(
                "Could not find dbt_project directory. "
                "Please ensure it exists relative to the repository root, or set "
                "DBT_PROJECT_DIR."
            )

    if dbt_project_dir is None:
        raise RuntimeError("dbt_project_dir was not resolved")

    dbt_project_dir_path = dbt_project_dir

    dbt_packages_dir = dbt_project_dir_path / "dbt_packages"
    dbt_utils_dir = (
        dbt_packages_dir / "dbt_utils" if dbt_packages_dir.exists() else None
    )

    if not dbt_packages_dir.exists() or not dbt_utils_dir or not dbt_utils_dir.exists():
        import subprocess

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
                logger.warning(
                    f"dbt deps failed (non-fatal): {result.stderr or result.stdout}"
                )
            else:
                logger.info("dbt packages installed successfully")
        except subprocess.TimeoutExpired:
            logger.warning(
                "dbt deps timed out (network may be unavailable), continuing without packages"
            )
        except Exception as e:
            logger.warning(f"Could not install dbt packages (non-fatal): {e}")

    dbt_project = DbtProject(
        project_dir=dbt_project_dir_path,
        target=environment,
    )

    dbt_project.prepare_if_dev()

    manifest_path = dbt_project.manifest_path
    if manifest_path and manifest_path.exists():
        logger.info("Using dbt manifest")

    dbt_resource = DbtCliResource(project_dir=dbt_project_dir_path)
    dbt_cloud_polling_sensor = None

    @dbt_assets(
        manifest=dbt_project.manifest_path,
        exclude=DBT_NASDAQ_EXCLUDE,
        dagster_dbt_translator=CustomizedDagsterDbtTranslator(),
    )
    def full_dbt_assets(context: dg.AssetExecutionContext, dbt: DbtCliResource):
        dbt_packages_dir = dbt_project_dir_path / "dbt_packages"
        dbt_utils_dir = (
            dbt_packages_dir / "dbt_utils" if dbt_packages_dir.exists() else None
        )

        if (
            not dbt_packages_dir.exists()
            or not dbt_utils_dir
            or not dbt_utils_dir.exists()
        ):
            context.log.info(
                "dbt packages not found. Running 'dbt deps' before build..."
            )
            try:
                deps_result = dbt.cli(["deps"], context=context).wait()
                return_code = getattr(deps_result, "return_code", None)
                if return_code not in (None, 0):
                    context.log.error("Failed to install dbt packages.")
                    raise RuntimeError(
                        "dbt packages installation failed. Please run 'dbt deps' manually"
                    )
                else:
                    context.log.info("dbt packages installed successfully")
            except Exception as e:
                context.log.error(f"Could not install dbt packages: {e}")
                raise RuntimeError(
                    "dbt packages are required but could not be installed. Please run 'dbt deps' manually"
                ) from e

        yield from dbt.cli(["build"], context=context).stream()
