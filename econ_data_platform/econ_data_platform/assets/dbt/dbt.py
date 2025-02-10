from pathlib import Path

from dagster_dbt import DbtProject, dbt_assets, DbtCliResource
import dagster as dg

dbt_project = DbtProject(
    project_dir=Path(__file__).joinpath("..", "..", "..").resolve(),
)

dbt_project.prepare_if_dev()


@dbt_assets(manifest=dbt_project.manifest_path)
def full_dbt_assets(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


dbt_defs = dg.Definitions(
    assets=[full_dbt_assets],
    resources={"dbt": DbtCliResource(project_dir=dbt_project)},
)