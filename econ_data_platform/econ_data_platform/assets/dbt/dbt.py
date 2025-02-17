from pathlib import Path

from dagster_dbt import DbtProject, dbt_assets, DbtCliResource, DagsterDbtTranslator
import dagster as dg
import os
from typing import Any, Optional
from collections.abc import Mapping

import logging
logging.getLogger('dagster_dbt').setLevel(logging.DEBUG)

class CustomizedDagsterDbtTranslator(DagsterDbtTranslator):
    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> Optional[str]:
        asset_path = dbt_resource_props["fqn"][1:-1]
        if asset_path:
            return "_".join(asset_path)
        return "default"

    def get_asset_key(self, dbt_resource_props):
        resource_type = dbt_resource_props["resource_type"]
        name = dbt_resource_props["name"]
        if resource_type == "source":
            return dg.AssetKey(name)
        else:
            return super().get_asset_key(dbt_resource_props)


environment = os.getenv("DBT_TARGET", "dev")



dbt_project = DbtProject(
    project_dir= Path(__file__).joinpath("..", "..", "..", "..", "..", "dbt_project").resolve(),
    target=environment
)

dbt_cli_resource = DbtCliResource(project_dir=dbt_project)


@dbt_assets(manifest=dbt_project.manifest_path,
            dagster_dbt_translator=CustomizedDagsterDbtTranslator())
def full_dbt_assets(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()



