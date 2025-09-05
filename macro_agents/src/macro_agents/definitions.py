from pathlib import Path
import dagster as dg

from .defs.resources.motherduck import motherduck_resource
from .defs.resources.fred import fred_resource
from .defs.resources.market_stack import marketstack_resource
from .defs.transformation.dbt import dbt_cli_resource

defs = dg.Definitions.merge(
    dg.load_from_defs_folder(project_root=Path(__file__).parent.parent),
    dg.Definitions(
        resources={
            "md": motherduck_resource,
            "fred": fred_resource,
            "marketstack": marketstack_resource,
            "dbt": dbt_cli_resource,
        },
    ),
)
