import dagster as dg

from econ_data_platform.assets.ingestion.bls import (
    housing_pulse_raw,
    housing_inventory_raw,
)
from econ_data_platform.assets.ingestion.fred import fred_data_raw
from econ_data_platform.assets.ingestion.realtor import realtor_definitions
from econ_data_platform.resources.motherduck import motherduck_resource
from econ_data_platform.resources.fred import fred_resource
from econ_data_platform.assets.dbt.dbt import dbt_definitions


defs = dg.Definitions.merge(
    *dbt_definitions,
    *realtor_definitions,
    dg.Definitions(
        assets=[
            fred_data_raw,
            housing_pulse_raw,
            housing_inventory_raw,
        ],
        resources={
            "md": motherduck_resource,
            "fred": fred_resource,
        },
    ),
)
