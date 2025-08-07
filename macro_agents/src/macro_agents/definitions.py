from pathlib import Path
import dagster as dg

from .defs.resources.motherduck import motherduck_resource
from .defs.resources.fred import fred_resource
from .defs.assets.ingestion.fred import fred_data_raw
from .defs.assets.ingestion.bls import housing_inventory_raw, housing_pulse_raw
from .defs.assets.ingestion.treasury_yields import treasury_yields_raw


@dg.definitions
def defs():
    return dg.Definitions(
        assets=[
            fred_data_raw,
            housing_inventory_raw,
            housing_pulse_raw,
            treasury_yields_raw,
        ],
        resources={
            "md": motherduck_resource,
            "fred": fred_resource,
        },
    )
