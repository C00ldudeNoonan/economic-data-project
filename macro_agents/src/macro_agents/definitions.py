from pathlib import Path
import dagster as dg

from .defs.resources.motherduck import motherduck_resource
from .defs.resources.fred import fred_resource
from .defs.assets.ingestion.fred import fred_raw
from .defs.assets.ingestion.bls import housing_inventory_raw, housing_pulse_raw
from .defs.assets.ingestion.treasury_yields import treasury_yields_raw
from .defs.resources.market_stack import marketstack_resource
from .defs.assets.ingestion.market_stack import global_markets_raw, fixed_income_etfs_raw, us_sector_etfs_raw, major_indices_raw, currency_etfs_raw, us_sector_etfs_raw





@dg.definitions
def defs():
    return dg.Definitions(
        assets=[
            fred_raw,
            housing_inventory_raw,
            housing_pulse_raw,
            treasury_yields_raw,
            global_markets_raw,
            fixed_income_etfs_raw,
            us_sector_etfs_raw, 
            major_indices_raw, 
            currency_etfs_raw
        ],
        resources={
            "md": motherduck_resource,
            "fred": fred_resource,
            "marketstack": marketstack_resource,  
        },
    )
