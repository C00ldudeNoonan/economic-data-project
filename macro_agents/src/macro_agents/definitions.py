from pathlib import Path
import dagster as dg

from macro_agents.defs.resources.motherduck import motherduck_resource
from macro_agents.defs.resources.fred import fred_resource
from macro_agents.defs.resources.market_stack import marketstack_resource
from macro_agents.defs.transformation.dbt import dbt_cli_resource
from macro_agents.defs.agents.economy_state_analyzer import (
    EconomicAnalysisResource,
    analyze_economy_state,
)
from macro_agents.defs.agents.asset_class_relationship_analyzer import (
    analyze_asset_class_relationships,
)
from macro_agents.defs.agents.investment_recommendations import (
    generate_investment_recommendations,
)
from macro_agents.defs.schedules import schedules, sensors, jobs
from macro_agents.defs.replication.sling import replication_assets, sling_resource
from macro_agents.defs.transformation.dbt import full_dbt_assets
from macro_agents.defs.transformation.financial_condition_index import (
    financial_conditions_index,
    fci_weights_config,
)
from macro_agents.defs.ingestion.market_stack import (
    us_sector_etfs_raw,
    currency_etfs_raw,
    major_indices_raw,
    fixed_income_etfs_raw,
    global_markets_raw,
    energy_commodities_raw,
    input_commodities_raw,
    agriculture_commodities_raw,
)
from macro_agents.defs.ingestion.fred import fred_raw
from macro_agents.defs.ingestion.treasury_yields import treasury_yields_raw
from macro_agents.defs.ingestion.bls import housing_inventory_raw, housing_pulse_raw


def find_project_root():
    cwd = Path.cwd()
    if (cwd / "pyproject.toml").exists():
        return cwd

    file_root = Path(__file__).parent.parent
    if (file_root / "pyproject.toml").exists():
        return file_root

    return file_root


project_root = find_project_root()

defs = dg.Definitions(
    assets=[
        replication_assets,
        full_dbt_assets,
        us_sector_etfs_raw,
        currency_etfs_raw,
        major_indices_raw,
        fixed_income_etfs_raw,
        global_markets_raw,
        energy_commodities_raw,
        input_commodities_raw,
        agriculture_commodities_raw,
        fred_raw,
        treasury_yields_raw,
        housing_inventory_raw,
        housing_pulse_raw,
        financial_conditions_index,
        fci_weights_config,
        analyze_economy_state,
        analyze_asset_class_relationships,
        generate_investment_recommendations,
    ],
    resources={
        "md": motherduck_resource,
        "fred": fred_resource,
        "marketstack": marketstack_resource,
        "dbt": dbt_cli_resource,
        "sling": sling_resource,
        "economic_analysis": EconomicAnalysisResource(
            model_name=dg.EnvVar("MODEL_NAME"),
            openai_api_key=dg.EnvVar("OPENAI_API_KEY"),
        ),
    },
    schedules=schedules,
    sensors=sensors,
    jobs=list(jobs.values()),
)
