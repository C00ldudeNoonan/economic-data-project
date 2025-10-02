from pathlib import Path
import dagster as dg

from macro_agents.defs.resources.motherduck import motherduck_resource
from macro_agents.defs.resources.fred import fred_resource
from macro_agents.defs.resources.market_stack import marketstack_resource
from macro_agents.defs.transformation.dbt import dbt_cli_resource
from macro_agents.defs.agents.analysis_agent import EconomicAnalyzer
from macro_agents.defs.agents.economic_cycle_analyzer import EconomicCycleAnalyzer
from macro_agents.defs.agents.asset_allocation_analyzer import AssetAllocationAnalyzer

defs = dg.Definitions.merge(
    dg.load_from_defs_folder(project_root=Path(__file__).parent.parent),
    dg.Definitions(
        resources={
            "md": motherduck_resource,
            "fred": fred_resource,
            "marketstack": marketstack_resource,
            "dbt": dbt_cli_resource,
            "analyzer": EconomicAnalyzer(
                model_name=dg.EnvVar.str("MODEL_NAME", default="gpt-4-turbo-preview"),
                openai_api_key=dg.EnvVar("OPENAI_API_KEY"),
            ),
            "cycle_analyzer": EconomicCycleAnalyzer(
                model_name=dg.EnvVar.str("MODEL_NAME", default="gpt-4-turbo-preview"),
                openai_api_key=dg.EnvVar("OPENAI_API_KEY"),
            ),
            "allocation_analyzer": AssetAllocationAnalyzer(
                model_name=dg.EnvVar.str("MODEL_NAME", default="gpt-4-turbo-preview"),
                openai_api_key=dg.EnvVar("OPENAI_API_KEY"),
            ),
        },
    ),
)
