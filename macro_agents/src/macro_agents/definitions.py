from pathlib import Path
import dagster as dg

from macro_agents.defs.resources.motherduck import motherduck_resource
from macro_agents.defs.resources.fred import fred_resource
from macro_agents.defs.resources.market_stack import marketstack_resource
from macro_agents.defs.transformation.dbt import dbt_cli_resource
from macro_agents.defs.agents.analysis_agent import (
    EconomicAnalyzer,
    sector_inflation_analysis,
    sector_inflation_specific_analysis,
)
from macro_agents.defs.agents.economic_cycle_analyzer import (
    EconomicCycleAnalyzer,
    economic_cycle_analysis,
)
from macro_agents.defs.agents.enhanced_economic_cycle_analyzer import (
    EnhancedEconomicCycleAnalyzer,
)
from macro_agents.defs.agents.asset_allocation_analyzer import (
    AssetAllocationAnalyzer,
    asset_allocation_recommendations,
    custom_asset_allocation,
)
from macro_agents.defs.agents.economic_dashboard import economic_monitoring_alerts
from macro_agents.defs.agents.dspy_evaluation import FinancialEvaluator, PromptOptimizer
from macro_agents.defs.agents.backtesting import BacktestingEngine
from macro_agents.defs.agents.model_improvement_pipeline import ModelImprovementPipeline
from macro_agents.defs.agents.backtesting_visualization import BacktestingVisualizer
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
        sector_inflation_analysis,
        sector_inflation_specific_analysis,
        economic_cycle_analysis,
        asset_allocation_recommendations,
        custom_asset_allocation,
        economic_monitoring_alerts,
    ],
    resources={
        "md": motherduck_resource,
        "fred": fred_resource,
        "marketstack": marketstack_resource,
        "dbt": dbt_cli_resource,
        "sling": sling_resource,
        "analyzer": EconomicAnalyzer(
            model_name=dg.EnvVar("MODEL_NAME"),
            openai_api_key=dg.EnvVar("OPENAI_API_KEY"),
        ),
        "cycle_analyzer": EconomicCycleAnalyzer(
            model_name=dg.EnvVar("MODEL_NAME"),
            openai_api_key=dg.EnvVar("OPENAI_API_KEY"),
        ),
        "enhanced_cycle_analyzer": EnhancedEconomicCycleAnalyzer(
            model_name=dg.EnvVar("MODEL_NAME"),
            openai_api_key=dg.EnvVar("OPENAI_API_KEY"),
        ),
        "allocation_analyzer": AssetAllocationAnalyzer(
            model_name=dg.EnvVar("MODEL_NAME"),
            openai_api_key=dg.EnvVar("OPENAI_API_KEY"),
        ),
        "evaluator": FinancialEvaluator(
            model_name=dg.EnvVar("MODEL_NAME"),
            openai_api_key=dg.EnvVar("OPENAI_API_KEY"),
        ),
        "optimizer": PromptOptimizer(
            model_name=dg.EnvVar("MODEL_NAME"),
            openai_api_key=dg.EnvVar("OPENAI_API_KEY"),
        ),
        "backtesting_engine": BacktestingEngine(
            model_name=dg.EnvVar("MODEL_NAME"),
            openai_api_key=dg.EnvVar("OPENAI_API_KEY"),
        ),
        "model_pipeline": ModelImprovementPipeline(
            model_name=dg.EnvVar("MODEL_NAME"),
            openai_api_key=dg.EnvVar("OPENAI_API_KEY"),
        ),
        "visualizer": BacktestingVisualizer(),
    },
    schedules=schedules,
    sensors=sensors,
    jobs=list(jobs.values()),
)
