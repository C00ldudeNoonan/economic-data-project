from pathlib import Path
import dagster as dg
import os

from macro_agents.defs.resources.motherduck import motherduck_resource
from macro_agents.defs.resources.fred import fred_resource
from macro_agents.defs.resources.market_stack import marketstack_resource
from macro_agents.defs.transformation.dbt import dbt_cli_resource
from macro_agents.defs.agents.analysis_agent import EconomicAnalyzer
from macro_agents.defs.agents.economic_cycle_analyzer import EconomicCycleAnalyzer
from macro_agents.defs.agents.enhanced_economic_cycle_analyzer import (
    EnhancedEconomicCycleAnalyzer,
)
from macro_agents.defs.agents.asset_allocation_analyzer import AssetAllocationAnalyzer
from macro_agents.defs.agents.dspy_evaluation import FinancialEvaluator, PromptOptimizer
from macro_agents.defs.agents.backtesting import BacktestingEngine
from macro_agents.defs.agents.model_improvement_pipeline import ModelImprovementPipeline
from macro_agents.defs.agents.backtesting_visualization import BacktestingVisualizer
from macro_agents.defs.schedules import schedules, sensors, jobs

# Find the project root by looking for pyproject.toml
# In local dev: use __file__ relative path
# In Dagster Cloud: use working directory (where pyproject.toml is bundled)
def find_project_root():
    # Try working directory first (for Dagster Cloud deployments)
    cwd = Path.cwd()
    if (cwd / "pyproject.toml").exists():
        return cwd
    
    # Try __file__ relative path (for local dev)
    file_root = Path(__file__).parent.parent
    if (file_root / "pyproject.toml").exists():
        return file_root
    
    # Fallback: use __file__ relative path anyway
    return file_root

project_root = find_project_root()

defs = dg.Definitions.merge(
    dg.load_from_defs_folder(project_root=project_root),
    dg.Definitions(
        resources={
            "md": motherduck_resource,
            "fred": fred_resource,
            "marketstack": marketstack_resource,
            "dbt": dbt_cli_resource,
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
    ),
)
