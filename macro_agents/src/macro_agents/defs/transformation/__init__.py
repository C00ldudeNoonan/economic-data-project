import dagster as dg

from macro_agents.defs.transformation.dbt import dbt_cli_resource, full_dbt_assets
from macro_agents.defs.transformation.checks import transformation_checks
from macro_agents.defs.transformation.financial_condition_index import (
    fci_weights_config,
    financial_conditions_index,
)


dbt_models_job = dg.define_asset_job(
    name="dbt_models_job",
    tags={"dagster/priority": "7", "dagster/max_runtime": 1800},
    selection=dg.AssetSelection.groups(
        "staging", "government", "markets", "commodities", "analysis", "data_quality"
    )
    - dg.AssetSelection.groups("backtesting"),
    description=(
        "Run all dbt models excluding backtesting models. This job should run "
        "before DSPy analysis jobs."
    ),
)

dbt_staging_models_job = dg.define_asset_job(
    name="dbt_staging_models_job",
    tags={"dagster/priority": "7", "dagster/max_runtime": 1800},
    selection=dg.AssetSelection.groups("staging"),
    description="Run dbt staging models.",
)

dbt_staging_telemetry_models_job = dg.define_asset_job(
    name="dbt_staging_telemetry_models_job",
    tags={"dagster/priority": "7", "dagster/max_runtime": 1800},
    selection=dg.AssetSelection.groups("staging_telemetry"),
    description="Run dbt staging telemetry models.",
)

dbt_government_models_job = dg.define_asset_job(
    name="dbt_government_models_job",
    tags={"dagster/priority": "7", "dagster/max_runtime": 1800},
    selection=dg.AssetSelection.groups("government"),
    description="Run dbt government models.",
)

dbt_markets_models_job = dg.define_asset_job(
    name="dbt_markets_models_job",
    tags={"dagster/priority": "7", "dagster/max_runtime": 1800},
    selection=dg.AssetSelection.groups("markets"),
    description="Run dbt markets models.",
)

dbt_commodities_models_job = dg.define_asset_job(
    name="dbt_commodities_models_job",
    tags={"dagster/priority": "7", "dagster/max_runtime": 1800},
    selection=dg.AssetSelection.groups("commodities"),
    description="Run dbt commodities models.",
)

dbt_analysis_models_job = dg.define_asset_job(
    name="dbt_analysis_models_job",
    tags={"dagster/priority": "7", "dagster/max_runtime": 1800},
    selection=dg.AssetSelection.groups("analysis"),
    description="Run dbt analysis models.",
)

dbt_analytics_telemetry_models_job = dg.define_asset_job(
    name="dbt_analytics_telemetry_models_job",
    tags={"dagster/priority": "7", "dagster/max_runtime": 1800},
    selection=dg.AssetSelection.groups("analytics_telemetry"),
    description="Run dbt analytics telemetry models.",
)

dbt_agents_preprocess_models_job = dg.define_asset_job(
    name="dbt_agents_preprocess_models_job",
    tags={"dagster/priority": "7", "dagster/max_runtime": 1800},
    selection=dg.AssetSelection.groups("agents_preprocess"),
    description="Run dbt agent preprocessing models.",
)

dbt_data_quality_models_job = dg.define_asset_job(
    name="dbt_data_quality_models_job",
    tags={"dagster/priority": "7", "dagster/max_runtime": 1800},
    selection=dg.AssetSelection.groups("data_quality"),
    description="Run dbt data quality anomaly detection models.",
)

dbt_backtesting_models_job = dg.define_asset_job(
    name="dbt_backtesting_models_job",
    tags={"dagster/priority": "-5", "dagster/max_runtime": 3600},
    selection=dg.AssetSelection.groups("backtesting")
    - dg.AssetSelection.assets(
        "backtest_analyze_economy_state",
        "backtest_analyze_asset_class_relationships",
        "backtest_generate_investment_recommendations",
        "evaluate_backtest_recommendations",
    ),
    description=(
        "Run only dbt backtesting snapshot models (excludes DSPy backtesting assets)."
    ),
)


defs = dg.Definitions(
    assets=[full_dbt_assets, financial_conditions_index, fci_weights_config],
    asset_checks=transformation_checks,
    jobs=[
        dbt_models_job,
        dbt_staging_models_job,
        dbt_staging_telemetry_models_job,
        dbt_government_models_job,
        dbt_markets_models_job,
        dbt_commodities_models_job,
        dbt_analysis_models_job,
        dbt_analytics_telemetry_models_job,
        dbt_agents_preprocess_models_job,
        dbt_data_quality_models_job,
        dbt_backtesting_models_job,
    ],
    resources={"dbt": dbt_cli_resource},
)
