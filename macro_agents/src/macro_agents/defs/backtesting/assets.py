from macro_agents.defs.backtesting.backtest_asset_class_relationship_analyzer import (
    backtest_analyze_asset_class_relationships,
)
from macro_agents.defs.backtesting.backtest_economy_state_analyzer import (
    backtest_analyze_economy_state,
)
from macro_agents.defs.backtesting.backtest_evaluator import (
    evaluate_backtest_recommendations,
)
from macro_agents.defs.backtesting.backtest_investment_recommendations import (
    backtest_generate_investment_recommendations,
)
from macro_agents.defs.backtesting.backtest_optimizer import (
    auto_promote_best_models_to_production,
    optimize_dspy_modules,
    prepare_optimization_training_data,
    promote_optimized_model_to_production,
)


backtesting_assets = [
    backtest_analyze_economy_state,
    backtest_analyze_asset_class_relationships,
    backtest_generate_investment_recommendations,
    evaluate_backtest_recommendations,
    prepare_optimization_training_data,
    optimize_dspy_modules,
    promote_optimized_model_to_production,
    auto_promote_best_models_to_production,
]
