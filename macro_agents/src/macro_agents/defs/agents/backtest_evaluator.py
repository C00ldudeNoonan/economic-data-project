from datetime import datetime
import dagster as dg
import json
import dspy

from macro_agents.defs.resources.motherduck import MotherDuckResource
from macro_agents.defs.agents.backtest_economy_state_analyzer import BacktestConfig
from macro_agents.defs.agents.backtest_investment_recommendations import (
    backtest_generate_investment_recommendations,
)
from macro_agents.defs.agents.backtest_utils import (
    extract_recommendations,
    get_asset_returns,
)


class RecommendationPassThroughModule(dspy.Module):
    """Simple DSPy module that passes through recommendations for evaluation."""

    def forward(self, recommendation, outcomes):
        """Pass through the recommendation and outcomes."""
        return dspy.Prediction(
            recommendation=recommendation,
            outcomes=outcomes,
        )


def recommendation_accuracy_metric(example, prediction, trace=None):
    """
    Custom metric function to evaluate recommendation accuracy vs SPY benchmark.

    This metric is used with dspy.evaluate() to assess recommendation performance.

    Args:
        example: Contains recommendation (direction, symbol) and actual outcomes
        prediction: Prediction from the module (contains recommendation and outcomes)
        trace: Optional trace information

    Returns:
        Score (0-1) representing accuracy, or detailed dict for aggregation
    """
    if hasattr(prediction, "recommendation"):
        recommendation = prediction.recommendation
        outcomes = prediction.outcomes
    else:
        recommendation = example.get("recommendation", {})
        outcomes = example.get("outcomes", {})

    direction = recommendation.get("direction")
    symbol = recommendation.get("symbol")

    if not direction or not symbol:
        return 0.0

    results = {}
    total_hits = 0
    total_periods = 0

    for period_key, period_data in outcomes.items():
        if period_data.get("actual_return") is None:
            continue

        actual_return = period_data.get("actual_return", 0.0)
        spy_return = period_data.get("spy_return", 0.0)
        outperformance = actual_return - spy_return

        if direction == "OVERWEIGHT":
            is_hit = outperformance > 0
        elif direction == "UNDERWEIGHT":
            is_hit = outperformance < 0
        else:
            is_hit = False

        results[period_key] = {
            "is_hit": is_hit,
            "actual_return": actual_return,
            "spy_return": spy_return,
            "outperformance": outperformance,
        }

        if is_hit:
            total_hits += 1
        total_periods += 1

    score = total_hits / total_periods if total_periods > 0 else 0.0

    if not hasattr(example, "_metric_details"):
        example._metric_details = {}
    example._metric_details[symbol] = {
        "score": score,
        "total_hits": total_hits,
        "total_periods": total_periods,
        "period_results": results,
        "symbol": symbol,
        "direction": direction,
    }

    return score


@dg.asset(
    kinds={"dspy", "duckdb"},
    group_name="backtesting",
    description="Evaluate backtest investment recommendations using DSPy evaluation framework",
    deps=[
        backtest_generate_investment_recommendations,
        dg.AssetKey(["us_sector_analysis_return"]),
    ],
)
def evaluate_backtest_recommendations(
    context: dg.AssetExecutionContext,
    config: BacktestConfig,
    md: MotherDuckResource,
) -> dg.MaterializeResult:
    """
    Asset that evaluates backtest investment recommendations using DSPy's evaluation framework.

    This asset:
    1. Retrieves recommendations from backtest_investment_recommendations
    2. Extracts specific asset recommendations
    3. Gets actual returns for 1m, 3m, 6m periods
    4. Uses DSPy.Evaluate with custom metric
    5. Stores evaluation results

    Returns:
        Dictionary with evaluation metadata and results
    """
    context.log.info(
        f"Starting evaluation of backtest recommendations for {config.backtest_date} "
        f"with model {config.model_name}..."
    )

    query = f"""
    SELECT recommendations_content
    FROM backtest_investment_recommendations
    WHERE backtest_date = '{config.backtest_date}'
        AND model_name = '{config.model_name}'
    ORDER BY analysis_timestamp DESC
    LIMIT 1
    """

    df = md.execute_query(query, read_only=True)
    if df.is_empty():
        raise ValueError(
            f"No backtest recommendations found for {config.backtest_date} "
            f"with model {config.model_name}"
        )

    recommendations_content = df[0, "recommendations_content"]

    recommendations = extract_recommendations(recommendations_content)
    context.log.info(f"Extracted {len(recommendations)} recommendations")

    if not recommendations:
        context.log.warning("No specific recommendations found in content")
        evaluation_timestamp = datetime.now()
        result_metadata = {
            "evaluation_completed": True,
            "evaluation_timestamp": evaluation_timestamp.isoformat(),
            "backtest_date": config.backtest_date,
            "model_name": config.model_name,
            "total_recommendations": 0,
            "hits_1m": 0,
            "misses_1m": 0,
            "accuracy_1m": 0.0,
            "hits_3m": 0,
            "misses_3m": 0,
            "accuracy_3m": 0.0,
            "hits_6m": 0,
            "misses_6m": 0,
            "accuracy_6m": 0.0,
            "avg_outperformance_1m": 0.0,
            "avg_outperformance_3m": 0.0,
            "avg_outperformance_6m": 0.0,
            "evaluation_details": [],
        }
        return dg.MaterializeResult(metadata=result_metadata)

    symbols = [rec["symbol"] for rec in recommendations]
    returns_data = get_asset_returns(
        md, symbols, config.backtest_date, periods=[1, 3, 6]
    )

    examples = []
    for rec in recommendations:
        symbol = rec["symbol"]
        if symbol not in returns_data:
            context.log.warning(f"No return data found for {symbol}")
            continue

        symbol_returns = returns_data[symbol]
        example = dspy.Example(
            recommendation={
                "symbol": symbol,
                "direction": rec["direction"],
                "confidence": rec.get("confidence"),
                "expected_return": rec.get("expected_return"),
            },
            outcomes=symbol_returns,
        ).with_inputs("recommendation", "outcomes")
        examples.append(example)

    if not examples:
        context.log.warning("No examples with return data found")
        result_metadata = {
            "evaluation_completed": True,
            "evaluation_timestamp": datetime.now().isoformat(),
            "backtest_date": config.backtest_date,
            "model_name": config.model_name,
            "total_recommendations": len(recommendations),
            "hits_1m": 0,
            "misses_1m": 0,
            "accuracy_1m": 0.0,
            "hits_3m": 0,
            "misses_3m": 0,
            "accuracy_3m": 0.0,
            "hits_6m": 0,
            "misses_6m": 0,
            "accuracy_6m": 0.0,
            "avg_outperformance_1m": 0.0,
            "avg_outperformance_3m": 0.0,
            "avg_outperformance_6m": 0.0,
            "evaluation_details": [],
        }
        return dg.MaterializeResult(metadata=result_metadata)

    context.log.info(f"Running evaluation on {len(examples)} examples...")

    module = RecommendationPassThroughModule()

    evaluation_results = []
    for example in examples:
        prediction = module(
            recommendation=example.recommendation, outcomes=example.outcomes
        )

        score = recommendation_accuracy_metric(example, prediction)

        if hasattr(example, "_metric_details") and example._metric_details:
            detail = list(example._metric_details.values())[0]
            evaluation_results.append(detail)
        else:
            recommendation = example.recommendation
            evaluation_results.append(
                {
                    "score": score if isinstance(score, (int, float)) else 0.0,
                    "symbol": recommendation.get("symbol"),
                    "direction": recommendation.get("direction"),
                    "total_hits": 0,
                    "total_periods": 0,
                    "period_results": {},
                }
            )

    hits_1m = 0
    misses_1m = 0
    hits_3m = 0
    misses_3m = 0
    hits_6m = 0
    misses_6m = 0

    for r in evaluation_results:
        period_results = r.get("period_results", {})

        if "1m" in period_results:
            if period_results["1m"].get("is_hit", False):
                hits_1m += 1
            else:
                misses_1m += 1

        if "3m" in period_results:
            if period_results["3m"].get("is_hit", False):
                hits_3m += 1
            else:
                misses_3m += 1

        if "6m" in period_results:
            if period_results["6m"].get("is_hit", False):
                hits_6m += 1
            else:
                misses_6m += 1

    total_1m = hits_1m + misses_1m
    total_3m = hits_3m + misses_3m
    total_6m = hits_6m + misses_6m

    accuracy_1m = hits_1m / total_1m if total_1m > 0 else 0.0
    accuracy_3m = hits_3m / total_3m if total_3m > 0 else 0.0
    accuracy_6m = hits_6m / total_6m if total_6m > 0 else 0.0

    outperformance_1m = [
        r.get("period_results", {}).get("1m", {}).get("outperformance", 0.0)
        for r in evaluation_results
        if r.get("period_results", {}).get("1m", {}).get("outperformance") is not None
    ]
    avg_outperformance_1m = (
        sum(outperformance_1m) / len(outperformance_1m) if outperformance_1m else 0.0
    )

    outperformance_3m = [
        r.get("period_results", {}).get("3m", {}).get("outperformance", 0.0)
        for r in evaluation_results
        if r.get("period_results", {}).get("3m", {}).get("outperformance") is not None
    ]
    avg_outperformance_3m = (
        sum(outperformance_3m) / len(outperformance_3m) if outperformance_3m else 0.0
    )

    outperformance_6m = [
        r.get("period_results", {}).get("6m", {}).get("outperformance", 0.0)
        for r in evaluation_results
        if r.get("period_results", {}).get("6m", {}).get("outperformance") is not None
    ]
    avg_outperformance_6m = (
        sum(outperformance_6m) / len(outperformance_6m) if outperformance_6m else 0.0
    )

    evaluation_timestamp = datetime.now()

    evaluation_details = []
    for result in evaluation_results:
        detail = {
            "symbol": result.get("symbol"),
            "direction": result.get("direction"),
            "score": result.get("score", 0.0),
            "period_results": result.get("period_results", {}),
        }
        evaluation_details.append(detail)

    json_result = {
        "backtest_date": config.backtest_date,
        "model_name": config.model_name,
        "evaluation_timestamp": evaluation_timestamp.isoformat(),
        "total_recommendations": len(evaluation_results),
        "hits_1m": hits_1m,
        "misses_1m": misses_1m,
        "accuracy_1m": round(accuracy_1m, 4),
        "hits_3m": hits_3m,
        "misses_3m": misses_3m,
        "accuracy_3m": round(accuracy_3m, 4),
        "hits_6m": hits_6m,
        "misses_6m": misses_6m,
        "accuracy_6m": round(accuracy_6m, 4),
        "avg_outperformance_1m": round(avg_outperformance_1m, 4),
        "avg_outperformance_3m": round(avg_outperformance_3m, 4),
        "avg_outperformance_6m": round(avg_outperformance_6m, 4),
        "evaluation_details": json.dumps(evaluation_details),
        "dagster_run_id": context.run_id,
        "dagster_asset_key": str(context.asset_key),
    }

    context.log.info("Writing evaluation results to database...")
    md.write_results_to_table(
        [json_result],
        output_table="backtest_evaluation_results",
        if_exists="append",
        context=context,
    )

    result_metadata = {
        "evaluation_completed": True,
        "evaluation_timestamp": evaluation_timestamp.isoformat(),
        "backtest_date": config.backtest_date,
        "model_name": config.model_name,
        "total_recommendations": len(evaluation_results),
        "hits_1m": hits_1m,
        "misses_1m": misses_1m,
        "accuracy_1m": round(accuracy_1m, 4),
        "hits_3m": hits_3m,
        "misses_3m": misses_3m,
        "accuracy_3m": round(accuracy_3m, 4),
        "hits_6m": hits_6m,
        "misses_6m": misses_6m,
        "accuracy_6m": round(accuracy_6m, 4),
        "avg_outperformance_1m": round(avg_outperformance_1m, 4),
        "avg_outperformance_3m": round(avg_outperformance_3m, 4),
        "avg_outperformance_6m": round(avg_outperformance_6m, 4),
        "evaluation_details": evaluation_details,
    }

    context.log.info(f"Evaluation complete: {result_metadata}")
    return dg.MaterializeResult(metadata=result_metadata)
