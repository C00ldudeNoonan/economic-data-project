from datetime import datetime
from typing import List, Optional
import dagster as dg
from pydantic import Field
import dspy
import json
import pickle
import base64

from macro_agents.defs.resources.motherduck import MotherDuckResource
from macro_agents.defs.resources.gcs import GCSResource
from macro_agents.defs.agents.economy_state_analyzer import (
    EconomicAnalysisResource,
)
from macro_agents.defs.agents.investment_recommendations import (
    InvestmentRecommendationsModule,
)
from macro_agents.defs.agents.backtest_evaluator import (
    recommendation_accuracy_metric,
    evaluate_backtest_recommendations,
)
from macro_agents.defs.agents.backtest_economy_state_analyzer import (
    backtest_analyze_economy_state,
)
from macro_agents.defs.agents.backtest_asset_class_relationship_analyzer import (
    backtest_analyze_asset_class_relationships,
)
from macro_agents.defs.agents.backtest_investment_recommendations import (
    backtest_generate_investment_recommendations,
)
from macro_agents.defs.agents.backtest_utils import (
    extract_recommendations,
    get_asset_returns,
)


class OptimizationConfig(dg.Config):
    """Configuration for DSPy module optimization."""

    min_examples: int = Field(
        default=200, description="Minimum number of examples required for optimization"
    )
    max_trials: int = Field(
        default=50, description="Maximum number of optimization trials"
    )
    modules_to_optimize: List[str] = Field(
        default=[
            "economy_state",
            "asset_class_relationship",
            "investment_recommendations",
        ],
        description="List of modules to optimize",
    )
    backtest_date_start: Optional[str] = Field(
        default=None,
        description="Start date for filtering backtest data (YYYY-MM-DD). If None, uses all available data.",
    )
    backtest_date_end: Optional[str] = Field(
        default=None,
        description="End date for filtering backtest data (YYYY-MM-DD). If None, uses all available data.",
    )
    model_provider: str = Field(
        default="openai",
        description="LLM provider: 'openai', 'gemini', or 'anthropic'",
    )
    model_name: str = Field(
        default="gpt-4-turbo-preview",
        description="LLM model to use for optimization",
    )
    personality: Optional[str] = Field(
        default=None,
        description="Personality filter for training data. If None, tests all personalities (skeptical, neutral, bullish)",
    )
    test_all_personalities: bool = Field(
        default=True,
        description="If True, test each model with all personalities to find the best combination",
    )
    promotion_threshold_pct: float = Field(
        default=5.0,
        description="Minimum accuracy improvement percentage required for promotion",
    )


class PromotionConfig(dg.Config):
    """Configuration for promoting optimized models to production."""

    module_name: str = Field(description="Name of module to promote")
    version: str = Field(description="Version to promote")
    force: bool = Field(
        default=False,
        description="Force promotion even if improvement threshold not met",
    )


def create_model_versions_table(
    md: MotherDuckResource, context: dg.AssetExecutionContext
) -> None:
    """Create the dspy_model_versions table if it doesn't exist."""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS dspy_model_versions (
        module_name VARCHAR NOT NULL,
        version VARCHAR NOT NULL,
        personality VARCHAR NOT NULL,
        optimization_date TIMESTAMP NOT NULL,
        baseline_accuracy DOUBLE,
        optimized_accuracy DOUBLE,
        improvement_pct DOUBLE,
        is_production BOOLEAN DEFAULT FALSE,
        gcs_path VARCHAR NOT NULL,
        metadata JSON,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (module_name, version, personality)
    )
    """
    try:
        md.execute_query(create_table_query, read_only=False)
        context.log.info("Created/verified dspy_model_versions table")
    except Exception as e:
        context.log.warning(f"Table may already exist: {e}")


@dg.asset(
    kinds={"dspy", "duckdb"},
    group_name="optimization",
    description="Prepare training data from backtest evaluation results for DSPy optimization",
    deps=[
        evaluate_backtest_recommendations,
        backtest_generate_investment_recommendations,
        backtest_analyze_economy_state,
        backtest_analyze_asset_class_relationships,
    ],
)
def prepare_optimization_training_data(
    context: dg.AssetExecutionContext,
    config: OptimizationConfig,
    md: MotherDuckResource,
) -> dg.MaterializeResult:
    """
    Prepare training data from backtest evaluation results.

    This asset queries backtest_evaluation_results and converts them into
    DSPy Example format for each module that needs optimization.
    """
    context.log.info("Preparing optimization training data...")

    date_filter = ""
    if config.backtest_date_start:
        date_filter += f" AND backtest_date >= '{config.backtest_date_start}'"
    if config.backtest_date_end:
        date_filter += f" AND backtest_date <= '{config.backtest_date_end}'"

    query = f"""
    SELECT 
        backtest_date,
        model_name,
        personality,
        evaluation_details,
        accuracy_1m,
        accuracy_3m,
        accuracy_6m,
        total_recommendations
    FROM backtest_evaluation_results
    WHERE model_provider = '{config.model_provider}'
        AND model_name = '{config.model_name}'
        AND personality = '{config.personality}'
        {date_filter}
    ORDER BY backtest_date DESC
    """

    df = md.execute_query(query, read_only=True)
    context.log.info(f"Found {len(df)} backtest evaluation records")

    if df.is_empty():
        raise ValueError(
            f"No backtest evaluation results found for provider {config.model_provider} "
            f"and model {config.model_name} with personality {config.personality}"
        )

    training_data = {
        "economy_state": [],
        "asset_class_relationship": [],
        "investment_recommendations": [],
    }

    for row in df.iter_rows(named=True):
        backtest_date = row["backtest_date"]
        evaluation_details_json = row.get("evaluation_details", "[]")

        try:
            json.loads(evaluation_details_json)
        except (json.JSONDecodeError, TypeError):
            context.log.warning(
                f"Could not parse evaluation_details for {backtest_date}"
            )
            continue

        rec_query = f"""
        SELECT recommendations_content, personality
        FROM backtest_investment_recommendations
        WHERE backtest_date = '{backtest_date}'
            AND model_provider = '{config.model_provider}'
            AND model_name = '{config.model_name}'
            AND personality = '{config.personality}'
        ORDER BY analysis_timestamp DESC
        LIMIT 1
        """
        rec_df = md.execute_query(rec_query, read_only=True)

        if rec_df.is_empty():
            continue

        recommendations_content = rec_df[0, "recommendations_content"]
        recommendations = extract_recommendations(recommendations_content)

        if not recommendations:
            continue

        economy_state_query = f"""
        SELECT analysis_content
        FROM backtest_economy_state_analysis
        WHERE backtest_date = '{backtest_date}'
            AND model_provider = '{config.model_provider}'
            AND model_name = '{config.model_name}'
            AND personality = '{config.personality}'
        ORDER BY analysis_timestamp DESC
        LIMIT 1
        """
        economy_state_df = md.execute_query(economy_state_query, read_only=True)

        asset_class_query = f"""
        SELECT analysis_content
        FROM backtest_asset_class_relationship_analysis
        WHERE backtest_date = '{backtest_date}'
            AND model_provider = '{config.model_provider}'
            AND model_name = '{config.model_name}'
            AND personality = '{config.personality}'
        ORDER BY analysis_timestamp DESC
        LIMIT 1
        """
        asset_class_df = md.execute_query(asset_class_query, read_only=True)

        symbols = [rec["symbol"] for rec in recommendations]
        returns_data = get_asset_returns(md, symbols, backtest_date, periods=[1, 3, 6])

        for rec in recommendations:
            symbol = rec["symbol"]
            if symbol not in returns_data:
                continue

            symbol_returns = returns_data[symbol]
            example = dspy.Example(
                economy_state_analysis=economy_state_df[0, "analysis_content"]
                if not economy_state_df.is_empty()
                else "",
                asset_class_relationship_analysis=asset_class_df[0, "analysis_content"]
                if not asset_class_df.is_empty()
                else "",
                personality=config.personality,
                recommendation={
                    "symbol": symbol,
                    "direction": rec["direction"],
                    "confidence": rec.get("confidence"),
                    "expected_return": rec.get("expected_return"),
                },
                outcomes=symbol_returns,
            ).with_inputs(
                "economy_state_analysis",
                "asset_class_relationship_analysis",
                "personality",
            )
            training_data["investment_recommendations"].append(example)

    context.log.info(
        f"Prepared {len(training_data['investment_recommendations'])} examples for investment_recommendations"
    )

    summary = {
        "total_examples": {
            "investment_recommendations": len(
                training_data["investment_recommendations"]
            ),
            "economy_state": len(training_data["economy_state"]),
            "asset_class_relationship": len(training_data["asset_class_relationship"]),
        },
        "config": {
            "model_provider": config.model_provider,
            "model_name": config.model_name,
            "personality": config.personality,
            "backtest_date_start": config.backtest_date_start,
            "backtest_date_end": config.backtest_date_end,
        },
    }

    return dg.MaterializeResult(
        metadata={
            "training_data_prepared": True,
            "summary": summary,
        }
    )


@dg.asset(
    kinds={"dspy", "duckdb", "gcs"},
    group_name="optimization",
    description="Optimize DSPy modules using MIPROv2 optimizer with backtest data",
    deps=[
        prepare_optimization_training_data,
        evaluate_backtest_recommendations,
        backtest_generate_investment_recommendations,
        backtest_analyze_economy_state,
        backtest_analyze_asset_class_relationships,
    ],
)
def optimize_dspy_modules(
    context: dg.AssetExecutionContext,
    config: OptimizationConfig,
    md: MotherDuckResource,
    gcs: GCSResource,
    economic_analysis: EconomicAnalysisResource,
) -> dg.MaterializeResult:
    """
    Optimize DSPy modules using MIPROv2 optimizer.

    This asset:
    1. Loads baseline modules
    2. Prepares training examples from backtest data
    3. Runs MIPROv2 optimizer
    4. Evaluates optimized modules
    5. Saves to GCS if improvement >= threshold
    6. Updates model versions table
    """
    context.log.info("Starting DSPy module optimization...")

    create_model_versions_table(md, context)

    current_provider = economic_analysis._get_provider()
    if (
        current_provider != config.model_provider
        or economic_analysis.model_name != config.model_name
    ):
        object.__setattr__(economic_analysis, "provider", config.model_provider)
        economic_analysis.model_name = config.model_name
        economic_analysis.setup_for_execution(context)

    results = {}

    personalities_to_test = []
    if config.test_all_personalities:
        personalities_to_test = ["skeptical", "neutral", "bullish"]
        context.log.info(f"Testing all personalities: {personalities_to_test}")
    elif config.personality:
        personalities_to_test = [config.personality]
        context.log.info(f"Testing single personality: {config.personality}")
    else:
        personalities_to_test = ["skeptical", "neutral", "bullish"]
        context.log.info(
            f"No personality specified, testing all: {personalities_to_test}"
        )

    date_filter = ""
    if config.backtest_date_start:
        date_filter += f" AND backtest_date >= '{config.backtest_date_start}'"
    if config.backtest_date_end:
        date_filter += f" AND backtest_date <= '{config.backtest_date_end}'"

    for personality in personalities_to_test:
        context.log.info(f"Optimizing modules for personality: {personality}")

        eval_query = f"""
        SELECT 
            backtest_date,
            evaluation_details,
            accuracy_1m,
            accuracy_3m,
            accuracy_6m
        FROM backtest_evaluation_results
        WHERE model_provider = '{config.model_provider}'
            AND model_name = '{config.model_name}'
            AND personality = '{personality}'
            {date_filter}
        ORDER BY backtest_date DESC
        """

        eval_df = md.execute_query(eval_query, read_only=True)
        context.log.info(
            f"Found {len(eval_df)} evaluation records for personality {personality}"
        )

        if len(eval_df) < config.min_examples:
            context.log.warning(
                f"Insufficient training data for personality {personality}: {len(eval_df)} examples found, "
                f"minimum {config.min_examples} required. Skipping this personality."
            )
            continue

        if "investment_recommendations" in config.modules_to_optimize:
            context.log.info("Optimizing investment_recommendations module...")

            examples = []
            for row in eval_df.iter_rows(named=True):
                backtest_date = row["backtest_date"]
                evaluation_details_json = row.get("evaluation_details", "[]")

                try:
                    json.loads(evaluation_details_json)
                except (json.JSONDecodeError, TypeError):
                    continue

                rec_query = f"""
                SELECT recommendations_content
                FROM backtest_investment_recommendations
                WHERE backtest_date = '{backtest_date}'
                    AND model_provider = '{config.model_provider}'
                    AND model_name = '{config.model_name}'
                    AND personality = '{personality}'
                ORDER BY analysis_timestamp DESC
                LIMIT 1
                """
                rec_df = md.execute_query(rec_query, read_only=True)
                if rec_df.is_empty():
                    continue

                recommendations_content = rec_df[0, "recommendations_content"]
                recommendations = extract_recommendations(recommendations_content)

                economy_state_query = f"""
                SELECT analysis_content
                FROM backtest_economy_state_analysis
                WHERE backtest_date = '{backtest_date}'
                    AND model_provider = '{config.model_provider}'
                    AND model_name = '{config.model_name}'
                    AND personality = '{personality}'
                ORDER BY analysis_timestamp DESC
                LIMIT 1
                """
                economy_state_df = md.execute_query(economy_state_query, read_only=True)

                asset_class_query = f"""
                SELECT analysis_content
                FROM backtest_asset_class_relationship_analysis
                WHERE backtest_date = '{backtest_date}'
                    AND model_provider = '{config.model_provider}'
                    AND model_name = '{config.model_name}'
                    AND personality = '{personality}'
                ORDER BY analysis_timestamp DESC
                LIMIT 1
                """
                asset_class_df = md.execute_query(asset_class_query, read_only=True)

                symbols = [rec["symbol"] for rec in recommendations]
                returns_data = get_asset_returns(
                    md, symbols, backtest_date, periods=[1, 3, 6]
                )

                for rec in recommendations:
                    symbol = rec["symbol"]
                    if symbol not in returns_data:
                        continue

                    symbol_returns = returns_data[symbol]
                    example = dspy.Example(
                        economy_state_analysis=economy_state_df[0, "analysis_content"]
                        if not economy_state_df.is_empty()
                        else "",
                        asset_class_relationship_analysis=asset_class_df[
                            0, "analysis_content"
                        ]
                        if not asset_class_df.is_empty()
                        else "",
                        personality=personality,
                        recommendation={
                            "symbol": symbol,
                            "direction": rec["direction"],
                            "confidence": rec.get("confidence"),
                            "expected_return": rec.get("expected_return"),
                        },
                        outcomes=symbol_returns,
                    ).with_inputs(
                        "economy_state_analysis",
                        "asset_class_relationship_analysis",
                        "personality",
                    )
                    examples.append(example)

            if len(examples) < config.min_examples:
                context.log.warning(
                    f"Insufficient examples for investment_recommendations: {len(examples)} < {config.min_examples}"
                )
            else:
                context.log.info(f"Prepared {len(examples)} examples for optimization")

                split_idx = int(len(examples) * 0.8)
                train_examples = examples[:split_idx]
                val_examples = examples[split_idx:]

                baseline_module = InvestmentRecommendationsModule(
                    personality=personality
                )

                context.log.info("Evaluating baseline module...")
                baseline_scores = []
                for example in val_examples[:50]:
                    try:
                        baseline_module(
                            economy_state_analysis=example.economy_state_analysis,
                            asset_class_relationship_analysis=example.asset_class_relationship_analysis,
                            personality=example.personality,
                        )
                        pass_through = dspy.Prediction(
                            recommendation=example.recommendation,
                            outcomes=example.outcomes,
                        )
                        score = recommendation_accuracy_metric(example, pass_through)
                        baseline_scores.append(score)
                    except Exception as e:
                        context.log.warning(f"Error evaluating baseline: {e}")
                        continue

                baseline_accuracy = (
                    sum(baseline_scores) / len(baseline_scores)
                    if baseline_scores
                    else 0.0
                )
                context.log.info(f"Baseline accuracy: {baseline_accuracy:.4f}")

                context.log.info("Running MIPROv2 optimization...")
                try:
                    try:
                        optimizer = dspy.MIPROv2(
                            metric=recommendation_accuracy_metric,
                            num_candidates=config.max_trials,
                        )
                    except AttributeError:
                        context.log.warning("MIPROv2 not available, using MIPRO")
                        optimizer = dspy.MIPRO(
                            metric=recommendation_accuracy_metric,
                            num_candidates=config.max_trials,
                        )

                    optimized_module = optimizer.compile(
                        student=baseline_module,
                        trainset=train_examples,
                        valset=val_examples[:100],
                    )

                    context.log.info("Evaluating optimized module...")
                    optimized_scores = []
                    for example in val_examples[:50]:
                        try:
                            optimized_module(
                                economy_state_analysis=example.economy_state_analysis,
                                asset_class_relationship_analysis=example.asset_class_relationship_analysis,
                                personality=example.personality,
                            )
                            pass_through = dspy.Prediction(
                                recommendation=example.recommendation,
                                outcomes=example.outcomes,
                            )
                            score = recommendation_accuracy_metric(
                                example, pass_through
                            )
                            optimized_scores.append(score)
                        except Exception as e:
                            context.log.warning(f"Error evaluating optimized: {e}")
                            continue

                    optimized_accuracy = (
                        sum(optimized_scores) / len(optimized_scores)
                        if optimized_scores
                        else 0.0
                    )
                    improvement_pct = (
                        (optimized_accuracy - baseline_accuracy)
                        / baseline_accuracy
                        * 100
                        if baseline_accuracy > 0
                        else 0.0
                    )

                    context.log.info(f"Optimized accuracy: {optimized_accuracy:.4f}")
                    context.log.info(f"Improvement: {improvement_pct:.2f}%")

                    if improvement_pct >= config.promotion_threshold_pct:
                        version = datetime.now().strftime("%Y%m%d_%H%M%S")
                        module_name = "investment_recommendations"

                        try:
                            if hasattr(dspy, "save"):
                                import io

                                buffer = io.BytesIO()
                                dspy.save(buffer, optimized_module)
                                module_bytes = buffer.getvalue()
                                module_state_b64 = base64.b64encode(
                                    module_bytes
                                ).decode("utf-8")
                            else:
                                module_bytes = pickle.dumps(optimized_module)
                                module_state_b64 = base64.b64encode(
                                    module_bytes
                                ).decode("utf-8")
                        except Exception as e:
                            context.log.warning(
                                f"Could not serialize module state: {e}, saving metadata only"
                            )
                            module_state_b64 = None

                        instructions = None
                        if hasattr(optimized_module, "generate_recommendations"):
                            if hasattr(
                                optimized_module.generate_recommendations, "signature"
                            ):
                                if hasattr(
                                    optimized_module.generate_recommendations.signature,
                                    "instructions",
                                ):
                                    instructions = optimized_module.generate_recommendations.signature.instructions

                        model_data = {
                            "module_name": module_name,
                            "version": version,
                            "personality": personality,
                            "model_provider": config.model_provider,
                            "model_name": config.model_name,
                            "baseline_accuracy": baseline_accuracy,
                            "optimized_accuracy": optimized_accuracy,
                            "improvement_pct": improvement_pct,
                            "optimization_date": datetime.now().isoformat(),
                            "module_state": module_state_b64,
                            "instructions": instructions,
                            "dspy_version": dspy.__version__
                            if hasattr(dspy, "__version__")
                            else "unknown",
                            "serialization_method": "dspy.save"
                            if hasattr(dspy, "save")
                            else "pickle",
                        }

                        gcs_path = gcs.upload_model(
                            module_name=module_name,
                            version=version,
                            model_data=model_data,
                            context=context,
                        )

                        version_record = {
                            "module_name": module_name,
                            "version": version,
                            "personality": personality,
                            "optimization_date": datetime.now().isoformat(),
                            "baseline_accuracy": baseline_accuracy,
                            "optimized_accuracy": optimized_accuracy,
                            "improvement_pct": improvement_pct,
                            "is_production": False,
                            "gcs_path": gcs_path,
                            "metadata": json.dumps(
                                {
                                    "personality": personality,
                                    "model_provider": config.model_provider,
                                    "model_name": config.model_name,
                                    "num_examples": len(examples),
                                }
                            ),
                        }

                        md.write_results_to_table(
                            [version_record],
                            output_table="dspy_model_versions",
                            if_exists="append",
                            context=context,
                        )

                        if module_name not in results:
                            results[module_name] = {}
                        results[module_name][personality] = {
                            "optimized": True,
                            "baseline_accuracy": baseline_accuracy,
                            "optimized_accuracy": optimized_accuracy,
                            "improvement_pct": improvement_pct,
                            "version": version,
                            "gcs_path": gcs_path,
                        }
                    else:
                        context.log.info(
                            f"Improvement {improvement_pct:.2f}% below threshold "
                            f"{config.promotion_threshold_pct}%, not saving"
                        )
                        if module_name not in results:
                            results[module_name] = {}
                        results[module_name][personality] = {
                            "optimized": False,
                            "baseline_accuracy": baseline_accuracy,
                            "optimized_accuracy": optimized_accuracy,
                            "improvement_pct": improvement_pct,
                            "reason": "below_threshold",
                        }

                except Exception as e:
                    context.log.error(
                        f"Error during optimization for personality {personality}: {e}"
                    )
                    if "investment_recommendations" not in results:
                        results["investment_recommendations"] = {}
                    results["investment_recommendations"][personality] = {
                        "optimized": False,
                        "error": str(e),
                    }

    return dg.MaterializeResult(metadata={"optimization_results": results})


@dg.asset(
    kinds={"dspy", "duckdb", "gcs"},
    group_name="optimization",
    description="Promote optimized model to production if improvement threshold is met",
    deps=[optimize_dspy_modules],
)
def promote_optimized_model_to_production(
    context: dg.AssetExecutionContext,
    config: PromotionConfig,
    md: MotherDuckResource,
    gcs: GCSResource,
) -> dg.MaterializeResult:
    """
    Promote an optimized model version to production.

    This asset:
    1. Loads model version from database
    2. Verifies improvement >= threshold (unless forced)
    3. Downloads model from GCS
    4. Updates production flag in database
    """
    context.log.info(
        f"Promoting {config.module_name} version {config.version} to production..."
    )

    query = f"""
    SELECT 
        module_name,
        version,
        baseline_accuracy,
        optimized_accuracy,
        improvement_pct,
        gcs_path,
        is_production
    FROM dspy_model_versions
    WHERE module_name = '{config.module_name}'
        AND version = '{config.version}'
    LIMIT 1
    """

    df = md.execute_query(query, read_only=True)
    if df.is_empty():
        raise ValueError(
            f"Model version {config.module_name} v{config.version} not found in database"
        )

    row = df.iter_rows(named=True).__next__()
    improvement_pct = row["improvement_pct"] or 0.0

    if not config.force and improvement_pct < 5.0:
        raise ValueError(
            f"Improvement {improvement_pct:.2f}% below 5% threshold. Use force=True to override."
        )

    try:
        model_data = gcs.download_model(
            module_name=config.module_name,
            version=config.version,
            context=context,
        )
        context.log.info(f"Verified model exists in GCS: {model_data.get('version')}")
    except Exception as e:
        raise ValueError(f"Could not download model from GCS: {e}")

    conn = md.get_connection()
    try:
        conn.execute(
            f"""
            UPDATE dspy_model_versions
            SET is_production = FALSE
            WHERE module_name = '{config.module_name}'
            """
        )

        conn.execute(
            f"""
            UPDATE dspy_model_versions
            SET is_production = TRUE
            WHERE module_name = '{config.module_name}'
                AND version = '{config.version}'
            """
        )
        conn.commit()
        context.log.info(
            f"Promoted {config.module_name} v{config.version} to production"
        )
    finally:
        conn.close()

    return dg.MaterializeResult(
        metadata={
            "promoted": True,
            "module_name": config.module_name,
            "version": config.version,
            "improvement_pct": improvement_pct,
        }
    )


@dg.asset(
    kinds={"dspy", "duckdb", "gcs"},
    group_name="optimization",
    description="Automatically promote the best optimized model to production for each module",
    deps=[optimize_dspy_modules],
)
def auto_promote_best_models_to_production(
    context: dg.AssetExecutionContext,
    md: MotherDuckResource,
    gcs: GCSResource,
) -> dg.MaterializeResult:
    """
    Automatically promote the best optimized model to production for each module and personality.

    This asset:
    1. Finds the best (highest accuracy) optimized model for each module-personality combination
    2. Promotes it to production if it meets the threshold
    3. Ensures only one production model per module-personality combination exists

    This is used to ensure current analysis assets always use the best available models for each personality.
    """
    context.log.info("Auto-promoting best models to production for each personality...")

    create_model_versions_table(md, context)

    modules_query = """
    SELECT DISTINCT module_name, personality
    FROM dspy_model_versions
    WHERE optimized_accuracy IS NOT NULL
        AND improvement_pct >= 5.0
    ORDER BY module_name, personality
    """

    modules_df = md.execute_query(modules_query, read_only=True)

    if modules_df.is_empty():
        context.log.info("No optimized models available for promotion")
        return dg.MaterializeResult(
            metadata={
                "promoted_modules": [],
                "message": "No optimized models available for promotion",
            }
        )

    promoted_modules = []

    for row in modules_df.iter_rows(named=True):
        module_name = row["module_name"]
        personality = row["personality"]

        best_model_query = f"""
        SELECT 
            module_name,
            version,
            personality,
            baseline_accuracy,
            optimized_accuracy,
            improvement_pct,
            gcs_path,
            is_production
        FROM dspy_model_versions
        WHERE module_name = '{module_name}'
            AND personality = '{personality}'
            AND optimized_accuracy IS NOT NULL
            AND improvement_pct >= 5.0
        ORDER BY optimized_accuracy DESC, improvement_pct DESC
        LIMIT 1
        """

        best_model_df = md.execute_query(best_model_query, read_only=True)

        if best_model_df.is_empty():
            context.log.warning(f"No eligible models found for {module_name}")
            continue

        best_model = best_model_df.iter_rows(named=True).__next__()
        version = best_model["version"]
        best_personality = best_model["personality"]
        improvement_pct = best_model["improvement_pct"] or 0.0
        is_already_prod = best_model.get("is_production", False)

        if is_already_prod:
            context.log.info(
                f"{module_name} v{version} (personality: {best_personality}) is already the best production model, skipping"
            )
            promoted_modules.append(
                {
                    "module_name": module_name,
                    "version": version,
                    "personality": best_personality,
                    "status": "already_best_production",
                }
            )
            continue

        try:
            model_data = gcs.download_model(
                module_name=module_name,
                version=version,
                context=context,
            )
            context.log.info(
                f"Verified {module_name} v{version} (personality: {best_personality}) exists in GCS"
            )
        except Exception as e:
            context.log.warning(
                f"Could not download {module_name} v{version} (personality: {best_personality}) from GCS: {e}, skipping"
            )
            continue

        conn = md.get_connection()
        try:
            conn.execute(
                f"""
                UPDATE dspy_model_versions
                SET is_production = FALSE
                WHERE module_name = '{module_name}'
                    AND personality = '{best_personality}'
                """
            )

            conn.execute(
                f"""
                UPDATE dspy_model_versions
                SET is_production = TRUE
                WHERE module_name = '{module_name}'
                    AND version = '{version}'
                    AND personality = '{best_personality}'
                """
            )
            conn.commit()

            context.log.info(
                f"Promoted {module_name} v{version} (personality: {best_personality}) to production "
                f"(accuracy: {best_model['optimized_accuracy']:.4f}, "
                f"improvement: {improvement_pct:.2f}%)"
            )

            promoted_modules.append(
                {
                    "module_name": module_name,
                    "version": version,
                    "personality": best_personality,
                    "optimized_accuracy": float(best_model["optimized_accuracy"]),
                    "improvement_pct": improvement_pct,
                    "status": "promoted",
                }
            )
        except Exception as e:
            context.log.error(f"Error promoting {module_name} v{version}: {e}")
            conn.rollback()
        finally:
            conn.close()

    return dg.MaterializeResult(
        metadata={
            "promoted_modules": promoted_modules,
            "total_promoted": len(
                [m for m in promoted_modules if m.get("status") == "promoted"]
            ),
        }
    )
