import dspy
from typing import Optional, Dict, Any, List
import json
from datetime import datetime, timedelta
import dagster as dg
from pydantic import Field

from macro_agents.defs.resources.motherduck import MotherDuckResource


class FinancialPredictionSignature(dspy.Signature):
    """Signature for financial asset prediction with structured output."""

    economic_data: str = dspy.InputField(
        desc="CSV data containing latest economic indicators with current values and percentage changes"
    )

    market_data: str = dspy.InputField(
        desc="CSV data containing recent market performance across different asset classes and sectors"
    )

    prediction_horizon: str = dspy.InputField(
        desc="Time horizon for prediction (e.g., '30 days', '3 months', '1 year')"
    )

    predictions: str = dspy.OutputField(
        desc="""Structured JSON predictions including:
        {
            "overweight_assets": [
                {"symbol": "XLK", "confidence": 0.8, "expected_return": 0.05, "rationale": "..."},
                {"symbol": "SPY", "confidence": 0.7, "expected_return": 0.03, "rationale": "..."}
            ],
            "underweight_assets": [
                {"symbol": "XLE", "confidence": 0.6, "expected_return": -0.02, "rationale": "..."}
            ],
            "market_outlook": "bullish/bearish/neutral",
            "key_risks": ["risk1", "risk2"],
            "confidence_score": 0.75
        }"""
    )


class FinancialPredictionModule(dspy.Module):
    """DSPy module for financial predictions with structured output."""

    def __init__(self):
        super().__init__()
        self.predict = dspy.ChainOfThought(FinancialPredictionSignature)

    def forward(
        self, economic_data: str, market_data: str, prediction_horizon: str = "30 days"
    ):
        return self.predict(
            economic_data=economic_data,
            market_data=market_data,
            prediction_horizon=prediction_horizon,
        )


class FinancialMetrics:
    """Custom DSPy metrics for financial prediction evaluation."""

    def __init__(self):
        self.name = "Financial Prediction Metrics"

    def __call__(
        self, gts: List[Dict[str, Any]], preds: List[Dict[str, Any]], **kwargs
    ) -> float:
        """Calculate financial prediction accuracy metrics."""
        if not gts or not preds:
            return 0.0

        total_score = 0.0
        valid_predictions = 0

        for gt, pred in zip(gts, preds):
            try:
                # Parse prediction JSON
                if isinstance(pred.predictions, str):
                    pred_data = json.loads(pred.predictions)
                else:
                    pred_data = pred.predictions

                # Calculate directional accuracy
                directional_score = self._calculate_directional_accuracy(gt, pred_data)

                # Calculate return accuracy (how close predicted returns are to actual)
                return_score = self._calculate_return_accuracy(gt, pred_data)

                # Calculate confidence calibration
                confidence_score = self._calculate_confidence_calibration(gt, pred_data)

                # Weighted combination
                prediction_score = (
                    0.4 * directional_score
                    + 0.4 * return_score
                    + 0.2 * confidence_score
                )

                total_score += prediction_score
                valid_predictions += 1

            except (json.JSONDecodeError, KeyError, TypeError):
                continue

        return total_score / valid_predictions if valid_predictions > 0 else 0.0

    def _calculate_directional_accuracy(
        self, gt: Dict[str, Any], pred: Dict[str, Any]
    ) -> float:
        """Calculate how often prediction direction matches actual direction."""
        correct_directions = 0
        total_predictions = 0

        # Check overweight predictions
        for asset in pred.get("overweight_assets", []):
            symbol = asset["symbol"]
            if symbol in gt.get("actual_returns", {}):
                actual_return = gt["actual_returns"][symbol]
                if actual_return > 0:  # Correctly predicted positive return
                    correct_directions += 1
                total_predictions += 1

        # Check underweight predictions
        for asset in pred.get("underweight_assets", []):
            symbol = asset["symbol"]
            if symbol in gt.get("actual_returns", {}):
                actual_return = gt["actual_returns"][symbol]
                if actual_return < 0:  # Correctly predicted negative return
                    correct_directions += 1
                total_predictions += 1

        return correct_directions / total_predictions if total_predictions > 0 else 0.0

    def _calculate_return_accuracy(
        self, gt: Dict[str, Any], pred: Dict[str, Any]
    ) -> float:
        """Calculate how close predicted returns are to actual returns."""
        total_error = 0.0
        total_predictions = 0

        # Check overweight predictions
        for asset in pred.get("overweight_assets", []):
            symbol = asset["symbol"]
            if symbol in gt.get("actual_returns", {}):
                predicted_return = asset.get("expected_return", 0.0)
                actual_return = gt["actual_returns"][symbol]
                error = abs(predicted_return - actual_return)
                total_error += error
                total_predictions += 1

        # Check underweight predictions
        for asset in pred.get("underweight_assets", []):
            symbol = asset["symbol"]
            if symbol in gt.get("actual_returns", {}):
                predicted_return = asset.get("expected_return", 0.0)
                actual_return = gt["actual_returns"][symbol]
                error = abs(predicted_return - actual_return)
                total_error += error
                total_predictions += 1

        if total_predictions == 0:
            return 0.0

        # Convert error to accuracy (lower error = higher accuracy)
        avg_error = total_error / total_predictions
        return max(0.0, 1.0 - avg_error)  # Cap at 1.0

    def _calculate_confidence_calibration(
        self, gt: Dict[str, Any], pred: Dict[str, Any]
    ) -> float:
        """Calculate how well-calibrated the confidence scores are."""
        correct_high_confidence = 0
        total_high_confidence = 0

        # Check high confidence predictions
        for asset in pred.get("overweight_assets", []) + pred.get(
            "underweight_assets", []
        ):
            confidence = asset.get("confidence", 0.0)
            if confidence >= 0.7:  # High confidence threshold
                symbol = asset["symbol"]
                if symbol in gt.get("actual_returns", {}):
                    actual_return = gt["actual_returns"][symbol]
                    predicted_direction = (
                        1 if asset in pred.get("overweight_assets", []) else -1
                    )
                    actual_direction = 1 if actual_return > 0 else -1

                    if predicted_direction == actual_direction:
                        correct_high_confidence += 1
                    total_high_confidence += 1

        return (
            correct_high_confidence / total_high_confidence
            if total_high_confidence > 0
            else 0.0
        )


class FinancialEvaluator(dg.ConfigurableResource):
    """DSPy-based evaluator for financial predictions."""

    model_name: str = Field(
        default="gpt-4-turbo-preview", description="LLM model to use for evaluation"
    )
    openai_api_key: str = Field(description="OpenAI API key for DSPy")

    def setup_for_execution(self, context) -> None:
        """Initialize DSPy when the resource is used."""
        # Initialize DSPy
        lm = dspy.LM(model=self.model_name, api_key=self.openai_api_key)
        dspy.settings.configure(lm=lm)

        # Initialize modules
        self._prediction_module = FinancialPredictionModule()
        self._metrics = FinancialMetrics()

    @property
    def prediction_module(self):
        """Get prediction module."""
        return self._prediction_module

    @property
    def metrics(self):
        """Get metrics."""
        return self._metrics

    def create_training_examples(
        self, md_resource: MotherDuckResource, lookback_days: int = 90
    ) -> List[dspy.Example]:
        """Create training examples from historical data."""
        # Get historical analysis and actual returns
        end_date = datetime.now()
        start_date = end_date - timedelta(days=lookback_days)

        query = """
        SELECT 
            analysis_content,
            analysis_type,
            analysis_timestamp
        FROM economic_cycle_analysis
        WHERE analysis_timestamp BETWEEN ? AND ?
        ORDER BY analysis_timestamp
        """

        df = md_resource.execute_query(
            query,
            read_only=True,
            params=[start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")],
        )

        examples = []

        for row in df.iter_rows(named=True):
            if row["analysis_type"] == "economic_cycle":
                # This is a simplified example - in practice you'd need to match
                # with actual returns and create proper training examples
                example = dspy.Example(
                    economic_data="",  # Would need to fetch actual data
                    market_data="",  # Would need to fetch actual data
                    prediction_horizon="30 days",
                    predictions="",  # Would need to extract from analysis
                    actual_returns={},  # Would need to calculate actual returns
                ).with_inputs("economic_data", "market_data", "prediction_horizon")

                examples.append(example)

        return examples

    def evaluate_model(self, test_examples: List[dspy.Example]) -> Dict[str, float]:
        """Evaluate the model on test examples."""
        # Generate predictions
        predictions = []
        for example in test_examples:
            pred = self.prediction_module(
                economic_data=example.economic_data,
                market_data=example.market_data,
                prediction_horizon=example.prediction_horizon,
            )
            predictions.append(pred)

        # Calculate metrics
        ground_truths = [example for example in test_examples]

        # Financial metrics
        financial_score = self.metrics(ground_truths, predictions)

        # Additional metrics
        directional_accuracy = self._calculate_directional_accuracy(
            ground_truths, predictions
        )
        return_accuracy = self._calculate_return_accuracy(ground_truths, predictions)
        confidence_calibration = self._calculate_confidence_calibration(
            ground_truths, predictions
        )

        return {
            "financial_score": financial_score,
            "directional_accuracy": directional_accuracy,
            "return_accuracy": return_accuracy,
            "confidence_calibration": confidence_calibration,
            "overall_score": (
                financial_score
                + directional_accuracy
                + return_accuracy
                + confidence_calibration
            )
            / 4,
        }

    def _calculate_directional_accuracy(
        self, gts: List[dspy.Example], preds: List[dspy.Example]
    ) -> float:
        """Calculate directional accuracy."""
        return self.metrics._calculate_directional_accuracy(
            [{"actual_returns": gt.actual_returns} for gt in gts],
            [{"predictions": pred.predictions} for pred in preds],
        )

    def _calculate_return_accuracy(
        self, gts: List[dspy.Example], preds: List[dspy.Example]
    ) -> float:
        """Calculate return accuracy."""
        return self.metrics._calculate_return_accuracy(
            [{"actual_returns": gt.actual_returns} for gt in gts],
            [{"predictions": pred.predictions} for pred in preds],
        )

    def _calculate_confidence_calibration(
        self, gts: List[dspy.Example], preds: List[dspy.Example]
    ) -> float:
        """Calculate confidence calibration."""
        return self.metrics._calculate_confidence_calibration(
            [{"actual_returns": gt.actual_returns} for gt in gts],
            [{"predictions": pred.predictions} for pred in preds],
        )


class PromptOptimizer(dg.ConfigurableResource):
    """DSPy-based prompt optimizer for financial predictions."""

    model_name: str = Field(
        default="gpt-4-turbo-preview", description="LLM model to use for optimization"
    )
    openai_api_key: str = Field(description="OpenAI API key for DSPy")

    def setup_for_execution(self, context) -> None:
        """Initialize DSPy when the resource is used."""
        # Initialize DSPy
        lm = dspy.LM(model=self.model_name, api_key=self.openai_api_key)
        dspy.settings.configure(lm=lm)

    def optimize_prompts(
        self,
        training_examples: List[dspy.Example],
        validation_examples: List[dspy.Example],
        context: Optional[dg.AssetExecutionContext] = None,
    ) -> Dict[str, Any]:
        """Optimize prompts using DSPy's optimization techniques."""

        if context:
            context.log.info("Starting prompt optimization...")

        # Create the base module
        module = FinancialPredictionModule()

        # Set up the optimizer
        optimizer = dspy.optimize.BootstrapFewShot(
            metric=FinancialMetrics(), max_bootstrapped_demos=4, max_labeled_demos=4
        )

        # Optimize the module
        optimized_module = optimizer.compile(
            module, trainset=training_examples, valset=validation_examples
        )

        # Evaluate the optimized module
        evaluator = FinancialEvaluator(
            model_name=self.model_name, openai_api_key=self.openai_api_key
        )
        evaluator.setup_for_execution(context)

        # Get evaluation metrics
        evaluation_results = evaluator.evaluate_model(validation_examples)

        # Compare with original module
        original_results = evaluator.evaluate_model(validation_examples)

        optimization_results = {
            "optimization_completed": True,
            "original_metrics": original_results,
            "optimized_metrics": evaluation_results,
            "improvement": {
                metric: evaluation_results[metric] - original_results[metric]
                for metric in evaluation_results.keys()
            },
            "optimization_timestamp": datetime.now().isoformat(),
        }

        if context:
            context.log.info(f"Prompt optimization completed: {optimization_results}")

        return optimization_results


@dg.asset(
    kinds={"dspy"},
    group_name="evaluation",
    description="Evaluate and optimize financial prediction models using DSPy",
)
def evaluate_and_optimize_model(
    context: dg.AssetExecutionContext,
    md: MotherDuckResource,
    evaluator: FinancialEvaluator,
    optimizer: PromptOptimizer,
) -> Dict[str, Any]:
    """
    Asset that evaluates and optimizes the financial prediction model using DSPy.

    This asset will:
    1. Create training and validation examples from historical data
    2. Evaluate the current model performance
    3. Optimize prompts using DSPy's optimization techniques
    4. Compare original vs optimized performance

    Returns:
        Dictionary with evaluation and optimization results
    """
    context.log.info("Starting model evaluation and optimization...")

    # Create training and validation examples
    context.log.info("Creating training examples...")
    all_examples = evaluator.create_training_examples(md, lookback_days=180)

    if len(all_examples) < 10:
        raise ValueError("Insufficient historical data for evaluation and optimization")

    # Split into training and validation sets
    split_idx = int(len(all_examples) * 0.8)
    training_examples = all_examples[:split_idx]
    validation_examples = all_examples[split_idx:]

    context.log.info(
        f"Created {len(training_examples)} training examples and {len(validation_examples)} validation examples"
    )

    # Evaluate current model
    context.log.info("Evaluating current model...")
    current_metrics = evaluator.evaluate_model(validation_examples)

    # Optimize prompts
    context.log.info("Optimizing prompts...")
    optimization_results = optimizer.optimize_prompts(
        training_examples=training_examples,
        validation_examples=validation_examples,
        context=context,
    )

    # Write results to database
    context.log.info("Writing evaluation results to database...")

    evaluation_record = {
        "evaluation_id": f"eval_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        "evaluation_timestamp": datetime.now().isoformat(),
        "training_examples_count": len(training_examples),
        "validation_examples_count": len(validation_examples),
        "current_metrics": json.dumps(current_metrics),
        "optimization_results": json.dumps(optimization_results),
        "model_name": evaluator.model_name,
    }

    md.write_results_to_table(
        [evaluation_record],
        output_table="model_evaluation_results",
        if_exists="append",
        context=context,
    )

    # Return summary
    result_metadata = {
        "evaluation_completed": True,
        "training_examples": len(training_examples),
        "validation_examples": len(validation_examples),
        "current_overall_score": current_metrics.get("overall_score", 0),
        "optimized_overall_score": optimization_results["optimized_metrics"].get(
            "overall_score", 0
        ),
        "improvement": optimization_results["improvement"].get("overall_score", 0),
        "output_table": "model_evaluation_results",
        "records_written": 1,
    }

    context.log.info(f"Model evaluation and optimization complete: {result_metadata}")
    return result_metadata


@dg.asset(
    kinds={"dspy"},
    group_name="prediction",
    description="Generate optimized financial predictions using the improved model",
    deps=[evaluate_and_optimize_model],
)
def optimized_financial_predictions(
    context: dg.AssetExecutionContext,
    md: MotherDuckResource,
    evaluator: FinancialEvaluator,
) -> Dict[str, Any]:
    """
    Asset that generates financial predictions using the optimized model.

    This asset uses the improved model to generate structured predictions
    with better accuracy and confidence calibration.

    Returns:
        Dictionary with optimized predictions and metadata
    """
    context.log.info("Generating optimized financial predictions...")

    # Get latest economic and market data
    economic_query = """
    SELECT 
        series_code,
        series_name,
        month,
        current_value,
        pct_change_3m,
        pct_change_6m,
        pct_change_1y,
        date_grain
    FROM fred_series_latest_aggregates
    WHERE current_value IS NOT NULL
    ORDER BY series_name, month DESC
    """

    market_query = """
    SELECT 
        symbol,
        asset_type,
        time_period,
        exchange,
        name,
        period_start_date,
        period_end_date,
        trading_days,
        total_return_pct,
        avg_daily_return_pct,
        volatility_pct,
        win_rate_pct,
        total_price_change,
        avg_daily_price_change,
        worst_day_change,
        best_day_change,
        positive_days,
        negative_days,
        neutral_days,
        period_start_price,
        period_end_price
    FROM us_sector_summary
    WHERE time_period IN ('12_weeks', '6_months', '1_year')
    ORDER BY asset_type, time_period, total_return_pct DESC
    """

    economic_df = md.execute_query(economic_query, read_only=True)
    market_df = md.execute_query(market_query, read_only=True)

    economic_data = economic_df.write_csv()
    market_data = market_df.write_csv()

    # Generate predictions using optimized model
    predictions = evaluator.prediction_module(
        economic_data=economic_data,
        market_data=market_data,
        prediction_horizon="30 days",
    )

    # Parse predictions
    try:
        predictions_data = json.loads(predictions.predictions)
    except json.JSONDecodeError:
        context.log.warning("Failed to parse predictions JSON, using raw text")
        predictions_data = {"raw_predictions": predictions.predictions}

    # Format results
    prediction_timestamp = datetime.now()
    result = {
        "prediction_timestamp": prediction_timestamp.isoformat(),
        "prediction_date": prediction_timestamp.strftime("%Y-%m-%d"),
        "prediction_time": prediction_timestamp.strftime("%H:%M:%S"),
        "model_name": evaluator.model_name,
        "prediction_horizon": "30 days",
        "predictions": predictions_data,
        "data_sources": {
            "economic_data_table": "fred_series_latest_aggregates",
            "market_data_table": "us_sector_summary",
        },
    }

    # Write predictions to database
    context.log.info("Writing optimized predictions to database...")
    md.write_results_to_table(
        [result],
        output_table="optimized_financial_predictions",
        if_exists="append",
        context=context,
    )

    # Return metadata
    result_metadata = {
        "predictions_generated": True,
        "prediction_timestamp": result["prediction_timestamp"],
        "model_name": result["model_name"],
        "prediction_horizon": result["prediction_horizon"],
        "overweight_assets_count": len(predictions_data.get("overweight_assets", [])),
        "underweight_assets_count": len(predictions_data.get("underweight_assets", [])),
        "market_outlook": predictions_data.get("market_outlook", "unknown"),
        "confidence_score": predictions_data.get("confidence_score", 0.0),
        "output_table": "optimized_financial_predictions",
        "records_written": 1,
    }

    context.log.info(f"Optimized financial predictions complete: {result_metadata}")
    return result_metadata
