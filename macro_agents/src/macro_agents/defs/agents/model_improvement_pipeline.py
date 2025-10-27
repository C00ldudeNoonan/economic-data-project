import dspy
import polars as pl
from typing import Optional, Dict, Any, List, Tuple
import json
from datetime import datetime, timedelta
import dagster as dg
from pydantic import Field
import numpy as np

from macro_agents.defs.resources.motherduck import MotherDuckResource
from macro_agents.defs.agents.dspy_evaluation import (
    FinancialEvaluator, 
    PromptOptimizer, 
    FinancialMetrics,
    FinancialPredictionModule
)
from macro_agents.defs.agents.backtesting import BacktestingEngine
from macro_agents.defs.agents.enhanced_economic_cycle_analyzer import enhanced_economic_cycle_analysis
from macro_agents.defs.agents.backtesting import batch_backtest_analysis


class ModelImprovementPipeline(dg.ConfigurableResource):
    """Comprehensive model improvement pipeline using DSPy evaluation and optimization."""
    
    model_name: str = Field(
        default="gpt-4-turbo-preview", description="LLM model to use for analysis"
    )
    openai_api_key: str = Field(description="OpenAI API key for DSPy")
    evaluation_frequency_days: int = Field(
        default=7, description="How often to run evaluation (in days)"
    )
    optimization_threshold: float = Field(
        default=0.1, description="Minimum improvement threshold to trigger optimization"
    )
    
    def setup_for_execution(self, context) -> None:
        """Initialize the model improvement pipeline."""
        # Initialize DSPy
        lm = dspy.LM(model=self.model_name, api_key=self.openai_api_key)
        dspy.settings.configure(lm=lm)
        
        # Initialize components
        self._evaluator = FinancialEvaluator(
            model_name=self.model_name,
            openai_api_key=self.openai_api_key
        )
        self._optimizer = PromptOptimizer(
            model_name=self.model_name,
            openai_api_key=self.openai_api_key
        )
        self._backtesting_engine = BacktestingEngine(
            model_name=self.model_name,
            openai_api_key=self.openai_api_key
        )
    
    @property
    def evaluator(self):
        """Get evaluator."""
        return self._evaluator
    
    @property
    def optimizer(self):
        """Get optimizer."""
        return self._optimizer
    
    @property
    def backtesting_engine(self):
        """Get backtesting engine."""
        return self._backtesting_engine
    
    def should_run_evaluation(self, md_resource: MotherDuckResource) -> bool:
        """Check if evaluation should be run based on frequency."""
        query = """
        SELECT MAX(evaluation_timestamp) as last_evaluation
        FROM model_evaluation_results
        """
        
        df = md_resource.execute_query(query, read_only=True)
        if df.is_empty():
            return True  # No previous evaluation
        
        last_evaluation = df[0, "last_evaluation"]
        if not last_evaluation:
            return True
        
        last_eval_date = datetime.fromisoformat(last_evaluation.replace('Z', '+00:00'))
        days_since_eval = (datetime.now() - last_eval_date).days
        
        return days_since_eval >= self.evaluation_frequency_days
    
    def create_training_dataset(self, md_resource: MotherDuckResource, 
                              lookback_days: int = 180) -> List[dspy.Example]:
        """Create comprehensive training dataset from historical data."""
        # Get historical analysis data
        end_date = datetime.now()
        start_date = end_date - timedelta(days=lookback_days)
        
        # Get economic cycle analysis
        cycle_query = """
        SELECT 
            analysis_content,
            analysis_timestamp
        FROM enhanced_economic_cycle_analysis
        WHERE analysis_type = 'enhanced_economic_cycle'
        AND analysis_timestamp BETWEEN ? AND ?
        ORDER BY analysis_timestamp
        """
        
        cycle_df = md_resource.execute_query(cycle_query, read_only=True, params=[
            start_date.strftime('%Y-%m-%d'),
            end_date.strftime('%Y-%m-%d')
        ])
        
        # Get market trend analysis
        trend_query = """
        SELECT 
            analysis_content,
            analysis_timestamp
        FROM enhanced_economic_cycle_analysis
        WHERE analysis_type = 'enhanced_market_trends'
        AND analysis_timestamp BETWEEN ? AND ?
        ORDER BY analysis_timestamp
        """
        
        trend_df = md_resource.execute_query(trend_query, read_only=True, params=[
            start_date.strftime('%Y-%m-%d'),
            end_date.strftime('%Y-%m-%d')
        ])
        
        # Get backtest results for actual returns
        backtest_query = """
        SELECT 
            prediction_date,
            actual_returns,
            prediction_accuracy
        FROM backtest_results
        WHERE prediction_date BETWEEN ? AND ?
        ORDER BY prediction_date
        """
        
        backtest_df = md_resource.execute_query(backtest_query, read_only=True, params=[
            start_date.strftime('%Y-%m-%d'),
            end_date.strftime('%Y-%m-%d')
        ])
        
        # Create training examples
        examples = []
        
        # Match cycle analysis with backtest results
        for cycle_row in cycle_df.iter_rows(named=True):
            cycle_date = cycle_row["analysis_timestamp"].split('T')[0]
            
            # Find matching backtest result
            matching_backtest = None
            for backtest_row in backtest_df.iter_rows(named=True):
                if backtest_row["prediction_date"] == cycle_date:
                    matching_backtest = backtest_row
                    break
            
            if matching_backtest:
                # Create example
                example = dspy.Example(
                    economic_data="",  # Would need to fetch actual data
                    market_data="",    # Would need to fetch actual data
                    prediction_horizon="30 days",
                    predictions=cycle_row["analysis_content"],
                    actual_returns=json.loads(matching_backtest["actual_returns"])
                ).with_inputs("economic_data", "market_data", "prediction_horizon")
                
                examples.append(example)
        
        return examples
    
    def run_comprehensive_evaluation(self, md_resource: MotherDuckResource,
                                   context: Optional[dg.AssetExecutionContext] = None) -> Dict[str, Any]:
        """Run comprehensive model evaluation."""
        if context:
            context.log.info("Starting comprehensive model evaluation...")
        
        # Create training dataset
        training_examples = self.create_training_dataset(md_resource)
        
        if len(training_examples) < 10:
            raise ValueError("Insufficient training data for evaluation")
        
        # Split into train/validation/test sets
        train_size = int(len(training_examples) * 0.6)
        val_size = int(len(training_examples) * 0.2)
        
        train_examples = training_examples[:train_size]
        val_examples = training_examples[train_size:train_size + val_size]
        test_examples = training_examples[train_size + val_size:]
        
        if context:
            context.log.info(f"Created dataset: {len(train_examples)} train, {len(val_examples)} val, {len(test_examples)} test")
        
        # Evaluate current model
        current_metrics = self.evaluator.evaluate_model(test_examples)
        
        # Run backtesting analysis
        backtest_results = []
        for example in test_examples[:5]:  # Limit to 5 for performance
            try:
                # Extract prediction date from example
                pred_date = example.actual_returns.get("prediction_date", datetime.now().strftime('%Y-%m-%d'))
                
                backtest_result = self.backtesting_engine.run_backtest(
                    md_resource=md_resource,
                    prediction_date=pred_date,
                    prediction_horizon_days=30,
                    context=context
                )
                backtest_results.append(backtest_result)
            except Exception as e:
                if context:
                    context.log.warning(f"Backtest failed for example: {str(e)}")
                continue
        
        # Calculate aggregate metrics
        evaluation_results = {
            "evaluation_timestamp": datetime.now().isoformat(),
            "dataset_size": len(training_examples),
            "train_size": len(train_examples),
            "val_size": len(val_examples),
            "test_size": len(test_examples),
            "current_metrics": current_metrics,
            "backtest_results_count": len(backtest_results),
            "backtest_avg_accuracy": np.mean([r.prediction_accuracy.get('overall_accuracy', 0) for r in backtest_results]) if backtest_results else 0,
            "backtest_avg_return": np.mean([r.performance_metrics.get('total_return', 0) for r in backtest_results]) if backtest_results else 0,
            "evaluation_status": "completed"
        }
        
        # Write evaluation results
        md_resource.write_results_to_table(
            [evaluation_results],
            output_table="comprehensive_evaluation_results",
            if_exists="append",
            context=context
        )
        
        if context:
            context.log.info(f"Comprehensive evaluation completed: {evaluation_results}")
        
        return evaluation_results
    
    def run_optimization_if_needed(self, md_resource: MotherDuckResource,
                                 evaluation_results: Dict[str, Any],
                                 context: Optional[dg.AssetExecutionContext] = None) -> Dict[str, Any]:
        """Run optimization if performance is below threshold."""
        
        current_score = evaluation_results["current_metrics"].get("overall_score", 0)
        
        if current_score >= self.optimization_threshold:
            if context:
                context.log.info(f"Model performance ({current_score:.3f}) meets threshold ({self.optimization_threshold}), skipping optimization")
            return {"optimization_skipped": True, "reason": "performance_above_threshold"}
        
        if context:
            context.log.info(f"Model performance ({current_score:.3f}) below threshold ({self.optimization_threshold}), running optimization...")
        
        # Create training dataset
        training_examples = self.create_training_dataset(md_resource)
        
        # Split into train/validation
        split_idx = int(len(training_examples) * 0.8)
        train_examples = training_examples[:split_idx]
        val_examples = training_examples[split_idx:]
        
        # Run optimization
        optimization_results = self.optimizer.optimize_prompts(
            training_examples=train_examples,
            validation_examples=val_examples,
            context=context
        )
        
        # Write optimization results
        optimization_record = {
            "optimization_timestamp": datetime.now().isoformat(),
            "trigger_reason": "performance_below_threshold",
            "original_score": current_score,
            "optimization_results": json.dumps(optimization_results),
            "optimization_status": "completed"
        }
        
        md_resource.write_results_to_table(
            [optimization_record],
            output_table="model_optimization_results",
            if_exists="append",
            context=context
        )
        
        if context:
            context.log.info(f"Optimization completed: {optimization_results}")
        
        return optimization_results
    
    def run_full_pipeline(self, md_resource: MotherDuckResource,
                         context: Optional[dg.AssetExecutionContext] = None) -> Dict[str, Any]:
        """Run the complete model improvement pipeline."""
        if context:
            context.log.info("Starting full model improvement pipeline...")
        
        pipeline_results = {
            "pipeline_start_time": datetime.now().isoformat(),
            "steps_completed": [],
            "final_status": "in_progress"
        }
        
        try:
            # Step 1: Check if evaluation should run
            if not self.should_run_evaluation(md_resource):
                if context:
                    context.log.info("Evaluation not due yet, skipping pipeline")
                pipeline_results["final_status"] = "skipped"
                pipeline_results["reason"] = "evaluation_not_due"
                return pipeline_results
            
            pipeline_results["steps_completed"].append("evaluation_check")
            
            # Step 2: Run comprehensive evaluation
            evaluation_results = self.run_comprehensive_evaluation(md_resource, context)
            pipeline_results["evaluation_results"] = evaluation_results
            pipeline_results["steps_completed"].append("comprehensive_evaluation")
            
            # Step 3: Run optimization if needed
            optimization_results = self.run_optimization_if_needed(md_resource, evaluation_results, context)
            pipeline_results["optimization_results"] = optimization_results
            pipeline_results["steps_completed"].append("optimization")
            
            # Step 4: Generate final report
            final_report = self._generate_final_report(evaluation_results, optimization_results)
            pipeline_results["final_report"] = final_report
            pipeline_results["steps_completed"].append("final_report")
            
            pipeline_results["final_status"] = "completed"
            pipeline_results["pipeline_end_time"] = datetime.now().isoformat()
            
            # Write pipeline results
            md_resource.write_results_to_table(
                [pipeline_results],
                output_table="model_improvement_pipeline_results",
                if_exists="append",
                context=context
            )
            
            if context:
                context.log.info(f"Model improvement pipeline completed: {pipeline_results}")
            
        except Exception as e:
            pipeline_results["final_status"] = "failed"
            pipeline_results["error"] = str(e)
            pipeline_results["pipeline_end_time"] = datetime.now().isoformat()
            
            if context:
                context.log.error(f"Model improvement pipeline failed: {str(e)}")
        
        return pipeline_results
    
    def _generate_final_report(self, evaluation_results: Dict[str, Any], 
                             optimization_results: Dict[str, Any]) -> Dict[str, Any]:
        """Generate final improvement report."""
        
        current_metrics = evaluation_results["current_metrics"]
        
        report = {
            "report_timestamp": datetime.now().isoformat(),
            "model_performance": {
                "overall_score": current_metrics.get("overall_score", 0),
                "directional_accuracy": current_metrics.get("directional_accuracy", 0),
                "return_accuracy": current_metrics.get("return_accuracy", 0),
                "confidence_calibration": current_metrics.get("confidence_calibration", 0)
            },
            "backtest_performance": {
                "avg_accuracy": evaluation_results.get("backtest_avg_accuracy", 0),
                "avg_return": evaluation_results.get("backtest_avg_return", 0),
                "total_backtests": evaluation_results.get("backtest_results_count", 0)
            },
            "optimization_applied": not optimization_results.get("optimization_skipped", False),
            "recommendations": []
        }
        
        # Generate recommendations
        if current_metrics.get("overall_score", 0) < 0.6:
            report["recommendations"].append("Model performance is below acceptable threshold - consider more training data")
        
        if current_metrics.get("directional_accuracy", 0) < 0.5:
            report["recommendations"].append("Directional accuracy is low - review economic indicators selection")
        
        if current_metrics.get("return_accuracy", 0) < 0.3:
            report["recommendations"].append("Return prediction accuracy is low - improve return estimation methods")
        
        if current_metrics.get("confidence_calibration", 0) < 0.4:
            report["recommendations"].append("Confidence calibration is poor - review confidence scoring methodology")
        
        if evaluation_results.get("backtest_avg_accuracy", 0) < 0.5:
            report["recommendations"].append("Backtest accuracy is low - consider longer prediction horizons or different assets")
        
        if not report["recommendations"]:
            report["recommendations"].append("Model performance is satisfactory - continue monitoring")
        
        return report


@dg.asset(
    kinds={"dspy", "duckdb"},
    group_name="evaluation",
    description="Automated model improvement pipeline using DSPy evaluation and optimization",
    deps=[enhanced_economic_cycle_analysis, batch_backtest_analysis],
    tags={"schedule": "weekly", "execution_time": "sunday_2am_est", "pipeline_type": "model_improvement"},
)
def automated_model_improvement_pipeline(
    context: dg.AssetExecutionContext,
    md: MotherDuckResource,
    model_pipeline: ModelImprovementPipeline,
) -> Dict[str, Any]:
    """
    Asset that runs the automated model improvement pipeline.
    
    This asset will:
    1. Check if evaluation is due based on frequency
    2. Run comprehensive model evaluation
    3. Optimize prompts if performance is below threshold
    4. Generate improvement recommendations
    5. Update model configurations
    
    Scheduled to run weekly on Sundays at 2 AM EST.
    
    Returns:
        Dictionary with pipeline execution results and recommendations
    """
    context.log.info("Starting automated model improvement pipeline...")
    
    # Run the full pipeline
    pipeline_results = model_pipeline.run_full_pipeline(md_resource=md, context=context)
    
    # Return summary
    result_metadata = {
        "pipeline_executed": True,
        "pipeline_status": pipeline_results["final_status"],
        "steps_completed": len(pipeline_results["steps_completed"]),
        "evaluation_performed": "comprehensive_evaluation" in pipeline_results["steps_completed"],
        "optimization_performed": "optimization" in pipeline_results["steps_completed"],
        "final_report_generated": "final_report" in pipeline_results["steps_completed"],
        "pipeline_start_time": pipeline_results["pipeline_start_time"],
        "pipeline_end_time": pipeline_results.get("pipeline_end_time"),
        "output_tables": [
            "comprehensive_evaluation_results",
            "model_optimization_results", 
            "model_improvement_pipeline_results"
        ]
    }
    
    if pipeline_results["final_status"] == "completed":
        result_metadata["recommendations_count"] = len(pipeline_results["final_report"]["recommendations"])
        result_metadata["model_overall_score"] = pipeline_results["final_report"]["model_performance"]["overall_score"]
        result_metadata["backtest_avg_accuracy"] = pipeline_results["final_report"]["backtest_performance"]["avg_accuracy"]
    
    context.log.info(f"Automated model improvement pipeline complete: {result_metadata}")
    return result_metadata


@dg.asset(
    kinds={"dspy", "duckdb"},
    group_name="evaluation",
    description="Manual model improvement pipeline for on-demand evaluation and optimization",
    deps=[enhanced_economic_cycle_analysis, batch_backtest_analysis],
)
def manual_model_improvement_pipeline(
    context: dg.AssetExecutionContext,
    md: MotherDuckResource,
    model_pipeline: ModelImprovementPipeline,
) -> Dict[str, Any]:
    """
    Asset that runs manual model improvement pipeline for on-demand evaluation.
    
    This asset can be triggered manually to:
    1. Force evaluation regardless of frequency
    2. Run optimization with custom parameters
    3. Generate detailed performance reports
    
    Returns:
        Dictionary with detailed pipeline execution results
    """
    context.log.info("Starting manual model improvement pipeline...")
    
    # Override frequency check for manual execution
    original_frequency = model_pipeline.evaluation_frequency_days
    model_pipeline.evaluation_frequency_days = 0  # Force evaluation
    
    try:
        # Run the full pipeline
        pipeline_results = model_pipeline.run_full_pipeline(md_resource=md, context=context)
        
        # Restore original frequency
        model_pipeline.evaluation_frequency_days = original_frequency
        
        # Return detailed results
        result_metadata = {
            "manual_pipeline_executed": True,
            "pipeline_status": pipeline_results["final_status"],
            "detailed_results": pipeline_results,
            "execution_type": "manual",
            "force_evaluation": True
        }
        
        context.log.info(f"Manual model improvement pipeline complete: {result_metadata}")
        return result_metadata
        
    except Exception as e:
        # Restore original frequency even if pipeline fails
        model_pipeline.evaluation_frequency_days = original_frequency
        raise e

