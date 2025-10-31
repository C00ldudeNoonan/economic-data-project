from typing import Optional, Dict, Any, List
import json
from datetime import datetime
import dagster as dg
import numpy as np

from macro_agents.defs.resources.motherduck import MotherDuckResource
from macro_agents.defs.agents.backtesting import (
    backtest_agent_predictions,
    batch_backtest_analysis,
)


class BacktestingVisualizer(dg.ConfigurableResource):
    """Visualization and reporting for backtesting results."""

    def setup_for_execution(self, context) -> None:
        """Initialize settings."""
        pass

    def create_performance_dashboard(
        self,
        md_resource: MotherDuckResource,
        context: Optional[dg.AssetExecutionContext] = None,
    ) -> Dict[str, Any]:
        """Create comprehensive performance dashboard."""
        if context:
            context.log.info("Creating performance dashboard...")

        # Get backtest results
        backtest_query = """
        SELECT 
            prediction_date,
            prediction_horizon_days,
            performance_metrics,
            prediction_accuracy,
            backtest_metadata
        FROM backtest_results
        ORDER BY prediction_date DESC
        LIMIT 50
        """

        backtest_df = md_resource.execute_query(backtest_query, read_only=True)

        if backtest_df.is_empty():
            return {"error": "No backtest results found"}

        # Parse metrics
        performance_data = []
        accuracy_data = []

        for row in backtest_df.iter_rows(named=True):
            try:
                metrics = json.loads(row["performance_metrics"])
                accuracy = json.loads(row["prediction_accuracy"])

                performance_data.append(
                    {
                        "prediction_date": row["prediction_date"],
                        "total_return": metrics.get("total_return", 0),
                        "sharpe_ratio": metrics.get("sharpe_ratio", 0),
                        "max_drawdown": metrics.get("max_drawdown", 0),
                        "volatility": metrics.get("volatility", 0),
                        "win_rate": metrics.get("win_rate", 0),
                    }
                )

                accuracy_data.append(
                    {
                        "prediction_date": row["prediction_date"],
                        "overall_accuracy": accuracy.get("overall_accuracy", 0),
                        "overweight_accuracy": accuracy.get("overweight_accuracy", 0),
                        "underweight_accuracy": accuracy.get("underweight_accuracy", 0),
                    }
                )

            except (json.JSONDecodeError, KeyError):
                continue

        # Create time series data for visualization
        dashboard_data = {
            "dashboard_timestamp": datetime.now().isoformat(),
            "total_backtests": len(performance_data),
            "performance_summary": self._calculate_performance_summary(
                performance_data
            ),
            "accuracy_summary": self._calculate_accuracy_summary(accuracy_data),
            "time_series_data": self._create_time_series_data(
                performance_data, accuracy_data
            ),
            "recommendations": self._generate_recommendations(
                performance_data, accuracy_data
            ),
        }

        # Write dashboard to database
        md_resource.write_results_to_table(
            [dashboard_data],
            output_table="backtesting_dashboard",
            if_exists="replace",
            context=context,
        )

        if context:
            context.log.info("Performance dashboard created successfully")

        return dashboard_data

    def _calculate_performance_summary(
        self, performance_data: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Calculate performance summary statistics."""
        if not performance_data:
            return {}

        returns = [d["total_return"] for d in performance_data]
        sharpe_ratios = [d["sharpe_ratio"] for d in performance_data]
        max_drawdowns = [d["max_drawdown"] for d in performance_data]
        volatilities = [d["volatility"] for d in performance_data]
        win_rates = [d["win_rate"] for d in performance_data]

        return {
            "avg_return": np.mean(returns),
            "median_return": np.median(returns),
            "std_return": np.std(returns),
            "best_return": np.max(returns),
            "worst_return": np.min(returns),
            "avg_sharpe_ratio": np.mean(sharpe_ratios),
            "avg_max_drawdown": np.mean(max_drawdowns),
            "worst_drawdown": np.min(max_drawdowns),
            "avg_volatility": np.mean(volatilities),
            "avg_win_rate": np.mean(win_rates),
            "positive_returns_count": sum(1 for r in returns if r > 0),
            "total_backtests": len(returns),
        }

    def _calculate_accuracy_summary(
        self, accuracy_data: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Calculate accuracy summary statistics."""
        if not accuracy_data:
            return {}

        overall_accuracies = [d["overall_accuracy"] for d in accuracy_data]
        overweight_accuracies = [
            d["overweight_accuracy"]
            for d in accuracy_data
            if d["overweight_accuracy"] > 0
        ]
        underweight_accuracies = [
            d["underweight_accuracy"]
            for d in accuracy_data
            if d["underweight_accuracy"] > 0
        ]

        return {
            "avg_overall_accuracy": np.mean(overall_accuracies),
            "std_overall_accuracy": np.std(overall_accuracies),
            "best_overall_accuracy": np.max(overall_accuracies),
            "worst_overall_accuracy": np.min(overall_accuracies),
            "avg_overweight_accuracy": np.mean(overweight_accuracies)
            if overweight_accuracies
            else 0,
            "avg_underweight_accuracy": np.mean(underweight_accuracies)
            if underweight_accuracies
            else 0,
            "high_accuracy_count": sum(1 for a in overall_accuracies if a > 0.7),
            "low_accuracy_count": sum(1 for a in overall_accuracies if a < 0.4),
        }

    def _create_time_series_data(
        self,
        performance_data: List[Dict[str, Any]],
        accuracy_data: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Create time series data for visualization."""
        time_series = {}

        try:
            # Performance over time
            if performance_data:
                dates = [d["prediction_date"] for d in performance_data]
                returns = [d["total_return"] for d in performance_data]
                sharpe_ratios = [d["sharpe_ratio"] for d in performance_data]
                win_rates = [d["win_rate"] for d in performance_data]
                volatilities = [d["volatility"] for d in performance_data]

                time_series["returns_over_time"] = {"dates": dates, "returns": returns}

                time_series["sharpe_ratios"] = {
                    "dates": dates,
                    "sharpe_ratios": sharpe_ratios,
                }

                time_series["win_rate_vs_volatility"] = {
                    "volatilities": volatilities,
                    "win_rates": win_rates,
                    "returns": returns,
                }

                # Calculate rolling averages for trend analysis
                if len(performance_data) > 5:
                    window = min(5, len(performance_data) // 2)
                    rolling_returns = np.convolve(
                        returns, np.ones(window) / window, mode="valid"
                    ).tolist()
                    rolling_dates = dates[window - 1 :]

                    time_series["performance_trend"] = {
                        "rolling_dates": rolling_dates,
                        "rolling_returns": rolling_returns,
                        "individual_dates": dates,
                        "individual_returns": returns,
                        "window_size": window,
                    }

            # Accuracy over time
            if accuracy_data:
                acc_dates = [d["prediction_date"] for d in accuracy_data]
                accuracies = [d["overall_accuracy"] for d in accuracy_data]
                overweight_accuracies = [
                    d["overweight_accuracy"]
                    for d in accuracy_data
                    if d["overweight_accuracy"] > 0
                ]
                underweight_accuracies = [
                    d["underweight_accuracy"]
                    for d in accuracy_data
                    if d["underweight_accuracy"] > 0
                ]

                time_series["accuracy_over_time"] = {
                    "dates": acc_dates,
                    "overall_accuracy": accuracies,
                }

                time_series["position_accuracy"] = {
                    "overweight_accuracy": overweight_accuracies,
                    "underweight_accuracy": underweight_accuracies,
                }

        except Exception as e:
            time_series["error"] = f"Time series data creation failed: {str(e)}"

        return time_series

    def _generate_recommendations(
        self,
        performance_data: List[Dict[str, Any]],
        accuracy_data: List[Dict[str, Any]],
    ) -> List[str]:
        """Generate recommendations based on performance analysis."""
        recommendations = []

        if not performance_data:
            return ["No performance data available for analysis"]

        # Performance analysis
        avg_return = np.mean([d["total_return"] for d in performance_data])
        avg_accuracy = (
            np.mean([d["overall_accuracy"] for d in accuracy_data])
            if accuracy_data
            else 0
        )

        if avg_return < 0:
            recommendations.append(
                "Average returns are negative - consider reviewing asset selection criteria"
            )
        elif avg_return < 0.02:
            recommendations.append(
                "Returns are low - consider optimizing for higher-return opportunities"
            )

        if avg_accuracy < 0.5:
            recommendations.append(
                "Prediction accuracy is below 50% - model needs significant improvement"
            )
        elif avg_accuracy < 0.6:
            recommendations.append(
                "Prediction accuracy is moderate - consider prompt optimization"
            )

        # Volatility analysis
        avg_volatility = np.mean([d["volatility"] for d in performance_data])
        if avg_volatility > 0.3:
            recommendations.append(
                "High volatility detected - consider risk management improvements"
            )

        # Consistency analysis
        returns_std = np.std([d["total_return"] for d in performance_data])
        if returns_std > 0.1:
            recommendations.append(
                "High return variability - consider more consistent prediction strategies"
            )

        # Win rate analysis
        avg_win_rate = np.mean([d["win_rate"] for d in performance_data])
        if avg_win_rate < 0.4:
            recommendations.append("Low win rate - review prediction methodology")

        if not recommendations:
            recommendations.append(
                "Model performance is satisfactory - continue monitoring"
            )

        return recommendations

    def create_model_evaluation_report(
        self,
        md_resource: MotherDuckResource,
        context: Optional[dg.AssetExecutionContext] = None,
    ) -> Dict[str, Any]:
        """Create comprehensive model evaluation report."""
        if context:
            context.log.info("Creating model evaluation report...")

        # Get evaluation results
        eval_query = """
        SELECT 
            evaluation_timestamp,
            current_metrics,
            optimization_results,
            evaluation_status
        FROM comprehensive_evaluation_results
        ORDER BY evaluation_timestamp DESC
        LIMIT 10
        """

        eval_df = md_resource.execute_query(eval_query, read_only=True)

        # Get optimization results
        opt_query = """
        SELECT 
            optimization_timestamp,
            optimization_results,
            optimization_status
        FROM model_optimization_results
        ORDER BY optimization_timestamp DESC
        LIMIT 10
        """

        opt_df = md_resource.execute_query(opt_query, read_only=True)

        # Parse results
        evaluation_history = []
        for row in eval_df.iter_rows(named=True):
            try:
                metrics = json.loads(row["current_metrics"])
                evaluation_history.append(
                    {
                        "timestamp": row["evaluation_timestamp"],
                        "overall_score": metrics.get("overall_score", 0),
                        "directional_accuracy": metrics.get("directional_accuracy", 0),
                        "return_accuracy": metrics.get("return_accuracy", 0),
                        "confidence_calibration": metrics.get(
                            "confidence_calibration", 0
                        ),
                        "status": row["evaluation_status"],
                    }
                )
            except (json.JSONDecodeError, KeyError):
                continue

        optimization_history = []
        for row in opt_df.iter_rows(named=True):
            try:
                opt_results = json.loads(row["optimization_results"])
                optimization_history.append(
                    {
                        "timestamp": row["optimization_timestamp"],
                        "improvement": opt_results.get("improvement", {}),
                        "status": row["optimization_status"],
                    }
                )
            except (json.JSONDecodeError, KeyError):
                continue

        # Create report
        report = {
            "report_timestamp": datetime.now().isoformat(),
            "evaluation_history": evaluation_history,
            "optimization_history": optimization_history,
            "model_health_score": self._calculate_model_health_score(
                evaluation_history
            ),
            "improvement_trend": self._analyze_improvement_trend(evaluation_history),
            "recommendations": self._generate_model_recommendations(
                evaluation_history, optimization_history
            ),
        }

        # Write report to database
        md_resource.write_results_to_table(
            [report],
            output_table="model_evaluation_report",
            if_exists="replace",
            context=context,
        )

        if context:
            context.log.info("Model evaluation report created successfully")

        return report

    def _calculate_model_health_score(
        self, evaluation_history: List[Dict[str, Any]]
    ) -> float:
        """Calculate overall model health score."""
        if not evaluation_history:
            return 0.0

        latest_eval = evaluation_history[0]
        scores = [
            latest_eval.get("overall_score", 0),
            latest_eval.get("directional_accuracy", 0),
            latest_eval.get("return_accuracy", 0),
            latest_eval.get("confidence_calibration", 0),
        ]

        return np.mean(scores)

    def _analyze_improvement_trend(
        self, evaluation_history: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Analyze improvement trend over time."""
        if len(evaluation_history) < 2:
            return {"trend": "insufficient_data", "change": 0.0}

        recent_scores = [
            eval_data.get("overall_score", 0) for eval_data in evaluation_history[:3]
        ]
        older_scores = [
            eval_data.get("overall_score", 0) for eval_data in evaluation_history[3:6]
        ]

        if not older_scores:
            return {"trend": "insufficient_data", "change": 0.0}

        recent_avg = np.mean(recent_scores)
        older_avg = np.mean(older_scores)
        change = recent_avg - older_avg

        if change > 0.05:
            trend = "improving"
        elif change < -0.05:
            trend = "declining"
        else:
            trend = "stable"

        return {
            "trend": trend,
            "change": change,
            "recent_avg": recent_avg,
            "older_avg": older_avg,
        }

    def _generate_model_recommendations(
        self,
        evaluation_history: List[Dict[str, Any]],
        optimization_history: List[Dict[str, Any]],
    ) -> List[str]:
        """Generate model improvement recommendations."""
        recommendations = []

        if not evaluation_history:
            return ["No evaluation history available"]

        latest_eval = evaluation_history[0]
        health_score = self._calculate_model_health_score(evaluation_history)

        if health_score < 0.5:
            recommendations.append(
                "Model health is poor - immediate optimization required"
            )
        elif health_score < 0.7:
            recommendations.append(
                "Model health is moderate - consider prompt optimization"
            )

        # Check specific metrics
        if latest_eval.get("directional_accuracy", 0) < 0.6:
            recommendations.append(
                "Directional accuracy is low - review economic indicator selection"
            )

        if latest_eval.get("return_accuracy", 0) < 0.4:
            recommendations.append(
                "Return prediction accuracy is low - improve return estimation methods"
            )

        if latest_eval.get("confidence_calibration", 0) < 0.5:
            recommendations.append(
                "Confidence calibration is poor - review confidence scoring"
            )

        # Check optimization frequency
        if len(optimization_history) == 0:
            recommendations.append(
                "No optimization history - consider running initial optimization"
            )
        elif len(optimization_history) < len(evaluation_history) / 2:
            recommendations.append(
                "Infrequent optimization - consider more regular optimization cycles"
            )

        if not recommendations:
            recommendations.append(
                "Model performance is satisfactory - continue current approach"
            )

        return recommendations


@dg.asset(
    kinds={"dspy", "duckdb"},
    group_name="backtesting",
    description="Generate comprehensive backtesting and model performance dashboard",
    deps=[backtest_agent_predictions, batch_backtest_analysis],
    tags={
        "schedule": "daily",
        "execution_time": "weekdays_7am_est",
        "report_type": "performance_dashboard",
    },
)
def backtesting_performance_dashboard(
    context: dg.AssetExecutionContext,
    md: MotherDuckResource,
    visualizer: BacktestingVisualizer,
) -> Dict[str, Any]:
    """
    Asset that generates comprehensive backtesting and model performance dashboard.

    This asset creates reports showing:
    - Performance metrics over time (structured time series data)
    - Prediction accuracy trends
    - Model improvement recommendations
    - Risk and return analysis

    Scheduled to run daily on weekdays at 7 AM EST.

    Returns:
        Dictionary with dashboard data including time series data and performance summaries
    """
    context.log.info("Generating backtesting performance dashboard...")

    # Create performance dashboard
    dashboard_data = visualizer.create_performance_dashboard(
        md_resource=md, context=context
    )

    # Create model evaluation report
    evaluation_report = visualizer.create_model_evaluation_report(
        md_resource=md, context=context
    )

    # Combine results
    combined_dashboard = {
        "dashboard_timestamp": datetime.now().isoformat(),
        "performance_dashboard": dashboard_data,
        "evaluation_report": evaluation_report,
        "dashboard_status": "completed",
    }

    # Write combined dashboard
    md.write_results_to_table(
        [combined_dashboard],
        output_table="combined_performance_dashboard",
        if_exists="replace",
        context=context,
    )

    # Return summary with structured data for visualization
    time_series_data = dashboard_data.get("time_series_data", {})
    result_metadata = {
        "dashboard_generated": True,
        "dashboard_timestamp": combined_dashboard["dashboard_timestamp"],
        "total_backtests": dashboard_data.get("total_backtests", 0),
        "performance_summary_available": "performance_summary" in dashboard_data,
        "time_series_data_keys": list(time_series_data.keys()),
        "time_series_data": time_series_data,
        "performance_summary": dashboard_data.get("performance_summary", {}),
        "accuracy_summary": dashboard_data.get("accuracy_summary", {}),
        "recommendations": dashboard_data.get("recommendations", []),
        "recommendations_count": len(dashboard_data.get("recommendations", [])),
        "model_health_score": evaluation_report.get("model_health_score", 0),
        "improvement_trend": evaluation_report.get("improvement_trend", {}).get(
            "trend", "unknown"
        ),
        "improvement_trend_details": evaluation_report.get("improvement_trend", {}),
        "output_tables": [
            "backtesting_dashboard",
            "model_evaluation_report",
            "combined_performance_dashboard",
        ],
        "records_written": 3,
    }

    context.log.info(f"Backtesting performance dashboard complete: {result_metadata}")
    return result_metadata
