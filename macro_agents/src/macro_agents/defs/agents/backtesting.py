import polars as pl
from typing import Optional, Dict, Any, List, Tuple, Union
import json
from datetime import datetime, timedelta
import dagster as dg
from pydantic import Field
import numpy as np
from dataclasses import dataclass
import re

from macro_agents.defs.resources.motherduck import MotherDuckResource


@dataclass
class BacktestResult:
    """Container for backtesting results."""
    prediction_date: str
    prediction_horizon_days: int
    predicted_assets: List[Dict[str, Any]]
    actual_returns: Dict[str, float]
    performance_metrics: Dict[str, float]
    prediction_accuracy: Dict[str, float]
    backtest_metadata: Dict[str, Any]


class PredictionExtractor:
    """Extracts asset predictions from agent recommendations."""
    
    @staticmethod
    def extract_asset_recommendations(analysis_text: str) -> List[Dict[str, Any]]:
        """
        Extract asset recommendations from analysis text.
        
        Looks for patterns like:
        - OVERWEIGHT: XLK (Technology), XLF (Financial)
        - LONG: SPY, QQQ
        - UNDERWEIGHT: XLE (Energy)
        """
        recommendations = []
        
        # Common asset symbols to look for
        asset_symbols = [
            # US Sector ETFs
            'XLK', 'XLC', 'XLY', 'XLF', 'XLI', 'XLU', 'XLP', 'XLRE', 'XLB', 'XLE', 'XLV',
            # Major Indices
            'SPY', 'QQQ', 'DIA', 'IWM', 'VIX',
            # Fixed Income
            'CWB', 'HYG', 'LQD', 'TIP', 'GOVT', 'MUB',
            # Currency
            'FXE', 'FXY', 'FXB', 'FXC', 'FXA', 'CEW', 'ETHE', 'IBIT',
            # Global Markets
            'EFA', 'EEM', 'VEA', 'VWO', 'ACWI', 'VT'
        ]
        
        # Look for OVERWEIGHT/LONG recommendations
        overweight_pattern = r'(?:OVERWEIGHT|LONG|OVERWEIGHTED):\s*([^.\n]+)'
        overweight_matches = re.findall(overweight_pattern, analysis_text, re.IGNORECASE)
        
        for match in overweight_matches:
            # Extract symbols from the match
            symbols_found = [symbol for symbol in asset_symbols if symbol in match]
            for symbol in symbols_found:
                recommendations.append({
                    'symbol': symbol,
                    'action': 'OVERWEIGHT',
                    'confidence': 'high' if 'strong' in match.lower() else 'medium',
                    'rationale': match.strip()
                })
        
        # Look for UNDERWEIGHT/SHORT recommendations
        underweight_pattern = r'(?:UNDERWEIGHT|SHORT|UNDERWEIGHTED):\s*([^.\n]+)'
        underweight_matches = re.findall(underweight_pattern, analysis_text, re.IGNORECASE)
        
        for match in underweight_matches:
            symbols_found = [symbol for symbol in asset_symbols if symbol in match]
            for symbol in symbols_found:
                recommendations.append({
                    'symbol': symbol,
                    'action': 'UNDERWEIGHT',
                    'confidence': 'high' if 'strong' in match.lower() else 'medium',
                    'rationale': match.strip()
                })
        
        # Look for specific percentage allocations
        allocation_pattern = r'(\w+):\s*(\d+(?:\.\d+)?)%'
        allocation_matches = re.findall(allocation_pattern, analysis_text)
        
        for symbol, percentage in allocation_matches:
            if symbol in asset_symbols:
                recommendations.append({
                    'symbol': symbol,
                    'action': 'ALLOCATE',
                    'allocation_pct': float(percentage),
                    'confidence': 'high',
                    'rationale': f"{percentage}% allocation"
                })
        
        return recommendations


class PerformanceCalculator:
    """Calculates performance metrics for backtesting."""
    
    @staticmethod
    def calculate_returns(price_data: pl.DataFrame, start_date: str, end_date: str) -> Dict[str, float]:
        """Calculate returns for assets between start and end dates."""
        returns = {}
        
        for symbol in price_data['symbol'].unique():
            symbol_data = price_data.filter(pl.col('symbol') == symbol).sort('date')
            
            if symbol_data.is_empty():
                continue
                
            # Get start and end prices
            start_price = symbol_data.filter(pl.col('date') <= start_date).sort('date', descending=True).select('close').head(1)
            end_price = symbol_data.filter(pl.col('date') <= end_date).sort('date', descending=True).select('close').head(1)
            
            if not start_price.is_empty() and not end_price.is_empty():
                start_val = start_price[0, 0]
                end_val = end_price[0, 0]
                if start_val and end_val and start_val > 0:
                    returns[symbol] = (end_val - start_val) / start_val
        
        return returns
    
    @staticmethod
    def calculate_metrics(returns: List[float], benchmark_returns: Optional[List[float]] = None) -> Dict[str, float]:
        """Calculate performance metrics from returns."""
        if not returns:
            return {}
        
        returns_array = np.array(returns)
        
        metrics = {
            'total_return': np.sum(returns_array),
            'mean_return': np.mean(returns_array),
            'volatility': np.std(returns_array),
            'sharpe_ratio': np.mean(returns_array) / np.std(returns_array) if np.std(returns_array) > 0 else 0,
            'max_drawdown': PerformanceCalculator._calculate_max_drawdown(returns_array),
            'win_rate': np.sum(returns_array > 0) / len(returns_array),
            'best_return': np.max(returns_array),
            'worst_return': np.min(returns_array)
        }
        
        if benchmark_returns:
            benchmark_array = np.array(benchmark_returns)
            metrics['alpha'] = np.mean(returns_array) - np.mean(benchmark_array)
            metrics['beta'] = np.cov(returns_array, benchmark_array)[0, 1] / np.var(benchmark_array) if np.var(benchmark_array) > 0 else 0
            metrics['information_ratio'] = metrics['alpha'] / np.std(returns_array - benchmark_array) if np.std(returns_array - benchmark_array) > 0 else 0
        
        return metrics
    
    @staticmethod
    def _calculate_max_drawdown(returns: np.ndarray) -> float:
        """Calculate maximum drawdown from returns."""
        cumulative = np.cumprod(1 + returns)
        running_max = np.maximum.accumulate(cumulative)
        drawdown = (cumulative - running_max) / running_max
        return np.min(drawdown)


class BacktestingEngine(dg.ConfigurableResource):
    """Main backtesting engine for economic agent predictions."""
    
    model_name: str = Field(
        default="gpt-4-turbo-preview", description="LLM model to use for analysis"
    )
    openai_api_key: str = Field(description="OpenAI API key for DSPy")
    
    def setup_for_execution(self, context) -> None:
        """Initialize backtesting engine."""
        pass
    
    def get_historical_analysis(self, md_resource: MotherDuckResource, as_of_date: str) -> Optional[Dict[str, Any]]:
        """Get analysis from a specific date."""
        query = """
        SELECT analysis_content, analysis_type, analysis_timestamp
        FROM economic_cycle_analysis
        WHERE analysis_timestamp <= ?
        ORDER BY analysis_timestamp DESC
        LIMIT 2
        """
        
        df = md_resource.execute_query(query, read_only=True, params=[as_of_date])
        
        if df.is_empty():
            return None
        
        result = {}
        for row in df.iter_rows(named=True):
            if row["analysis_type"] == "economic_cycle":
                result["economic_cycle"] = row["analysis_content"]
            elif row["analysis_type"] == "market_trends":
                result["market_trends"] = row["analysis_content"]
        
        return result if len(result) == 2 else None
    
    def get_asset_allocation_analysis(self, md_resource: MotherDuckResource, as_of_date: str) -> Optional[str]:
        """Get asset allocation analysis from a specific date."""
        query = """
        SELECT analysis_content
        FROM asset_allocation_recommendations
        WHERE analysis_timestamp <= ?
        ORDER BY analysis_timestamp DESC
        LIMIT 1
        """
        
        df = md_resource.execute_query(query, read_only=True, params=[as_of_date])
        
        if df.is_empty():
            return None
        
        return df[0, "analysis_content"]
    
    def get_historical_prices(self, md_resource: MotherDuckResource, symbols: List[str], 
                            start_date: str, end_date: str) -> pl.DataFrame:
        """Get historical price data for symbols."""
        symbols_str = "', '".join(symbols)
        
        query = f"""
        SELECT 
            symbol,
            date,
            close,
            open,
            high,
            low,
            volume
        FROM us_sector_etfs_raw
        WHERE symbol IN ('{symbols_str}')
        AND date BETWEEN ? AND ?
        
        UNION ALL
        
        SELECT 
            symbol,
            date,
            close,
            open,
            high,
            low,
            volume
        FROM major_indices_raw
        WHERE symbol IN ('{symbols_str}')
        AND date BETWEEN ? AND ?
        
        UNION ALL
        
        SELECT 
            symbol,
            date,
            close,
            open,
            high,
            low,
            volume
        FROM fixed_income_etfs_raw
        WHERE symbol IN ('{symbols_str}')
        AND date BETWEEN ? AND ?
        
        UNION ALL
        
        SELECT 
            symbol,
            date,
            close,
            open,
            high,
            low,
            volume
        FROM currency_etfs_raw
        WHERE symbol IN ('{symbols_str}')
        AND date BETWEEN ? AND ?
        
        UNION ALL
        
        SELECT 
            symbol,
            date,
            close,
            open,
            high,
            low,
            volume
        FROM global_markets_raw
        WHERE symbol IN ('{symbols_str}')
        AND date BETWEEN ? AND ?
        
        ORDER BY symbol, date
        """
        
        return md_resource.execute_query(query, read_only=True, params=[
            start_date, end_date, start_date, end_date, start_date, end_date,
            start_date, end_date, start_date, end_date
        ])
    
    def run_backtest(self, 
                    md_resource: MotherDuckResource,
                    prediction_date: str,
                    prediction_horizon_days: int = 30,
                    context: Optional[dg.AssetExecutionContext] = None) -> BacktestResult:
        """Run a backtest for a specific prediction date."""
        
        if context:
            context.log.info(f"Running backtest for prediction date: {prediction_date}")
        
        # Calculate end date
        pred_dt = datetime.strptime(prediction_date, '%Y-%m-%d')
        end_dt = pred_dt + timedelta(days=prediction_horizon_days)
        end_date = end_dt.strftime('%Y-%m-%d')
        
        # Get historical analysis
        analysis = self.get_historical_analysis(md_resource, prediction_date)
        if not analysis:
            raise ValueError(f"No analysis found for date {prediction_date}")
        
        # Get asset allocation recommendations
        allocation_analysis = self.get_asset_allocation_analysis(md_resource, prediction_date)
        if not allocation_analysis:
            raise ValueError(f"No asset allocation analysis found for date {prediction_date}")
        
        # Extract predictions
        extractor = PredictionExtractor()
        predicted_assets = extractor.extract_asset_recommendations(allocation_analysis)
        
        if not predicted_assets:
            raise ValueError("No asset recommendations found in analysis")
        
        if context:
            context.log.info(f"Found {len(predicted_assets)} asset recommendations")
        
        # Get symbols to analyze
        symbols = [asset['symbol'] for asset in predicted_assets]
        
        # Get historical prices
        price_data = self.get_historical_prices(
            md_resource, symbols, prediction_date, end_date
        )
        
        if price_data.is_empty():
            raise ValueError(f"No price data found for symbols: {symbols}")
        
        # Calculate actual returns
        calculator = PerformanceCalculator()
        actual_returns = calculator.calculate_returns(price_data, prediction_date, end_date)
        
        # Calculate performance metrics
        returns_list = list(actual_returns.values())
        performance_metrics = calculator.calculate_metrics(returns_list)
        
        # Calculate prediction accuracy
        prediction_accuracy = self._calculate_prediction_accuracy(predicted_assets, actual_returns)
        
        # Create backtest result
        result = BacktestResult(
            prediction_date=prediction_date,
            prediction_horizon_days=prediction_horizon_days,
            predicted_assets=predicted_assets,
            actual_returns=actual_returns,
            performance_metrics=performance_metrics,
            prediction_accuracy=prediction_accuracy,
            backtest_metadata={
                'total_assets_analyzed': len(symbols),
                'assets_with_data': len(actual_returns),
                'backtest_timestamp': datetime.now().isoformat(),
                'model_name': self.model_name
            }
        )
        
        if context:
            context.log.info(f"Backtest completed: {len(actual_returns)} assets analyzed")
        
        return result
    
    def _calculate_prediction_accuracy(self, predicted_assets: List[Dict[str, Any]], 
                                     actual_returns: Dict[str, float]) -> Dict[str, float]:
        """Calculate accuracy metrics for predictions."""
        if not predicted_assets or not actual_returns:
            return {}
        
        # Separate overweights and underweights
        overweights = [asset for asset in predicted_assets if asset['action'] in ['OVERWEIGHT', 'ALLOCATE']]
        underweights = [asset for asset in predicted_assets if asset['action'] == 'UNDERWEIGHT']
        
        accuracy_metrics = {}
        
        # Calculate accuracy for overweights (should have positive returns)
        if overweights:
            overweight_symbols = [asset['symbol'] for asset in overweights]
            overweight_returns = [actual_returns.get(symbol, 0) for symbol in overweight_symbols if symbol in actual_returns]
            
            if overweight_returns:
                accuracy_metrics['overweight_accuracy'] = sum(1 for r in overweight_returns if r > 0) / len(overweight_returns)
                accuracy_metrics['overweight_avg_return'] = np.mean(overweight_returns)
                accuracy_metrics['overweight_positive_count'] = sum(1 for r in overweight_returns if r > 0)
                accuracy_metrics['overweight_total_count'] = len(overweight_returns)
        
        # Calculate accuracy for underweights (should have negative returns)
        if underweights:
            underweight_symbols = [asset['symbol'] for asset in underweights]
            underweight_returns = [actual_returns.get(symbol, 0) for symbol in underweight_symbols if symbol in actual_returns]
            
            if underweight_returns:
                accuracy_metrics['underweight_accuracy'] = sum(1 for r in underweight_returns if r < 0) / len(underweight_returns)
                accuracy_metrics['underweight_avg_return'] = np.mean(underweight_returns)
                accuracy_metrics['underweight_negative_count'] = sum(1 for r in underweight_returns if r < 0)
                accuracy_metrics['underweight_total_count'] = len(underweight_returns)
        
        # Overall accuracy
        all_predicted_symbols = [asset['symbol'] for asset in predicted_assets]
        all_predicted_returns = [actual_returns.get(symbol, 0) for symbol in all_predicted_symbols if symbol in actual_returns]
        
        if all_predicted_returns:
            # For overall accuracy, we consider the direction of prediction vs actual return
            correct_predictions = 0
            for asset in predicted_assets:
                symbol = asset['symbol']
                if symbol in actual_returns:
                    actual_return = actual_returns[symbol]
                    if asset['action'] in ['OVERWEIGHT', 'ALLOCATE'] and actual_return > 0:
                        correct_predictions += 1
                    elif asset['action'] == 'UNDERWEIGHT' and actual_return < 0:
                        correct_predictions += 1
            
            accuracy_metrics['overall_accuracy'] = correct_predictions / len(all_predicted_returns)
            accuracy_metrics['total_predictions'] = len(all_predicted_returns)
            accuracy_metrics['correct_predictions'] = correct_predictions
        
        return accuracy_metrics
    
    def write_backtest_results(self, 
                             md_resource: MotherDuckResource,
                             backtest_result: BacktestResult,
                             output_table: str = "backtest_results",
                             context: Optional[dg.AssetExecutionContext] = None) -> None:
        """Write backtest results to database."""
        
        # Format results for database
        result_record = {
            "backtest_id": f"{backtest_result.prediction_date}_{backtest_result.prediction_horizon_days}d",
            "prediction_date": backtest_result.prediction_date,
            "prediction_horizon_days": backtest_result.prediction_horizon_days,
            "predicted_assets": json.dumps(backtest_result.predicted_assets),
            "actual_returns": json.dumps(backtest_result.actual_returns),
            "performance_metrics": json.dumps(backtest_result.performance_metrics),
            "prediction_accuracy": json.dumps(backtest_result.prediction_accuracy),
            "backtest_metadata": json.dumps(backtest_result.backtest_metadata),
            "backtest_timestamp": backtest_result.backtest_metadata['backtest_timestamp']
        }
        
        # Write to database
        md_resource.write_results_to_table(
            [result_record],
            output_table=output_table,
            if_exists="append",
            context=context
        )


@dg.asset(
    kinds={"backtesting", "analysis", "evaluation"},
    description="Run backtesting analysis on historical agent predictions",
    compute_kind="python",
)
def backtest_agent_predictions(
    context: dg.AssetExecutionContext,
    md: MotherDuckResource,
    backtesting_engine: BacktestingEngine,
) -> Dict[str, Any]:
    """
    Asset that runs backtesting analysis on historical agent predictions.
    
    This asset will:
    1. Take a historical prediction date
    2. Extract asset recommendations from that date
    3. Calculate actual returns for the prediction horizon
    4. Compare predicted vs actual performance
    5. Generate accuracy metrics
    
    Returns:
        Dictionary with backtesting results and metadata
    """
    context.log.info("Starting backtesting analysis...")
    
    # For now, we'll backtest the most recent analysis
    # In a production system, you might want to parameterize this
    query = """
    SELECT MAX(analysis_timestamp) as latest_timestamp
    FROM economic_cycle_analysis
    """
    
    df = md.execute_query(query, read_only=True)
    if df.is_empty():
        raise ValueError("No historical analysis found for backtesting")
    
    latest_timestamp = df[0, "latest_timestamp"]
    prediction_date = latest_timestamp.split('T')[0]  # Extract date part
    
    # Run backtest
    backtest_result = backtesting_engine.run_backtest(
        md_resource=md,
        prediction_date=prediction_date,
        prediction_horizon_days=30,  # 30-day prediction horizon
        context=context
    )
    
    # Write results to database
    context.log.info("Writing backtest results to database...")
    backtesting_engine.write_backtest_results(
        md_resource=md,
        backtest_result=backtest_result,
        output_table="backtest_results",
        context=context
    )
    
    # Return summary
    result_metadata = {
        "backtest_completed": True,
        "prediction_date": backtest_result.prediction_date,
        "prediction_horizon_days": backtest_result.prediction_horizon_days,
        "total_assets_analyzed": backtest_result.backtest_metadata['total_assets_analyzed'],
        "assets_with_data": backtest_result.backtest_metadata['assets_with_data'],
        "overall_accuracy": backtest_result.prediction_accuracy.get('overall_accuracy', 0),
        "overweight_accuracy": backtest_result.prediction_accuracy.get('overweight_accuracy', 0),
        "underweight_accuracy": backtest_result.prediction_accuracy.get('underweight_accuracy', 0),
        "total_return": backtest_result.performance_metrics.get('total_return', 0),
        "sharpe_ratio": backtest_result.performance_metrics.get('sharpe_ratio', 0),
        "output_table": "backtest_results",
        "records_written": 1
    }
    
    context.log.info(f"Backtesting analysis complete: {result_metadata}")
    return result_metadata


@dg.asset(
    kinds={"backtesting", "analysis", "batch_evaluation"},
    description="Run batch backtesting analysis on multiple historical predictions",
    compute_kind="python",
)
def batch_backtest_analysis(
    context: dg.AssetExecutionContext,
    md: MotherDuckResource,
    backtesting_engine: BacktestingEngine,
) -> Dict[str, Any]:
    """
    Asset that runs batch backtesting analysis on multiple historical predictions.
    
    This will backtest predictions from the last 6 months to provide
    comprehensive performance evaluation.
    
    Returns:
        Dictionary with batch backtesting results and summary statistics
    """
    context.log.info("Starting batch backtesting analysis...")
    
    # Get prediction dates from the last 6 months
    six_months_ago = (datetime.now() - timedelta(days=180)).strftime('%Y-%m-%d')
    
    query = """
    SELECT DISTINCT DATE(analysis_timestamp) as prediction_date
    FROM economic_cycle_analysis
    WHERE analysis_timestamp >= ?
    ORDER BY prediction_date DESC
    LIMIT 20
    """
    
    df = md.execute_query(query, read_only=True, params=[six_months_ago])
    
    if df.is_empty():
        raise ValueError("No historical analysis found for batch backtesting")
    
    prediction_dates = [row[0] for row in df.iter_rows()]
    
    context.log.info(f"Found {len(prediction_dates)} prediction dates for batch backtesting")
    
    # Run backtests for each date
    batch_results = []
    successful_backtests = 0
    
    for pred_date in prediction_dates:
        try:
            backtest_result = backtesting_engine.run_backtest(
                md_resource=md,
                prediction_date=pred_date,
                prediction_horizon_days=30,
                context=context
            )
            batch_results.append(backtest_result)
            successful_backtests += 1
            
            # Write individual result
            backtesting_engine.write_backtest_results(
                md_resource=md,
                backtest_result=backtest_result,
                output_table="backtest_results",
                context=context
            )
            
        except Exception as e:
            context.log.warning(f"Failed to backtest {pred_date}: {str(e)}")
            continue
    
    if not batch_results:
        raise ValueError("No successful backtests completed")
    
    # Calculate aggregate metrics
    all_accuracies = [result.prediction_accuracy.get('overall_accuracy', 0) for result in batch_results]
    all_returns = [result.performance_metrics.get('total_return', 0) for result in batch_results]
    all_sharpe_ratios = [result.performance_metrics.get('sharpe_ratio', 0) for result in batch_results]
    
    aggregate_metrics = {
        "total_backtests": successful_backtests,
        "avg_accuracy": np.mean(all_accuracies) if all_accuracies else 0,
        "avg_total_return": np.mean(all_returns) if all_returns else 0,
        "avg_sharpe_ratio": np.mean(all_sharpe_ratios) if all_sharpe_ratios else 0,
        "accuracy_std": np.std(all_accuracies) if all_accuracies else 0,
        "return_std": np.std(all_returns) if all_returns else 0,
        "best_accuracy": np.max(all_accuracies) if all_accuracies else 0,
        "worst_accuracy": np.min(all_accuracies) if all_accuracies else 0,
        "best_return": np.max(all_returns) if all_returns else 0,
        "worst_return": np.min(all_returns) if all_returns else 0
    }
    
    # Write batch summary
    batch_summary = {
        "batch_id": f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        "analysis_period_start": six_months_ago,
        "analysis_period_end": datetime.now().strftime('%Y-%m-%d'),
        "prediction_dates_analyzed": prediction_dates,
        "successful_backtests": successful_backtests,
        "aggregate_metrics": aggregate_metrics,
        "batch_timestamp": datetime.now().isoformat()
    }
    
    md.write_results_to_table(
        [batch_summary],
        output_table="batch_backtest_summary",
        if_exists="append",
        context=context
    )
    
    # Return summary
    result_metadata = {
        "batch_backtest_completed": True,
        "total_backtests": successful_backtests,
        "avg_accuracy": aggregate_metrics["avg_accuracy"],
        "avg_total_return": aggregate_metrics["avg_total_return"],
        "avg_sharpe_ratio": aggregate_metrics["avg_sharpe_ratio"],
        "accuracy_consistency": 1 - aggregate_metrics["accuracy_std"],  # Higher is more consistent
        "output_tables": ["backtest_results", "batch_backtest_summary"],
        "records_written": successful_backtests + 1
    }
    
    context.log.info(f"Batch backtesting analysis complete: {result_metadata}")
    return result_metadata

