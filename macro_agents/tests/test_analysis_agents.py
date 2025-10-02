"""
Unit tests for analysis agents.
"""

import pytest
import polars as pl
from unittest.mock import Mock, patch, MagicMock
from macro_agents.defs.agents.analysis_agent import EconomicAnalyzer, EconomicAnalysisModule
from macro_agents.defs.agents.economic_cycle_analyzer import EconomicCycleAnalyzer, EconomicCycleModule
from macro_agents.defs.agents.asset_allocation_analyzer import AssetAllocationAnalyzer, AssetAllocationModule
from macro_agents.defs.resources.motherduck import MotherDuckResource


class TestEconomicAnalyzer:
    """Test cases for EconomicAnalyzer."""

    def test_initialization(self):
        """Test EconomicAnalyzer initialization."""
        analyzer = EconomicAnalyzer(
            model_name="gpt-4-turbo-preview",
            openai_api_key="test_key"
        )
        
        assert analyzer.model_name == "gpt-4-turbo-preview"
        assert analyzer.openai_api_key == "test_key"

    @patch('dspy.LM')
    @patch('dspy.settings.configure')
    def test_setup_for_execution(self, mock_configure, mock_lm):
        """Test setup for execution."""
        analyzer = EconomicAnalyzer(
            model_name="gpt-4-turbo-preview",
            openai_api_key="test_key"
        )
        
        mock_context = Mock()
        analyzer.setup_for_execution(mock_context)
        
        mock_lm.assert_called_once()
        mock_configure.assert_called_once()

    def test_format_results_as_json(self):
        """Test formatting results as JSON."""
        analyzer = EconomicAnalyzer(
            model_name="gpt-4-turbo-preview",
            openai_api_key="test_key"
        )
        
        category_results = {
            "Technology": "Tech analysis",
            "Healthcare": "Health analysis"
        }
        
        json_results = analyzer.format_results_as_json(
            category_results,
            sample_size=50,
            metadata={"test": "value"}
        )
        
        assert len(json_results) == 2
        assert json_results[0]["category"] == "Technology"
        assert json_results[0]["num_samples"] == 50
        assert json_results[0]["test"] == "value"


class TestEconomicCycleAnalyzer:
    """Test cases for EconomicCycleAnalyzer."""

    def test_initialization(self):
        """Test EconomicCycleAnalyzer initialization."""
        analyzer = EconomicCycleAnalyzer(
            model_name="gpt-4-turbo-preview",
            openai_api_key="test_key"
        )
        
        assert analyzer.model_name == "gpt-4-turbo-preview"
        assert analyzer.openai_api_key == "test_key"

    @patch('dspy.LM')
    @patch('dspy.settings.configure')
    def test_setup_for_execution(self, mock_configure, mock_lm):
        """Test setup for execution."""
        analyzer = EconomicCycleAnalyzer(
            model_name="gpt-4-turbo-preview",
            openai_api_key="test_key"
        )
        
        mock_context = Mock()
        analyzer.setup_for_execution(mock_context)
        
        mock_lm.assert_called_once()
        mock_configure.assert_called_once()

    def test_get_economic_data(self):
        """Test getting economic data."""
        analyzer = EconomicCycleAnalyzer(
            model_name="gpt-4-turbo-preview",
            openai_api_key="test_key"
        )
        
        # Mock MotherDuck resource
        mock_md = Mock(spec=MotherDuckResource)
        mock_df = pl.DataFrame({
            "series_code": ["GDP", "CPI"],
            "series_name": ["GDP", "CPI"],
            "current_value": [100.0, 2.0],
            "pct_change_3m": [0.01, 0.02]
        })
        mock_md.execute_query.return_value = mock_df
        
        result = analyzer.get_economic_data(mock_md)
        
        assert "series_code" in result
        assert "GDP" in result
        mock_md.execute_query.assert_called_once()

    def test_get_market_data(self):
        """Test getting market data."""
        analyzer = EconomicCycleAnalyzer(
            model_name="gpt-4-turbo-preview",
            openai_api_key="test_key"
        )
        
        # Mock MotherDuck resource
        mock_md = Mock(spec=MotherDuckResource)
        mock_df = pl.DataFrame({
            "symbol": ["AAPL", "GOOGL"],
            "asset_type": ["stock", "stock"],
            "total_return_pct": [10.0, 15.0]
        })
        mock_md.execute_query.return_value = mock_df
        
        result = analyzer.get_market_data(mock_md)
        
        assert "symbol" in result
        assert "AAPL" in result
        mock_md.execute_query.assert_called_once()

    def test_format_cycle_analysis_as_json(self):
        """Test formatting cycle analysis as JSON."""
        analyzer = EconomicCycleAnalyzer(
            model_name="gpt-4-turbo-preview",
            openai_api_key="test_key"
        )
        
        analysis_result = {
            "economic_cycle_analysis": "Cycle analysis",
            "market_trend_analysis": "Trend analysis",
            "analysis_timestamp": "2024-01-01T00:00:00",
            "model_name": "gpt-4-turbo-preview"
        }
        
        json_results = analyzer.format_cycle_analysis_as_json(
            analysis_result,
            metadata={"test": "value"}
        )
        
        assert len(json_results) == 2
        assert json_results[0]["analysis_type"] == "economic_cycle"
        assert json_results[1]["analysis_type"] == "market_trends"


class TestAssetAllocationAnalyzer:
    """Test cases for AssetAllocationAnalyzer."""

    def test_initialization(self):
        """Test AssetAllocationAnalyzer initialization."""
        analyzer = AssetAllocationAnalyzer(
            model_name="gpt-4-turbo-preview",
            openai_api_key="test_key"
        )
        
        assert analyzer.model_name == "gpt-4-turbo-preview"
        assert analyzer.openai_api_key == "test_key"

    @patch('dspy.LM')
    @patch('dspy.settings.configure')
    def test_setup_for_execution(self, mock_configure, mock_lm):
        """Test setup for execution."""
        analyzer = AssetAllocationAnalyzer(
            model_name="gpt-4-turbo-preview",
            openai_api_key="test_key"
        )
        
        mock_context = Mock()
        analyzer.setup_for_execution(mock_context)
        
        mock_lm.assert_called_once()
        mock_configure.assert_called_once()

    def test_get_latest_analysis(self):
        """Test getting latest analysis."""
        analyzer = AssetAllocationAnalyzer(
            model_name="gpt-4-turbo-preview",
            openai_api_key="test_key"
        )
        
        # Mock MotherDuck resource
        mock_md = Mock(spec=MotherDuckResource)
        mock_df = pl.DataFrame({
            "analysis_content": ["cycle analysis", "trend analysis"],
            "analysis_type": ["economic_cycle", "market_trends"]
        })
        mock_md.execute_query.return_value = mock_df
        
        cycle_analysis, trend_analysis = analyzer.get_latest_analysis(mock_md)
        
        assert cycle_analysis == "cycle analysis"
        assert trend_analysis == "trend analysis"
        mock_md.execute_query.assert_called_once()

    def test_format_allocation_as_json(self):
        """Test formatting allocation as JSON."""
        analyzer = AssetAllocationAnalyzer(
            model_name="gpt-4-turbo-preview",
            openai_api_key="test_key"
        )
        
        allocation_result = {
            "allocation_recommendations": "Allocation recommendations",
            "analysis_timestamp": "2024-01-01T00:00:00",
            "model_name": "gpt-4-turbo-preview"
        }
        
        json_result = analyzer.format_allocation_as_json(
            allocation_result,
            metadata={"test": "value"}
        )
        
        assert json_result["analysis_type"] == "asset_allocation_recommendations"
        assert json_result["analysis_content"] == "Allocation recommendations"
        assert json_result["test"] == "value"


class TestDSPyModules:
    """Test cases for DSPy modules."""

    def test_economic_analysis_module(self):
        """Test EconomicAnalysisModule."""
        module = EconomicAnalysisModule()
        assert module.analyze is not None

    def test_economic_cycle_module(self):
        """Test EconomicCycleModule."""
        module = EconomicCycleModule()
        assert module.analyze_cycle is not None

    def test_asset_allocation_module(self):
        """Test AssetAllocationModule."""
        module = AssetAllocationModule()
        assert module.analyze_allocation is not None