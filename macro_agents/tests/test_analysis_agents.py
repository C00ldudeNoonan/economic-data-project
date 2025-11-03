"""
Unit tests for analysis agents.
"""

import polars as pl
from unittest.mock import Mock
from macro_agents.defs.agents.analysis_agent import (
    EconomicAnalyzer,
)
from macro_agents.defs.agents.economic_cycle_analyzer import (
    EconomicCycleAnalyzer,
)
from macro_agents.defs.agents.asset_allocation_analyzer import (
    AssetAllocationAnalyzer,
)
from macro_agents.defs.resources.motherduck import MotherDuckResource


class TestEconomicAnalyzer:
    """Test cases for EconomicAnalyzer."""

    def test_initialization(self):
        """Test EconomicAnalyzer initialization."""
        analyzer = EconomicAnalyzer(
            model_name="gpt-4-turbo-preview", openai_api_key="test_key"
        )

        assert analyzer.model_name == "gpt-4-turbo-preview"
        assert analyzer.openai_api_key == "test_key"


class TestEconomicCycleAnalyzer:
    """Test cases for EconomicCycleAnalyzer."""

    def test_initialization(self):
        """Test EconomicCycleAnalyzer initialization."""
        analyzer = EconomicCycleAnalyzer(
            model_name="gpt-4-turbo-preview", openai_api_key="test_key"
        )

        assert analyzer.model_name == "gpt-4-turbo-preview"
        assert analyzer.openai_api_key == "test_key"

    def test_get_economic_data(self):
        """Test getting economic data."""
        analyzer = EconomicCycleAnalyzer(
            model_name="gpt-4-turbo-preview", openai_api_key="test_key"
        )

        # Mock MotherDuck resource
        mock_md = Mock(spec=MotherDuckResource)
        mock_df = pl.DataFrame(
            {
                "series_code": ["GDP", "CPI"],
                "series_name": ["GDP", "CPI"],
                "current_value": [100.0, 2.0],
                "pct_change_3m": [0.01, 0.02],
            }
        )
        mock_md.execute_query.return_value = mock_df

        result = analyzer.get_economic_data(mock_md)

        assert "series_code" in result
        assert "GDP" in result
        mock_md.execute_query.assert_called_once()

    def test_get_market_data(self):
        """Test getting market data."""
        analyzer = EconomicCycleAnalyzer(
            model_name="gpt-4-turbo-preview", openai_api_key="test_key"
        )

        # Mock MotherDuck resource
        mock_md = Mock(spec=MotherDuckResource)
        mock_df = pl.DataFrame(
            {
                "symbol": ["AAPL", "GOOGL"],
                "asset_type": ["stock", "stock"],
                "total_return_pct": [10.0, 15.0],
            }
        )
        mock_md.execute_query.return_value = mock_df

        result = analyzer.get_market_data(mock_md)

        assert "symbol" in result
        assert "AAPL" in result
        mock_md.execute_query.assert_called_once()


class TestAssetAllocationAnalyzer:
    """Test cases for AssetAllocationAnalyzer."""

    def test_initialization(self):
        """Test AssetAllocationAnalyzer initialization."""
        analyzer = AssetAllocationAnalyzer(
            model_name="gpt-4-turbo-preview", openai_api_key="test_key"
        )

        assert analyzer.model_name == "gpt-4-turbo-preview"
        assert analyzer.openai_api_key == "test_key"

    def test_get_latest_analysis(self):
        """Test getting latest analysis."""
        analyzer = AssetAllocationAnalyzer(
            model_name="gpt-4-turbo-preview", openai_api_key="test_key"
        )

        # Mock MotherDuck resource
        mock_md = Mock(spec=MotherDuckResource)
        mock_df = pl.DataFrame(
            {
                "analysis_content": ["cycle analysis", "trend analysis"],
                "analysis_type": ["economic_cycle", "market_trends"],
            }
        )
        mock_md.execute_query.return_value = mock_df

        cycle_analysis, trend_analysis = analyzer.get_latest_analysis(mock_md)

        assert cycle_analysis == "cycle analysis"
        assert trend_analysis == "trend analysis"
        mock_md.execute_query.assert_called_once()
