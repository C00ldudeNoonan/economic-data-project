"""
Unit tests for analysis agents.
"""

import polars as pl
from unittest.mock import Mock
from macro_agents.defs.agents.economy_state_analyzer import (
    EconomicAnalysisResource,
)
from macro_agents.defs.resources.motherduck import MotherDuckResource


class TestEconomicAnalysisResource:
    """Test cases for EconomicAnalysisResource."""

    def test_initialization(self):
        """Test EconomicAnalysisResource initialization."""
        resource = EconomicAnalysisResource(
            model_name="gpt-4-turbo-preview", openai_api_key="test_key"
        )

        assert resource.model_name == "gpt-4-turbo-preview"
        assert resource.openai_api_key == "test_key"

    def test_get_economic_data(self):
        """Test getting economic data."""
        resource = EconomicAnalysisResource(
            model_name="gpt-4-turbo-preview", openai_api_key="test_key"
        )

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

        result = resource.get_economic_data(mock_md)

        assert isinstance(result, str)
        assert "series_code" in result or "GDP" in result
        mock_md.execute_query.assert_called_once()

    def test_get_market_data(self):
        """Test getting market data."""
        resource = EconomicAnalysisResource(
            model_name="gpt-4-turbo-preview", openai_api_key="test_key"
        )

        mock_md = Mock(spec=MotherDuckResource)
        mock_df = pl.DataFrame(
            {
                "symbol": ["AAPL", "GOOGL"],
                "asset_type": ["stock", "stock"],
                "total_return_pct": [10.0, 15.0],
            }
        )
        mock_md.execute_query.return_value = mock_df

        result = resource.get_market_data(mock_md)

        assert isinstance(result, str)
        assert "symbol" in result or "AAPL" in result
        mock_md.execute_query.assert_called_once()

    def test_get_commodity_data(self):
        """Test getting commodity data."""
        resource = EconomicAnalysisResource(
            model_name="gpt-4-turbo-preview", openai_api_key="test_key"
        )

        mock_md = Mock(spec=MotherDuckResource)
        mock_df = pl.DataFrame(
            {
                "commodity_name": ["Crude Oil", "Gold"],
                "commodity_category": ["energy", "input"],
                "total_return_pct": [5.0, 3.0],
            }
        )
        mock_md.execute_query.return_value = mock_df

        result = resource.get_commodity_data(mock_md)

        assert isinstance(result, str)
        mock_md.execute_query.assert_called_once()
