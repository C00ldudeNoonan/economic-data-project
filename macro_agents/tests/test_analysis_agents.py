"""
Unit tests for analysis agents.
"""

import polars as pl
import pytest
from unittest.mock import Mock, patch, MagicMock
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
                "commodity_category": ["energy", "energy"],
                "total_return_pct": [5.0, 3.0],
            }
        )
        mock_md.execute_query.return_value = mock_df

        result = resource.get_commodity_data(mock_md)

        assert isinstance(result, str)
        mock_md.execute_query.assert_called_once()

    def test_frozen_resource_cannot_modify_fields(self):
        """Test that frozen resource fields cannot be modified directly."""
        resource = EconomicAnalysisResource(
            model_name="gpt-4-turbo-preview",
            provider="openai",
            openai_api_key="test_key",
        )

        # Verify resource is frozen - attempting to modify should raise an error
        # Pydantic frozen models raise ValidationError, Dagster wraps it as DagsterInvalidInvocationError
        with pytest.raises((Exception, ValueError)) as exc_info:
            resource.model_name = "gpt-4o"

        # Should raise an error about frozen instance or invalid invocation
        error_message = str(exc_info.value).lower()
        assert (
            "frozen" in error_message
            or "cannot" in error_message
            or "invalid" in error_message
            or "read-only" in error_message
        )

        # Verify original value is unchanged
        assert resource.model_name == "gpt-4-turbo-preview"

    @patch("dspy.LM")
    def test_setup_for_execution_with_overrides(self, mock_lm):
        """Test that setup_for_execution accepts and uses provider/model overrides."""
        resource = EconomicAnalysisResource(
            model_name="gpt-4-turbo-preview",
            provider="openai",
            openai_api_key="test_key",
            anthropic_api_key="anthropic_test_key",
        )

        mock_context = Mock()
        mock_lm_instance = MagicMock()
        mock_lm_instance.history = []
        mock_lm.return_value = mock_lm_instance

        # Setup with overrides
        resource.setup_for_execution(
            mock_context,
            provider_override="anthropic",
            model_name_override="claude-3-5-haiku-20241022",
        )

        # Verify DSPy LM was called with the override model string
        mock_lm.assert_called_once()
        call_args = mock_lm.call_args
        assert call_args[1]["model"] == "anthropic/claude-3-5-haiku-20241022"
        assert call_args[1]["api_key"] == "anthropic_test_key"

        # Verify resource fields are unchanged (frozen)
        assert resource.model_name == "gpt-4-turbo-preview"
        assert resource.provider == "openai"

    @patch("dspy.LM")
    def test_setup_for_execution_without_overrides(self, mock_lm):
        """Test that setup_for_execution uses resource fields when no overrides provided."""
        resource = EconomicAnalysisResource(
            model_name="gpt-4o",
            provider="openai",
            openai_api_key="test_key",
        )

        mock_context = Mock()
        mock_lm_instance = MagicMock()
        mock_lm_instance.history = []
        mock_lm.return_value = mock_lm_instance

        # Setup without overrides
        resource.setup_for_execution(mock_context)

        # Verify DSPy LM was called with the resource's model
        mock_lm.assert_called_once()
        call_args = mock_lm.call_args
        assert call_args[1]["model"] == "openai/gpt-4o"
        assert call_args[1]["api_key"] == "test_key"

    @patch("dspy.LM")
    def test_setup_for_execution_provider_override_only(self, mock_lm):
        """Test that setup_for_execution works with only provider override."""
        resource = EconomicAnalysisResource(
            model_name="gpt-4-turbo-preview",
            provider="openai",
            openai_api_key="test_key",
            anthropic_api_key="anthropic_test_key",
        )

        mock_context = Mock()
        mock_lm_instance = MagicMock()
        mock_lm_instance.history = []
        mock_lm.return_value = mock_lm_instance

        # Setup with only provider override
        resource.setup_for_execution(
            mock_context,
            provider_override="anthropic",
        )

        # Verify DSPy LM was called with anthropic provider but resource's model name
        mock_lm.assert_called_once()
        call_args = mock_lm.call_args
        assert call_args[1]["model"] == "anthropic/gpt-4-turbo-preview"
        assert call_args[1]["api_key"] == "anthropic_test_key"

    @patch("dspy.LM")
    def test_setup_for_execution_model_override_only(self, mock_lm):
        """Test that setup_for_execution works with only model name override."""
        resource = EconomicAnalysisResource(
            model_name="gpt-4-turbo-preview",
            provider="openai",
            openai_api_key="test_key",
        )

        mock_context = Mock()
        mock_lm_instance = MagicMock()
        mock_lm_instance.history = []
        mock_lm.return_value = mock_lm_instance

        # Setup with only model name override
        resource.setup_for_execution(
            mock_context,
            model_name_override="gpt-4o",
        )

        # Verify DSPy LM was called with resource's provider but override model name
        mock_lm.assert_called_once()
        call_args = mock_lm.call_args
        assert call_args[1]["model"] == "openai/gpt-4o"
        assert call_args[1]["api_key"] == "test_key"
