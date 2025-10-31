"""
Pytest configuration and fixtures.
"""

import pytest
import tempfile
import os
from unittest.mock import Mock
from macro_agents.defs.resources.motherduck import MotherDuckResource


@pytest.fixture
def temp_duckdb_file():
    """Create a temporary DuckDB file for testing."""
    with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as tmp_file:
        yield tmp_file.name
        # Clean up
        if os.path.exists(tmp_file.name):
            os.unlink(tmp_file.name)


@pytest.fixture
def motherduck_resource(temp_duckdb_file):
    """Create a MotherDuck resource for testing."""
    return MotherDuckResource(
        md_token="test_token", environment="dev", local_path=temp_duckdb_file
    )


@pytest.fixture
def sample_economic_data():
    """Sample economic data for testing."""
    return {
        "series_code": ["GDP", "CPI", "UNRATE"],
        "series_name": ["GDP", "CPI", "Unemployment Rate"],
        "current_value": [100.0, 2.0, 3.5],
        "pct_change_3m": [0.01, 0.02, -0.01],
        "pct_change_6m": [0.02, 0.04, -0.02],
        "pct_change_1y": [0.03, 0.06, -0.05],
        "date_grain": ["Monthly", "Monthly", "Monthly"],
    }


@pytest.fixture
def sample_market_data():
    """Sample market data for testing."""
    return {
        "symbol": ["AAPL", "GOOGL", "MSFT"],
        "asset_type": ["stock", "stock", "stock"],
        "time_period": ["12_weeks", "12_weeks", "12_weeks"],
        "total_return_pct": [10.0, 15.0, 8.0],
        "volatility_pct": [20.0, 25.0, 18.0],
        "win_rate_pct": [60.0, 65.0, 55.0],
    }


@pytest.fixture
def mock_dspy_lm():
    """Mock DSPy LM for testing."""
    mock_lm = Mock()
    mock_lm.return_value = Mock()
    return mock_lm


@pytest.fixture
def mock_dspy_settings():
    """Mock DSPy settings for testing."""
    with pytest.MonkeyPatch().context() as m:
        m.setattr("dspy.settings.configure", Mock())
        yield
