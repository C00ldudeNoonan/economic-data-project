"""Compatibility shim for economy state analysis modules.

This module preserves historical import paths while the implementation
has been split into smaller modules under economy_state/.
"""

from macro_agents.defs.analysis.economy_state.assets import (
    analyze_economy_state,
    analyze_economy_state_v2,
)
from macro_agents.defs.analysis.economy_state.config import EconomicAnalysisConfig
from macro_agents.defs.analysis.economy_state.modules import EconomyStateModule
from macro_agents.defs.analysis.economy_state.rate_limits import (
    _check_rate_limit,
    _estimate_tokens,
)
from macro_agents.defs.analysis.economy_state.resource import EconomicAnalysisResource
from macro_agents.defs.analysis.economy_state.signatures import (
    EconomyStateAnalysisSignature,
)
from macro_agents.defs.analysis.economy_state.summary import (
    extract_economy_state_summary,
)
from macro_agents.defs.analysis.economy_state.token_usage import (
    _calculate_cost,
    _get_token_usage,
)

__all__ = [
    "analyze_economy_state",
    "analyze_economy_state_v2",
    "EconomicAnalysisConfig",
    "EconomyStateModule",
    "EconomicAnalysisResource",
    "EconomyStateAnalysisSignature",
    "extract_economy_state_summary",
    "_check_rate_limit",
    "_estimate_tokens",
    "_get_token_usage",
    "_calculate_cost",
]
