import dspy

from macro_agents.defs.analysis.economy_state.signatures import (
    EconomyStateAnalysisSignature,
)


class EconomyStateModule(dspy.Module):
    """DSPy module for analyzing current economy state."""

    def __init__(self, personality: str = "skeptical"):
        super().__init__()
        self.personality = personality
        self.analyze_state = dspy.ChainOfThought(EconomyStateAnalysisSignature)

    def forward(
        self,
        economic_data: str,
        commodity_data: str,
        financial_conditions_index: str,
        housing_data: str = "",
        yield_curve_data: str = "",
        economic_trends: str = "",
        personality: str | None = None,
    ):
        personality_to_use = personality or self.personality
        return self.analyze_state(
            economic_data=economic_data,
            commodity_data=commodity_data,
            financial_conditions_index=financial_conditions_index,
            housing_data=housing_data or "No housing data available",
            yield_curve_data=yield_curve_data or "No yield curve data available",
            economic_trends=economic_trends or "No economic trends data available",
            personality=personality_to_use,
        )
