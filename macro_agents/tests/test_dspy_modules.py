"""
Tests for DSPy modules and signatures.
"""

from unittest.mock import Mock

import dspy
from macro_agents.defs.analysis.asset_relationships.asset_class_relationship_analyzer import (
    AssetClassRelationshipModule,
    AssetClassRelationshipSignature,
)
from macro_agents.defs.analysis.economy_state.economy_state_analyzer import (
    EconomyStateAnalysisSignature,
    EconomyStateModule,
)
from macro_agents.defs.analysis.investments.investment_recommendations import (
    InvestmentRecommendationsModule,
    InvestmentRecommendationsSignature,
)


class TestEconomyStateAnalysisSignature:
    """Test cases for EconomyStateAnalysisSignature."""

    def test_signature_fields(self):
        """Test that signature has correct input and output fields."""
        assert issubclass(EconomyStateAnalysisSignature, dspy.Signature)

        annotations = EconomyStateAnalysisSignature.__annotations__
        assert "economic_data" in annotations
        assert "commodity_data" in annotations
        assert "financial_conditions_index" in annotations
        assert "housing_data" in annotations
        assert "yield_curve_data" in annotations
        assert "economic_trends" in annotations
        assert "personality" in annotations
        assert "analysis" in annotations

    def test_signature_description(self):
        """Test that signature has proper description."""
        assert EconomyStateAnalysisSignature.__doc__ is not None
        assert "economic indicators" in EconomyStateAnalysisSignature.__doc__.lower()


class TestEconomyStateModule:
    """Test cases for EconomyStateModule."""

    def test_module_initialization_default_personality(self):
        """Test module initialization with default personality."""
        module = EconomyStateModule()
        assert module.personality == "skeptical"
        assert hasattr(module, "analyze_state")
        assert isinstance(module.analyze_state, dspy.ChainOfThought)

    def test_module_initialization_custom_personality(self):
        """Test module initialization with custom personality."""
        module = EconomyStateModule(personality="bullish")
        assert module.personality == "bullish"

    def test_module_forward_with_default_personality(self):
        """Test module forward method using default personality."""
        module = EconomyStateModule(personality="neutral")

        mock_result = Mock()
        mock_result.analysis = "Test analysis output"
        module.analyze_state = Mock(return_value=mock_result)

        result = module.forward(
            economic_data="test economic data",
            commodity_data="test commodity data",
            financial_conditions_index="test financial conditions",
        )

        assert result.analysis == "Test analysis output"
        module.analyze_state.assert_called_once_with(
            economic_data="test economic data",
            commodity_data="test commodity data",
            financial_conditions_index="test financial conditions",
            housing_data="No housing data available",
            yield_curve_data="No yield curve data available",
            economic_trends="No economic trends data available",
            personality="neutral",
        )

    def test_module_forward_with_override_personality(self):
        """Test module forward method with personality override."""
        module = EconomyStateModule(personality="skeptical")

        mock_result = Mock()
        mock_result.analysis = "Test analysis output"
        module.analyze_state = Mock(return_value=mock_result)

        result = module.forward(
            economic_data="test economic data",
            commodity_data="test commodity data",
            financial_conditions_index="test financial conditions",
            personality="bullish",
        )

        assert result.analysis == "Test analysis output"
        module.analyze_state.assert_called_once_with(
            economic_data="test economic data",
            commodity_data="test commodity data",
            financial_conditions_index="test financial conditions",
            housing_data="No housing data available",
            yield_curve_data="No yield curve data available",
            economic_trends="No economic trends data available",
            personality="bullish",
        )

    def test_module_forward_with_empty_data(self):
        """Test module forward method with empty data."""
        module = EconomyStateModule()

        mock_result = Mock()
        mock_result.analysis = "Analysis with empty data"
        module.analyze_state = Mock(return_value=mock_result)

        result = module.forward(
            economic_data="",
            commodity_data="",
            financial_conditions_index="",
        )

        assert result.analysis == "Analysis with empty data"
        module.analyze_state.assert_called_once()
        # Verify it was called with default values for new parameters
        call_args = module.analyze_state.call_args
        assert call_args.kwargs["housing_data"] == "No housing data available"
        assert call_args.kwargs["yield_curve_data"] == "No yield curve data available"
        assert (
            call_args.kwargs["economic_trends"] == "No economic trends data available"
        )

    def test_module_forward_with_explicit_new_data(self):
        """Test module forward method with new data parameters explicitly provided."""
        module = EconomyStateModule(personality="neutral")

        mock_result = Mock()
        mock_result.analysis = "Test analysis output"
        module.analyze_state = Mock(return_value=mock_result)

        result = module.forward(
            economic_data="test economic data",
            commodity_data="test commodity data",
            financial_conditions_index="test financial conditions",
            housing_data="test housing data",
            yield_curve_data="test yield curve data",
            economic_trends="test economic trends",
        )

        assert result.analysis == "Test analysis output"
        module.analyze_state.assert_called_once_with(
            economic_data="test economic data",
            commodity_data="test commodity data",
            financial_conditions_index="test financial conditions",
            housing_data="test housing data",
            yield_curve_data="test yield curve data",
            economic_trends="test economic trends",
            personality="neutral",
        )


class TestInvestmentRecommendationsSignature:
    """Test cases for InvestmentRecommendationsSignature."""

    def test_signature_fields(self):
        """Test that signature has correct input and output fields."""
        assert issubclass(InvestmentRecommendationsSignature, dspy.Signature)

        annotations = InvestmentRecommendationsSignature.__annotations__
        assert "economy_state_analysis" in annotations
        assert "asset_class_relationship_analysis" in annotations
        assert "personality" in annotations
        assert "recommendations" in annotations

    def test_signature_description(self):
        """Test that signature has proper description."""
        assert InvestmentRecommendationsSignature.__doc__ is not None
        assert (
            "investment recommendations"
            in InvestmentRecommendationsSignature.__doc__.lower()
        )


class TestInvestmentRecommendationsModule:
    """Test cases for InvestmentRecommendationsModule."""

    def test_module_initialization_default_personality(self):
        """Test module initialization with default personality."""
        module = InvestmentRecommendationsModule()
        assert module.personality == "skeptical"
        assert hasattr(module, "generate_recommendations")
        assert isinstance(module.generate_recommendations, dspy.ChainOfThought)

    def test_module_initialization_custom_personality(self):
        """Test module initialization with custom personality."""
        module = InvestmentRecommendationsModule(personality="bullish")
        assert module.personality == "bullish"

    def test_module_forward_with_default_personality(self):
        """Test module forward method using default personality."""
        module = InvestmentRecommendationsModule(personality="neutral")

        mock_result = Mock()
        mock_result.recommendations = "Test recommendations output"
        module.generate_recommendations = Mock(return_value=mock_result)

        result = module.forward(
            economy_state_analysis="test economy state",
            asset_class_relationship_analysis="test relationships",
        )

        assert result.recommendations == "Test recommendations output"
        module.generate_recommendations.assert_called_once_with(
            economy_state_analysis="test economy state",
            asset_class_relationship_analysis="test relationships",
            personality="neutral",
        )

    def test_module_forward_with_override_personality(self):
        """Test module forward method with personality override."""
        module = InvestmentRecommendationsModule(personality="skeptical")

        mock_result = Mock()
        mock_result.recommendations = "Test recommendations output"
        module.generate_recommendations = Mock(return_value=mock_result)

        result = module.forward(
            economy_state_analysis="test economy state",
            asset_class_relationship_analysis="test relationships",
            personality="bullish",
        )

        assert result.recommendations == "Test recommendations output"
        module.generate_recommendations.assert_called_once_with(
            economy_state_analysis="test economy state",
            asset_class_relationship_analysis="test relationships",
            personality="bullish",
        )

    def test_module_forward_with_empty_data(self):
        """Test module forward method with empty data."""
        module = InvestmentRecommendationsModule()

        mock_result = Mock()
        mock_result.recommendations = "Recommendations with empty data"
        module.generate_recommendations = Mock(return_value=mock_result)

        result = module.forward(
            economy_state_analysis="",
            asset_class_relationship_analysis="",
        )

        assert result.recommendations == "Recommendations with empty data"
        module.generate_recommendations.assert_called_once()


class TestAssetClassRelationshipSignature:
    """Test cases for AssetClassRelationshipSignature."""

    def test_signature_fields(self):
        """Test that signature has correct input and output fields."""
        assert issubclass(AssetClassRelationshipSignature, dspy.Signature)

        annotations = AssetClassRelationshipSignature.__annotations__
        assert "economy_state_analysis" in annotations
        assert "market_data" in annotations
        assert "correlation_data" in annotations
        assert "commodity_data" in annotations
        assert "relationship_analysis" in annotations

    def test_signature_description(self):
        """Test that signature has proper description."""
        assert AssetClassRelationshipSignature.__doc__ is not None
        assert "asset class" in AssetClassRelationshipSignature.__doc__.lower()


class TestAssetClassRelationshipModule:
    """Test cases for AssetClassRelationshipModule."""

    def test_module_initialization(self):
        """Test module initialization."""
        module = AssetClassRelationshipModule()
        assert hasattr(module, "analyze_relationships")
        assert isinstance(module.analyze_relationships, dspy.ChainOfThought)

    def test_module_forward_with_all_data(self):
        """Test module forward method with all data provided."""
        module = AssetClassRelationshipModule()

        mock_result = Mock()
        mock_result.relationship_analysis = "Test relationship analysis"
        module.analyze_relationships = Mock(return_value=mock_result)

        result = module.forward(
            economy_state_analysis="test economy state",
            market_data="test market data",
            correlation_data="test correlation data",
            commodity_data="test commodity data",
        )

        assert result.relationship_analysis == "Test relationship analysis"
        module.analyze_relationships.assert_called_once_with(
            economy_state_analysis="test economy state",
            market_data="test market data",
            correlation_data="test correlation data",
            commodity_data="test commodity data",
        )

    def test_module_forward_with_empty_correlation_data(self):
        """Test module forward method with empty correlation data."""
        module = AssetClassRelationshipModule()

        mock_result = Mock()
        mock_result.relationship_analysis = "Test relationship analysis"
        module.analyze_relationships = Mock(return_value=mock_result)

        result = module.forward(
            economy_state_analysis="test economy state",
            market_data="test market data",
            correlation_data="",
            commodity_data="test commodity data",
        )

        assert result.relationship_analysis == "Test relationship analysis"
        module.analyze_relationships.assert_called_once_with(
            economy_state_analysis="test economy state",
            market_data="test market data",
            correlation_data="No correlation data available.",
            commodity_data="test commodity data",
        )

    def test_module_forward_with_empty_commodity_data(self):
        """Test module forward method with empty commodity data."""
        module = AssetClassRelationshipModule()

        mock_result = Mock()
        mock_result.relationship_analysis = "Test relationship analysis"
        module.analyze_relationships = Mock(return_value=mock_result)

        result = module.forward(
            economy_state_analysis="test economy state",
            market_data="test market data",
            correlation_data="test correlation data",
            commodity_data="",
        )

        assert result.relationship_analysis == "Test relationship analysis"
        module.analyze_relationships.assert_called_once_with(
            economy_state_analysis="test economy state",
            market_data="test market data",
            correlation_data="test correlation data",
            commodity_data="No commodity data available.",
        )

    def test_module_forward_with_all_empty_data(self):
        """Test module forward method with all empty data."""
        module = AssetClassRelationshipModule()

        mock_result = Mock()
        mock_result.relationship_analysis = "Analysis with empty data"
        module.analyze_relationships = Mock(return_value=mock_result)

        result = module.forward(
            economy_state_analysis="",
            market_data="",
            correlation_data="",
            commodity_data="",
        )

        assert result.relationship_analysis == "Analysis with empty data"
        module.analyze_relationships.assert_called_once()


class TestPersonalityHandling:
    """Test cases for personality handling across modules."""

    def test_personality_values(self):
        """Test that personality values are handled correctly."""
        valid_personalities = ["skeptical", "neutral", "bullish"]

        for personality in valid_personalities:
            economy_module = EconomyStateModule(personality=personality)
            assert economy_module.personality == personality

            recommendations_module = InvestmentRecommendationsModule(
                personality=personality
            )
            assert recommendations_module.personality == personality

    def test_personality_override_priority(self):
        """Test that provided personality takes priority over default."""
        module = EconomyStateModule(personality="skeptical")

        mock_result = Mock()
        mock_result.analysis = "Test"
        module.analyze_state = Mock(return_value=mock_result)

        module.forward(
            economic_data="test",
            commodity_data="test",
            financial_conditions_index="test financial conditions",
            personality="bullish",
        )

        module.analyze_state.assert_called_once_with(
            economic_data="test",
            commodity_data="test",
            financial_conditions_index="test financial conditions",
            housing_data="No housing data available",
            yield_curve_data="No yield curve data available",
            economic_trends="No economic trends data available",
            personality="bullish",
        )


class TestModuleIntegration:
    """Integration tests for modules working together."""

    def test_economy_state_module_with_mock_lm(self):
        """Test EconomyStateModule with mocked LM."""
        module = EconomyStateModule(personality="neutral")

        mock_result = Mock()
        mock_result.analysis = "Comprehensive economic analysis"
        module.analyze_state = Mock(return_value=mock_result)

        result = module.forward(
            economic_data="GDP,100.0,0.01,0.02,0.03",
            commodity_data="Oil,50.0,0.05",
            financial_conditions_index="FCI: 0.5",
        )

        assert result.analysis == "Comprehensive economic analysis"
        assert module.analyze_state.called
        # Verify new parameters are included
        call_args = module.analyze_state.call_args
        assert "housing_data" in call_args.kwargs
        assert "yield_curve_data" in call_args.kwargs
        assert "economic_trends" in call_args.kwargs

    def test_investment_recommendations_module_with_mock_lm(self):
        """Test InvestmentRecommendationsModule with mocked LM."""
        module = InvestmentRecommendationsModule(personality="bullish")

        mock_result = Mock()
        mock_result.recommendations = "OVERWEIGHT: XLK, SPY"
        module.generate_recommendations = Mock(return_value=mock_result)

        result = module.forward(
            economy_state_analysis="Economy in expansion phase",
            asset_class_relationship_analysis="Tech sector showing strength",
        )

        assert result.recommendations == "OVERWEIGHT: XLK, SPY"
        assert module.generate_recommendations.called

    def test_asset_class_relationship_module_with_mock_lm(self):
        """Test AssetClassRelationshipModule with mocked LM."""
        module = AssetClassRelationshipModule()

        mock_result = Mock()
        mock_result.relationship_analysis = "Strong correlation between tech and GDP"
        module.analyze_relationships = Mock(return_value=mock_result)

        result = module.forward(
            economy_state_analysis="Economy expanding",
            market_data="XLK,10.0,15.0",
            correlation_data="GDP vs XLK: 0.75",
            commodity_data="Oil,50.0,0.05",
        )

        assert result.relationship_analysis == "Strong correlation between tech and GDP"
        assert module.analyze_relationships.called


class TestExtractFunctionsWithEmptyContent:
    """Test cases for extract functions handling None/empty content."""

    def test_extract_economy_state_summary_with_none(self):
        """Test extract_economy_state_summary handles None content."""
        from macro_agents.defs.analysis.economy_state.economy_state_analyzer import (
            extract_economy_state_summary,
        )

        result = extract_economy_state_summary(None)
        assert isinstance(result, dict)
        assert len(result) == 0

    def test_extract_economy_state_summary_with_empty_string(self):
        """Test extract_economy_state_summary handles empty string."""
        from macro_agents.defs.analysis.economy_state.economy_state_analyzer import (
            extract_economy_state_summary,
        )

        result = extract_economy_state_summary("")
        assert isinstance(result, dict)
        assert len(result) == 0

    def test_extract_economy_state_summary_with_valid_content(self):
        """Test extract_economy_state_summary with valid content."""
        from macro_agents.defs.analysis.economy_state.economy_state_analyzer import (
            extract_economy_state_summary,
        )

        content = """
        Current Economic Cycle Position: Expansion
        Confidence: 0.75
        Risk Factors: Inflation concerns, geopolitical tensions
        """
        result = extract_economy_state_summary(content)
        assert isinstance(result, dict)
        assert "economic_cycle_position" in result
        assert result["economic_cycle_position"] == "Expansion"
        assert "confidence_level" in result
        assert result["confidence_level"] == 0.75

    def test_extract_relationship_summary_with_none(self):
        """Test extract_relationship_summary handles None content."""
        from macro_agents.defs.analysis.asset_relationships.asset_class_relationship_analyzer import (
            extract_relationship_summary,
        )

        result = extract_relationship_summary(None)
        assert isinstance(result, dict)
        assert len(result) == 0

    def test_extract_relationship_summary_with_empty_string(self):
        """Test extract_relationship_summary handles empty string."""
        from macro_agents.defs.analysis.asset_relationships.asset_class_relationship_analyzer import (
            extract_relationship_summary,
        )

        result = extract_relationship_summary("")
        assert isinstance(result, dict)
        assert len(result) == 0

    def test_extract_recommendations_summary_with_none(self):
        """Test extract_recommendations_summary handles None content."""
        from macro_agents.defs.analysis.investments.investment_recommendations import (
            extract_recommendations_summary,
        )

        result = extract_recommendations_summary(None)
        assert isinstance(result, dict)
        assert "total_overweight_count" in result
        assert "total_underweight_count" in result
        assert result["total_overweight_count"] == 0
        assert result["total_underweight_count"] == 0

    def test_extract_recommendations_summary_with_empty_string(self):
        """Test extract_recommendations_summary handles empty string."""
        from macro_agents.defs.analysis.investments.investment_recommendations import (
            extract_recommendations_summary,
        )

        result = extract_recommendations_summary("")
        assert isinstance(result, dict)
        assert "total_overweight_count" in result
        assert "total_underweight_count" in result
        assert result["total_overweight_count"] == 0
        assert result["total_underweight_count"] == 0


class TestEconomicIndicatorNarrativeSignature:
    """Test cases for EconomicIndicatorNarrativeSignature."""

    def test_signature_fields(self):
        """Test that signature has correct input and output fields."""
        from macro_agents.defs.analysis.narratives.economic_narrative_generator import (
            EconomicIndicatorNarrativeSignature,
        )

        assert issubclass(EconomicIndicatorNarrativeSignature, dspy.Signature)

        annotations = EconomicIndicatorNarrativeSignature.__annotations__
        # Input fields
        assert "indicator_name" in annotations
        assert "indicator_category" in annotations
        assert "current_value" in annotations
        assert "previous_value" in annotations
        assert "expected_value" in annotations
        assert "historical_context" in annotations
        assert "related_indicators" in annotations
        assert "current_fed_policy" in annotations
        assert "personality" in annotations
        # Output fields
        assert "headline" in annotations
        assert "summary" in annotations
        assert "market_implications" in annotations
        assert "retail_investor_takeaway" in annotations
        assert "historical_comparison" in annotations
        assert "confidence_level" in annotations

    def test_signature_description(self):
        """Test that signature has proper description."""
        from macro_agents.defs.analysis.narratives.economic_narrative_generator import (
            EconomicIndicatorNarrativeSignature,
        )

        assert EconomicIndicatorNarrativeSignature.__doc__ is not None
        assert "narrative" in EconomicIndicatorNarrativeSignature.__doc__.lower()


class TestIndicatorForecastSignature:
    """Test cases for IndicatorForecastSignature."""

    def test_signature_fields(self):
        """Test that signature has correct input and output fields."""
        from macro_agents.defs.analysis.narratives.economic_narrative_generator import (
            IndicatorForecastSignature,
        )

        assert issubclass(IndicatorForecastSignature, dspy.Signature)

        annotations = IndicatorForecastSignature.__annotations__
        # Input fields
        assert "indicator_name" in annotations
        assert "historical_data" in annotations
        assert "leading_indicators" in annotations
        assert "current_economic_conditions" in annotations
        assert "personality" in annotations
        # Output fields
        assert "next_release_forecast" in annotations
        assert "three_month_outlook" in annotations
        assert "forecast_rationale" in annotations

    def test_signature_description(self):
        """Test that signature has proper description."""
        from macro_agents.defs.analysis.narratives.economic_narrative_generator import (
            IndicatorForecastSignature,
        )

        assert IndicatorForecastSignature.__doc__ is not None
        assert "forecast" in IndicatorForecastSignature.__doc__.lower()


class TestEconomicNarrativeModule:
    """Test cases for EconomicNarrativeModule."""

    def test_module_initialization_default_personality(self):
        """Test module initialization with default personality."""
        from macro_agents.defs.analysis.narratives.economic_narrative_generator import (
            EconomicNarrativeModule,
        )

        module = EconomicNarrativeModule()
        assert module.personality == "neutral"
        assert hasattr(module, "generate_narrative")
        assert isinstance(module.generate_narrative, dspy.ChainOfThought)

    def test_module_initialization_custom_personality(self):
        """Test module initialization with custom personality."""
        from macro_agents.defs.analysis.narratives.economic_narrative_generator import (
            EconomicNarrativeModule,
        )

        module = EconomicNarrativeModule(personality="skeptical")
        assert module.personality == "skeptical"

        module = EconomicNarrativeModule(personality="bullish")
        assert module.personality == "bullish"

    def test_module_forward_with_default_personality(self):
        """Test module forward method using default personality."""
        from macro_agents.defs.analysis.narratives.economic_narrative_generator import (
            EconomicNarrativeModule,
        )

        module = EconomicNarrativeModule(personality="neutral")

        mock_result = Mock()
        mock_result.headline = "Inflation Cools as CPI Falls"
        mock_result.summary = "Test summary"
        mock_result.market_implications = "Test implications"
        mock_result.retail_investor_takeaway = "Test takeaway"
        mock_result.historical_comparison = "Test comparison"
        mock_result.confidence_level = "high"
        module.generate_narrative = Mock(return_value=mock_result)

        result = module.forward(
            indicator_name="CPI",
            indicator_category="inflation",
            current_value="3.2%",
            previous_value="3.5%",
            expected_value="3.3%",
            historical_context="Historical CPI data",
        )

        assert result.headline == "Inflation Cools as CPI Falls"
        module.generate_narrative.assert_called_once_with(
            indicator_name="CPI",
            indicator_category="inflation",
            current_value="3.2%",
            previous_value="3.5%",
            expected_value="3.3%",
            historical_context="Historical CPI data",
            related_indicators="No related indicators available",
            current_fed_policy="No Fed policy context available",
            personality="neutral",
        )

    def test_module_forward_with_override_personality(self):
        """Test module forward method with personality override."""
        from macro_agents.defs.analysis.narratives.economic_narrative_generator import (
            EconomicNarrativeModule,
        )

        module = EconomicNarrativeModule(personality="neutral")

        mock_result = Mock()
        mock_result.headline = "Test headline"
        module.generate_narrative = Mock(return_value=mock_result)

        module.forward(
            indicator_name="NFP",
            indicator_category="employment",
            current_value="250,000",
            previous_value="200,000",
            expected_value="220,000",
            historical_context="Historical NFP data",
            personality="bullish",
        )

        call_args = module.generate_narrative.call_args
        assert call_args.kwargs["personality"] == "bullish"

    def test_module_forward_with_optional_fields(self):
        """Test module forward method with optional fields provided."""
        from macro_agents.defs.analysis.narratives.economic_narrative_generator import (
            EconomicNarrativeModule,
        )

        module = EconomicNarrativeModule()

        mock_result = Mock()
        module.generate_narrative = Mock(return_value=mock_result)

        module.forward(
            indicator_name="CPI",
            indicator_category="inflation",
            current_value="3.2%",
            previous_value="3.5%",
            expected_value="3.3%",
            historical_context="Historical CPI data",
            related_indicators="Core CPI: 3.8%",
            current_fed_policy="Fed maintaining hawkish stance",
        )

        call_args = module.generate_narrative.call_args
        assert call_args.kwargs["related_indicators"] == "Core CPI: 3.8%"
        assert (
            call_args.kwargs["current_fed_policy"] == "Fed maintaining hawkish stance"
        )


class TestIndicatorForecastModule:
    """Test cases for IndicatorForecastModule."""

    def test_module_initialization_default_personality(self):
        """Test module initialization with default personality."""
        from macro_agents.defs.analysis.narratives.economic_narrative_generator import (
            IndicatorForecastModule,
        )

        module = IndicatorForecastModule()
        assert module.personality == "neutral"
        assert hasattr(module, "generate_forecast")
        assert isinstance(module.generate_forecast, dspy.ChainOfThought)

    def test_module_initialization_custom_personality(self):
        """Test module initialization with custom personality."""
        from macro_agents.defs.analysis.narratives.economic_narrative_generator import (
            IndicatorForecastModule,
        )

        module = IndicatorForecastModule(personality="skeptical")
        assert module.personality == "skeptical"

    def test_module_forward_with_default_personality(self):
        """Test module forward method using default personality."""
        from macro_agents.defs.analysis.narratives.economic_narrative_generator import (
            IndicatorForecastModule,
        )

        module = IndicatorForecastModule(personality="neutral")

        mock_result = Mock()
        mock_result.next_release_forecast = "3.0% with range 2.8-3.2%"
        mock_result.three_month_outlook = "Declining trend expected"
        mock_result.forecast_rationale = "Based on leading indicators"
        module.generate_forecast = Mock(return_value=mock_result)

        result = module.forward(
            indicator_name="CPI",
            historical_data="Historical CPI data CSV",
        )

        assert result.next_release_forecast == "3.0% with range 2.8-3.2%"
        module.generate_forecast.assert_called_once_with(
            indicator_name="CPI",
            historical_data="Historical CPI data CSV",
            leading_indicators="No leading indicators available",
            current_economic_conditions="No economic conditions summary available",
            personality="neutral",
        )

    def test_module_forward_with_all_fields(self):
        """Test module forward method with all optional fields."""
        from macro_agents.defs.analysis.narratives.economic_narrative_generator import (
            IndicatorForecastModule,
        )

        module = IndicatorForecastModule(personality="bullish")

        mock_result = Mock()
        module.generate_forecast = Mock(return_value=mock_result)

        module.forward(
            indicator_name="NFP",
            historical_data="Historical jobs data",
            leading_indicators="Initial claims: 210K",
            current_economic_conditions="Economy in expansion",
            personality="skeptical",
        )

        call_args = module.generate_forecast.call_args
        assert call_args.kwargs["leading_indicators"] == "Initial claims: 210K"
        assert call_args.kwargs["current_economic_conditions"] == "Economy in expansion"
        assert call_args.kwargs["personality"] == "skeptical"


class TestIndicatorConfigs:
    """Test cases for indicator configuration."""

    def test_get_indicator_config_known_indicator(self):
        """Test getting config for a known indicator."""
        from macro_agents.defs.analysis.narratives.economic_narrative_generator import (
            get_indicator_config,
        )

        config = get_indicator_config("CPIAUCSL")
        assert config["name"] == "Consumer Price Index (CPI)"
        assert config["category"] == "inflation"
        assert config["frequency"] == "monthly"
        assert "CPILFESL" in config["related_series"]

    def test_get_indicator_config_unknown_indicator(self):
        """Test getting config for an unknown indicator."""
        from macro_agents.defs.analysis.narratives.economic_narrative_generator import (
            get_indicator_config,
        )

        config = get_indicator_config("UNKNOWN_SERIES")
        assert config["name"] == "UNKNOWN_SERIES"
        assert config["category"] == "other"
        assert config["frequency"] == "unknown"
        assert config["related_series"] == []

    def test_indicator_configs_categories(self):
        """Test that indicator configs cover expected categories."""
        from macro_agents.defs.analysis.narratives.economic_narrative_generator import (
            INDICATOR_CONFIGS,
        )

        categories = {config["category"] for config in INDICATOR_CONFIGS.values()}
        assert "inflation" in categories
        assert "employment" in categories
        assert "growth" in categories
        assert "consumer" in categories
        assert "housing" in categories
        assert "monetary_policy" in categories


class TestExtractRecommendationsFloatParsing:
    """Test cases for extract_recommendations handling trailing punctuation in floats."""

    def test_extract_recommendations_with_trailing_period(self):
        """Test extract_recommendations handles confidence with trailing period."""
        from macro_agents.defs.backtesting.backtest_utils import extract_recommendations

        content = "OVERWEIGHT XLK with confidence 0.6. Expected return 5.2%."
        recommendations = extract_recommendations(content)

        assert len(recommendations) > 0
        xlk_rec = next((r for r in recommendations if r["symbol"] == "XLK"), None)
        assert xlk_rec is not None
        assert xlk_rec["direction"] == "OVERWEIGHT"
        assert xlk_rec["confidence"] == 0.6
        assert xlk_rec["expected_return"] == 5.2

    def test_extract_recommendations_with_trailing_comma(self):
        """Test extract_recommendations handles confidence with trailing comma."""
        from macro_agents.defs.backtesting.backtest_utils import extract_recommendations

        content = "OVERWEIGHT SPY with confidence 0.75, expected return 8.3%"
        recommendations = extract_recommendations(content)

        assert len(recommendations) > 0
        spy_rec = next((r for r in recommendations if r["symbol"] == "SPY"), None)
        assert spy_rec is not None
        assert spy_rec["confidence"] == 0.75
        assert spy_rec["expected_return"] == 8.3

    def test_extract_recommendations_with_trailing_semicolon(self):
        """Test extract_recommendations handles confidence with trailing semicolon."""
        from macro_agents.defs.backtesting.backtest_utils import extract_recommendations

        content = "UNDERWEIGHT XLE with confidence 0.4; expected return -2.1%"
        recommendations = extract_recommendations(content)

        assert len(recommendations) > 0
        xle_rec = next((r for r in recommendations if r["symbol"] == "XLE"), None)
        assert xle_rec is not None
        assert xle_rec["direction"] == "UNDERWEIGHT"
        assert xle_rec["confidence"] == 0.4
        assert xle_rec["expected_return"] == -2.1

    def test_extract_recommendations_with_percent_sign(self):
        """Test extract_recommendations handles return values with percent sign."""
        from macro_agents.defs.backtesting.backtest_utils import extract_recommendations

        content = "OVERWEIGHT QQQ confidence 0.8 expected return 12.5%"
        recommendations = extract_recommendations(content)

        assert len(recommendations) > 0
        qqq_rec = next((r for r in recommendations if r["symbol"] == "QQQ"), None)
        assert qqq_rec is not None
        assert qqq_rec["expected_return"] == 12.5

    def test_extract_recommendations_with_invalid_float_gracefully(self):
        """Test extract_recommendations handles invalid float strings gracefully."""
        from macro_agents.defs.backtesting.backtest_utils import extract_recommendations

        content = "OVERWEIGHT XLK with confidence invalid. Expected return also.invalid"
        recommendations = extract_recommendations(content)

        assert len(recommendations) > 0
        xlk_rec = next((r for r in recommendations if r["symbol"] == "XLK"), None)
        assert xlk_rec is not None
        assert xlk_rec["confidence"] is None
        assert xlk_rec["expected_return"] is None

    def test_extract_recommendations_with_multiple_trailing_punctuation(self):
        """Test extract_recommendations handles multiple trailing punctuation."""
        from macro_agents.defs.backtesting.backtest_utils import extract_recommendations

        content = "OVERWEIGHT DIA confidence 0.9., expected return 6.7%."
        recommendations = extract_recommendations(content)

        assert len(recommendations) > 0
        dia_rec = next((r for r in recommendations if r["symbol"] == "DIA"), None)
        assert dia_rec is not None
        assert dia_rec["confidence"] == 0.9
        assert dia_rec["expected_return"] == 6.7
