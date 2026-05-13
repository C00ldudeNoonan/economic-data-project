"""
Tests for domain sub-agents and aggregator modules.
"""

from unittest.mock import Mock
import dspy

from macro_agents.defs.analysis.economy_state.domain_sub_agents import (
    LaborMarketAnalysisSignature,
    LaborMarketModule,
    FinancialConditionsAnalysisSignature,
    FinancialConditionsModule,
    CommoditiesAnalysisSignature,
    CommoditiesModule,
    SectorAnalysisSignature,
    SectorModule,
    MarketStructureAnalysisSignature,
    MarketStructureModule,
)
from macro_agents.defs.analysis.economy_state.economic_state_aggregator import (
    EconomicStateAggregatorSignature,
    EconomicStateAggregatorModule,
    DomainAnalysisResult,
)


# =============================================================================
# LABOR MARKET MODULE TESTS
# =============================================================================


class TestLaborMarketAnalysisSignature:
    """Test cases for LaborMarketAnalysisSignature."""

    def test_signature_fields(self):
        """Test that signature has correct input and output fields."""
        assert issubclass(LaborMarketAnalysisSignature, dspy.Signature)

        annotations = LaborMarketAnalysisSignature.__annotations__
        assert "labor_data" in annotations
        assert "employment_trends" in annotations
        assert "personality" in annotations
        assert "labor_analysis" in annotations

    def test_signature_description(self):
        """Test that signature has proper description."""
        assert LaborMarketAnalysisSignature.__doc__ is not None
        assert "labor market" in LaborMarketAnalysisSignature.__doc__.lower()


class TestLaborMarketModule:
    """Test cases for LaborMarketModule."""

    def test_module_initialization_default_personality(self):
        """Test module initialization with default personality."""
        module = LaborMarketModule()
        assert module.personality == "neutral"
        assert hasattr(module, "analyze")
        assert isinstance(module.analyze, dspy.ChainOfThought)

    def test_module_initialization_custom_personality(self):
        """Test module initialization with custom personality."""
        module = LaborMarketModule(personality="bullish")
        assert module.personality == "bullish"

    def test_module_forward_with_default_personality(self):
        """Test module forward method using default personality."""
        module = LaborMarketModule(personality="neutral")

        mock_result = Mock()
        mock_result.labor_analysis = "Test labor analysis output"
        module.analyze = Mock(return_value=mock_result)

        result = module.forward(
            labor_data="test labor data",
            employment_trends="test trends",
        )

        assert result.labor_analysis == "Test labor analysis output"
        module.analyze.assert_called_once_with(
            labor_data="test labor data",
            employment_trends="test trends",
            personality="neutral",
        )

    def test_module_forward_with_override_personality(self):
        """Test module forward method with personality override."""
        module = LaborMarketModule(personality="skeptical")

        mock_result = Mock()
        mock_result.labor_analysis = "Test labor analysis output"
        module.analyze = Mock(return_value=mock_result)

        result = module.forward(
            labor_data="test labor data",
            personality="bullish",
        )

        assert result.labor_analysis == "Test labor analysis output"
        module.analyze.assert_called_once_with(
            labor_data="test labor data",
            employment_trends="No trend data available",
            personality="bullish",
        )

    def test_module_forward_with_empty_trends(self):
        """Test module forward method with empty trends data."""
        module = LaborMarketModule()

        mock_result = Mock()
        mock_result.labor_analysis = "Analysis with empty trends"
        module.analyze = Mock(return_value=mock_result)

        result = module.forward(
            labor_data="test labor data",
            employment_trends="",
        )

        assert result.labor_analysis == "Analysis with empty trends"
        call_args = module.analyze.call_args
        assert call_args.kwargs["employment_trends"] == "No trend data available"


# =============================================================================
# FINANCIAL CONDITIONS MODULE TESTS
# =============================================================================


class TestFinancialConditionsAnalysisSignature:
    """Test cases for FinancialConditionsAnalysisSignature."""

    def test_signature_fields(self):
        """Test that signature has correct input and output fields."""
        assert issubclass(FinancialConditionsAnalysisSignature, dspy.Signature)

        annotations = FinancialConditionsAnalysisSignature.__annotations__
        assert "fci_data" in annotations
        assert "yield_curve_data" in annotations
        assert "credit_data" in annotations
        assert "personality" in annotations
        assert "financial_analysis" in annotations

    def test_signature_description(self):
        """Test that signature has proper description."""
        assert FinancialConditionsAnalysisSignature.__doc__ is not None
        assert (
            "financial conditions"
            in FinancialConditionsAnalysisSignature.__doc__.lower()
        )


class TestFinancialConditionsModule:
    """Test cases for FinancialConditionsModule."""

    def test_module_initialization_default_personality(self):
        """Test module initialization with default personality."""
        module = FinancialConditionsModule()
        assert module.personality == "neutral"
        assert hasattr(module, "analyze")
        assert isinstance(module.analyze, dspy.ChainOfThought)

    def test_module_initialization_custom_personality(self):
        """Test module initialization with custom personality."""
        module = FinancialConditionsModule(personality="skeptical")
        assert module.personality == "skeptical"

    def test_module_forward_with_all_data(self):
        """Test module forward method with all data provided."""
        module = FinancialConditionsModule(personality="neutral")

        mock_result = Mock()
        mock_result.financial_analysis = "Test financial analysis"
        module.analyze = Mock(return_value=mock_result)

        result = module.forward(
            fci_data="test FCI data",
            yield_curve_data="test yield curve",
            credit_data="test credit data",
        )

        assert result.financial_analysis == "Test financial analysis"
        module.analyze.assert_called_once_with(
            fci_data="test FCI data",
            yield_curve_data="test yield curve",
            credit_data="test credit data",
            personality="neutral",
        )

    def test_module_forward_with_empty_optional_data(self):
        """Test module forward method with empty optional data."""
        module = FinancialConditionsModule()

        mock_result = Mock()
        mock_result.financial_analysis = "Analysis with defaults"
        module.analyze = Mock(return_value=mock_result)

        result = module.forward(
            fci_data="test FCI data",
        )

        assert result.financial_analysis == "Analysis with defaults"
        call_args = module.analyze.call_args
        assert call_args.kwargs["yield_curve_data"] == "No yield curve data available"
        assert call_args.kwargs["credit_data"] == "No credit data available"


# =============================================================================
# COMMODITIES MODULE TESTS
# =============================================================================


class TestCommoditiesAnalysisSignature:
    """Test cases for CommoditiesAnalysisSignature."""

    def test_signature_fields(self):
        """Test that signature has correct input and output fields."""
        assert issubclass(CommoditiesAnalysisSignature, dspy.Signature)

        annotations = CommoditiesAnalysisSignature.__annotations__
        assert "energy_data" in annotations
        assert "input_commodities_data" in annotations
        assert "agriculture_data" in annotations
        assert "personality" in annotations
        assert "commodity_analysis" in annotations

    def test_signature_description(self):
        """Test that signature has proper description."""
        assert CommoditiesAnalysisSignature.__doc__ is not None
        assert "commodity" in CommoditiesAnalysisSignature.__doc__.lower()


class TestCommoditiesModule:
    """Test cases for CommoditiesModule."""

    def test_module_initialization_default_personality(self):
        """Test module initialization with default personality."""
        module = CommoditiesModule()
        assert module.personality == "neutral"
        assert hasattr(module, "analyze")
        assert isinstance(module.analyze, dspy.ChainOfThought)

    def test_module_forward_with_all_data(self):
        """Test module forward method with all data provided."""
        module = CommoditiesModule(personality="bullish")

        mock_result = Mock()
        mock_result.commodity_analysis = "Test commodity analysis"
        module.analyze = Mock(return_value=mock_result)

        result = module.forward(
            energy_data="test energy data",
            input_commodities_data="test input data",
            agriculture_data="test agriculture data",
        )

        assert result.commodity_analysis == "Test commodity analysis"
        module.analyze.assert_called_once_with(
            energy_data="test energy data",
            input_commodities_data="test input data",
            agriculture_data="test agriculture data",
            personality="bullish",
        )

    def test_module_forward_with_empty_optional_data(self):
        """Test module forward method with empty optional data."""
        module = CommoditiesModule()

        mock_result = Mock()
        mock_result.commodity_analysis = "Analysis with defaults"
        module.analyze = Mock(return_value=mock_result)

        result = module.forward(
            energy_data="test energy data",
        )

        assert result.commodity_analysis == "Analysis with defaults"
        call_args = module.analyze.call_args
        assert (
            call_args.kwargs["input_commodities_data"]
            == "No industrial commodities data available"
        )
        assert (
            call_args.kwargs["agriculture_data"]
            == "No agricultural commodities data available"
        )


# =============================================================================
# SECTOR MODULE TESTS
# =============================================================================


class TestSectorAnalysisSignature:
    """Test cases for SectorAnalysisSignature."""

    def test_signature_fields(self):
        """Test that signature has correct input and output fields."""
        assert issubclass(SectorAnalysisSignature, dspy.Signature)

        annotations = SectorAnalysisSignature.__annotations__
        assert "sector_data" in annotations
        assert "correlation_data" in annotations
        assert "personality" in annotations
        assert "sector_analysis" in annotations

    def test_signature_description(self):
        """Test that signature has proper description."""
        assert SectorAnalysisSignature.__doc__ is not None
        assert "sector" in SectorAnalysisSignature.__doc__.lower()


class TestSectorModule:
    """Test cases for SectorModule."""

    def test_module_initialization_default_personality(self):
        """Test module initialization with default personality."""
        module = SectorModule()
        assert module.personality == "neutral"
        assert hasattr(module, "analyze")
        assert isinstance(module.analyze, dspy.ChainOfThought)

    def test_module_forward_with_all_data(self):
        """Test module forward method with all data provided."""
        module = SectorModule(personality="skeptical")

        mock_result = Mock()
        mock_result.sector_analysis = "Test sector analysis"
        module.analyze = Mock(return_value=mock_result)

        result = module.forward(
            sector_data="test sector data",
            correlation_data="test correlation data",
        )

        assert result.sector_analysis == "Test sector analysis"
        module.analyze.assert_called_once_with(
            sector_data="test sector data",
            correlation_data="test correlation data",
            personality="skeptical",
        )

    def test_module_forward_with_empty_correlation_data(self):
        """Test module forward method with empty correlation data."""
        module = SectorModule()

        mock_result = Mock()
        mock_result.sector_analysis = "Analysis without correlation"
        module.analyze = Mock(return_value=mock_result)

        result = module.forward(
            sector_data="test sector data",
            correlation_data="",
        )

        assert result.sector_analysis == "Analysis without correlation"
        call_args = module.analyze.call_args
        assert call_args.kwargs["correlation_data"] == "No correlation data available"


# =============================================================================
# MARKET STRUCTURE MODULE TESTS
# =============================================================================


class TestMarketStructureAnalysisSignature:
    """Test cases for MarketStructureAnalysisSignature."""

    def test_signature_fields(self):
        """Test that signature has correct input and output fields."""
        assert issubclass(MarketStructureAnalysisSignature, dspy.Signature)

        annotations = MarketStructureAnalysisSignature.__annotations__
        assert "indices_data" in annotations
        assert "fixed_income_data" in annotations
        assert "global_markets_data" in annotations
        assert "personality" in annotations
        assert "market_analysis" in annotations

    def test_signature_description(self):
        """Test that signature has proper description."""
        assert MarketStructureAnalysisSignature.__doc__ is not None
        assert "market" in MarketStructureAnalysisSignature.__doc__.lower()


class TestMarketStructureModule:
    """Test cases for MarketStructureModule."""

    def test_module_initialization_default_personality(self):
        """Test module initialization with default personality."""
        module = MarketStructureModule()
        assert module.personality == "neutral"
        assert hasattr(module, "analyze")
        assert isinstance(module.analyze, dspy.ChainOfThought)

    def test_module_forward_with_all_data(self):
        """Test module forward method with all data provided."""
        module = MarketStructureModule(personality="neutral")

        mock_result = Mock()
        mock_result.market_analysis = "Test market analysis"
        module.analyze = Mock(return_value=mock_result)

        result = module.forward(
            indices_data="test indices data",
            fixed_income_data="test fixed income",
            global_markets_data="test global markets",
        )

        assert result.market_analysis == "Test market analysis"
        module.analyze.assert_called_once_with(
            indices_data="test indices data",
            fixed_income_data="test fixed income",
            global_markets_data="test global markets",
            personality="neutral",
        )

    def test_module_forward_with_empty_optional_data(self):
        """Test module forward method with empty optional data."""
        module = MarketStructureModule()

        mock_result = Mock()
        mock_result.market_analysis = "Analysis with defaults"
        module.analyze = Mock(return_value=mock_result)

        result = module.forward(
            indices_data="test indices data",
        )

        assert result.market_analysis == "Analysis with defaults"
        call_args = module.analyze.call_args
        assert call_args.kwargs["fixed_income_data"] == "No fixed income data available"
        assert (
            call_args.kwargs["global_markets_data"]
            == "No global markets data available"
        )


# =============================================================================
# AGGREGATOR MODULE TESTS
# =============================================================================


class TestEconomicStateAggregatorSignature:
    """Test cases for EconomicStateAggregatorSignature."""

    def test_signature_fields(self):
        """Test that signature has correct input and output fields."""
        assert issubclass(EconomicStateAggregatorSignature, dspy.Signature)

        annotations = EconomicStateAggregatorSignature.__annotations__
        assert "labor_analysis" in annotations
        assert "financial_analysis" in annotations
        assert "commodity_analysis" in annotations
        assert "sector_analysis" in annotations
        assert "market_analysis" in annotations
        assert "personality" in annotations
        assert "economic_state" in annotations

    def test_signature_description(self):
        """Test that signature has proper description."""
        assert EconomicStateAggregatorSignature.__doc__ is not None
        assert "synthesize" in EconomicStateAggregatorSignature.__doc__.lower()


class TestEconomicStateAggregatorModule:
    """Test cases for EconomicStateAggregatorModule."""

    def test_module_initialization_default_personality(self):
        """Test module initialization with default personality."""
        module = EconomicStateAggregatorModule()
        assert module.personality == "neutral"
        assert hasattr(module, "aggregate")
        assert isinstance(module.aggregate, dspy.ChainOfThought)

    def test_module_initialization_custom_personality(self):
        """Test module initialization with custom personality."""
        module = EconomicStateAggregatorModule(personality="bullish")
        assert module.personality == "bullish"

    def test_module_forward_with_all_analyses(self):
        """Test module forward method with all domain analyses."""
        module = EconomicStateAggregatorModule(personality="neutral")

        mock_result = Mock()
        mock_result.economic_state = "Comprehensive economic state analysis"
        module.aggregate = Mock(return_value=mock_result)

        result = module.forward(
            labor_analysis="labor analysis",
            financial_analysis="financial analysis",
            commodity_analysis="commodity analysis",
            sector_analysis="sector analysis",
            market_analysis="market analysis",
        )

        assert result.economic_state == "Comprehensive economic state analysis"
        module.aggregate.assert_called_once_with(
            labor_analysis="labor analysis",
            financial_analysis="financial analysis",
            commodity_analysis="commodity analysis",
            sector_analysis="sector analysis",
            market_analysis="market analysis",
            personality="neutral",
        )

    def test_module_forward_with_personality_override(self):
        """Test module forward method with personality override."""
        module = EconomicStateAggregatorModule(personality="neutral")

        mock_result = Mock()
        mock_result.economic_state = "Bearish economic analysis"
        module.aggregate = Mock(return_value=mock_result)

        result = module.forward(
            labor_analysis="labor analysis",
            financial_analysis="financial analysis",
            commodity_analysis="commodity analysis",
            sector_analysis="sector analysis",
            market_analysis="market analysis",
            personality="skeptical",
        )

        assert result.economic_state == "Bearish economic analysis"
        call_args = module.aggregate.call_args
        assert call_args.kwargs["personality"] == "skeptical"


# =============================================================================
# DOMAIN ANALYSIS RESULT TESTS
# =============================================================================


class TestDomainAnalysisResult:
    """Test cases for DomainAnalysisResult container."""

    def test_initialization_empty(self):
        """Test initialization with empty values."""
        result = DomainAnalysisResult()
        assert result.labor_analysis == ""
        assert result.financial_analysis == ""
        assert result.commodity_analysis == ""
        assert result.sector_analysis == ""
        assert result.market_analysis == ""

    def test_initialization_with_values(self):
        """Test initialization with values."""
        result = DomainAnalysisResult(
            labor_analysis="labor",
            financial_analysis="financial",
            commodity_analysis="commodity",
            sector_analysis="sector",
            market_analysis="market",
        )
        assert result.labor_analysis == "labor"
        assert result.financial_analysis == "financial"
        assert result.commodity_analysis == "commodity"
        assert result.sector_analysis == "sector"
        assert result.market_analysis == "market"

    def test_to_dict(self):
        """Test to_dict method."""
        result = DomainAnalysisResult(
            labor_analysis="labor",
            financial_analysis="financial",
        )
        d = result.to_dict()

        assert isinstance(d, dict)
        assert d["labor_analysis"] == "labor"
        assert d["financial_analysis"] == "financial"
        assert d["commodity_analysis"] == ""
        assert d["sector_analysis"] == ""
        assert d["market_analysis"] == ""

    def test_all_complete_true(self):
        """Test all_complete returns True when all analyses present."""
        result = DomainAnalysisResult(
            labor_analysis="labor",
            financial_analysis="financial",
            commodity_analysis="commodity",
            sector_analysis="sector",
            market_analysis="market",
        )
        assert result.all_complete() is True

    def test_all_complete_false(self):
        """Test all_complete returns False when analyses missing."""
        result = DomainAnalysisResult(
            labor_analysis="labor",
            financial_analysis="financial",
        )
        assert result.all_complete() is False

    def test_summary(self):
        """Test summary method."""
        result = DomainAnalysisResult(
            labor_analysis="labor",
            financial_analysis="financial",
            commodity_analysis="commodity",
        )
        summary = result.summary()

        assert "3/5" in summary
        assert "labor" in summary.lower()
        assert "True" in summary or "true" in summary.lower()


# =============================================================================
# PERSONALITY HANDLING TESTS
# =============================================================================


class TestSubAgentPersonalityHandling:
    """Test cases for personality handling across sub-agent modules."""

    def test_all_modules_accept_personality(self):
        """Test that all modules accept personality parameter."""
        valid_personalities = ["skeptical", "neutral", "bullish"]

        for personality in valid_personalities:
            labor = LaborMarketModule(personality=personality)
            assert labor.personality == personality

            financial = FinancialConditionsModule(personality=personality)
            assert financial.personality == personality

            commodities = CommoditiesModule(personality=personality)
            assert commodities.personality == personality

            sector = SectorModule(personality=personality)
            assert sector.personality == personality

            market = MarketStructureModule(personality=personality)
            assert market.personality == personality

            aggregator = EconomicStateAggregatorModule(personality=personality)
            assert aggregator.personality == personality

    def test_personality_override_works(self):
        """Test that personality override takes priority."""
        module = LaborMarketModule(personality="neutral")

        mock_result = Mock()
        mock_result.labor_analysis = "Test"
        module.analyze = Mock(return_value=mock_result)

        module.forward(
            labor_data="test",
            personality="bullish",
        )

        call_args = module.analyze.call_args
        assert call_args.kwargs["personality"] == "bullish"


# =============================================================================
# INTEGRATION TESTS
# =============================================================================


class TestSubAgentIntegration:
    """Integration tests for sub-agent modules working together."""

    def test_full_pipeline_with_mocks(self):
        """Test full pipeline from sub-agents to aggregator."""
        # Create all sub-agent modules
        labor = LaborMarketModule(personality="neutral")
        financial = FinancialConditionsModule(personality="neutral")
        commodities = CommoditiesModule(personality="neutral")
        sector = SectorModule(personality="neutral")
        market = MarketStructureModule(personality="neutral")
        aggregator = EconomicStateAggregatorModule(personality="neutral")

        # Mock sub-agent responses
        labor_mock = Mock()
        labor_mock.labor_analysis = "Strong labor market"
        labor.analyze = Mock(return_value=labor_mock)

        financial_mock = Mock()
        financial_mock.financial_analysis = "Tight financial conditions"
        financial.analyze = Mock(return_value=financial_mock)

        commodities_mock = Mock()
        commodities_mock.commodity_analysis = "Rising commodity prices"
        commodities.analyze = Mock(return_value=commodities_mock)

        sector_mock = Mock()
        sector_mock.sector_analysis = "Tech leading"
        sector.analyze = Mock(return_value=sector_mock)

        market_mock = Mock()
        market_mock.market_analysis = "Risk-on sentiment"
        market.analyze = Mock(return_value=market_mock)

        aggregator_mock = Mock()
        aggregator_mock.economic_state = "Economy in expansion phase"
        aggregator.aggregate = Mock(return_value=aggregator_mock)

        # Run sub-agents
        labor_result = labor.forward(labor_data="test")
        financial_result = financial.forward(fci_data="test")
        commodities_result = commodities.forward(energy_data="test")
        sector_result = sector.forward(sector_data="test")
        market_result = market.forward(indices_data="test")

        # Run aggregator
        final_result = aggregator.forward(
            labor_analysis=labor_result.labor_analysis,
            financial_analysis=financial_result.financial_analysis,
            commodity_analysis=commodities_result.commodity_analysis,
            sector_analysis=sector_result.sector_analysis,
            market_analysis=market_result.market_analysis,
        )

        # Verify all modules were called
        assert labor.analyze.called
        assert financial.analyze.called
        assert commodities.analyze.called
        assert sector.analyze.called
        assert market.analyze.called
        assert aggregator.aggregate.called

        # Verify final result
        assert final_result.economic_state == "Economy in expansion phase"
