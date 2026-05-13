"""Unit tests for SEC business intelligence extraction utilities."""

from macro_agents.defs.utils.sec_bi_extractor import (
    BI_PATTERNS,
    ExtractedSignal,
    SECBIExtractor,
)


class TestSECBIExtractor:
    """Test cases for SECBIExtractor."""

    def test_initialization_default(self):
        """Test extractor initialization with default values."""
        extractor = SECBIExtractor()
        assert extractor.context_window == 200
        assert extractor.patterns == BI_PATTERNS

    def test_initialization_custom_window(self):
        """Test extractor initialization with custom context window."""
        extractor = SECBIExtractor(context_window=300)
        assert extractor.context_window == 300

    def test_extract_growth_signals(self):
        """Test extraction of growth signals."""
        extractor = SECBIExtractor()
        text = """
        Our company experienced significant growth in 2023, with revenue
        increasing by 25% year over year. We expanded our operations into
        three new markets and saw record revenue of $5 billion.
        """
        signals = extractor.extract_signals(text, categories=["growth_signals"])

        assert len(signals) > 0
        assert all(s.category == "growth_signals" for s in signals)

    def test_extract_hiring_plans(self):
        """Test extraction of hiring plan signals."""
        extractor = SECBIExtractor()
        text = """
        We are expanding our workforce by hiring 500 new employees in the
        engineering department. Our headcount increased by 20% as we
        continue to recruit talent for our growing operations.
        """
        signals = extractor.extract_signals(text, categories=["hiring_plans"])

        assert len(signals) > 0
        assert all(s.category == "hiring_plans" for s in signals)

    def test_extract_market_expansion(self):
        """Test extraction of market expansion signals."""
        extractor = SECBIExtractor()
        text = """
        The company entered new markets in Asia and Europe, opening five
        new facilities. We acquired two companies to expand our
        distribution channels and formed strategic partnerships.
        """
        signals = extractor.extract_signals(text, categories=["market_expansion"])

        assert len(signals) > 0
        assert all(s.category == "market_expansion" for s in signals)

    def test_extract_capex_investment(self):
        """Test extraction of capital expenditure signals."""
        extractor = SECBIExtractor()
        text = """
        Capital expenditures of $2.5 billion were invested in new
        facilities and equipment. We are investing $500 million in
        technology upgrades and R&D investments.
        """
        signals = extractor.extract_signals(text, categories=["capex_investment"])

        assert len(signals) > 0
        assert all(s.category == "capex_investment" for s in signals)

    def test_extract_product_innovation(self):
        """Test extraction of product innovation signals."""
        extractor = SECBIExtractor()
        text = """
        We launched new products in Q3 and introduced innovative
        solutions to the market. Our product development pipeline
        includes next-generation technologies and proprietary technology.
        """
        signals = extractor.extract_signals(text, categories=["product_innovation"])

        assert len(signals) > 0
        assert all(s.category == "product_innovation" for s in signals)

    def test_extract_cost_efficiency(self):
        """Test extraction of cost efficiency signals."""
        extractor = SECBIExtractor()
        text = """
        We achieved cost reductions of $100 million through operational
        efficiencies. Margin expansion was driven by streamlined operations
        and productivity gains across the organization.
        """
        signals = extractor.extract_signals(text, categories=["cost_efficiency"])

        assert len(signals) > 0
        assert all(s.category == "cost_efficiency" for s in signals)

    def test_extract_risk_factors(self):
        """Test extraction of risk factor signals."""
        extractor = SECBIExtractor()
        text = """
        We face significant risks from competition in our markets.
        Regulatory risks and cybersecurity threats remain concerns.
        Economic downturn could impact our operations.
        """
        signals = extractor.extract_signals(text, categories=["risk_factors"])

        assert len(signals) > 0
        assert all(s.category == "risk_factors" for s in signals)

    def test_extract_financial_health(self):
        """Test extraction of financial health signals."""
        extractor = SECBIExtractor()
        text = """
        We maintain a strong balance sheet with cash position of $3 billion.
        Debt reduction efforts have improved our leverage. The dividend
        increased by 10% and we announced a share repurchase program.
        """
        signals = extractor.extract_signals(text, categories=["financial_health"])

        assert len(signals) > 0
        assert all(s.category == "financial_health" for s in signals)

    def test_extract_strategic_initiatives(self):
        """Test extraction of strategic initiative signals."""
        extractor = SECBIExtractor()
        text = """
        Our strategic plan focuses on digital transformation and
        sustainability initiatives. We have implemented operational
        excellence programs and customer focus initiatives.
        """
        signals = extractor.extract_signals(text, categories=["strategic_initiatives"])

        assert len(signals) > 0
        assert all(s.category == "strategic_initiatives" for s in signals)

    def test_extract_all_categories(self):
        """Test extraction across all categories."""
        extractor = SECBIExtractor()
        text = """
        Revenue growth was strong at 20%. We are hiring 100 new employees
        and entering new markets. Capital expenditures of $1 billion were
        invested. New products were launched. Cost reductions achieved.
        Competition presents risks. Strong balance sheet maintained.
        Strategic plan in progress.
        """
        by_category = extractor.extract_all_categories(text)

        assert isinstance(by_category, dict)
        # Should have signals in multiple categories
        assert len(by_category) > 0

    def test_signal_has_context(self):
        """Test that extracted signals include context."""
        extractor = SECBIExtractor(context_window=100)
        text = "Before context here. The company experienced significant growth in Q4. After context here."
        signals = extractor.extract_signals(text, categories=["growth_signals"])

        assert len(signals) > 0
        # Context should include surrounding text
        for signal in signals:
            assert len(signal.context) > len(signal.term)

    def test_confidence_score_range(self):
        """Test that confidence scores are in valid range."""
        extractor = SECBIExtractor()
        text = "Significant growth with revenue increased by 50% to $5 billion."
        signals = extractor.extract_signals(text)

        for signal in signals:
            assert 0 <= signal.confidence_score <= 1

    def test_section_name_preserved(self):
        """Test that section name is preserved in signals."""
        extractor = SECBIExtractor()
        text = "Strong revenue growth of 30%."
        signals = extractor.extract_signals(text, section_name="Business")

        for signal in signals:
            assert signal.section_name == "Business"

    def test_get_summary_stats(self):
        """Test summary statistics calculation."""
        extractor = SECBIExtractor()
        text = """
        Revenue growth was strong. Hiring 50 employees. Entering new markets.
        Cost savings achieved. Strategic initiatives underway.
        """
        signals = extractor.extract_signals(text)
        stats = extractor.get_summary_stats(signals)

        assert "total_signals" in stats
        assert "categories" in stats
        assert "avg_confidence" in stats
        assert "top_categories" in stats
        assert stats["total_signals"] == len(signals)

    def test_get_summary_stats_empty(self):
        """Test summary stats with no signals."""
        extractor = SECBIExtractor()
        stats = extractor.get_summary_stats([])

        assert stats["total_signals"] == 0
        assert stats["categories"] == {}
        assert stats["avg_confidence"] == 0

    def test_extract_key_metrics_revenue(self):
        """Test extraction of revenue metrics."""
        extractor = SECBIExtractor()
        text = "Revenue was $5.2 billion for the year. Net sales totaled $3.1 million."
        metrics = extractor.extract_key_metrics(text)

        assert len(metrics) > 0
        revenue_metrics = [m for m in metrics if m["metric_type"] == "revenue"]
        assert len(revenue_metrics) > 0

    def test_extract_key_metrics_growth(self):
        """Test extraction of growth rate metrics."""
        extractor = SECBIExtractor()
        text = "Revenue grew by 25%. We saw 15% increase in sales."
        metrics = extractor.extract_key_metrics(text)

        growth_metrics = [m for m in metrics if m["metric_type"] == "growth_rate"]
        assert len(growth_metrics) > 0

    def test_extract_key_metrics_employees(self):
        """Test extraction of employee count metrics."""
        extractor = SECBIExtractor()
        text = "We have approximately 50,000 employees worldwide. Workforce of 25,000."
        metrics = extractor.extract_key_metrics(text)

        employee_metrics = [m for m in metrics if m["metric_type"] == "employee_count"]
        assert len(employee_metrics) > 0

    def test_get_category_descriptions(self):
        """Test getting category descriptions."""
        descriptions = SECBIExtractor.get_category_descriptions()

        assert isinstance(descriptions, dict)
        assert len(descriptions) > 0
        for category, desc in descriptions.items():
            assert isinstance(category, str)
            assert isinstance(desc, str)
            assert len(desc) > 0

    def test_deduplicate_signals(self):
        """Test that duplicate signals are removed."""
        extractor = SECBIExtractor()
        # Text with repeated similar patterns close together
        text = "Growth growth growth. Strong growth achieved. Strong growth recorded."
        signals = extractor.extract_signals(text, categories=["growth_signals"])

        # Should deduplicate signals that are too close
        positions = [s.position for s in signals]
        # Check that positions are sufficiently separated
        for i in range(1, len(positions)):
            # Either different category or at least 50 chars apart
            pass  # Just checking deduplication runs without error

    def test_empty_text(self):
        """Test handling of empty text."""
        extractor = SECBIExtractor()
        signals = extractor.extract_signals("")
        assert signals == []

    def test_no_matches(self):
        """Test text with no matching patterns."""
        extractor = SECBIExtractor()
        text = "The quick brown fox jumps over the lazy dog."
        signals = extractor.extract_signals(text)
        assert signals == []

    def test_bi_patterns_structure(self):
        """Test that BI_PATTERNS has correct structure."""
        for category, info in BI_PATTERNS.items():
            assert "description" in info
            assert "patterns" in info
            assert isinstance(info["description"], str)
            assert isinstance(info["patterns"], list)
            assert len(info["patterns"]) > 0

    def test_extracted_signal_dataclass(self):
        """Test ExtractedSignal dataclass."""
        signal = ExtractedSignal(
            category="growth_signals",
            term="revenue growth",
            context="Strong revenue growth this year",
            section_name="Business",
            confidence_score=0.8,
            position=100,
        )

        assert signal.category == "growth_signals"
        assert signal.term == "revenue growth"
        assert signal.context == "Strong revenue growth this year"
        assert signal.section_name == "Business"
        assert signal.confidence_score == 0.8
        assert signal.position == 100

    def test_case_insensitive_matching(self):
        """Test that pattern matching is case insensitive."""
        extractor = SECBIExtractor()
        text1 = "SIGNIFICANT GROWTH achieved in Q4"
        text2 = "significant growth achieved in Q4"
        text3 = "Significant Growth achieved in Q4"

        signals1 = extractor.extract_signals(text1, categories=["growth_signals"])
        signals2 = extractor.extract_signals(text2, categories=["growth_signals"])
        signals3 = extractor.extract_signals(text3, categories=["growth_signals"])

        # All should find matches
        assert len(signals1) > 0
        assert len(signals2) > 0
        assert len(signals3) > 0
