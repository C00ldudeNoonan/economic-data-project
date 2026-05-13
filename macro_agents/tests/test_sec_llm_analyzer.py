"""Tests for DSPy SEC filing analyzer module."""

from unittest.mock import MagicMock

import dspy

from macro_agents.defs.utils.sec_llm_analyzer import (
    SECFilingAnalysisSignature,
    SECFilingAnalyzer,
)


class TestSECFilingAnalysisSignature:
    """Test cases for the DSPy signature definition."""

    def test_input_fields_defined(self):
        """Signature should have the expected input fields."""
        fields = SECFilingAnalysisSignature.input_fields
        assert "filing_text" in fields
        assert "form_type" in fields
        assert "section_name" in fields

    def test_output_fields_defined(self):
        """Signature should have all expected output fields."""
        fields = SECFilingAnalysisSignature.output_fields
        expected = [
            "executive_summary",
            "key_topics",
            "sentiment",
            "named_entities",
            "financial_metrics",
            "forward_looking_statements",
            "risk_factors",
        ]
        for field_name in expected:
            assert field_name in fields, f"Missing output field: {field_name}"

    def test_output_field_count(self):
        """Signature should have exactly 7 output fields."""
        assert len(SECFilingAnalysisSignature.output_fields) == 7


class TestSECFilingAnalyzer:
    """Test cases for the SECFilingAnalyzer DSPy module."""

    def test_initialization(self):
        """Module should initialize with a ChainOfThought predictor."""
        analyzer = SECFilingAnalyzer()
        assert hasattr(analyzer, "analyze")
        assert isinstance(analyzer.analyze, dspy.ChainOfThought)

    def test_is_dspy_module(self):
        """Module should inherit from dspy.Module."""
        analyzer = SECFilingAnalyzer()
        assert isinstance(analyzer, dspy.Module)

    def test_forward_calls_analyze(self):
        """forward() should call the analyze predictor."""
        analyzer = SECFilingAnalyzer()
        mock_analyze = MagicMock()
        mock_result = MagicMock()
        mock_analyze.return_value = mock_result
        analyzer.analyze = mock_analyze

        result = analyzer.forward(
            filing_text="Company revenue grew 15%.",
            form_type="10-K",
            section_name="Business",
        )

        mock_analyze.assert_called_once()
        assert result == mock_result

    def test_forward_truncates_long_text(self):
        """forward() should truncate text longer than 12000 chars."""
        analyzer = SECFilingAnalyzer()
        mock_analyze = MagicMock()
        analyzer.analyze = mock_analyze

        long_text = "A" * 20000
        analyzer.forward(
            filing_text=long_text,
            form_type="10-Q",
            section_name="Risk Factors",
        )

        call_kwargs = mock_analyze.call_args[1]
        passed_text = call_kwargs["filing_text"]
        assert len(passed_text) < 20000
        assert passed_text.endswith("[TRUNCATED]")

    def test_forward_preserves_short_text(self):
        """forward() should not truncate text under 12000 chars."""
        analyzer = SECFilingAnalyzer()
        mock_analyze = MagicMock()
        analyzer.analyze = mock_analyze

        short_text = "Short filing text."
        analyzer.forward(
            filing_text=short_text,
            form_type="10-K",
            section_name="Business",
        )

        call_kwargs = mock_analyze.call_args[1]
        assert call_kwargs["filing_text"] == short_text
