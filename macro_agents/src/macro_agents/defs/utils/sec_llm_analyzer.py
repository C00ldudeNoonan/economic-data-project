"""DSPy module for LLM-powered SEC filing analysis.

Provides structured extraction of summaries, topics, sentiment,
named entities, financial metrics, and risk factors from SEC filing sections.
"""

import dspy


class SECFilingAnalysisSignature(dspy.Signature):
    """Analyze an SEC filing section and extract structured metadata."""

    filing_text: str = dspy.InputField(
        desc="Raw text content from an SEC filing section"
    )
    form_type: str = dspy.InputField(desc="SEC form type (e.g., 10-K, 10-Q, 10-K/A)")
    section_name: str = dspy.InputField(
        desc="Filing section name (e.g., Business, Risk Factors, MD&A)"
    )

    executive_summary: str = dspy.OutputField(
        desc="Concise 2-3 sentence summary of the section's key points"
    )
    key_topics: str = dspy.OutputField(
        desc="Comma-separated list of main topics discussed (e.g., revenue growth, market expansion, regulatory compliance)"
    )
    sentiment: str = dspy.OutputField(
        desc="Overall sentiment: bullish, neutral, or bearish"
    )
    named_entities: str = dspy.OutputField(
        desc="JSON object with keys: people (list of names), organizations (list of company/org names), products (list of product/service names)"
    )
    financial_metrics: str = dspy.OutputField(
        desc="JSON object with keys: metrics (list of objects with name, value, period fields for any financial figures mentioned)"
    )
    forward_looking_statements: str = dspy.OutputField(
        desc="Key forward-looking statements or guidance, semicolon-separated"
    )
    risk_factors: str = dspy.OutputField(
        desc="Key risk factors identified, semicolon-separated"
    )


class SECFilingAnalyzer(dspy.Module):
    """DSPy module for analyzing SEC filing sections using ChainOfThought."""

    def __init__(self):
        super().__init__()
        self.analyze = dspy.ChainOfThought(SECFilingAnalysisSignature)

    def forward(
        self,
        filing_text: str,
        form_type: str,
        section_name: str,
    ):
        # Truncate very long texts to fit context window
        max_chars = 12000
        if len(filing_text) > max_chars:
            filing_text = filing_text[:max_chars] + "\n\n[TRUNCATED]"

        return self.analyze(
            filing_text=filing_text,
            form_type=form_type,
            section_name=section_name,
        )
