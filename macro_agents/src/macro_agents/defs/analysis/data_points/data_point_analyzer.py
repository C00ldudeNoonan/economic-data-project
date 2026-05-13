"""DSPy module for analyzing and prioritizing interesting data points.

This module uses AI to:
1. Contextualize individual findings with explanations
2. Prioritize findings by economic significance
"""

import dspy


class AnalyzeDataPointSignature(dspy.Signature):
    """Contextualize a single interesting data point with AI analysis."""

    finding_type: str = dspy.InputField(
        desc="Type: big_short_term_move, trend_change_inflection, correlation_anomaly, statistical_outlier"
    )
    data_point_name: str = dspy.InputField(
        desc="Name of metric (e.g., 'UNRATE', 'SPY', 'CPI')"
    )
    metric_category: str = dspy.InputField(
        desc="Category: economic, market, commodity, sentiment, correlation"
    )
    current_value: str = dspy.InputField(
        desc="Current value and recent changes (3m, 6m, 1y)"
    )
    historical_context: str = dspy.InputField(
        desc="Z-score, percentile rank, historical comparison data"
    )
    economic_context: str = dspy.InputField(
        desc="Current economic cycle phase and conditions"
    )

    explanation: str = dspy.OutputField(
        desc="""3-4 sentence explanation covering:
        1. WHAT changed (specific metric and magnitude)
        2. WHY this is significant (historical context, rarity)
        3. IMPLICATIONS (what this might signal for economy/markets)

        Keep technical but accessible. Use specific numbers."""
    )
    significance_narrative: str = dspy.OutputField(
        desc="1-sentence attention-grabbing summary (e.g., 'Unemployment rate posts largest 6-month decline since 2020 recovery')"
    )


class PrioritizeFindingsSignature(dspy.Signature):
    """Rank findings by economic importance using AI judgment."""

    findings_csv: str = dspy.InputField(
        desc="CSV of all findings with columns: data_point, finding_type, metric_category, significance_score, current_value, change_period, change_pct, z_score"
    )
    economic_context: str = dspy.InputField(
        desc="Current economic cycle phase and market regime"
    )

    prioritized_findings: str = dspy.OutputField(
        desc="""Comma-separated list of top 10-15 data_point names, ranked by importance.

        Prioritization criteria (in order):
        1. Leading indicators > lagging indicators
        2. Unusual moves in critical metrics (unemployment, inflation, Fed funds, VIX) > minor moves
        3. Inflection points (trend changes) > trend continuation
        4. Breadth indicators (sector performance, sentiment shifts) > single-asset moves
        5. Forward-looking signals > backward-looking

        Focus on actionable economic signals, not noise."""
    )
    rationale: str = dspy.OutputField(
        desc="2-3 sentence explanation of why these findings matter most this week, considering current economic environment"
    )


class DataPointAnalyzerModule(dspy.Module):
    """DSPy module for analyzing and prioritizing interesting data points."""

    def __init__(self):
        super().__init__()
        self.analyze_point = dspy.ChainOfThought(AnalyzeDataPointSignature)
        self.prioritize = dspy.ChainOfThought(PrioritizeFindingsSignature)

    def analyze_finding(
        self,
        finding_type: str,
        data_point_name: str,
        metric_category: str,
        current_value: str,
        historical_context: str,
        economic_context: str,
    ) -> dspy.Prediction:
        """
        Analyze a single interesting data point and generate explanation.

        Args:
            finding_type: Type of finding (big_short_term_move, etc.)
            data_point_name: Name of the metric
            metric_category: Category (economic, market, etc.)
            current_value: Current value and changes
            historical_context: Z-score and historical data
            economic_context: Current economic environment

        Returns:
            DSPy Prediction with explanation and significance_narrative
        """
        return self.analyze_point(
            finding_type=finding_type,
            data_point_name=data_point_name,
            metric_category=metric_category,
            current_value=current_value,
            historical_context=historical_context,
            economic_context=economic_context,
        )

    def prioritize_findings(
        self, findings_csv: str, economic_context: str
    ) -> dspy.Prediction:
        """
        Prioritize findings by economic significance using AI.

        Args:
            findings_csv: CSV of all findings
            economic_context: Current economic environment

        Returns:
            DSPy Prediction with prioritized_findings list and rationale
        """
        return self.prioritize(
            findings_csv=findings_csv, economic_context=economic_context
        )
