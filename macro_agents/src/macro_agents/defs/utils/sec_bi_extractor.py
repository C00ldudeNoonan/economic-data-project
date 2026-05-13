"""SEC filing business intelligence extraction utilities."""

import re
from dataclasses import dataclass


@dataclass
class ExtractedSignal:
    """Represents an extracted business intelligence signal."""

    category: str
    term: str
    context: str
    section_name: str
    confidence_score: float
    position: int


# Business intelligence search patterns organized by category
BI_PATTERNS = {
    "growth_signals": {
        "description": "Indicators of business growth and expansion",
        "patterns": [
            r"(?:significant|substantial|strong|robust)\s+(?:growth|increase|expansion)",
            r"(?:revenue|sales|earnings)\s+(?:growth|increased|grew)\s+(?:by\s+)?\d+",
            r"(?:expanding|expand|grew|grow)\s+(?:our\s+)?(?:operations|business|presence|market)",
            r"(?:new|additional)\s+(?:markets?|regions?|territories?|customers?)",
            r"(?:increased|increasing|higher)\s+(?:demand|orders|bookings)",
            r"(?:record|all-time high)\s+(?:revenue|sales|earnings|profits?)",
            r"(?:outperformed|exceeded|beat)\s+(?:expectations|guidance|estimates)",
            r"(?:positive|favorable)\s+(?:outlook|trajectory|trend)",
            r"market\s+share\s+(?:gains?|increased|grew|growth)",
            r"(?:accelerat(?:ed|ing)|momentum)\s+(?:growth|expansion)",
        ],
    },
    "hiring_plans": {
        "description": "Indicators of workforce expansion",
        "patterns": [
            r"(?:hiring|recruit(?:ing|ment)|adding)\s+(?:\d+\s+)?(?:new\s+)?(?:employees?|staff|workers?|personnel|talent)",
            r"(?:expand(?:ing|ed)?|grow(?:ing)?)\s+(?:our\s+)?(?:workforce|team|headcount|staff)",
            r"(?:new|additional)\s+(?:positions?|roles?|jobs?|openings?)",
            r"(?:increased|increasing)\s+(?:headcount|hiring|recruitment)",
            r"(?:talent|employee)\s+(?:acquisition|investment)",
            r"workforce\s+(?:expansion|growth|development)",
            r"(?:plan(?:s|ning)?|expect(?:s|ing)?)\s+to\s+hire",
            r"(?:added|adding)\s+\d+\s+(?:new\s+)?(?:employees?|positions?)",
        ],
    },
    "market_expansion": {
        "description": "Geographic or market segment expansion",
        "patterns": [
            r"(?:enter(?:ed|ing)?|expansion?\s+into)\s+(?:new\s+)?(?:markets?|regions?|countries?|territories?)",
            r"(?:international|global|geographic)\s+(?:expansion|growth|presence)",
            r"(?:opened|opening|launch(?:ed|ing)?)\s+(?:new\s+)?(?:facilities?|offices?|locations?|stores?)",
            r"(?:acquired|acquiring|acquisition)\s+(?:of\s+)?(?:companies?|businesses?|operations?)",
            r"(?:strategic|new)\s+(?:partnerships?|alliances?|joint\s+ventures?)",
            r"(?:expanded|expanding)\s+(?:distribution|sales)\s+(?:channels?|networks?)",
            r"(?:penetrat(?:ed|ing)|enter(?:ed|ing))\s+(?:the\s+)?(?:\w+\s+)?market",
            r"(?:new|additional)\s+(?:customer|client)\s+(?:segments?|verticals?)",
        ],
    },
    "capex_investment": {
        "description": "Capital expenditure and investment signals",
        "patterns": [
            r"(?:capital\s+)?(?:expenditures?|investments?|spending)\s+(?:of\s+)?\$?\d+",
            r"(?:investing|invested)\s+(?:\$?\d+\s+)?(?:in|into)\s+(?:new\s+)?(?:facilities?|equipment|technology|infrastructure)",
            r"(?:construction|building|development)\s+of\s+(?:new\s+)?(?:facilities?|plants?|centers?)",
            r"(?:upgrade|upgrading|moderniz(?:e|ing|ation))\s+(?:our\s+)?(?:facilities?|equipment|systems?|infrastructure)",
            r"(?:research\s+and\s+development|R&D)\s+(?:investments?|spending|expenditures?)",
            r"(?:technology|digital)\s+(?:investments?|transformation|initiatives?)",
            r"(?:capacity|production)\s+(?:expansion|increase|investments?)",
        ],
    },
    "product_innovation": {
        "description": "New product or service launches",
        "patterns": [
            r"(?:launch(?:ed|ing)?|introduc(?:ed|ing)?|releas(?:ed|ing)?)\s+(?:new\s+)?(?:products?|services?|solutions?|offerings?)",
            r"(?:new|innovative|next-generation)\s+(?:products?|services?|solutions?|technologies?|platforms?)",
            r"(?:product|service)\s+(?:development|innovation|pipeline)",
            r"(?:proprietary|patented)\s+(?:technology|solutions?|products?)",
            r"(?:expanded|expanding)\s+(?:product|service)\s+(?:lines?|offerings?|portfolio)",
            r"(?:R&D|research)\s+(?:breakthroughs?|advances?|innovations?)",
            r"(?:intellectual\s+property|patents?)\s+(?:portfolio|growth|filings?)",
        ],
    },
    "cost_efficiency": {
        "description": "Cost reduction and efficiency improvements",
        "patterns": [
            r"(?:cost|expense)\s+(?:reductions?|savings?|efficienc(?:y|ies)|optimization)",
            r"(?:operational|operating)\s+(?:efficienc(?:y|ies)|improvements?|leverage)",
            r"(?:margin|margins)\s+(?:expansion|improvement|increased)",
            r"(?:streamlin(?:ed|ing)|optimiz(?:ed|ing))\s+(?:operations?|processes?|costs?)",
            r"(?:productivity|efficiency)\s+(?:gains?|improvements?|initiatives?)",
            r"(?:restructuring|reorganization)\s+(?:benefits?|savings?)",
            r"(?:reduced|lower(?:ed)?|decreased)\s+(?:costs?|expenses?|overhead)",
        ],
    },
    "risk_factors": {
        "description": "Key business risks and challenges",
        "patterns": [
            r"(?:significant|material|substantial)\s+(?:risks?|uncertaint(?:y|ies)|challenges?)",
            r"(?:competition|competitive\s+pressure)\s+(?:from|in|may)",
            r"(?:regulatory|legal|compliance)\s+(?:risks?|challenges?|requirements?)",
            r"(?:economic|market)\s+(?:downturn|volatility|uncertainty|conditions?)",
            r"(?:supply\s+chain|supplier)\s+(?:risks?|disruptions?|challenges?)",
            r"(?:cybersecurity|data\s+(?:security|breach))\s+(?:risks?|threats?)",
            r"(?:climate|environmental)\s+(?:risks?|regulations?|impact)",
            r"(?:labor|workforce)\s+(?:shortages?|challenges?|costs?)",
            r"(?:inflation(?:ary)?|cost)\s+(?:pressures?|increases?|impacts?)",
        ],
    },
    "financial_health": {
        "description": "Financial strength indicators",
        "patterns": [
            r"(?:strong|healthy|solid)\s+(?:balance\s+sheet|financial\s+position|liquidity|cash\s+flow)",
            r"(?:cash|liquidity)\s+(?:position|reserves?)\s+(?:of\s+)?\$?\d+",
            r"(?:debt|leverage)\s+(?:reduction|repayment|decreased)",
            r"(?:credit|debt)\s+(?:rating|upgrade)",
            r"(?:dividend|shareholder\s+returns?)\s+(?:increased?|growth|payments?)",
            r"(?:share|stock)\s+(?:repurchase|buyback)\s+(?:program|authorization)",
            r"(?:free\s+cash\s+flow|FCF)\s+(?:of\s+)?\$?\d+",
            r"(?:working\s+capital|capital\s+structure)\s+(?:improved|optimization)",
        ],
    },
    "strategic_initiatives": {
        "description": "Key strategic plans and initiatives",
        "patterns": [
            r"(?:strategic|long-term)\s+(?:plan|initiative|priority|focus|objective)",
            r"(?:transformation|turnaround)\s+(?:plan|strategy|initiative|program)",
            r"(?:digital|technology)\s+(?:strategy|transformation|initiatives?)",
            r"(?:sustainability|ESG)\s+(?:initiatives?|goals?|commitments?|strategy)",
            r"(?:operational\s+excellence|continuous\s+improvement)\s+(?:initiatives?|programs?)",
            r"(?:customer|client)\s+(?:focus|experience|satisfaction)\s+(?:initiatives?|improvements?)",
            r"(?:brand|marketing)\s+(?:strategy|investments?|initiatives?)",
        ],
    },
}


class SECBIExtractor:
    """Extract business intelligence signals from SEC filing text."""

    def __init__(self, context_window: int = 200):
        """
        Initialize the extractor.

        Args:
            context_window: Number of characters to include around matches
        """
        self.context_window = context_window
        self.patterns = BI_PATTERNS

    def extract_signals(
        self,
        text: str,
        section_name: str = "unknown",
        categories: list[str] | None = None,
    ) -> list[ExtractedSignal]:
        """
        Extract business intelligence signals from text.

        Args:
            text: Text content to analyze
            section_name: Name of the section being analyzed
            categories: Optional list of categories to extract (default: all)

        Returns:
            List of ExtractedSignal objects
        """
        signals = []
        text_lower = text.lower()

        # Determine which categories to search
        search_categories = categories or list(self.patterns.keys())

        for category in search_categories:
            if category not in self.patterns:
                continue

            category_info = self.patterns[category]
            for pattern in category_info["patterns"]:
                matches = list(re.finditer(pattern, text_lower, re.IGNORECASE))

                for match in matches:
                    # Extract context around the match
                    start = max(0, match.start() - self.context_window)
                    end = min(len(text), match.end() + self.context_window)
                    context = text[start:end].strip()

                    # Clean up context - remove extra whitespace
                    context = re.sub(r"\s+", " ", context)

                    # Calculate confidence score based on match quality
                    confidence = self._calculate_confidence(
                        match.group(), context, category
                    )

                    signals.append(
                        ExtractedSignal(
                            category=category,
                            term=match.group(),
                            context=context,
                            section_name=section_name,
                            confidence_score=confidence,
                            position=match.start(),
                        )
                    )

        # Remove duplicates (same term in overlapping contexts)
        signals = self._deduplicate_signals(signals)

        return signals

    def _calculate_confidence(self, term: str, context: str, category: str) -> float:
        """
        Calculate confidence score for a signal.

        Args:
            term: The matched term
            context: The surrounding context
            category: The signal category

        Returns:
            Confidence score between 0 and 1
        """
        score = 0.5  # Base score

        # Longer matches are more specific
        if len(term) > 20:
            score += 0.1
        if len(term) > 30:
            score += 0.1

        # Numeric values increase confidence
        if re.search(r"\d+", term):
            score += 0.1

        # Dollar amounts are high confidence
        if re.search(r"\$\d+", context):
            score += 0.1

        # Presence of quantifiers increases confidence
        quantifiers = ["significant", "substantial", "major", "record", "strong"]
        if any(q in context.lower() for q in quantifiers):
            score += 0.1

        # Cap at 1.0
        return min(1.0, score)

    def _deduplicate_signals(
        self, signals: list[ExtractedSignal]
    ) -> list[ExtractedSignal]:
        """Remove duplicate signals based on position proximity."""
        if not signals:
            return signals

        # Sort by position
        signals.sort(key=lambda x: (x.category, x.position))

        deduplicated = []
        for signal in signals:
            # Check if this signal is too close to the last one in same category
            is_duplicate = False
            for existing in deduplicated:
                if (
                    existing.category == signal.category
                    and abs(existing.position - signal.position) < 50
                ):
                    is_duplicate = True
                    # Keep the one with higher confidence
                    if signal.confidence_score > existing.confidence_score:
                        deduplicated.remove(existing)
                        deduplicated.append(signal)
                    break

            if not is_duplicate:
                deduplicated.append(signal)

        return deduplicated

    def extract_all_categories(
        self, text: str, section_name: str = "unknown"
    ) -> dict[str, list[ExtractedSignal]]:
        """
        Extract signals for all categories, organized by category.

        Args:
            text: Text content to analyze
            section_name: Name of the section being analyzed

        Returns:
            Dict mapping category name to list of signals
        """
        all_signals = self.extract_signals(text, section_name)

        # Organize by category
        by_category: dict[str, list[ExtractedSignal]] = {}
        for signal in all_signals:
            if signal.category not in by_category:
                by_category[signal.category] = []
            by_category[signal.category].append(signal)

        return by_category

    def get_summary_stats(self, signals: list[ExtractedSignal]) -> dict:
        """
        Get summary statistics for extracted signals.

        Args:
            signals: List of extracted signals

        Returns:
            Dict with summary statistics
        """
        if not signals:
            return {
                "total_signals": 0,
                "categories": {},
                "avg_confidence": 0,
                "top_categories": [],
            }

        # Count by category
        category_counts: dict[str, int] = {}
        category_confidence: dict[str, list[float]] = {}

        for signal in signals:
            category_counts[signal.category] = (
                category_counts.get(signal.category, 0) + 1
            )
            if signal.category not in category_confidence:
                category_confidence[signal.category] = []
            category_confidence[signal.category].append(signal.confidence_score)

        # Calculate average confidence per category
        category_avg_confidence = {
            cat: sum(scores) / len(scores)
            for cat, scores in category_confidence.items()
        }

        # Sort categories by count
        top_categories = sorted(
            category_counts.items(), key=lambda x: x[1], reverse=True
        )

        return {
            "total_signals": len(signals),
            "categories": category_counts,
            "avg_confidence": sum(s.confidence_score for s in signals) / len(signals),
            "category_avg_confidence": category_avg_confidence,
            "top_categories": [cat for cat, _ in top_categories[:5]],
        }

    def extract_key_metrics(self, text: str) -> list[dict]:
        """
        Extract key numerical metrics from text.

        Args:
            text: Text content to analyze

        Returns:
            List of dicts with metric information
        """
        metrics = []

        # Revenue/sales patterns
        revenue_patterns = [
            r"(?:revenue|sales|net\s+sales)\s+(?:of|was|were|totaled)\s+\$?([\d,.]+)\s*(?:billion|million|B|M)?",
            r"\$?([\d,.]+)\s*(?:billion|million|B|M)?\s+(?:in\s+)?(?:revenue|sales)",
        ]

        # Earnings patterns
        earnings_patterns = [
            r"(?:net\s+income|earnings|profit)\s+(?:of|was|were)\s+\$?([\d,.]+)\s*(?:billion|million|B|M)?",
            r"(?:EPS|earnings\s+per\s+share)\s+(?:of|was|were)\s+\$?([\d,.]+)",
        ]

        # Growth rate patterns
        growth_patterns = [
            r"(?:grew|increased|growth)\s+(?:by\s+)?([\d.]+)\s*%",
            r"([\d.]+)\s*%\s+(?:increase|growth|higher)",
        ]

        # Employee count patterns
        employee_patterns = [
            r"(?:approximately|about|over)?\s*([\d,]+)\s+(?:employees|workers|staff)",
            r"(?:workforce|headcount)\s+(?:of\s+)?([\d,]+)",
        ]

        pattern_groups = [
            ("revenue", revenue_patterns),
            ("earnings", earnings_patterns),
            ("growth_rate", growth_patterns),
            ("employee_count", employee_patterns),
        ]

        for metric_type, patterns in pattern_groups:
            for pattern in patterns:
                matches = re.finditer(pattern, text, re.IGNORECASE)
                for match in matches:
                    value = match.group(1) if match.groups() else match.group()
                    metrics.append(
                        {
                            "metric_type": metric_type,
                            "value": value,
                            "full_match": match.group(),
                            "position": match.start(),
                        }
                    )

        return metrics

    @staticmethod
    def get_category_descriptions() -> dict[str, str]:
        """Get descriptions of all signal categories."""
        return {
            category: str(info["description"]) for category, info in BI_PATTERNS.items()
        }
