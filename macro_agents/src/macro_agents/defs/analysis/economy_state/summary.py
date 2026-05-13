import re
from typing import Any


def extract_economy_state_summary(analysis_content: str) -> dict[str, Any]:
    """Extract key insights from economy state analysis for metadata."""
    summary: dict[str, Any] = {}

    if not analysis_content:
        return summary

    cycle_match = re.search(
        r"(?:Current Economic Cycle Position|Cycle Position|Economic Cycle):\s*([^.\n]+)",
        analysis_content,
        re.IGNORECASE,
    )
    if cycle_match:
        summary["economic_cycle_position"] = cycle_match.group(1).strip()

    confidence_match = re.search(
        r"confidence[:\s]+([0-9.]+)", analysis_content, re.IGNORECASE
    )
    if confidence_match:
        try:
            summary["confidence_level"] = float(confidence_match.group(1))
        except ValueError:
            pass

    risk_section = re.search(
        r"(?:Risk Factors|Key Risks|Risks):\s*([^0-9]+?)(?:\d+\.|$)",
        analysis_content,
        re.IGNORECASE | re.DOTALL,
    )
    if risk_section:
        risks_text = risk_section.group(1)[:500]
        summary["key_risks_summary"] = risks_text.strip()

    inflation_match = re.search(
        r"(?:Inflation|CPI|PCE).*?(?:trend|trajectory|level):\s*([^.\n]+)",
        analysis_content,
        re.IGNORECASE,
    )
    if inflation_match:
        summary["inflation_trend"] = inflation_match.group(1).strip()

    gdp_match = re.search(
        r"(?:GDP|growth).*?(?:trend|outlook|growth):\s*([^.\n]+)",
        analysis_content,
        re.IGNORECASE,
    )
    if gdp_match:
        summary["gdp_outlook"] = gdp_match.group(1).strip()

    return summary
