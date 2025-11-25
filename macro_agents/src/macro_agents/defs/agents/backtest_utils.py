import re
from typing import List, Dict, Any

from macro_agents.defs.resources.motherduck import MotherDuckResource


def extract_recommendations(recommendations_content: str) -> List[Dict[str, Any]]:
    """
    Extract specific asset recommendations from recommendations text.

    Returns a list of recommendations with:
    - symbol: Asset symbol (e.g., "XLK", "SPY", "XLE")
    - direction: "OVERWEIGHT" or "UNDERWEIGHT"
    - confidence: Confidence level if mentioned (0-1)
    - expected_return: Expected return if mentioned
    """
    recommendations = []

    overweight_pattern = (
        r"(?:OVERWEIGHT|overweight)[:\s]+(?:.*?)([A-Z]{1,5}(?:\.[A-Z]+)?)(?:\s|,|\.|$)"
    )
    overweight_matches = re.finditer(
        overweight_pattern, recommendations_content, re.IGNORECASE
    )

    for match in overweight_matches:
        symbol = match.group(1).strip()
        if symbol and len(symbol) <= 6:
            context_start = max(0, match.start() - 100)
            context_end = min(len(recommendations_content), match.end() + 100)
            context = recommendations_content[context_start:context_end]

            confidence_match = re.search(
                r"confidence[:\s]+([0-9.]+)", context, re.IGNORECASE
            )
            confidence = None
            if confidence_match:
                confidence_str = confidence_match.group(1).rstrip(".,;")
                try:
                    confidence = float(confidence_str)
                    if confidence > 1:
                        confidence = confidence / 100
                except (ValueError, AttributeError):
                    confidence = None

            return_match = re.search(
                r"(?:expected|return)[:\s]+([-0-9.]+)%?", context, re.IGNORECASE
            )
            expected_return = None
            if return_match:
                return_str = return_match.group(1).rstrip(".,;%")
                try:
                    expected_return = float(return_str)
                except (ValueError, AttributeError):
                    expected_return = None

            recommendations.append(
                {
                    "symbol": symbol,
                    "direction": "OVERWEIGHT",
                    "confidence": confidence,
                    "expected_return": expected_return,
                }
            )

    underweight_pattern = r"(?:UNDERWEIGHT|underweight)[:\s]+(?:.*?)([A-Z]{1,5}(?:\.[A-Z]+)?)(?:\s|,|\.|$)"
    underweight_matches = re.finditer(
        underweight_pattern, recommendations_content, re.IGNORECASE
    )

    for match in underweight_matches:
        symbol = match.group(1).strip()
        if symbol and len(symbol) <= 6:
            context_start = max(0, match.start() - 100)
            context_end = min(len(recommendations_content), match.end() + 100)
            context = recommendations_content[context_start:context_end]

            confidence_match = re.search(
                r"confidence[:\s]+([0-9.]+)", context, re.IGNORECASE
            )
            confidence = None
            if confidence_match:
                confidence_str = confidence_match.group(1).rstrip(".,;")
                try:
                    confidence = float(confidence_str)
                    if confidence > 1:
                        confidence = confidence / 100
                except (ValueError, AttributeError):
                    confidence = None

            return_match = re.search(
                r"(?:expected|return)[:\s]+([-0-9.]+)%?", context, re.IGNORECASE
            )
            expected_return = None
            if return_match:
                return_str = return_match.group(1).rstrip(".,;%")
                try:
                    expected_return = float(return_str)
                except (ValueError, AttributeError):
                    expected_return = None

            recommendations.append(
                {
                    "symbol": symbol,
                    "direction": "UNDERWEIGHT",
                    "confidence": confidence,
                    "expected_return": expected_return,
                }
            )

    sector_etf_pattern = r"\b(XLK|XLE|XLF|XLI|XLV|XLP|XLY|XLB|XLU|SPY|QQQ|DIA|IWM)\b"
    sector_matches = re.finditer(
        sector_etf_pattern, recommendations_content, re.IGNORECASE
    )

    for match in sector_matches:
        symbol = match.group(1).upper()
        if not any(r["symbol"] == symbol for r in recommendations):
            context_start = max(0, match.start() - 200)
            context_end = min(len(recommendations_content), match.end() + 200)
            context = recommendations_content[context_start:context_end].lower()

            if (
                "overweight" in context
                and "underweight" not in context[: context.find(symbol.lower())]
            ):
                recommendations.append(
                    {
                        "symbol": symbol,
                        "direction": "OVERWEIGHT",
                        "confidence": None,
                        "expected_return": None,
                    }
                )
            elif "underweight" in context:
                recommendations.append(
                    {
                        "symbol": symbol,
                        "direction": "UNDERWEIGHT",
                        "confidence": None,
                        "expected_return": None,
                    }
                )

    seen = set()
    unique_recommendations = []
    for rec in recommendations:
        key = (rec["symbol"], rec["direction"])
        if key not in seen:
            seen.add(key)
            unique_recommendations.append(rec)

    return unique_recommendations


def get_asset_returns(
    md_resource: MotherDuckResource,
    symbols: List[str],
    backtest_date: str,
    periods: List[int] = [1, 3, 6],  # months
) -> Dict[str, Dict[str, Any]]:
    """
    Get actual returns for assets for multiple forward-looking periods.

    Args:
        md_resource: MotherDuck resource
        symbols: List of asset symbols to get returns for
        backtest_date: Date string (YYYY-MM-DD) for the backtest cutoff
        periods: List of months to calculate forward returns for

    Returns:
        Dictionary mapping symbol to return data:
        {
            "XLK": {
                "1m": {"actual_return": 5.2, "spy_return": 3.1, "outperformance": 2.1},
                "3m": {...},
                "6m": {...}
            },
            ...
        }
    """
    results = {}

    spy_returns = {}
    for period_months in periods:
        # Calculate monthly forward returns from monthly_avg_close prices
        # SPY is in major_indicies_analysis_return
        query = f"""
        WITH monthly_data AS (
            SELECT 
                month_date,
                monthly_avg_close,
                LEAD(monthly_avg_close, {period_months}) OVER (
                    PARTITION BY symbol, exchange
                    ORDER BY month_date
                ) AS forward_close
            FROM major_indicies_analysis_return
            WHERE symbol = 'SPY'
        )
        SELECT 
            month_date,
            CASE 
                WHEN forward_close IS NOT NULL AND monthly_avg_close > 0
                THEN ROUND((forward_close - monthly_avg_close) / monthly_avg_close * 100, 2)
                ELSE NULL
            END AS pct_change_forward
        FROM monthly_data
        WHERE month_date = '{backtest_date}'
        LIMIT 1
        """

        df = md_resource.execute_query(query, read_only=True)
        if not df.is_empty() and df[0, "pct_change_forward"] is not None:
            spy_return = df[0, "pct_change_forward"]
            spy_returns[f"{period_months}m"] = spy_return
        else:
            spy_returns[f"{period_months}m"] = 0.0

    for symbol in symbols:
        symbol_returns = {}

        for period_months in periods:
            # Calculate monthly forward returns from monthly_avg_close prices
            # Check all possible tables to find the symbol
            query = f"""
            WITH all_symbols AS (
                SELECT symbol, exchange, month_date, monthly_avg_close
                FROM major_indicies_analysis_return
                WHERE symbol = '{symbol}'
                UNION ALL
                SELECT symbol, exchange, month_date, monthly_avg_close
                FROM us_sector_analysis_return
                WHERE symbol = '{symbol}'
                UNION ALL
                SELECT symbol, exchange, month_date, monthly_avg_close
                FROM global_markets_analysis_return
                WHERE symbol = '{symbol}'
                UNION ALL
                SELECT symbol, exchange, month_date, monthly_avg_close
                FROM currency_analysis_return
                WHERE symbol = '{symbol}'
                UNION ALL
                SELECT symbol, exchange, month_date, monthly_avg_close
                FROM fixed_income_analysis_return
                WHERE symbol = '{symbol}'
            ),
            monthly_data AS (
                SELECT 
                    month_date,
                    monthly_avg_close,
                    LEAD(monthly_avg_close, {period_months}) OVER (
                        PARTITION BY symbol, exchange
                        ORDER BY month_date
                    ) AS forward_close
                FROM all_symbols
            )
            SELECT 
                month_date,
                CASE 
                    WHEN forward_close IS NOT NULL AND monthly_avg_close > 0
                    THEN ROUND((forward_close - monthly_avg_close) / monthly_avg_close * 100, 2)
                    ELSE NULL
                END AS pct_change_forward
            FROM monthly_data
            WHERE month_date = '{backtest_date}'
            LIMIT 1
            """

            df = md_resource.execute_query(query, read_only=True)
            if not df.is_empty() and df[0, "pct_change_forward"] is not None:
                actual_return = df[0, "pct_change_forward"]
                spy_return = spy_returns.get(f"{period_months}m", 0.0)
                outperformance = actual_return - spy_return

                symbol_returns[f"{period_months}m"] = {
                    "actual_return": actual_return,
                    "spy_return": spy_return,
                    "outperformance": outperformance,
                }
            else:
                symbol_returns[f"{period_months}m"] = {
                    "actual_return": None,
                    "spy_return": spy_returns.get(f"{period_months}m", 0.0),
                    "outperformance": None,
                }

        results[symbol] = symbol_returns

    return results
