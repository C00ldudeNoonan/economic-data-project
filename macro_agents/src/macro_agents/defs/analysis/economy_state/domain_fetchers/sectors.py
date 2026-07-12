"""Sector-ETF domain data fetchers."""

import logging
from typing import TYPE_CHECKING

from google.api_core.exceptions import GoogleAPIError

from macro_agents.defs.analysis.economy_state.domain_fetchers._common import df_to_csv

if TYPE_CHECKING:
    from macro_agents.defs.resources.bigquery_warehouse import BigQueryWarehouseResource

logger = logging.getLogger(__name__)

# US sector ETFs
SECTOR_SYMBOLS = [
    "XLK",
    "XLF",
    "XLE",
    "XLV",
    "XLI",
    "XLY",
    "XLP",
    "XLU",
    "XLB",
    "XLRE",
    "XLC",
]


def get_sector_data(
    md_resource: "BigQueryWarehouseResource",
    cutoff_date: str | None = None,
    time_period: str = "6_months",
) -> str:
    """Fetch US sector ETF performance data.

    Args:
        md_resource: BigQuery resource for database queries
        cutoff_date: Optional date for backtesting (YYYY-MM-DD)
        time_period: Time period for returns (e.g., '6_months')

    Returns:
        CSV string of sector performance data
    """
    if cutoff_date:
        query = f"""
        SELECT
            symbol,
            name,
            time_period,
            total_return_pct,
            avg_daily_return_pct,
            volatility_pct,
            win_rate_pct,
            period_start_price,
            period_end_price
        FROM agent_market_performance_snapshot
        WHERE snapshot_date = '{cutoff_date}'
            AND market_category = 'sector'
            AND time_period = '{time_period}'
        ORDER BY total_return_pct DESC
        """
    else:
        query = f"""
        SELECT
            symbol,
            name,
            time_period,
            total_return_pct,
            avg_daily_return_pct,
            volatility_pct,
            win_rate_pct,
            period_start_price,
            period_end_price
        FROM agent_market_performance
        WHERE market_category = 'sector'
            AND time_period = '{time_period}'
        ORDER BY total_return_pct DESC
        """

    df = md_resource.execute_query(query, read_only=True)
    return df_to_csv(df)


def get_sector_correlation_data(
    md_resource: "BigQueryWarehouseResource",
    cutoff_date: str | None = None,
    sample_size: int = 30,
) -> str:
    """Fetch sector correlation with economic indicators.

    Args:
        md_resource: BigQuery resource for database queries
        cutoff_date: Optional date for backtesting (YYYY-MM-DD)
        sample_size: Number of top correlations to return

    Returns:
        CSV string of sector-indicator correlations, or "" if the correlation
        table is unavailable (e.g. not yet materialized).
    """
    symbols_list = "', '".join(SECTOR_SYMBOLS)

    query = f"""
    SELECT
        symbol,
        series_code,
        series_name,
        correlation_econ_vs_q1_returns,
        correlation_econ_vs_q2_returns,
        correlation_econ_vs_q3_returns
    FROM agent_leading_econ_return_indicator
    WHERE symbol IN ('{symbols_list}')
    ORDER BY ABS(correlation_econ_vs_q1_returns) DESC
    LIMIT {sample_size}
    """

    # The correlation model may not be materialized yet; treat a warehouse
    # error as "no data" (empty string) rather than failing the whole
    # economy-state analysis, but log it so the degradation is visible.
    try:
        df = md_resource.execute_query(query, read_only=True)
    except GoogleAPIError as exc:
        logger.warning("Sector correlation query failed, returning no data: %s", exc)
        return ""

    if df.is_empty():
        return ""
    return df_to_csv(df)
