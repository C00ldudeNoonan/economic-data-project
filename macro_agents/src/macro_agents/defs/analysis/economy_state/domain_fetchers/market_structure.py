"""Market-structure domain data fetchers (indices, fixed income, global)."""

from typing import TYPE_CHECKING

from macro_agents.defs.analysis.economy_state.domain_fetchers._common import df_to_csv

if TYPE_CHECKING:
    from macro_agents.defs.resources.bigquery_warehouse import BigQueryWarehouseResource

# Fixed income ETFs
FIXED_INCOME_SYMBOLS = ["HYG", "LQD", "TIP", "GOVT", "MUB", "AGG", "BND"]

# Global market and currency ETFs
GLOBAL_MARKET_SYMBOLS = [
    "EEM",
    "EFA",
    "ACWI",
    "FXI",
    "EWJ",
    "EWG",
    "FXE",
    "FXY",
    "UUP",
]


def _symbol_scoped_market_query(
    symbols: list[str], cutoff_date: str | None, time_period: str
) -> str:
    """Build a major-index performance query scoped to a symbol list.

    Shared by the fixed-income and global-market fetchers, which differ only in
    their symbol set. A backtest cutoff_date adds a period_end_date bound.
    """
    symbols_list = "', '".join(symbols)
    cutoff_filter = f"AND period_end_date <= '{cutoff_date}'" if cutoff_date else ""
    return f"""
        SELECT
            symbol,
            name,
            time_period,
            total_return_pct,
            avg_daily_return_pct,
            volatility_pct,
            period_start_price,
            period_end_price
        FROM agent_market_performance
        WHERE symbol IN ('{symbols_list}')
            AND market_category = 'major_index'
            AND time_period = '{time_period}'
            {cutoff_filter}
        ORDER BY total_return_pct DESC
        """


def get_major_indices_data(
    md_resource: "BigQueryWarehouseResource",
    cutoff_date: str | None = None,
    time_period: str = "6_months",
) -> str:
    """Fetch major indices performance data.

    Args:
        md_resource: BigQuery resource for database queries
        cutoff_date: Optional date for backtesting (YYYY-MM-DD)
        time_period: Time period for returns (e.g., '6_months')

    Returns:
        CSV string of major indices data
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
        FROM agent_market_performance
        WHERE time_period = '{time_period}'
            AND market_category = 'major_index'
            AND period_end_date <= '{cutoff_date}'
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
        WHERE market_category = 'major_index'
            AND time_period = '{time_period}'
        ORDER BY total_return_pct DESC
        """

    df = md_resource.execute_query(query, read_only=True)
    return df_to_csv(df)


def get_fixed_income_data(
    md_resource: "BigQueryWarehouseResource",
    cutoff_date: str | None = None,
    time_period: str = "6_months",
) -> str:
    """Fetch fixed income ETF performance data.

    Args:
        md_resource: BigQuery resource for database queries
        cutoff_date: Optional date for backtesting (YYYY-MM-DD)
        time_period: Time period for returns (e.g., '6_months')

    Returns:
        CSV string of fixed income ETF data
    """
    query = _symbol_scoped_market_query(FIXED_INCOME_SYMBOLS, cutoff_date, time_period)
    df = md_resource.execute_query(query, read_only=True)
    return df_to_csv(df)


def get_global_markets_data(
    md_resource: "BigQueryWarehouseResource",
    cutoff_date: str | None = None,
    time_period: str = "6_months",
) -> str:
    """Fetch global markets and currency ETF data.

    Args:
        md_resource: BigQuery resource for database queries
        cutoff_date: Optional date for backtesting (YYYY-MM-DD)
        time_period: Time period for returns (e.g., '6_months')

    Returns:
        CSV string of global markets data
    """
    query = _symbol_scoped_market_query(GLOBAL_MARKET_SYMBOLS, cutoff_date, time_period)
    df = md_resource.execute_query(query, read_only=True)
    return df_to_csv(df)
