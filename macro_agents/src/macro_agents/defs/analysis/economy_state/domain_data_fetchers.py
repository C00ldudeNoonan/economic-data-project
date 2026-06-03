"""Domain-specific data fetching utilities.

This module provides functions to fetch data for each domain sub-agent
from MotherDuck, filtering to the relevant series and tables.
"""

import io
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from macro_agents.defs.resources.bigquery_warehouse import BigQueryWarehouseResource


# Labor market FRED series codes
LABOR_SERIES = [
    "UNRATE",  # Unemployment Rate
    "CIVPART",  # Labor Force Participation Rate
    "JTSJOL",  # Job Openings
    "ICSA",  # Initial Claims
    "PAYEMS",  # Total Nonfarm Payrolls
    "AHETPI",  # Average Hourly Earnings
    "JTSQUR",  # Quits Rate
    "U6RATE",  # Underemployment Rate
    "EMRATIO",  # Employment-Population Ratio
    "UNEMPLOY",  # Total Unemployed
]

# Financial/monetary FRED series codes
FINANCIAL_SERIES = [
    "FEDFUNDS",  # Fed Funds Rate
    "DFF",  # Fed Funds Effective Rate
    "M1SL",  # M1 Money Stock
    "M2SL",  # M2 Money Stock
    "BAMLH0A0HYM2",  # High Yield Spread
    "BAMLC0A0CM",  # Corporate Spread
    "AAA10Y",  # AAA Corporate Spread
    "BAA10Y",  # BAA Corporate Spread
    "MORTGAGE30US",  # 30-Year Mortgage Rate
    "NFCI",  # Financial Conditions Index
]


def get_labor_market_data(
    md_resource: "BigQueryWarehouseResource",
    cutoff_date: str | None = None,
    max_months: int = 3,
) -> str:
    """Fetch labor market specific data from FRED series.

    Args:
        md_resource: MotherDuck resource for database queries
        cutoff_date: Optional date for backtesting (YYYY-MM-DD)
        max_months: Number of months of history to include

    Returns:
        CSV string of labor market indicators
    """
    series_list = "', '".join(LABOR_SERIES)

    if cutoff_date:
        query = f"""
        SELECT
            series_code,
            series_name,
            month,
            current_value,
            pct_change_3m,
            pct_change_6m,
            pct_change_1y,
            date_grain
        FROM agent_fred_series_latest_aggregates_snapshot
        WHERE snapshot_date = '{cutoff_date}'
            AND series_code IN ('{series_list}')
            AND current_value IS NOT NULL
            AND month >= (
                SELECT MAX(month) - INTERVAL '{max_months - 1} months'
                FROM agent_fred_series_latest_aggregates_snapshot
                WHERE snapshot_date = '{cutoff_date}'
            )
        ORDER BY series_name, month DESC
        """
    else:
        query = f"""
        SELECT
            series_code,
            series_name,
            month,
            current_value,
            pct_change_3m,
            pct_change_6m,
            pct_change_1y,
            date_grain
        FROM agent_fred_series_latest_aggregates
        WHERE series_code IN ('{series_list}')
            AND current_value IS NOT NULL
            AND month >= (
                SELECT MAX(month) - INTERVAL '{max_months - 1} months'
                FROM agent_fred_series_latest_aggregates
            )
        ORDER BY series_name, month DESC
        """

    df = md_resource.execute_query(query, read_only=True)
    csv_buffer = io.BytesIO()
    df.write_csv(csv_buffer)
    return csv_buffer.getvalue().decode("utf-8")


def get_labor_trends_data(
    md_resource: "BigQueryWarehouseResource",
    cutoff_date: str | None = None,
    max_months: int = 6,
) -> str:
    """Fetch month-over-month employment trends.

    Args:
        md_resource: MotherDuck resource for database queries
        cutoff_date: Optional date for backtesting (YYYY-MM-DD)
        max_months: Number of months of trend data

    Returns:
        CSV string of employment trend data
    """
    labor_trend_series = ["UNRATE", "PAYEMS", "ICSA", "AHETPI"]
    series_list = "', '".join(labor_trend_series)
    date_filter = f"AND date <= '{cutoff_date}'" if cutoff_date else ""

    query = f"""
    SELECT
        series_code,
        series_name,
        date,
        value,
        period_diff,
        data_source
    FROM agent_fred_monthly_diff
    WHERE series_code IN ('{series_list}')
        {date_filter}
    ORDER BY series_name, date DESC
    LIMIT {max_months * len(labor_trend_series)}
    """

    df = md_resource.execute_query(query, read_only=True)
    if df.is_empty():
        return ""
    csv_buffer = io.BytesIO()
    df.write_csv(csv_buffer)
    return csv_buffer.getvalue().decode("utf-8")


def get_financial_conditions_data(
    md_resource: "BigQueryWarehouseResource",
    cutoff_date: str | None = None,
    max_months: int = 12,
) -> str:
    """Fetch Financial Conditions Index data.

    Args:
        md_resource: MotherDuck resource for database queries
        cutoff_date: Optional date for backtesting (YYYY-MM-DD)
        max_months: Number of months of FCI history

    Returns:
        CSV string of FCI data
    """
    limit_clause = f"LIMIT {max_months}" if max_months else ""

    if cutoff_date:
        query = f"""
        SELECT
            date,
            FCI,
            equity_score,
            housing_score,
            "10yr_score" as treasury_10yr_score
        FROM agent_financial_conditions_index
        WHERE date <= '{cutoff_date}'
        ORDER BY date DESC
        {limit_clause}
        """
    else:
        query = f"""
        SELECT
            date,
            FCI,
            equity_score,
            housing_score,
            "10yr_score" as treasury_10yr_score
        FROM agent_financial_conditions_index
        ORDER BY date DESC
        {limit_clause}
        """

    df = md_resource.execute_query(query, read_only=True)
    csv_buffer = io.BytesIO()
    df.write_csv(csv_buffer)
    return csv_buffer.getvalue().decode("utf-8")


def get_yield_curve_data(
    md_resource: "BigQueryWarehouseResource",
    cutoff_date: str | None = None,
    max_months: int = 6,
) -> str:
    """Fetch yield curve data with spreads.

    Args:
        md_resource: MotherDuck resource for database queries
        cutoff_date: Optional date for backtesting (YYYY-MM-DD)
        max_months: Number of months of yield curve history

    Returns:
        CSV string of yield curve data with spreads
    """
    date_filter = f"AND date <= '{cutoff_date}'" if cutoff_date else ""
    limit_clause = f"LIMIT {max_months}" if max_months else ""

    query = f"""
    SELECT
        date,
        yield_10y,
        yield_2y,
        yield_3m,
        yield_30y,
        spread_10y_2y,
        spread_10y_3m,
        curve_shape
    FROM agent_treasury_yield_curve_spreads
    WHERE date IS NOT NULL {date_filter}
    ORDER BY date DESC
    {limit_clause}
    """

    df = md_resource.execute_query(query, read_only=True)
    if df.is_empty():
        return ""
    csv_buffer = io.BytesIO()
    df.write_csv(csv_buffer)
    return csv_buffer.getvalue().decode("utf-8")


def get_credit_data(
    md_resource: "BigQueryWarehouseResource",
    cutoff_date: str | None = None,
    max_months: int = 3,
) -> str:
    """Fetch credit spread and monetary policy data.

    Args:
        md_resource: MotherDuck resource for database queries
        cutoff_date: Optional date for backtesting (YYYY-MM-DD)
        max_months: Number of months of credit data

    Returns:
        CSV string of credit and monetary data
    """
    series_list = "', '".join(FINANCIAL_SERIES)

    if cutoff_date:
        query = f"""
        SELECT
            series_code,
            series_name,
            month,
            current_value,
            pct_change_3m,
            pct_change_6m,
            pct_change_1y
        FROM agent_fred_series_latest_aggregates_snapshot
        WHERE snapshot_date = '{cutoff_date}'
            AND series_code IN ('{series_list}')
            AND current_value IS NOT NULL
            AND month >= (
                SELECT MAX(month) - INTERVAL '{max_months - 1} months'
                FROM agent_fred_series_latest_aggregates_snapshot
                WHERE snapshot_date = '{cutoff_date}'
            )
        ORDER BY series_name, month DESC
        """
    else:
        query = f"""
        SELECT
            series_code,
            series_name,
            month,
            current_value,
            pct_change_3m,
            pct_change_6m,
            pct_change_1y
        FROM agent_fred_series_latest_aggregates
        WHERE series_code IN ('{series_list}')
            AND current_value IS NOT NULL
            AND month >= (
                SELECT MAX(month) - INTERVAL '{max_months - 1} months'
                FROM agent_fred_series_latest_aggregates
            )
        ORDER BY series_name, month DESC
        """

    df = md_resource.execute_query(query, read_only=True)
    csv_buffer = io.BytesIO()
    df.write_csv(csv_buffer)
    return csv_buffer.getvalue().decode("utf-8")


def get_energy_commodities_data(
    md_resource: "BigQueryWarehouseResource",
    cutoff_date: str | None = None,
    time_period: str = "6_months",
) -> str:
    """Fetch energy commodities data.

    Args:
        md_resource: MotherDuck resource for database queries
        cutoff_date: Optional date for backtesting (YYYY-MM-DD)
        time_period: Time period for returns (e.g., '6_months')

    Returns:
        CSV string of energy commodities data
    """
    if cutoff_date:
        query = f"""
        SELECT
            commodity_name,
            commodity_unit,
            time_period,
            total_return_pct,
            avg_daily_return_pct,
            volatility_pct,
            period_start_price,
            period_end_price
        FROM agent_commodity_performance_snapshot
        WHERE snapshot_date = '{cutoff_date}'
            AND commodity_category = 'energy'
            AND time_period = '{time_period}'
        ORDER BY total_return_pct DESC
        """
    else:
        query = f"""
        SELECT
            commodity_name,
            commodity_unit,
            time_period,
            total_return_pct,
            avg_daily_return_pct,
            volatility_pct,
            period_start_price,
            period_end_price
        FROM agent_commodity_performance
        WHERE commodity_category = 'energy'
            AND time_period = '{time_period}'
        ORDER BY total_return_pct DESC
        """

    df = md_resource.execute_query(query, read_only=True)
    csv_buffer = io.BytesIO()
    df.write_csv(csv_buffer)
    return csv_buffer.getvalue().decode("utf-8")


def get_input_commodities_data(
    md_resource: "BigQueryWarehouseResource",
    cutoff_date: str | None = None,
    time_period: str = "6_months",
) -> str:
    """Fetch industrial/input commodities data.

    Args:
        md_resource: MotherDuck resource for database queries
        cutoff_date: Optional date for backtesting (YYYY-MM-DD)
        time_period: Time period for returns (e.g., '6_months')

    Returns:
        CSV string of input commodities data
    """
    if cutoff_date:
        query = f"""
        SELECT
            commodity_name,
            commodity_unit,
            time_period,
            total_return_pct,
            avg_daily_return_pct,
            volatility_pct,
            period_start_price,
            period_end_price
        FROM agent_commodity_performance_snapshot
        WHERE snapshot_date = '{cutoff_date}'
            AND commodity_category = 'input'
            AND time_period = '{time_period}'
        ORDER BY total_return_pct DESC
        """
    else:
        query = f"""
        SELECT
            commodity_name,
            commodity_unit,
            time_period,
            total_return_pct,
            avg_daily_return_pct,
            volatility_pct,
            period_start_price,
            period_end_price
        FROM agent_commodity_performance
        WHERE commodity_category = 'input'
            AND time_period = '{time_period}'
        ORDER BY total_return_pct DESC
        """

    df = md_resource.execute_query(query, read_only=True)
    csv_buffer = io.BytesIO()
    df.write_csv(csv_buffer)
    return csv_buffer.getvalue().decode("utf-8")


def get_agriculture_commodities_data(
    md_resource: "BigQueryWarehouseResource",
    cutoff_date: str | None = None,
    time_period: str = "6_months",
) -> str:
    """Fetch agricultural commodities data.

    Args:
        md_resource: MotherDuck resource for database queries
        cutoff_date: Optional date for backtesting (YYYY-MM-DD)
        time_period: Time period for returns (e.g., '6_months')

    Returns:
        CSV string of agricultural commodities data
    """
    if cutoff_date:
        query = f"""
        SELECT
            commodity_name,
            commodity_unit,
            time_period,
            total_return_pct,
            avg_daily_return_pct,
            volatility_pct,
            period_start_price,
            period_end_price
        FROM agent_commodity_performance_snapshot
        WHERE snapshot_date = '{cutoff_date}'
            AND commodity_category = 'agriculture'
            AND time_period = '{time_period}'
        ORDER BY total_return_pct DESC
        """
    else:
        query = f"""
        SELECT
            commodity_name,
            commodity_unit,
            time_period,
            total_return_pct,
            avg_daily_return_pct,
            volatility_pct,
            period_start_price,
            period_end_price
        FROM agent_commodity_performance
        WHERE commodity_category = 'agriculture'
            AND time_period = '{time_period}'
        ORDER BY total_return_pct DESC
        """

    df = md_resource.execute_query(query, read_only=True)
    csv_buffer = io.BytesIO()
    df.write_csv(csv_buffer)
    return csv_buffer.getvalue().decode("utf-8")


def get_sector_data(
    md_resource: "BigQueryWarehouseResource",
    cutoff_date: str | None = None,
    time_period: str = "6_months",
) -> str:
    """Fetch US sector ETF performance data.

    Args:
        md_resource: MotherDuck resource for database queries
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
    csv_buffer = io.BytesIO()
    df.write_csv(csv_buffer)
    return csv_buffer.getvalue().decode("utf-8")


def get_sector_correlation_data(
    md_resource: "BigQueryWarehouseResource",
    cutoff_date: str | None = None,
    sample_size: int = 30,
) -> str:
    """Fetch sector correlation with economic indicators.

    Args:
        md_resource: MotherDuck resource for database queries
        cutoff_date: Optional date for backtesting (YYYY-MM-DD)
        sample_size: Number of top correlations to return

    Returns:
        CSV string of sector-indicator correlations
    """
    # Filter to sector ETFs only
    sector_symbols = [
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
    symbols_list = "', '".join(sector_symbols)

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

    try:
        df = md_resource.execute_query(query, read_only=True)
        if df.is_empty():
            return ""
        csv_buffer = io.BytesIO()
        df.write_csv(csv_buffer)
        return csv_buffer.getvalue().decode("utf-8")
    except Exception:
        return ""


def get_major_indices_data(
    md_resource: "BigQueryWarehouseResource",
    cutoff_date: str | None = None,
    time_period: str = "6_months",
) -> str:
    """Fetch major indices performance data.

    Args:
        md_resource: MotherDuck resource for database queries
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
    csv_buffer = io.BytesIO()
    df.write_csv(csv_buffer)
    return csv_buffer.getvalue().decode("utf-8")


def get_fixed_income_data(
    md_resource: "BigQueryWarehouseResource",
    cutoff_date: str | None = None,
    time_period: str = "6_months",
) -> str:
    """Fetch fixed income ETF performance data.

    Args:
        md_resource: MotherDuck resource for database queries
        cutoff_date: Optional date for backtesting (YYYY-MM-DD)
        time_period: Time period for returns (e.g., '6_months')

    Returns:
        CSV string of fixed income ETF data
    """
    # Fixed income ETFs
    fi_symbols = ["HYG", "LQD", "TIP", "GOVT", "MUB", "AGG", "BND"]
    symbols_list = "', '".join(fi_symbols)

    if cutoff_date:
        query = f"""
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
            period_start_price,
            period_end_price
        FROM agent_market_performance
        WHERE symbol IN ('{symbols_list}')
            AND market_category = 'major_index'
            AND time_period = '{time_period}'
        ORDER BY total_return_pct DESC
        """

    df = md_resource.execute_query(query, read_only=True)
    csv_buffer = io.BytesIO()
    df.write_csv(csv_buffer)
    return csv_buffer.getvalue().decode("utf-8")


def get_global_markets_data(
    md_resource: "BigQueryWarehouseResource",
    cutoff_date: str | None = None,
    time_period: str = "6_months",
) -> str:
    """Fetch global markets and currency ETF data.

    Args:
        md_resource: MotherDuck resource for database queries
        cutoff_date: Optional date for backtesting (YYYY-MM-DD)
        time_period: Time period for returns (e.g., '6_months')

    Returns:
        CSV string of global markets data
    """
    # Global market and currency ETFs
    global_symbols = [
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
    symbols_list = "', '".join(global_symbols)

    if cutoff_date:
        query = f"""
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
            period_start_price,
            period_end_price
        FROM agent_market_performance
        WHERE symbol IN ('{symbols_list}')
            AND market_category = 'major_index'
            AND time_period = '{time_period}'
        ORDER BY total_return_pct DESC
        """

    df = md_resource.execute_query(query, read_only=True)
    csv_buffer = io.BytesIO()
    df.write_csv(csv_buffer)
    return csv_buffer.getvalue().decode("utf-8")
