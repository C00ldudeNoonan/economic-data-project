"""Financial-conditions domain data fetchers."""

from typing import TYPE_CHECKING

from macro_agents.defs.analysis.economy_state.domain_fetchers._common import df_to_csv

if TYPE_CHECKING:
    from macro_agents.defs.resources.bigquery_warehouse import BigQueryWarehouseResource

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


def get_financial_conditions_data(
    md_resource: "BigQueryWarehouseResource",
    cutoff_date: str | None = None,
    max_months: int = 12,
) -> str:
    """Fetch Financial Conditions Index data.

    Args:
        md_resource: BigQuery resource for database queries
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
    return df_to_csv(df)


def get_yield_curve_data(
    md_resource: "BigQueryWarehouseResource",
    cutoff_date: str | None = None,
    max_months: int = 6,
) -> str:
    """Fetch yield curve data with spreads.

    Args:
        md_resource: BigQuery resource for database queries
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
    return df_to_csv(df)


def get_credit_data(
    md_resource: "BigQueryWarehouseResource",
    cutoff_date: str | None = None,
    max_months: int = 3,
) -> str:
    """Fetch credit spread and monetary policy data.

    Args:
        md_resource: BigQuery resource for database queries
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
                SELECT MAX(month) - INTERVAL {max_months - 1} MONTH
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
                SELECT MAX(month) - INTERVAL {max_months - 1} MONTH
                FROM agent_fred_series_latest_aggregates
            )
        ORDER BY series_name, month DESC
        """

    df = md_resource.execute_query(query, read_only=True)
    return df_to_csv(df)
