"""Labor-market domain data fetchers."""

from typing import TYPE_CHECKING

from macro_agents.defs.analysis.economy_state.domain_fetchers._common import df_to_csv

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


def get_labor_market_data(
    md_resource: "BigQueryWarehouseResource",
    cutoff_date: str | None = None,
    max_months: int = 3,
) -> str:
    """Fetch labor market specific data from FRED series.

    Args:
        md_resource: BigQuery resource for database queries
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
            pct_change_1y,
            date_grain
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


def get_labor_trends_data(
    md_resource: "BigQueryWarehouseResource",
    cutoff_date: str | None = None,
    max_months: int = 6,
) -> str:
    """Fetch month-over-month employment trends.

    Args:
        md_resource: BigQuery resource for database queries
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
    return df_to_csv(df)
