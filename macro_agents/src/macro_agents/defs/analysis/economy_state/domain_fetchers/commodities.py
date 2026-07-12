"""Commodities domain data fetchers (energy, input, agriculture)."""

from typing import TYPE_CHECKING

from macro_agents.defs.analysis.economy_state.domain_fetchers._common import df_to_csv

if TYPE_CHECKING:
    from macro_agents.defs.resources.bigquery_warehouse import BigQueryWarehouseResource


def _commodity_query(category: str, cutoff_date: str | None, time_period: str) -> str:
    """Build the commodity-performance query for a single category.

    Energy/input/agriculture differ only by ``commodity_category``; snapshot vs
    live table is selected by whether a backtest cutoff_date is supplied.
    """
    if cutoff_date:
        return f"""
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
            AND commodity_category = '{category}'
            AND time_period = '{time_period}'
        ORDER BY total_return_pct DESC
        """
    return f"""
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
        WHERE commodity_category = '{category}'
            AND time_period = '{time_period}'
        ORDER BY total_return_pct DESC
        """


def get_energy_commodities_data(
    md_resource: "BigQueryWarehouseResource",
    cutoff_date: str | None = None,
    time_period: str = "6_months",
) -> str:
    """Fetch energy commodities data.

    Args:
        md_resource: BigQuery resource for database queries
        cutoff_date: Optional date for backtesting (YYYY-MM-DD)
        time_period: Time period for returns (e.g., '6_months')

    Returns:
        CSV string of energy commodities data
    """
    query = _commodity_query("energy", cutoff_date, time_period)
    df = md_resource.execute_query(query, read_only=True)
    return df_to_csv(df)


def get_input_commodities_data(
    md_resource: "BigQueryWarehouseResource",
    cutoff_date: str | None = None,
    time_period: str = "6_months",
) -> str:
    """Fetch industrial/input commodities data.

    Args:
        md_resource: BigQuery resource for database queries
        cutoff_date: Optional date for backtesting (YYYY-MM-DD)
        time_period: Time period for returns (e.g., '6_months')

    Returns:
        CSV string of input commodities data
    """
    query = _commodity_query("input", cutoff_date, time_period)
    df = md_resource.execute_query(query, read_only=True)
    return df_to_csv(df)


def get_agriculture_commodities_data(
    md_resource: "BigQueryWarehouseResource",
    cutoff_date: str | None = None,
    time_period: str = "6_months",
) -> str:
    """Fetch agricultural commodities data.

    Args:
        md_resource: BigQuery resource for database queries
        cutoff_date: Optional date for backtesting (YYYY-MM-DD)
        time_period: Time period for returns (e.g., '6_months')

    Returns:
        CSV string of agricultural commodities data
    """
    query = _commodity_query("agriculture", cutoff_date, time_period)
    df = md_resource.execute_query(query, read_only=True)
    return df_to_csv(df)
