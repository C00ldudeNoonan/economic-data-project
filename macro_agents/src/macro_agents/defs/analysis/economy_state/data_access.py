import io

from macro_agents.defs.resources.bigquery_warehouse import BigQueryWarehouseResource


def get_economic_data(
    md_resource: BigQueryWarehouseResource,
    cutoff_date: str | None = None,
    max_series: int | None = None,
    latest_month_only: bool = False,
    max_months_per_series: int | None = 3,
) -> str:
    """Get latest economic data from FRED series."""
    months_per_series = max_months_per_series or 3
    if cutoff_date:
        if latest_month_only:
            month_filter = f"AND month = (SELECT MAX(month) FROM agent_fred_series_latest_aggregates_snapshot WHERE snapshot_date = '{cutoff_date}')"
            limit_clause = f"LIMIT {max_series}" if max_series else ""
        else:
            month_filter = f"AND month >= (SELECT MAX(month) - INTERVAL '{months_per_series - 1} months' FROM agent_fred_series_latest_aggregates_snapshot WHERE snapshot_date = '{cutoff_date}')"
            limit_clause = (
                f"LIMIT {max_series * months_per_series}" if max_series else ""
            )
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
            AND current_value IS NOT NULL
            {month_filter}
        ORDER BY series_name, month DESC
        {limit_clause}
        """
    else:
        if latest_month_only:
            month_filter = "AND month = (SELECT MAX(month) FROM agent_fred_series_latest_aggregates)"
            limit_clause = f"LIMIT {max_series}" if max_series else ""
        else:
            month_filter = f"AND month >= (SELECT MAX(month) - INTERVAL '{months_per_series - 1} months' FROM agent_fred_series_latest_aggregates)"
            limit_clause = (
                f"LIMIT {max_series * months_per_series}" if max_series else ""
            )
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
        WHERE current_value IS NOT NULL
            {month_filter}
        ORDER BY series_name, month DESC
        {limit_clause}
        """

    df = md_resource.execute_query(query)
    csv_buffer = io.BytesIO()
    df.write_csv(csv_buffer)
    return csv_buffer.getvalue().decode("utf-8")


def get_market_data(
    md_resource: BigQueryWarehouseResource,
    cutoff_date: str | None = None,
    max_assets: int | None = 20,
    time_periods: list[str] | None = None,
) -> str:
    """Get latest market performance data including US sectors and major indices."""
    if time_periods is None:
        time_periods = ["6_months"]

    periods_str = "', '".join(time_periods)
    limit_clause = f"LIMIT {max_assets}" if max_assets else ""

    if cutoff_date:
        query = f"""
        SELECT 
            symbol,
            asset_type,
            time_period,
            exchange,
            name,
            period_start_date,
            period_end_date,
            trading_days,
            total_return_pct,
            avg_daily_return_pct,
            volatility_pct,
            win_rate_pct,
            total_price_change,
            avg_daily_price_change,
            worst_day_change,
            best_day_change,
            positive_days,
            negative_days,
            neutral_days,
            period_start_price,
            period_end_price,
            market_category
        FROM agent_market_performance_snapshot
        WHERE time_period IN ('{periods_str}')
            AND (
                (market_category = 'sector' AND snapshot_date = '{cutoff_date}')
                OR (market_category = 'major_index' AND period_end_date <= '{cutoff_date}')
            )
        ORDER BY market_category, asset_type, time_period, total_return_pct DESC
        {limit_clause}
        """
    else:
        query = f"""
        SELECT 
            symbol,
            asset_type,
            time_period,
            exchange,
            name,
            period_start_date,
            period_end_date,
            trading_days,
            total_return_pct,
            avg_daily_return_pct,
            volatility_pct,
            win_rate_pct,
            total_price_change,
            avg_daily_price_change,
            worst_day_change,
            best_day_change,
            positive_days,
            negative_days,
            neutral_days,
            period_start_price,
            period_end_price,
            market_category
        FROM agent_market_performance
        WHERE time_period IN ('{periods_str}')
        ORDER BY market_category, asset_type, time_period, total_return_pct DESC
        {limit_clause}
        """

    df = md_resource.execute_query(query)
    csv_buffer = io.BytesIO()
    df.write_csv(csv_buffer)
    return csv_buffer.getvalue().decode("utf-8")


def get_financial_conditions_index(
    md_resource: BigQueryWarehouseResource,
    cutoff_date: str | None = None,
    max_months: int | None = 12,
) -> str:
    """Get Financial Conditions Index data."""
    limit_clause = f"LIMIT {max_months}" if max_months else ""
    if cutoff_date:
        query = f"""
        SELECT 
            date,
            FCI,
            equity_score,
            housing_score,
            treasury_10yr_score
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
            treasury_10yr_score
        FROM agent_financial_conditions_index
        ORDER BY date DESC
        {limit_clause}
        """

    df = md_resource.execute_query(query, read_only=True)
    csv_buffer = io.BytesIO()
    df.write_csv(csv_buffer)
    return csv_buffer.getvalue().decode("utf-8")


def get_commodity_data(
    md_resource: BigQueryWarehouseResource,
    cutoff_date: str | None = None,
    max_commodities: int | None = 15,
    time_periods: list[str] | None = None,
) -> str:
    """Get latest commodity performance data from all commodity summary tables."""
    if time_periods is None:
        time_periods = ["6_months"]

    periods_str = "', '".join(time_periods)
    limit_clause = f"LIMIT {max_commodities}" if max_commodities else ""

    if cutoff_date:
        query = f"""
        SELECT 
            commodity_name,
            commodity_unit,
            commodity_category,
            time_period,
            period_start_date,
            period_end_date,
            trading_days,
            total_return_pct,
            avg_daily_return_pct,
            volatility_pct,
            win_rate_pct,
            total_price_change,
            avg_daily_price_change,
            worst_day_change,
            best_day_change,
            positive_days,
            negative_days,
            neutral_days,
            period_start_price,
            period_end_price
        FROM agent_commodity_performance_snapshot
        WHERE snapshot_date = '{cutoff_date}'
            AND time_period IN ('{periods_str}')
        ORDER BY commodity_category, commodity_name, time_period, total_return_pct DESC
        {limit_clause}
        """
    else:
        query = f"""
        SELECT 
            commodity_name,
            commodity_unit,
            commodity_category,
            time_period,
            period_start_date,
            period_end_date,
            trading_days,
            total_return_pct,
            avg_daily_return_pct,
            volatility_pct,
            win_rate_pct,
            total_price_change,
            avg_daily_price_change,
            worst_day_change,
            best_day_change,
            positive_days,
            negative_days,
            neutral_days,
            period_start_price,
            period_end_price
        FROM agent_commodity_performance
        WHERE time_period IN ('{periods_str}')
        ORDER BY commodity_category, commodity_name, time_period, total_return_pct DESC
        {limit_clause}
        """

    df = md_resource.execute_query(query)
    csv_buffer = io.BytesIO()
    df.write_csv(csv_buffer)
    return csv_buffer.getvalue().decode("utf-8")


def get_correlation_data(
    md_resource: BigQueryWarehouseResource,
    sample_size: int = 50,
    sampling_strategy: str = "top_correlations",
    cutoff_date: str | None = None,
) -> str:
    """Get correlation data between economic indicators and asset returns."""
    snapshot_table = "agent_leading_econ_return_indicator_snapshot"

    if cutoff_date:
        if md_resource.table_exists(snapshot_table):
            table_name = snapshot_table
        else:
            table_name = "agent_leading_econ_return_indicator"
    else:
        table_name = "agent_leading_econ_return_indicator"

    if cutoff_date:
        if hasattr(md_resource, "query_sampled_data"):
            try:
                correlation_data = md_resource.query_sampled_data(
                    table_name=table_name,
                    filters={"snapshot_date": cutoff_date}
                    if table_name == snapshot_table
                    else {},
                    sample_size=sample_size,
                    sampling_strategy=sampling_strategy,
                )
                return correlation_data
            except Exception:
                return ""
        else:
            try:
                if table_name == snapshot_table:
                    query = f"""
                    SELECT *
                    FROM {table_name}
                    WHERE snapshot_date = '{cutoff_date}'
                    ORDER BY GREATEST(
                        ABS(COALESCE(correlation_econ_vs_q1_returns, 0)),
                        ABS(COALESCE(correlation_econ_vs_q2_returns, 0)),
                        ABS(COALESCE(correlation_econ_vs_q3_returns, 0))
                    ) DESC
                    LIMIT {sample_size}
                    """
                else:
                    query = f"""
                    SELECT *
                    FROM {table_name}
                    ORDER BY GREATEST(
                        ABS(COALESCE(correlation_econ_vs_q1_returns, 0)),
                        ABS(COALESCE(correlation_econ_vs_q2_returns, 0)),
                        ABS(COALESCE(correlation_econ_vs_q3_returns, 0))
                    ) DESC
                    LIMIT {sample_size}
                    """
                df = md_resource.execute_query(query)
                csv_buffer = io.StringIO()
                df.write_csv(csv_buffer)
                return csv_buffer.getvalue()
            except Exception:
                return ""
    else:
        if hasattr(md_resource, "query_sampled_data"):
            correlation_data = md_resource.query_sampled_data(
                table_name=table_name,
                filters={},
                sample_size=sample_size,
                sampling_strategy=sampling_strategy,
            )
            return correlation_data
        else:
            query = f"""
            SELECT *
            FROM {table_name}
            ORDER BY GREATEST(
                ABS(COALESCE(correlation_econ_vs_q1_returns, 0)),
                ABS(COALESCE(correlation_econ_vs_q2_returns, 0)),
                ABS(COALESCE(correlation_econ_vs_q3_returns, 0))
            ) DESC
            LIMIT {sample_size}
            """
            df = md_resource.execute_query(query)
            csv_buffer = io.StringIO()
            df.write_csv(csv_buffer)
            return csv_buffer.getvalue()


def get_housing_data(
    md_resource: BigQueryWarehouseResource,
    cutoff_date: str | None = None,
    latest_month_only: bool = False,
    max_months: int | None = 6,
) -> str:
    """Get housing market data including inventory and mortgage rates."""
    months_limit = max_months or 6
    if cutoff_date:
        if latest_month_only:
            month_filter = f"AND month = (SELECT MAX(month) FROM agent_housing_inventory_latest_aggregates WHERE month <= '{cutoff_date}')"
        else:
            month_filter = f"AND month >= (SELECT MAX(month) - INTERVAL '{months_limit - 1} months' FROM agent_housing_inventory_latest_aggregates WHERE month <= '{cutoff_date}')"
    else:
        if latest_month_only:
            month_filter = "AND month = (SELECT MAX(month) FROM agent_housing_inventory_latest_aggregates)"
        else:
            month_filter = f"AND month >= (SELECT MAX(month) - INTERVAL '{months_limit - 1} months' FROM agent_housing_inventory_latest_aggregates)"

    inventory_query = f"""
    SELECT 
        series_code,
        series_name,
        month,
        current_value,
        pct_change_3m,
        pct_change_6m,
        pct_change_1y,
        date_grain
    FROM agent_housing_inventory_latest_aggregates
    WHERE current_value IS NOT NULL
        {month_filter}
    ORDER BY series_name, month DESC
    """

    mortgage_query = """
    SELECT 
        date,
        mortgage_rate,
        median_price_no_down_payment,
        median_price_20_pct_down_payment,
        monthly_payment_no_down_payment,
        monthly_payment_20_pct_down_payment
    FROM agent_housing_mortgage_rates
    WHERE date = (SELECT MAX(date) FROM agent_housing_mortgage_rates)
    """

    if cutoff_date:
        inventory_query = f"""
        SELECT 
            series_code,
            series_name,
            month,
            current_value,
            pct_change_3m,
            pct_change_6m,
            pct_change_1y,
            date_grain
        FROM agent_housing_inventory_latest_aggregates
        WHERE month <= '{cutoff_date}'
            AND current_value IS NOT NULL
        ORDER BY series_name, month DESC
        """
        mortgage_query = f"""
        SELECT 
            date,
            mortgage_rate,
            median_price_no_down_payment,
            median_price_20_pct_down_payment,
            monthly_payment_no_down_payment,
            monthly_payment_20_pct_down_payment
        FROM agent_housing_mortgage_rates
        WHERE date <= '{cutoff_date}'
            AND date = (
                SELECT MAX(date) 
                FROM agent_housing_mortgage_rates 
                WHERE date <= '{cutoff_date}'
            )
        """

    inventory_df = md_resource.execute_query(inventory_query, read_only=True)
    mortgage_df = md_resource.execute_query(mortgage_query, read_only=True)

    inventory_csv = ""
    if not inventory_df.is_empty():
        csv_buffer = io.BytesIO()
        inventory_df.write_csv(csv_buffer)
        inventory_csv = csv_buffer.getvalue().decode("utf-8")

    mortgage_csv = ""
    if not mortgage_df.is_empty():
        csv_buffer = io.BytesIO()
        mortgage_df.write_csv(csv_buffer)
        mortgage_csv = csv_buffer.getvalue().decode("utf-8")

    if inventory_csv and mortgage_csv:
        return f"Housing Inventory Data:\n{inventory_csv}\n\nMortgage Rates Data:\n{mortgage_csv}"
    if inventory_csv:
        return f"Housing Inventory Data:\n{inventory_csv}"
    if mortgage_csv:
        return f"Mortgage Rates Data:\n{mortgage_csv}"
    return ""


def get_yield_curve_data(
    md_resource: BigQueryWarehouseResource,
    cutoff_date: str | None = None,
    max_months: int | None = 12,
) -> str:
    """Get yield curve data and calculate spreads."""
    date_filter = f"AND date <= '{cutoff_date}'" if cutoff_date else ""
    limit_clause = f"LIMIT {max_months}" if max_months else ""

    query = f"""
    SELECT 
        date,
        yield_1m,
        yield_3m,
        yield_6m,
        yield_1y,
        yield_2y,
        yield_3y,
        yield_5y,
        yield_7y,
        yield_10y,
        yield_20y,
        yield_30y,
        spread_10y_2y,
        spread_10y_3m,
        spread_2y_3m,
        spread_30y_2y,
        curve_shape,
        inversion_status
    FROM agent_treasury_yield_curve_spreads
    WHERE date = (
        SELECT MAX(date)
        FROM agent_treasury_yield_curve_spreads
        WHERE date IS NOT NULL {date_filter}
    )
    ORDER BY date DESC
    {limit_clause}
    """

    df = md_resource.execute_query(query, read_only=True)
    if df.is_empty():
        return ""
    csv_buffer = io.BytesIO()
    df.write_csv(csv_buffer)
    return csv_buffer.getvalue().decode("utf-8")


def get_economic_trends(
    md_resource: BigQueryWarehouseResource,
    cutoff_date: str | None = None,
    max_months: int | None = 12,
) -> str:
    """Get month-over-month changes for key economic indicators using fred_monthly_diff."""
    key_indicators = [
        "GDPC1",
        "CPIAUCSL",
        "UNRATE",
        "PAYEMS",
        "FEDFUNDS",
        "INDPRO",
        "RSXFS",
        "HOUST",
    ]

    indicators_list = "', '".join(key_indicators)
    date_filter = f"AND date <= '{cutoff_date}'" if cutoff_date else ""
    limit_clause = (
        f"LIMIT {max_months * len(key_indicators)}" if max_months else "LIMIT 100"
    )

    query = f"""
    SELECT 
        series_code,
        series_name,
        date,
        value,
        period_diff,
        data_source
    FROM agent_fred_monthly_diff
    WHERE series_code IN ('{indicators_list}')
        {date_filter}
    ORDER BY series_name, date DESC
    {limit_clause}
    """

    df = md_resource.execute_query(query, read_only=True)
    if df.is_empty():
        return ""
    csv_buffer = io.BytesIO()
    df.write_csv(csv_buffer)
    return csv_buffer.getvalue().decode("utf-8")
