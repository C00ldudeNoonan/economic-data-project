"""Warehouse data access for the Financial Conditions Index."""

import polars as pl

from macro_agents.defs.resources.bigquery_warehouse import BigQueryWarehouseResource


def fetch_financial_data(
    motherduck_resource: BigQueryWarehouseResource,
) -> pl.DataFrame:
    """
    Fetch financial data from dbt models and prepare it for FCI calculation.
    """

    # Fetch FRED series data
    fred_query = """
    SELECT 
        date,
        series_name,
        value
    FROM stg_fred_series 
    WHERE series_name IN (
        'High Yield Bond Rate',
        'Dow Jones Industrial Average', 
        'Federal Funds Effective Rate',
        '30 year Mortgage Rate',
        'Zillow Housing Index',
        'Dollar Index',
        '10 year Treasury Rate'
    )
    AND date >= '2014-04-30'
    ORDER BY date, series_name
    """

    fred_data = motherduck_resource.execute_query(fred_query, read_only=True)

    # Process each series
    processed_data = {}

    # High Yield Bond Rate (tripleb)
    bond_data = fred_data.filter(pl.col("series_name") == "High Yield Bond Rate")
    if len(bond_data) > 0:
        bond_processed = (
            bond_data.with_columns(
                [
                    pl.col("date").cast(pl.Date),
                    pl.when(pl.col("value") == ".")
                    .then(None)
                    .otherwise(pl.col("value"))
                    .cast(pl.Float64)
                    .alias("value"),  # Handle "." values
                ]
            )
            .sort("date")
            .with_columns(
                [
                    pl.col("value")
                    .rolling_mean(window_size=90, min_samples=1)
                    .alias("90_day_avg")
                ]
            )
            .with_columns([pl.col("90_day_avg").shift(3).alias("prev_90_day_avg")])
            .with_columns(
                [(pl.col("90_day_avg") - pl.col("prev_90_day_avg")).alias("tripleb")]
            )
            .select(["date", "tripleb"])
            .filter(pl.col("date") >= pl.date(2014, 4, 30))
        )
        processed_data["tripleb"] = bond_processed

    # Dow Jones Industrial Average (equity)
    dow_data = fred_data.filter(pl.col("series_name") == "Dow Jones Industrial Average")
    if len(dow_data) > 0:
        dow_processed = (
            dow_data.with_columns(
                [
                    pl.col("date").cast(pl.Date),
                    pl.when(pl.col("value") == ".")
                    .then(None)
                    .otherwise(pl.col("value"))
                    .cast(pl.Float64)
                    .alias("value"),  # Handle "." values
                ]
            )
            .sort("date")
            .with_columns([pl.col("value").shift(3).alias("prev_value")])
            .with_columns(
                [((pl.col("value") / pl.col("prev_value")).log() * 100).alias("equity")]
            )
            .select(["date", "equity"])
            .filter(pl.col("date") >= pl.date(2014, 4, 30))
        )
        processed_data["equity"] = dow_processed

    # Federal Funds Rate (FFR)
    ffr_data = fred_data.filter(pl.col("series_name") == "Federal Funds Effective Rate")
    if len(ffr_data) > 0:
        ffr_processed = (
            ffr_data.with_columns(
                [
                    pl.col("date").cast(pl.Date),
                    pl.when(pl.col("value") == ".")
                    .then(None)
                    .otherwise(pl.col("value"))
                    .cast(pl.Float64)
                    .alias("value"),  # Handle "." values
                ]
            )
            .sort("date")
            .with_columns(
                [
                    pl.col("value")
                    .rolling_mean(window_size=90, min_samples=1)
                    .alias("90_day_avg")
                ]
            )
            .with_columns([pl.col("90_day_avg").shift(3).alias("prev_90_day_avg")])
            .with_columns(
                [(pl.col("90_day_avg") - pl.col("prev_90_day_avg")).alias("FFR")]
            )
            .select(["date", "FFR"])
            .filter(pl.col("date") >= pl.date(2014, 4, 30))
        )
        processed_data["FFR"] = ffr_processed

    # 30 year Mortgage Rate
    mortgage_data = fred_data.filter(pl.col("series_name") == "30 year Mortgage Rate")
    if len(mortgage_data) > 0:
        mortgage_processed = (
            mortgage_data.with_columns(
                [
                    pl.col("date").cast(pl.Date),
                    pl.when(pl.col("value") == ".")
                    .then(None)
                    .otherwise(pl.col("value"))
                    .cast(pl.Float64)
                    .alias("value"),  # Handle "." values
                ]
            )
            .sort("date")
            .with_columns(
                [
                    pl.col("value")
                    .rolling_mean(window_size=90, min_samples=1)
                    .alias("90_day_avg")
                ]
            )
            .with_columns([pl.col("90_day_avg").shift(1).alias("prev_90_day_avg")])
            .with_columns(
                [(pl.col("90_day_avg") - pl.col("prev_90_day_avg")).alias("mortgage")]
            )
            .select(["date", "mortgage"])
            .filter(pl.col("date") >= pl.date(2014, 4, 30))
        )
        processed_data["mortgage"] = mortgage_processed

    # Zillow Housing Index (housing)
    housing_data = fred_data.filter(pl.col("series_name") == "Zillow Housing Index")
    if len(housing_data) > 0:
        housing_processed = (
            housing_data.with_columns(
                [
                    pl.col("date").cast(pl.Date),
                    pl.when(pl.col("value") == ".")
                    .then(None)
                    .otherwise(pl.col("value"))
                    .cast(pl.Float64)
                    .alias("value"),  # Handle "." values
                ]
            )
            .sort("date")
            .with_columns([pl.col("value").shift(3).alias("prev_value")])
            .with_columns(
                [
                    ((pl.col("value") / pl.col("prev_value")).log() * 100).alias(
                        "housing"
                    )
                ]
            )
            .select(["date", "housing"])
            .filter(pl.col("date") >= pl.date(2014, 4, 30))
        )
        processed_data["housing"] = housing_processed

    # Dollar Index
    dollar_data = fred_data.filter(pl.col("series_name") == "Dollar Index")
    if len(dollar_data) > 0:
        dollar_processed = (
            dollar_data.with_columns(
                [
                    pl.col("date").cast(pl.Date),
                    pl.when(pl.col("value") == ".")
                    .then(None)
                    .otherwise(pl.col("value"))
                    .cast(pl.Float64)
                    .alias("value"),  # Handle "." values
                ]
            )
            .sort("date")
            .with_columns(
                [
                    pl.col("value")
                    .rolling_mean(window_size=90, min_samples=1)
                    .alias("90_day_avg")
                ]
            )
            .with_columns([pl.col("90_day_avg").shift(3).alias("prev_90_day_avg")])
            .with_columns(
                [
                    ((pl.col("90_day_avg") / pl.col("prev_90_day_avg")).log()).alias(
                        "dollar"
                    )
                ]
            )
            .with_columns([pl.col("dollar").fill_null(0)])
            .select(["date", "dollar"])
            .filter(pl.col("date") >= pl.date(2014, 4, 30))
        )
        processed_data["dollar"] = dollar_processed

    # 10 year Treasury Rate
    treasury_data = fred_data.filter(pl.col("series_name") == "10 year Treasury Rate")
    if len(treasury_data) > 0:
        treasury_processed = (
            treasury_data.with_columns(
                [
                    pl.col("date").cast(pl.Date),
                    pl.when(pl.col("value") == ".")
                    .then(None)
                    .otherwise(pl.col("value"))
                    .cast(pl.Float64)
                    .alias("value"),  # Handle "." values
                ]
            )
            .sort("date")
            .with_columns(
                [
                    pl.col("value")
                    .rolling_mean(window_size=90, min_samples=1)
                    .alias("90_day_avg")
                ]
            )
            .with_columns([pl.col("90_day_avg").shift(3).alias("prev_90_day_avg")])
            .with_columns(
                [(pl.col("90_day_avg") - pl.col("prev_90_day_avg")).alias("10yr")]
            )
            .select(["date", "10yr"])
            .filter(pl.col("date") >= pl.date(2014, 4, 30))
        )
        processed_data["10yr"] = treasury_processed

    # Merge all data
    merged_df = None
    for series_name, data in processed_data.items():
        if merged_df is None:
            merged_df = data
        else:
            merged_df = merged_df.join(data, on="date", how="inner")

    if merged_df is None:
        return pl.DataFrame()
    return merged_df


def load_fci_weights(
    motherduck_resource: BigQueryWarehouseResource,
) -> dict[str, list[float]]:
    """
    Load FCI weights from the fci_weights_config table.

    Returns:
        Dictionary with component names as keys and weight lists as values
    """
    query = """
    SELECT 
        period,
        FFR,
        "10yr",
        mortgage,
        tripleb,
        equity,
        housing,
        dollar
    FROM fci_weights_config
    ORDER BY period
    """

    weights_df = motherduck_resource.execute_query(query, read_only=True)

    if len(weights_df) == 0:
        raise ValueError(
            "No weights found in fci_weights_config table. Make sure fci_weights_config asset is materialized."
        )

    # Convert DataFrame to dictionary format expected by calculate_fci_scores
    weights_dict = {
        "period": weights_df["period"].to_list(),
        "FFR": weights_df["FFR"].to_list(),
        "10yr": weights_df["10yr"].to_list(),
        "mortgage": weights_df["mortgage"].to_list(),
        "tripleb": weights_df["tripleb"].to_list(),
        "equity": weights_df["equity"].to_list(),
        "housing": weights_df["housing"].to_list(),
        "dollar": weights_df["dollar"].to_list(),
    }

    return weights_dict
