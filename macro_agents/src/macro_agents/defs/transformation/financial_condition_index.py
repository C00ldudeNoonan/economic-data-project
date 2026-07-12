"""Financial Conditions Index assets (data access + scoring live in fci_data.py / fci_calculations.py)."""

from datetime import datetime

import dagster as dg
import polars as pl

from macro_agents.defs.resources.bigquery_warehouse import BigQueryWarehouseResource
from macro_agents.defs.transformation.fci_calculations import calculate_fci_scores
from macro_agents.defs.transformation.fci_data import (
    fetch_financial_data,
    load_fci_weights,
)


@dg.asset(
    kinds={"bigquery"},
    group_name="transformation",
    deps=[dg.AssetKey(["stg_fred_series"]), dg.AssetKey(["fci_weights_config"])],
    description="Financial Conditions Index calculated from FRED economic indicators using weighted rolling windows",
)
def financial_conditions_index(
    context: dg.AssetExecutionContext,
    bq: BigQueryWarehouseResource,
) -> dg.MaterializeResult:
    """
    Dagster asset that creates the Financial Conditions Index (FCI).

    The FCI combines multiple financial indicators including:
    - High Yield Bond Rates (tripleb)
    - Dow Jones Industrial Average (equity)
    - Federal Funds Rate (FFR)
    - 30-year Mortgage Rate (mortgage)
    - Zillow Housing Index (housing)
    - Dollar Index (dollar)
    - 10-year Treasury Rate (10yr)

    Each component is weighted using a 12-month rolling window with specific weights
    to create a comprehensive measure of financial market conditions.

    This asset depends on:
    - The dbt model 'stg_fred_series' to be materialized first
    - The 'fci_weights_config' asset to provide the weighting scheme

    Returns:
        MaterializeResult with metadata about the FCI calculation
    """
    context.log.info("Starting Financial Conditions Index calculation...")

    try:
        # Load weights from fci_weights_config table
        context.log.info("Loading FCI weights from fci_weights_config table...")
        weights = load_fci_weights(bq)
        context.log.info(f"Loaded weights for {len(weights.get('period', []))} periods")

        context.log.info("Fetching financial data from stg_fred_series...")

        # Fetch and process data
        merged_df = fetch_financial_data(bq)

        if merged_df is None or len(merged_df) == 0:
            raise ValueError(
                "No data available for FCI calculation. Make sure stg_fred_series is materialized."
            )

        context.log.info(f"Successfully fetched data with {len(merged_df)} records")
        context.log.info(
            f"Date range: {merged_df['date'].min()} to {merged_df['date'].max()}"
        )

        # Calculate FCI scores
        context.log.info("Calculating FCI scores with weighted rolling windows...")
        fci_df = calculate_fci_scores(merged_df, weights)

        # Add metadata
        fci_df = fci_df.with_columns(
            [
                pl.lit(datetime.now()).alias("created_at"),
                pl.lit("Financial Conditions Index").alias("index_name"),
            ]
        )

        # Save to database
        context.log.info("Saving FCI data to database...")
        bq.drop_create_duck_db_table("financial_conditions_index", fci_df)

        # Get summary statistics
        latest_fci = fci_df.filter(pl.col("FCI").is_not_null()).tail(1)
        if len(latest_fci) > 0:
            latest_value = latest_fci["FCI"].item()
            latest_date = latest_fci["date"].item()
            context.log.info(f"Latest FCI value: {latest_value:.4f} on {latest_date}")

        # Calculate summary statistics
        fci_stats = fci_df.filter(pl.col("FCI").is_not_null()).select(
            [
                pl.col("FCI").min().alias("min_fci"),
                pl.col("FCI").max().alias("max_fci"),
                pl.col("FCI").mean().alias("avg_fci"),
                pl.col("FCI").std().alias("std_fci"),
                pl.col("FCI").count().alias("total_records"),
            ]
        )

        stats = fci_stats.to_dicts()[0] if len(fci_stats) > 0 else {}

        context.log.info(
            f"FCI Statistics: Min={stats.get('min_fci', 'N/A'):.4f}, "
            f"Max={stats.get('max_fci', 'N/A'):.4f}, "
            f"Avg={stats.get('avg_fci', 'N/A'):.4f}, "
            f"Std={stats.get('std_fci', 'N/A'):.4f}"
        )

        # Convert first 10 rows to JSON-serializable format
        # Convert date/datetime columns to strings before serialization
        first_10_rows = []
        if len(fci_df) > 0:
            sample_df = fci_df.head(10)
            # Convert date and datetime columns to strings
            for col in sample_df.columns:
                if sample_df[col].dtype == pl.Date:
                    sample_df = sample_df.with_columns(
                        pl.col(col).cast(pl.Utf8).alias(col)
                    )
                elif sample_df[col].dtype == pl.Datetime:
                    sample_df = sample_df.with_columns(
                        pl.col(col).dt.strftime("%Y-%m-%d %H:%M:%S").alias(col)
                    )
            first_10_rows = sample_df.to_dicts()

        return dg.MaterializeResult(
            metadata={
                "total_records": len(fci_df),
                "date_range_start": str(merged_df["date"].min()),
                "date_range_end": str(merged_df["date"].max()),
                "latest_fci_value": float(latest_value)
                if len(latest_fci) > 0
                else None,
                "latest_fci_date": str(latest_date) if len(latest_fci) > 0 else None,
                "min_fci": float(stats.get("min_fci", 0)),
                "max_fci": float(stats.get("max_fci", 0)),
                "avg_fci": float(stats.get("avg_fci", 0)),
                "std_fci": float(stats.get("std_fci", 0)),
                "components": [
                    "equity",
                    "tripleb",
                    "mortgage",
                    "housing",
                    "dollar",
                    "10yr",
                    "FFR",
                ],
                "window_size": 12,
                "created_at": datetime.now().isoformat(),
                "first_10_rows": first_10_rows,
            }
        )

    except Exception as e:
        context.log.error(f"Error creating Financial Conditions Index: {str(e)}")
        raise dg.Failure(
            description=f"Failed to create Financial Conditions Index: {str(e)}"
        )


@dg.asset(
    kinds={"bigquery"},
    group_name="transformation",
    description="FCI weights configuration table for reference and analysis",
    tags={"analysis_type": "configuration", "data_type": "weights"},
)
def fci_weights_config(
    context: dg.AssetExecutionContext,
    bq: BigQueryWarehouseResource,
) -> dg.MaterializeResult:
    """
    Dagster asset that creates a weights configuration table for the FCI calculation.

    This table stores the weights used for each component in the FCI calculation,
    making it easy to reference and potentially modify the weighting scheme.

    Returns:
        MaterializeResult with metadata about the weights configuration
    """
    context.log.info("Creating FCI weights configuration table...")

    # Define the weights for FCI calculation
    weights = {
        "period": list(range(4, 17)),
        "FFR": [
            0.099943944,
            0.068578534,
            0.050928985,
            0.030388756,
            0.025687511,
            0.020009094,
            0.015811759,
            0.011351882,
            0.007392853,
            0.003964395,
            0.001711082,
            0.000393424,
            0.000128902,
        ],
        "10yr": [
            -0.008148666,
            -0.014000342,
            -0.018387348,
            -0.021524343,
            -0.023217226,
            -0.024365968,
            -0.025223595,
            -0.025907416,
            -0.026403294,
            -0.02669953,
            -0.020122033,
            -0.013446104,
            -0.006723265,
        ],
        "mortgage": [
            0.217427793,
            0.14524869,
            0.119052555,
            0.077495153,
            0.062434293,
            0.045143184,
            0.03369689,
            0.02483504,
            0.018457125,
            0.013733173,
            0.008657804,
            0.004898748,
            0.002101614,
        ],
        "tripleb": [
            0.079267719,
            0.091179148,
            0.098643896,
            0.100472542,
            0.10064806,
            0.099579542,
            0.097660772,
            0.09535261,
            0.092766486,
            0.090084434,
            0.066538625,
            0.043675043,
            0.021503906,
        ],
        "equity": [
            -0.021318565,
            -0.020215983,
            -0.018436156,
            -0.016157903,
            -0.014443706,
            -0.01302431,
            -0.011752768,
            -0.010660868,
            -0.009703574,
            -0.008865985,
            -0.006342937,
            -0.004042458,
            -0.001937252,
        ],
        "housing": [
            -0.03222844,
            -0.031273991,
            -0.029701866,
            -0.026759405,
            -0.019779925,
            -0.013423982,
            -0.006051851,
            0.000769171,
            0.004236312,
            0.006672419,
            0.007860665,
            0.008861208,
            0.009192217,
        ],
        "dollar": [
            0.048,
            0.048,
            0.045,
            0.039,
            0.031,
            0.023,
            0.017,
            0.012,
            0.008,
            0.005,
            0.002,
            0,
            0,
        ],
    }

    # Create DataFrame
    weights_df = pl.DataFrame(weights)

    # Add metadata columns
    weights_df = weights_df.with_columns(
        [
            pl.lit(datetime.now()).alias("created_at"),
            pl.lit("FCI Weights Configuration").alias("config_name"),
            pl.lit("Financial Conditions Index").alias("index_type"),
        ]
    )

    # Save to database
    bq.drop_create_duck_db_table("fci_weights_config", weights_df)

    context.log.info(
        f"FCI weights configuration saved with {len(weights_df)} weight periods"
    )

    return dg.MaterializeResult(
        metadata={
            "total_periods": len(weights_df),
            "components": list(weights.keys()),
            "period_range": f"{weights['period'][0]} to {weights['period'][-1]}",
            "created_at": datetime.now().isoformat(),
        }
    )
