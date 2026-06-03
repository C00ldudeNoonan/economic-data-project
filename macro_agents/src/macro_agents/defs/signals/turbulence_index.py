"""Turbulence Index — multi-asset regime detection via Mahalanobis distance.

Academic basis: Kritzman and Li (2010, Financial Analysts Journal)

Uses Mahalanobis distance to measure how statistically unusual current
multi-asset returns are. Under multivariate normality, d_t × n follows
chi-squared. Turbulence is highly persistent — once it begins, it continues
for weeks. d_t > 75th percentile of historical distribution = turbulent regime.

Captures correlation breakdowns that pure volatility measures miss.
"""

import dagster as dg
import numpy as np
import polars as pl
from scipy.spatial.distance import mahalanobis

from macro_agents.defs.resources.bigquery_warehouse import BigQueryWarehouseResource


SIGNALS_GROUP = "computed_signals"

# Multi-asset universe for turbulence computation
ASSET_QUERIES = {
    "SPY": "SELECT date, adj_close FROM stg_major_indices WHERE symbol = 'SPY' AND adj_close IS NOT NULL",
    "QQQ": "SELECT date, adj_close FROM stg_major_indices WHERE symbol = 'QQQ' AND adj_close IS NOT NULL",
    "IWM": "SELECT date, adj_close FROM stg_major_indices WHERE symbol = 'IWM' AND adj_close IS NOT NULL",
    "GOVT": "SELECT date, adj_close FROM stg_fixed_income WHERE symbol = 'GOVT' AND adj_close IS NOT NULL",
    "HYG": "SELECT date, adj_close FROM stg_fixed_income WHERE symbol = 'HYG' AND adj_close IS NOT NULL",
    "GLD": "SELECT date, adj_close FROM stg_input_commodities WHERE symbol = 'GLD' AND adj_close IS NOT NULL",
}


@dg.asset(
    group_name=SIGNALS_GROUP,
    kinds={"python", "duckdb"},
    deps=[
        dg.AssetKey(["stg_major_indices"]),
        dg.AssetKey(["stg_fixed_income"]),
        dg.AssetKey(["stg_input_commodities"]),
    ],
    description="Turbulence index from Mahalanobis distance of multi-asset returns",
)
def turbulence_index_signals(
    context: dg.AssetExecutionContext,
    md: BigQueryWarehouseResource,
) -> dg.MaterializeResult:
    context.log.info("Fetching multi-asset prices...")
    asset_frames: dict[str, pl.DataFrame] = {}
    for name, query in ASSET_QUERIES.items():
        try:
            df = md.execute_query(query, read_only=True)
            if not df.is_empty():
                asset_frames[name] = df.rename({"adj_close": name})
        except Exception as e:
            context.log.warning(f"Could not fetch {name}: {e}")

    if len(asset_frames) < 4:
        context.log.warning(
            f"Only {len(asset_frames)} assets available, need at least 4"
        )
        return dg.MaterializeResult(
            metadata={"rows": 0, "status": "insufficient_assets"}
        )

    # Join all assets on date
    asset_names = list(asset_frames.keys())
    combined = asset_frames[asset_names[0]]
    for name in asset_names[1:]:
        combined = combined.join(
            asset_frames[name].select(["date", name]), on="date", how="inner"
        )

    combined = combined.sort("date")

    context.log.info(
        f"Computing turbulence for {len(asset_names)} assets over {len(combined)} days"
    )

    # Compute daily returns
    prices = combined.select(asset_names).to_numpy()
    returns = np.diff(prices, axis=0) / prices[:-1]
    return_dates = combined["date"].to_list()[1:]

    # Rolling 252-day window for Mahalanobis distance
    window_size = 252
    results = []

    for i in range(window_size, len(returns)):
        window = returns[i - window_size : i]

        # Check for NaN
        if np.isnan(window).any():
            continue

        mu = window.mean(axis=0)
        cov = np.cov(window.T)

        try:
            cov_inv = np.linalg.pinv(cov)
            current_return = returns[i]
            if np.isnan(current_return).any():
                continue
            turb = float(mahalanobis(current_return, mu, cov_inv))
        except (np.linalg.LinAlgError, ValueError):
            continue

        # Date is return_dates[i] because the signal measures how unusual
        # returns[i] (observed at end of this day) is vs the historical window.
        results.append(
            {
                "date": return_dates[i],
                "turbulence_raw": round(turb, 4),
            }
        )

    if not results:
        return dg.MaterializeResult(
            metadata={"rows": 0, "status": "computation_failed"}
        )

    df = pl.DataFrame(results)

    # Expanding-window percentile rank (no look-ahead bias)
    turb_values = df["turbulence_raw"].to_numpy()
    pct_rank = np.full(len(turb_values), np.nan)
    for i in range(1, len(turb_values)):
        history = turb_values[: i + 1]
        pct_rank[i] = float((history < turb_values[i]).sum()) / i
    df = df.with_columns(pl.Series("turbulence_percentile", np.round(pct_rank, 4)))

    # Add rolling statistics
    df = df.with_columns(
        [
            # 20-day average
            pl.col("turbulence_raw")
            .rolling_mean(window_size=20, min_samples=5)
            .round(4)
            .alias("turbulence_20d_avg"),
            # Rolling z-score (252-day)
            (
                (
                    pl.col("turbulence_raw")
                    - pl.col("turbulence_raw").rolling_mean(
                        window_size=252, min_samples=60
                    )
                )
                / pl.col("turbulence_raw").rolling_std(window_size=252, min_samples=60)
            )
            .round(2)
            .alias("turbulence_zscore"),
        ]
    )

    # Regime classification
    df = df.with_columns(
        pl.when(pl.col("turbulence_percentile") > 0.90)
        .then(pl.lit("extreme"))
        .when(pl.col("turbulence_percentile") > 0.75)
        .then(pl.lit("turbulent"))
        .when(pl.col("turbulence_percentile") > 0.50)
        .then(pl.lit("normal"))
        .otherwise(pl.lit("calm"))
        .alias("turbulence_regime")
    )

    # Signal status
    df = df.with_columns(
        pl.when(pl.col("turbulence_percentile") > 0.90)
        .then(pl.lit("high"))
        .when(pl.col("turbulence_percentile") > 0.75)
        .then(pl.lit("medium"))
        .when(pl.col("turbulence_percentile") > 0.60)
        .then(pl.lit("low"))
        .otherwise(pl.lit("normal"))
        .alias("turbulence_status")
    )

    context.log.info(f"Writing {len(df)} turbulence index rows to MotherDuck")
    md.upsert_data("turbulence_index_signals", df, ["date"], context=context)

    return dg.MaterializeResult(
        metadata={
            "rows": len(df),
            "assets_used": asset_names,
            "latest_turbulence": float(df["turbulence_raw"][-1]),
            "latest_regime": str(df["turbulence_regime"][-1]),
            "date_range": f"{df['date'][0]} to {df['date'][-1]}",
        }
    )
