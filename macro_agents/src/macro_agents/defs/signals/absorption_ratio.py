"""Absorption Ratio — systemic risk measure via PCA.

Academic basis: Kritzman, Li, Page, Rigobon (2011, Journal of Portfolio Management)

The absorption ratio measures the fraction of total variance of a set of asset
returns explained by a fixed number of eigenvectors. Higher AR = markets more
tightly coupled = more fragile. The 15-day shift (ΔAR) rising > 1 std above
its mean signals elevated fragility.

Historical range for U.S. equities: 0.75–0.85
"""

import dagster as dg
import numpy as np
import polars as pl

from macro_agents.defs.resources.motherduck import MotherDuckResource


SIGNALS_GROUP = "computed_signals"


@dg.asset(
    group_name=SIGNALS_GROUP,
    kinds={"python", "duckdb"},
    deps=[dg.AssetKey(["sp500_companies_prices_raw"])],
    description="Absorption ratio systemic risk signal from S&P 500 stock returns PCA",
)
def absorption_ratio_signals(
    context: dg.AssetExecutionContext,
    md: MotherDuckResource,
) -> dg.MaterializeResult:
    context.log.info("Fetching S&P 500 daily prices...")
    prices_df = md.execute_query(
        """
        SELECT date, symbol, adj_close
        FROM sp500_companies_prices_raw
        WHERE adj_close IS NOT NULL
          AND date >= CURRENT_DATE - INTERVAL '3 years'
        ORDER BY date, symbol
        """,
        read_only=True,
    )

    if prices_df.is_empty():
        context.log.warning("No S&P 500 price data found")
        return dg.MaterializeResult(metadata={"rows": 0, "status": "no_data"})

    context.log.info(f"Got {len(prices_df)} price rows, pivoting to wide format...")
    prices_wide = prices_df.pivot(on="symbol", index="date", values="adj_close").sort(
        "date"
    )

    dates = prices_wide["date"].to_list()
    price_matrix = prices_wide.drop("date").to_numpy()

    # Compute daily returns
    returns = np.diff(price_matrix, axis=0) / price_matrix[:-1]
    return_dates = dates[1:]

    # Filter to stocks with sufficient data (>80% non-NaN in any 500-day window)
    window_size = 500
    n_components_frac = 0.2  # Top 20% of eigenvalues

    results = []
    context.log.info(f"Computing rolling {window_size}-day absorption ratio...")

    for i in range(window_size, len(returns)):
        window = returns[i - window_size : i]

        # Keep columns with <20% missing
        valid_mask = np.isnan(window).mean(axis=0) < 0.2
        if valid_mask.sum() < 20:
            continue

        window_clean = window[:, valid_mask]
        # Forward-fill NaN within columns
        for col in range(window_clean.shape[1]):
            col_data = window_clean[:, col]
            mask = np.isnan(col_data)
            if mask.any() and not mask.all():
                idx = np.where(~mask, np.arange(len(col_data)), 0)
                np.maximum.accumulate(idx, out=idx)
                window_clean[:, col] = col_data[idx]

        # Remove any remaining NaN rows
        row_mask = ~np.isnan(window_clean).any(axis=1)
        if row_mask.sum() < 100:
            continue
        window_clean = window_clean[row_mask]

        # Correlation matrix eigenvalue decomposition
        try:
            corr = np.corrcoef(window_clean.T)
            eigenvalues = np.linalg.eigvalsh(corr)
            eigenvalues = eigenvalues[eigenvalues > 0]  # Keep positive only

            n_components = max(1, int(len(eigenvalues) * n_components_frac))
            ar = float(eigenvalues[-n_components:].sum() / eigenvalues.sum())

            results.append(
                {
                    "date": return_dates[i - 1],
                    "absorption_ratio": round(ar, 6),
                    "n_stocks": int(valid_mask.sum()),
                    "n_components": n_components,
                }
            )
        except np.linalg.LinAlgError:
            continue

    if not results:
        context.log.warning("Could not compute absorption ratio")
        return dg.MaterializeResult(metadata={"rows": 0, "status": "insufficient_data"})

    df = pl.DataFrame(results)

    # Add derived metrics
    df = df.with_columns(
        [
            # 1-year rolling average
            pl.col("absorption_ratio")
            .rolling_mean(window_size=252, min_samples=60)
            .alias("ar_1y_avg"),
            # 1-year rolling std
            pl.col("absorption_ratio")
            .rolling_std(window_size=252, min_samples=60)
            .alias("ar_1y_std"),
            # 15-day shift (ΔAR)
            pl.col("absorption_ratio")
            .rolling_mean(window_size=15, min_samples=5)
            .alias("ar_15d_avg"),
        ]
    )

    df = df.with_columns(
        [
            # Z-score
            ((pl.col("absorption_ratio") - pl.col("ar_1y_avg")) / pl.col("ar_1y_std"))
            .round(2)
            .alias("ar_zscore"),
            # ΔAR = 15-day AR / 1-year AR
            (pl.col("ar_15d_avg") / pl.col("ar_1y_avg")).round(4).alias("ar_delta"),
        ]
    )

    # Status classification
    df = df.with_columns(
        pl.when(pl.col("ar_zscore") > 2.0)
        .then(pl.lit("high"))
        .when(pl.col("ar_zscore") > 1.0)
        .then(pl.lit("medium"))
        .when(pl.col("ar_zscore") > 0.5)
        .then(pl.lit("low"))
        .otherwise(pl.lit("normal"))
        .alias("ar_status")
    )

    # Round final columns
    df = df.with_columns(
        [
            pl.col("absorption_ratio").round(4),
            pl.col("ar_1y_avg").round(4),
            pl.col("ar_15d_avg").round(4),
        ]
    ).drop("ar_1y_std")

    context.log.info(f"Writing {len(df)} absorption ratio rows to MotherDuck")
    md.upsert_data("absorption_ratio_signals", df, ["date"], context=context)

    return dg.MaterializeResult(
        metadata={
            "rows": len(df),
            "latest_ar": float(df["absorption_ratio"][-1]),
            "latest_status": str(df["ar_status"][-1]),
            "date_range": f"{df['date'][0]} to {df['date'][-1]}",
        }
    )
