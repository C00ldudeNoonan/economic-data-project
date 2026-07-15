"""Entropy-Based Complexity Measures — permutation entropy of market returns.

Academic basis: Zunino et al. (2010)

Permutation entropy measures the complexity/randomness of a time series.
Research shows that permutation entropy of stock indices decreased steadily
before major financial crises — markets became more "ordered" (herding)
before crashes.

Low entropy = more predictable (inefficient/herding)
High entropy = more random (efficient)
Declining trend = potential fragility signal
"""

import math
from collections import Counter

import dagster as dg
import numpy as np
import polars as pl

from macro_agents.defs.resources.bigquery_warehouse import (
    BigQueryWarehouseResource,
    default_dataset_for_schema,
)


SIGNALS_GROUP = "computed_signals"


def permutation_entropy(series: np.ndarray, order: int = 3, delay: int = 1) -> float:
    """Compute permutation entropy of a time series.

    Args:
        series: 1D array of values
        order: Embedding dimension (permutation order). Default 3.
        delay: Time delay between elements. Default 1.

    Returns:
        Normalized permutation entropy in [0, 1].
    """
    n = len(series)
    if n < delay * (order - 1) + 1:
        return float("nan")

    perms = []
    for i in range(n - delay * (order - 1)):
        window = tuple(series[i + j * delay] for j in range(order))
        perms.append(tuple(np.argsort(window)))

    counts = Counter(perms)
    total = sum(counts.values())
    probs = [c / total for c in counts.values()]

    entropy = -sum(p * np.log2(p) for p in probs if p > 0)
    max_entropy = np.log2(math.factorial(order))

    return float(entropy / max_entropy) if max_entropy > 0 else float("nan")


@dg.asset(
    group_name=SIGNALS_GROUP,
    kinds={"python", "bigquery"},
    deps=[dg.AssetKey(["stg_major_indices"])],
    description="Permutation entropy complexity measure for SPY and QQQ returns",
)
def entropy_complexity_signals(
    context: dg.AssetExecutionContext,
    bq: BigQueryWarehouseResource,
) -> dg.MaterializeResult:
    context.log.info("Fetching SPY and QQQ prices...")
    # stg_major_indices is a dbt staging view living in the
    # economics_staging schema, not this resource's default
    # (economics_raw) dataset.
    staging_dataset = default_dataset_for_schema("economics_staging")
    prices_df = bq.execute_query(
        f"""
        SELECT date, symbol, adj_close
        FROM {staging_dataset}.stg_major_indices
        WHERE symbol IN ('SPY', 'QQQ')
          AND adj_close IS NOT NULL
          AND date >= CURRENT_DATE - INTERVAL 3 YEAR
        ORDER BY date
        """,
        read_only=True,
    )

    if prices_df.is_empty():
        return dg.MaterializeResult(metadata={"rows": 0, "status": "no_data"})

    # Pivot and compute returns
    wide = prices_df.pivot(on="symbol", index="date", values="adj_close").sort("date")
    dates = wide["date"].to_list()

    results = []
    window_size = 60
    order = 3

    for symbol in ["SPY", "QQQ"]:
        if symbol not in wide.columns:
            continue
        prices = wide[symbol].to_numpy().astype(float)
        returns = np.diff(prices) / prices[:-1]

        for i in range(window_size, len(returns)):
            window = returns[i - window_size : i]
            if np.isnan(window).any():
                continue
            pe = permutation_entropy(window, order=order)
            results.append(
                {
                    "date": dates[i],  # last price date in the window
                    "symbol": symbol,
                    "perm_entropy": round(pe, 4),
                }
            )

    if not results:
        return dg.MaterializeResult(
            metadata={"rows": 0, "status": "computation_failed"}
        )

    df = pl.DataFrame(results)

    # Pivot to have SPY and QQQ side by side
    spy_entropy = df.filter(pl.col("symbol") == "SPY").select(
        ["date", pl.col("perm_entropy").alias("spy_perm_entropy")]
    )
    qqq_entropy = df.filter(pl.col("symbol") == "QQQ").select(
        ["date", pl.col("perm_entropy").alias("qqq_perm_entropy")]
    )

    result_df = spy_entropy.join(
        qqq_entropy, on="date", how="full", coalesce=True
    ).sort("date")

    # Add rolling statistics
    result_df = result_df.with_columns(
        [
            # 20-day average for trend detection
            pl.col("spy_perm_entropy")
            .rolling_mean(window_size=20, min_samples=10)
            .round(4)
            .alias("spy_entropy_20d_avg"),
            # 60-day average for baseline
            pl.col("spy_perm_entropy")
            .rolling_mean(window_size=60, min_samples=30)
            .round(4)
            .alias("spy_entropy_60d_avg"),
            # Z-score (252-day rolling)
            (
                (
                    pl.col("spy_perm_entropy")
                    - pl.col("spy_perm_entropy").rolling_mean(
                        window_size=252, min_samples=60
                    )
                )
                / pl.col("spy_perm_entropy").rolling_std(
                    window_size=252, min_samples=60
                )
            )
            .round(2)
            .alias("spy_entropy_zscore"),
        ]
    )

    # Trend detection: declining entropy = fragility signal
    result_df = result_df.with_columns(
        pl.when(
            (pl.col("spy_entropy_20d_avg") < pl.col("spy_entropy_60d_avg"))
            & (pl.col("spy_entropy_zscore") < -1.0)
        )
        .then(pl.lit("high"))
        .when(pl.col("spy_entropy_20d_avg") < pl.col("spy_entropy_60d_avg"))
        .then(pl.lit("medium"))
        .when(pl.col("spy_entropy_zscore") < -0.5)
        .then(pl.lit("low"))
        .otherwise(pl.lit("normal"))
        .alias("entropy_status")
    )

    context.log.info(f"Writing {len(result_df)} entropy rows to MotherDuck")
    bq.upsert_data("entropy_complexity_signals", result_df, ["date"], context=context)

    return dg.MaterializeResult(
        metadata={
            "rows": len(result_df),
            "latest_spy_entropy": float(result_df["spy_perm_entropy"][-1])
            if result_df["spy_perm_entropy"][-1] is not None
            else None,
            "latest_status": str(result_df["entropy_status"][-1]),
            "date_range": f"{result_df['date'][0]} to {result_df['date'][-1]}",
        }
    )
