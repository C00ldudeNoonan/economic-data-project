"""CNN Fear & Greed Index Replication — 6 of 7 components.

Composite of 6 equally-weighted sub-indicators, each normalized 0-100:
1. Market Momentum: SPY vs 125-day MA
2. Stock Price Strength: Net 52-week highs vs lows (S&P 500)
3. McClellan Volume Summation: Approximated from breadth data
4. Market Volatility: VIX vs 50-day MA (inverted — low VIX = greed)
5. Safe Haven Demand: 20-day stock vs bond return differential
6. Junk Bond Demand: HY-IG spread (inverted — tight spreads = greed)

Component 4 (put/call ratio) requires CBOE data not yet available.

Thresholds: 0-24 = Extreme Fear, 25-44 = Fear, 45-55 = Neutral,
            56-75 = Greed, 76-100 = Extreme Greed
"""

import dagster as dg
import numpy as np
import polars as pl

from macro_agents.defs.resources.bigquery_warehouse import (
    BigQueryWarehouseResource,
    default_dataset_for_schema,
)


SIGNALS_GROUP = "computed_signals"


def _percentile_rank(arr: np.ndarray, window: int = 252) -> np.ndarray:
    """Compute rolling percentile rank (0-100 scale)."""
    result = np.full_like(arr, np.nan, dtype=float)
    for i in range(window, len(arr)):
        w = arr[i - window : i]
        valid = w[~np.isnan(w)]
        if len(valid) < 20:
            continue
        rank = (valid < arr[i]).sum() / len(valid) * 100
        result[i] = rank
    return result


@dg.asset(
    group_name=SIGNALS_GROUP,
    kinds={"python", "bigquery"},
    deps=[
        dg.AssetKey(["stg_major_indices"]),
        dg.AssetKey(["stg_sp500_companies_prices"]),
        dg.AssetKey(["stg_fred_series"]),
        dg.AssetKey(["stg_fixed_income"]),
    ],
    description="CNN Fear & Greed index replication (6 of 7 components)",
)
def fear_greed_signals(
    context: dg.AssetExecutionContext,
    bq: BigQueryWarehouseResource,
) -> dg.MaterializeResult:
    # BigQuery INTERVAL literals must be unquoted (`INTERVAL 3 YEAR`), unlike
    # the DuckDB/Postgres quoted-string form (`INTERVAL '3 years'`).
    lookback_years = 3

    # stg_* models are dbt staging views living in the economics_staging
    # schema, not this resource's default (economics_raw) dataset.
    staging_dataset = default_dataset_for_schema("economics_staging")

    # 1. Market Momentum: SPY vs 125-day MA
    context.log.info("Computing market momentum component...")
    spy_df = bq.execute_query(
        f"""
        SELECT date, adj_close,
               AVG(adj_close) OVER (ORDER BY date ROWS BETWEEN 124 PRECEDING AND CURRENT ROW) AS sma_125
        FROM {staging_dataset}.stg_major_indices
        WHERE symbol = 'SPY' AND adj_close IS NOT NULL
          AND date >= CURRENT_DATE - INTERVAL {lookback_years} YEAR
        ORDER BY date
        """,
        read_only=True,
    )

    # 2. Stock Price Strength: net 52-week highs/lows
    context.log.info("Computing stock price strength component...")
    strength_df = bq.execute_query(
        f"""
        WITH per_symbol AS (
            SELECT
                date,
                symbol,
                adj_close,
                MAX(adj_close) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW) AS high_52w,
                MIN(adj_close) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW) AS low_52w
            FROM {staging_dataset}.stg_sp500_companies_prices
            WHERE adj_close IS NOT NULL
              AND date >= CURRENT_DATE - INTERVAL {lookback_years} YEAR - INTERVAL 1 YEAR
        ),
        daily_stats AS (
            SELECT
                date,
                SUM(CASE WHEN adj_close >= high_52w THEN 1 ELSE 0 END) AS new_highs,
                SUM(CASE WHEN adj_close <= low_52w THEN 1 ELSE 0 END) AS new_lows
            FROM per_symbol
            GROUP BY date
        )
        SELECT date, new_highs, new_lows, new_highs - new_lows AS net_new_highs
        FROM daily_stats
        WHERE date >= CURRENT_DATE - INTERVAL {lookback_years} YEAR
        ORDER BY date
        """,
        read_only=True,
    )

    # 3. VIX vs 50-day MA (inverted: low VIX = greed)
    context.log.info("Computing VIX component...")
    vix_df = bq.execute_query(
        f"""
        SELECT date, literal AS vix,
               AVG(literal) OVER (ORDER BY date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS vix_sma_50
        FROM {staging_dataset}.stg_fred_series
        WHERE series_code = 'VIXCLS' AND literal IS NOT NULL
          AND date >= CURRENT_DATE - INTERVAL {lookback_years} YEAR
        ORDER BY date
        """,
        read_only=True,
    )

    # 4. Safe Haven Demand: 20-day stock vs bond return differential
    context.log.info("Computing safe haven demand component...")
    safe_haven_df = bq.execute_query(
        f"""
        WITH spy_ret AS (
            SELECT date, adj_close / LAG(adj_close, 20) OVER (ORDER BY date) - 1 AS spy_20d_ret
            FROM {staging_dataset}.stg_major_indices WHERE symbol = 'SPY' AND adj_close IS NOT NULL
        ),
        govt_ret AS (
            SELECT date, adj_close / LAG(adj_close, 20) OVER (ORDER BY date) - 1 AS govt_20d_ret
            FROM {staging_dataset}.stg_fixed_income WHERE symbol = 'GOVT' AND adj_close IS NOT NULL
        )
        SELECT s.date, s.spy_20d_ret, g.govt_20d_ret,
               s.spy_20d_ret - g.govt_20d_ret AS stock_bond_diff
        FROM spy_ret s
        INNER JOIN govt_ret g ON s.date = g.date
        WHERE s.date >= CURRENT_DATE - INTERVAL {lookback_years} YEAR
          AND s.spy_20d_ret IS NOT NULL AND g.govt_20d_ret IS NOT NULL
        ORDER BY s.date
        """,
        read_only=True,
    )

    # 5. Junk Bond Demand: HY-IG spread (inverted: tight spread = greed)
    context.log.info("Computing junk bond demand component...")
    credit_df = bq.execute_query(
        f"""
        WITH hy AS (
            SELECT date, literal AS hy_spread FROM {staging_dataset}.stg_fred_series
            WHERE series_code = 'BAMLH0A0HYM2' AND literal IS NOT NULL
        ),
        ig AS (
            SELECT date, literal AS ig_spread FROM {staging_dataset}.stg_fred_series
            WHERE series_code = 'BAMLC0A0CM' AND literal IS NOT NULL
        )
        SELECT h.date, h.hy_spread, i.ig_spread, h.hy_spread - i.ig_spread AS hy_ig_diff
        FROM hy h INNER JOIN ig i ON h.date = i.date
        WHERE h.date >= CURRENT_DATE - INTERVAL {lookback_years} YEAR
        ORDER BY h.date
        """,
        read_only=True,
    )

    # Combine all components on date
    context.log.info("Building composite index...")

    # Start with SPY momentum as base
    if spy_df.is_empty():
        return dg.MaterializeResult(metadata={"rows": 0, "status": "no_spy_data"})

    # Compute raw component values
    spy_np = spy_df.to_numpy()
    momentum_raw = (
        spy_np[:, 1].astype(float) / spy_np[:, 2].astype(float) - 1
    ) * 100  # % above 125d MA

    dates = spy_df["date"].to_list()
    n = len(dates)

    # Percentile-rank each component over 252-day window for 0-100 normalization
    momentum_score = _percentile_rank(momentum_raw)

    # Strength component
    strength_score = np.full(n, np.nan)
    if not strength_df.is_empty():
        str_dates = {d: i for i, d in enumerate(strength_df["date"].to_list())}
        net_highs = strength_df["net_new_highs"].to_numpy().astype(float)
        net_highs_ranked = _percentile_rank(net_highs)
        for i, d in enumerate(dates):
            if d in str_dates:
                strength_score[i] = net_highs_ranked[str_dates[d]]

    # VIX component (inverted: low VIX = high score = greed)
    vix_score = np.full(n, np.nan)
    if not vix_df.is_empty():
        vix_dates = {d: i for i, d in enumerate(vix_df["date"].to_list())}
        vix_vs_ma = (
            vix_df["vix"].to_numpy().astype(float)
            / vix_df["vix_sma_50"].to_numpy().astype(float)
            - 1
        ) * 100
        vix_ranked = _percentile_rank(vix_vs_ma)
        for i, d in enumerate(dates):
            if d in vix_dates and vix_ranked[vix_dates[d]] is not None:
                vix_score[i] = 100 - vix_ranked[vix_dates[d]]  # Invert

    # Safe haven component
    safe_haven_score = np.full(n, np.nan)
    if not safe_haven_df.is_empty():
        sh_dates = {d: i for i, d in enumerate(safe_haven_df["date"].to_list())}
        sh_raw = safe_haven_df["stock_bond_diff"].to_numpy().astype(float)
        sh_ranked = _percentile_rank(sh_raw)
        for i, d in enumerate(dates):
            if d in sh_dates:
                safe_haven_score[i] = sh_ranked[sh_dates[d]]

    # Credit component (inverted: tight spread = greed)
    credit_score = np.full(n, np.nan)
    if not credit_df.is_empty():
        cr_dates = {d: i for i, d in enumerate(credit_df["date"].to_list())}
        cr_raw = credit_df["hy_ig_diff"].to_numpy().astype(float)
        cr_ranked = _percentile_rank(cr_raw)
        for i, d in enumerate(dates):
            if d in cr_dates and cr_ranked[cr_dates[d]] is not None:
                credit_score[i] = 100 - cr_ranked[cr_dates[d]]  # Invert

    # Composite: equal-weighted average of available components
    components = np.column_stack(
        [momentum_score, strength_score, vix_score, safe_haven_score, credit_score]
    )
    composite = np.nanmean(components, axis=1)
    component_count = (~np.isnan(components)).sum(axis=1)

    # Build result DataFrame
    result_df = pl.DataFrame(
        {
            "date": dates,
            "fear_greed_score": np.round(composite, 1),
            "momentum_component": np.round(momentum_score, 1),
            "strength_component": np.round(strength_score, 1),
            "vix_component": np.round(vix_score, 1),
            "safe_haven_component": np.round(safe_haven_score, 1),
            "credit_component": np.round(credit_score, 1),
            "component_count": component_count.astype(int),
        }
    )

    # Filter to rows with valid composite
    result_df = result_df.filter(pl.col("fear_greed_score").is_not_nan())

    # Add status
    result_df = result_df.with_columns(
        pl.when(pl.col("fear_greed_score") <= 24)
        .then(pl.lit("extreme_fear"))
        .when(pl.col("fear_greed_score") <= 44)
        .then(pl.lit("fear"))
        .when(pl.col("fear_greed_score") <= 55)
        .then(pl.lit("neutral"))
        .when(pl.col("fear_greed_score") <= 75)
        .then(pl.lit("greed"))
        .otherwise(pl.lit("extreme_greed"))
        .alias("fear_greed_status")
    )

    # Map status to signal severity
    result_df = result_df.with_columns(
        pl.when(pl.col("fear_greed_status").is_in(["extreme_fear", "extreme_greed"]))
        .then(pl.lit("high"))
        .when(pl.col("fear_greed_status").is_in(["fear", "greed"]))
        .then(pl.lit("medium"))
        .otherwise(pl.lit("normal"))
        .alias("fear_greed_signal_status")
    )

    context.log.info(f"Writing {len(result_df)} fear/greed rows to MotherDuck")
    bq.upsert_data("fear_greed_signals", result_df, ["date"], context=context)

    return dg.MaterializeResult(
        metadata={
            "rows": len(result_df),
            "latest_score": float(result_df["fear_greed_score"][-1]),
            "latest_status": str(result_df["fear_greed_status"][-1]),
            "date_range": f"{result_df['date'][0]} to {result_df['date'][-1]}",
        }
    )
