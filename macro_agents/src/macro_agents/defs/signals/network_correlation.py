"""Network Correlation Analysis — MST from stock correlation matrix.

Academic basis: Mantegna (1999)

Converts pairwise correlations to distances (d_ij = sqrt(2(1-rho_ij))),
builds a Minimum Spanning Tree connecting stocks. Track network metrics:
- Declining total MST length = correlations rising = diversification failing
- Sector structure breaking down = crisis regime
- High betweenness centrality = systemically important stocks

Computationally expensive — runs weekly.
"""

import dagster as dg
import networkx as nx
import numpy as np
import polars as pl

from macro_agents.defs.resources.bigquery_warehouse import BigQueryWarehouseResource


SIGNALS_GROUP = "computed_signals"


@dg.asset(
    group_name=SIGNALS_GROUP,
    kinds={"python", "duckdb"},
    deps=[dg.AssetKey(["sp500_companies_prices_raw"])],
    description="Network correlation analysis via Minimum Spanning Tree of S&P 500 stocks",
)
def network_correlation_signals(
    context: dg.AssetExecutionContext,
    md: BigQueryWarehouseResource,
) -> dg.MaterializeResult:
    context.log.info("Fetching S&P 500 prices for network analysis...")
    prices_df = md.execute_query(
        """
        SELECT date, symbol, adj_close
        FROM sp500_companies_prices_raw
        WHERE adj_close IS NOT NULL
          AND CAST(date AS DATE) >= CURRENT_DATE - INTERVAL '2 years'
        ORDER BY date, symbol
        """,
        read_only=True,
    )

    if prices_df.is_empty():
        return dg.MaterializeResult(metadata={"rows": 0, "status": "no_data"})

    # Pivot to wide format
    prices_wide = prices_df.pivot(on="symbol", index="date", values="adj_close").sort(
        "date"
    )
    dates = prices_wide["date"].to_list()
    symbols = [c for c in prices_wide.columns if c != "date"]

    price_matrix = prices_wide.select(symbols).to_numpy()
    returns = np.diff(price_matrix, axis=0) / price_matrix[:-1]

    # Compute network metrics on rolling 63-day (quarterly) windows
    window_size = 63
    results = []

    context.log.info(
        f"Computing rolling {window_size}-day MST metrics for {len(symbols)} stocks..."
    )

    for i in range(window_size, len(returns), 5):  # Step by 5 days for efficiency
        window = returns[i - window_size : i]

        # Filter to stocks with sufficient data
        valid_mask = np.isnan(window).mean(axis=0) < 0.1
        if valid_mask.sum() < 50:
            continue

        window_clean = window[:, valid_mask]
        valid_symbols = [s for s, v in zip(symbols, valid_mask) if v]

        # Forward-fill NaN
        for col in range(window_clean.shape[1]):
            col_data = window_clean[:, col]
            mask = np.isnan(col_data)
            if mask.any() and not mask.all():
                idx = np.where(~mask, np.arange(len(col_data)), 0)
                np.maximum.accumulate(idx, out=idx)
                window_clean[:, col] = col_data[idx]

        row_mask = ~np.isnan(window_clean).any(axis=1)
        if row_mask.sum() < 30:
            continue
        window_clean = window_clean[row_mask]

        # Correlation matrix
        try:
            corr = np.corrcoef(window_clean.T)
        except (np.linalg.LinAlgError, ValueError):
            continue

        # Distance matrix: d_ij = sqrt(2(1 - rho_ij))
        n_stocks = corr.shape[0]
        dist = np.sqrt(2 * (1 - corr))
        np.fill_diagonal(dist, 0)

        # Build complete graph and compute MST
        G = nx.Graph()
        for j in range(n_stocks):
            for k in range(j + 1, n_stocks):
                if not np.isnan(dist[j, k]) and np.isfinite(dist[j, k]):
                    G.add_edge(valid_symbols[j], valid_symbols[k], weight=dist[j, k])

        if G.number_of_edges() < n_stocks - 1:
            continue

        try:
            mst = nx.minimum_spanning_tree(G)
        except nx.NetworkXError:
            continue

        # Network metrics
        total_mst_length = sum(d["weight"] for _, _, d in mst.edges(data=True))
        avg_path_length = total_mst_length / max(mst.number_of_edges(), 1)

        degrees = dict(mst.degree())
        max_degree = max(degrees.values()) if degrees else 0

        betweenness = nx.betweenness_centrality(mst)
        max_betweenness = max(betweenness.values()) if betweenness else 0
        most_central_stock = (
            max(betweenness, key=betweenness.get) if betweenness else ""
        )

        # Simple clustering: connected components after removing highest-betweenness edges
        mst_copy = mst.copy()
        edges_by_betweenness = sorted(
            mst_copy.edges(data=True),
            key=lambda x: x[2].get("weight", 0),
            reverse=True,
        )
        # Remove top 5% of edges to find natural clusters
        n_remove = max(1, int(len(edges_by_betweenness) * 0.05))
        for u, v, _ in edges_by_betweenness[:n_remove]:
            mst_copy.remove_edge(u, v)
        cluster_count = nx.number_connected_components(mst_copy)

        results.append(
            {
                "date": dates[i],  # last price date in the window
                "n_stocks": n_stocks,
                "mst_total_length": round(total_mst_length, 2),
                "mst_avg_path_length": round(avg_path_length, 4),
                "mst_max_degree": max_degree,
                "mst_max_betweenness": round(max_betweenness, 4),
                "most_central_stock": most_central_stock,
                "mst_cluster_count": cluster_count,
            }
        )

    if not results:
        return dg.MaterializeResult(
            metadata={"rows": 0, "status": "computation_failed"}
        )

    df = pl.DataFrame(results)

    # Add derived metrics
    df = df.with_columns(
        [
            # Rolling statistics on MST length (lower = more correlated = more fragile)
            pl.col("mst_total_length")
            .rolling_mean(window_size=10, min_samples=3)
            .round(2)
            .alias("mst_length_10pt_avg"),
            # Z-score
            (
                (
                    pl.col("mst_total_length")
                    - pl.col("mst_total_length").rolling_mean(
                        window_size=50, min_samples=10
                    )
                )
                / pl.col("mst_total_length").rolling_std(window_size=50, min_samples=10)
            )
            .round(2)
            .alias("mst_length_zscore"),
        ]
    )

    # Network fragility score: declining MST length + high centrality concentration
    df = df.with_columns(
        pl.when(
            (pl.col("mst_length_zscore") < -1.5)
            & (pl.col("mst_max_betweenness") > 0.15)
        )
        .then(pl.lit("high"))
        .when(pl.col("mst_length_zscore") < -1.0)
        .then(pl.lit("medium"))
        .when(pl.col("mst_length_zscore") < -0.5)
        .then(pl.lit("low"))
        .otherwise(pl.lit("normal"))
        .alias("network_status")
    )

    context.log.info(f"Writing {len(df)} network correlation rows to MotherDuck")
    md.upsert_data("network_correlation_signals", df, ["date"], context=context)

    return dg.MaterializeResult(
        metadata={
            "rows": len(df),
            "latest_mst_length": float(df["mst_total_length"][-1]),
            "latest_status": str(df["network_status"][-1]),
            "date_range": f"{df['date'][0]} to {df['date'][-1]}",
        }
    )
