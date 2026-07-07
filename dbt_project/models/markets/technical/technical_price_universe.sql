{{ config(
    description='Unified daily OHLCV universe across stock/ETF/index price sources for the technical-analysis framework (issue #109)'
) }}

/*
    Technical Price Universe

    Grain: one row per source_universe, symbol, exchange, date.

    Normalizes adjusted OHLCV columns from existing market staging models
    into a single spine that downstream indicator/signal models consume.

    - S&P 500 stocks use split-adjusted prices (stg_split_adjusted_prices) so
      indicators are not distorted by splits.
    - ETF/index sources prefer MarketStack-adjusted fields, falling back
      to raw values when the adjusted column is NULL.

    No new ingestion: only sources already present in the project.
*/

{% set etf_universes = [
    {'model': 'stg_us_sectors', 'universe': 'us_sector_etf'},
    {'model': 'stg_major_indices', 'universe': 'major_index'},
    {'model': 'stg_fixed_income', 'universe': 'fixed_income_etf'},
    {'model': 'stg_currency', 'universe': 'currency_etf'},
    {'model': 'stg_commodity_etfs', 'universe': 'commodity_etf'},
    {'model': 'stg_factor_etfs', 'universe': 'factor_etf'},
    {'model': 'stg_global_markets', 'universe': 'global_market'},
] %}

WITH unioned AS (

    SELECT
        'sp500_stock' AS source_universe,
        symbol,
        COALESCE(exchange, 'UNKNOWN') AS exchange,
        name,
        asset_type,
        'USD' AS price_currency,
        source_table,
        date,
        split_adj_open AS open,
        split_adj_high AS high,
        split_adj_low AS low,
        split_adj_close AS close,
        split_adj_volume AS volume
    FROM {{ ref('stg_split_adjusted_prices') }}

    {% for entry in etf_universes %}
    UNION ALL

    SELECT
        '{{ entry.universe }}' AS source_universe,
        symbol,
        COALESCE(exchange, 'UNKNOWN') AS exchange,
        name,
        asset_type,
        price_currency,
        '{{ entry.model }}' AS source_table,
        date,
        COALESCE(adj_open, open) AS open,
        COALESCE(adj_high, high) AS high,
        COALESCE(adj_low, low) AS low,
        COALESCE(adj_close, close) AS close,
        COALESCE(adj_volume, volume) AS volume
    FROM {{ ref(entry.model) }}
    {% endfor %}

),

filtered AS (
    SELECT *
    FROM unioned
    WHERE date IS NOT NULL
      AND symbol IS NOT NULL
      AND close IS NOT NULL
      AND close > 0
    -- Collapse duplicate vendor rows at the grain, keeping the most-traded row
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY source_universe, symbol, exchange, date
        ORDER BY volume DESC
    ) = 1
)

SELECT
    source_universe,
    symbol,
    exchange,
    name,
    asset_type,
    price_currency,
    source_table,
    date,
    open,
    -- Vendor data occasionally reports close outside the high/low range
    -- (see test_ohlc_consistency). Clamp so low <= close <= high always
    -- holds; range-based indicators (stochastic, Williams %R, ATR,
    -- Donchian) rely on this invariant.
    GREATEST(COALESCE(high, close), close) AS high,
    LEAST(COALESCE(low, close), close) AS low,
    close,
    volume,
    -- Trading-bar counter per instrument; downstream models use this for
    -- indicator warmup gating and forward-return offsets.
    ROW_NUMBER() OVER (
        PARTITION BY source_universe, symbol, exchange
        ORDER BY date
    ) AS bars_available
FROM filtered
