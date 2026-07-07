{{ config(
    description='Latest factor ETF performance and rolling correlation to sector, style, and thematic ETFs'
) }}

WITH factor_metadata AS (
    SELECT *
    FROM UNNEST([
        STRUCT('VLUE' AS factor_symbol, 'value' AS factor_name),
        STRUCT('QUAL' AS factor_symbol, 'quality' AS factor_name),
        STRUCT('MTUM' AS factor_symbol, 'momentum' AS factor_name),
        STRUCT('SIZE' AS factor_symbol, 'size' AS factor_name),
        STRUCT('USMV' AS factor_symbol, 'minimum_volatility' AS factor_name)
    ])
),

comparison_metadata AS (
    SELECT *
    FROM UNNEST([
        STRUCT('XLK' AS comparison_symbol, 'sector_etf' AS comparison_universe, 'Technology' AS comparison_name),
        STRUCT('XLC' AS comparison_symbol, 'sector_etf' AS comparison_universe, 'Communication Services' AS comparison_name),
        STRUCT('XLY' AS comparison_symbol, 'sector_etf' AS comparison_universe, 'Consumer Discretionary' AS comparison_name),
        STRUCT('XLF' AS comparison_symbol, 'sector_etf' AS comparison_universe, 'Financials' AS comparison_name),
        STRUCT('XLI' AS comparison_symbol, 'sector_etf' AS comparison_universe, 'Industrials' AS comparison_name),
        STRUCT('XLU' AS comparison_symbol, 'sector_etf' AS comparison_universe, 'Utilities' AS comparison_name),
        STRUCT('XLP' AS comparison_symbol, 'sector_etf' AS comparison_universe, 'Consumer Staples' AS comparison_name),
        STRUCT('XLRE' AS comparison_symbol, 'sector_etf' AS comparison_universe, 'Real Estate' AS comparison_name),
        STRUCT('XLB' AS comparison_symbol, 'sector_etf' AS comparison_universe, 'Materials' AS comparison_name),
        STRUCT('XLE' AS comparison_symbol, 'sector_etf' AS comparison_universe, 'Energy' AS comparison_name),
        STRUCT('XLV' AS comparison_symbol, 'sector_etf' AS comparison_universe, 'Health Care' AS comparison_name),
        STRUCT('SPY' AS comparison_symbol, 'broad_market_etf' AS comparison_universe, 'S&P 500' AS comparison_name),
        STRUCT('QQQ' AS comparison_symbol, 'broad_market_etf' AS comparison_universe, 'Nasdaq 100' AS comparison_name),
        STRUCT('DIA' AS comparison_symbol, 'broad_market_etf' AS comparison_universe, 'Dow Jones Industrial Average' AS comparison_name),
        STRUCT('RSP' AS comparison_symbol, 'broad_market_etf' AS comparison_universe, 'S&P 500 Equal Weight' AS comparison_name),
        STRUCT('IWM' AS comparison_symbol, 'style_etf' AS comparison_universe, 'Russell 2000' AS comparison_name),
        STRUCT('IWD' AS comparison_symbol, 'style_etf' AS comparison_universe, 'Russell 1000 Value' AS comparison_name),
        STRUCT('IWF' AS comparison_symbol, 'style_etf' AS comparison_universe, 'Russell 1000 Growth' AS comparison_name),
        STRUCT('IYT' AS comparison_symbol, 'thematic_etf' AS comparison_universe, 'Transportation' AS comparison_name),
        STRUCT('SOXX' AS comparison_symbol, 'thematic_etf' AS comparison_universe, 'Semiconductors' AS comparison_name)
    ])
),

factor_prices AS (
    SELECT
        symbol AS factor_symbol,
        exchange,
        date,
        adj_close,
        SAFE_DIVIDE(
            adj_close - LAG(adj_close) OVER (
                PARTITION BY symbol, exchange
                ORDER BY date
            ),
            LAG(adj_close) OVER (
                PARTITION BY symbol, exchange
                ORDER BY date
            )
        ) AS factor_daily_return
    FROM {{ ref('stg_factor_etfs') }}
    WHERE adj_close IS NOT NULL
),

sector_prices AS (
    SELECT
        symbol AS comparison_symbol,
        exchange,
        date,
        adj_close,
        SAFE_DIVIDE(
            adj_close - LAG(adj_close) OVER (
                PARTITION BY symbol, exchange
                ORDER BY date
            ),
            LAG(adj_close) OVER (
                PARTITION BY symbol, exchange
                ORDER BY date
            )
        ) AS comparison_daily_return
    FROM {{ ref('stg_us_sectors') }}
    WHERE adj_close IS NOT NULL
),

major_index_prices AS (
    SELECT
        major_indices.symbol AS comparison_symbol,
        major_indices.exchange,
        major_indices.date,
        major_indices.adj_close,
        SAFE_DIVIDE(
            major_indices.adj_close - LAG(major_indices.adj_close) OVER (
                PARTITION BY major_indices.symbol, major_indices.exchange
                ORDER BY major_indices.date
            ),
            LAG(major_indices.adj_close) OVER (
                PARTITION BY major_indices.symbol, major_indices.exchange
                ORDER BY major_indices.date
            )
        ) AS comparison_daily_return
    FROM {{ ref('stg_major_indices') }} AS major_indices
    INNER JOIN comparison_metadata
        ON major_indices.symbol = comparison_metadata.comparison_symbol
    WHERE major_indices.adj_close IS NOT NULL
      AND comparison_metadata.comparison_universe != 'sector_etf'
),

comparison_prices AS (
    SELECT
        comparison_symbol,
        exchange,
        date,
        adj_close,
        comparison_daily_return
    FROM sector_prices
    UNION ALL
    SELECT
        comparison_symbol,
        exchange,
        date,
        adj_close,
        comparison_daily_return
    FROM major_index_prices
),

latest_common_date AS (
    SELECT MAX(factor_prices.date) AS as_of_date
    FROM factor_prices
    INNER JOIN comparison_prices
        ON factor_prices.date = comparison_prices.date
),

joined_returns AS (
    SELECT
        factor_prices.factor_symbol,
        comparison_prices.comparison_symbol,
        factor_prices.date,
        factor_prices.factor_daily_return,
        comparison_prices.comparison_daily_return
    FROM factor_prices
    INNER JOIN comparison_prices
        ON factor_prices.date = comparison_prices.date
    CROSS JOIN latest_common_date AS latest
    WHERE factor_prices.factor_daily_return IS NOT NULL
      AND comparison_prices.comparison_daily_return IS NOT NULL
      AND factor_prices.date BETWEEN DATE_SUB(latest.as_of_date, INTERVAL 365 DAY)
        AND latest.as_of_date
),

rolling_correlations AS (
    SELECT
        latest.as_of_date,
        joined_returns.factor_symbol,
        joined_returns.comparison_symbol,
        COUNTIF(
            joined_returns.date >= DATE_SUB(latest.as_of_date, INTERVAL 90 DAY)
        ) AS observations_3mo,
        ROUND(CORR(
            CASE
                WHEN joined_returns.date >= DATE_SUB(latest.as_of_date, INTERVAL 90 DAY)
                THEN joined_returns.factor_daily_return
            END,
            CASE
                WHEN joined_returns.date >= DATE_SUB(latest.as_of_date, INTERVAL 90 DAY)
                THEN joined_returns.comparison_daily_return
            END
        ), 4) AS corr_3mo,
        COUNT(*) AS observations_1yr,
        ROUND(CORR(joined_returns.factor_daily_return, joined_returns.comparison_daily_return), 4) AS corr_1yr
    FROM joined_returns
    CROSS JOIN latest_common_date AS latest
    GROUP BY latest.as_of_date, joined_returns.factor_symbol, joined_returns.comparison_symbol
),

latest_factor_performance AS (
    SELECT
        symbol AS factor_symbol,
        date,
        pct_change_1mo,
        pct_change_3mo,
        pct_change_1yr,
        std_diff_1yr
    FROM {{ ref('factor_analysis_return') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY symbol
        ORDER BY date DESC
    ) = 1
),

sector_performance AS (
    SELECT
        symbol AS comparison_symbol,
        date,
        pct_change_1mo,
        pct_change_3mo,
        pct_change_1yr,
        std_diff_1yr
    FROM {{ ref('us_sector_analysis_return') }}
),

major_index_performance AS (
    SELECT
        major_indices.symbol AS comparison_symbol,
        major_indices.date,
        major_indices.pct_change_1mo,
        major_indices.pct_change_3mo,
        major_indices.pct_change_1yr,
        major_indices.std_diff_1yr
    FROM {{ ref('major_indicies_analysis_return') }} AS major_indices
    INNER JOIN comparison_metadata
        ON major_indices.symbol = comparison_metadata.comparison_symbol
    WHERE comparison_metadata.comparison_universe != 'sector_etf'
),

comparison_performance AS (
    SELECT
        comparison_symbol,
        date,
        pct_change_1mo,
        pct_change_3mo,
        pct_change_1yr,
        std_diff_1yr
    FROM sector_performance
    UNION ALL
    SELECT
        comparison_symbol,
        date,
        pct_change_1mo,
        pct_change_3mo,
        pct_change_1yr,
        std_diff_1yr
    FROM major_index_performance
),

latest_comparison_performance AS (
    SELECT
        comparison_symbol,
        date,
        pct_change_1mo,
        pct_change_3mo,
        pct_change_1yr,
        std_diff_1yr
    FROM comparison_performance
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY comparison_symbol
        ORDER BY date DESC
    ) = 1
)

SELECT
    CONCAT(
        correlations.factor_symbol,
        ':',
        correlations.comparison_symbol,
        ':',
        CAST(correlations.as_of_date AS STRING)
    ) AS factor_sector_key,
    correlations.as_of_date,
    correlations.factor_symbol,
    factor_metadata.factor_name,
    correlations.comparison_symbol,
    comparison_metadata.comparison_name,
    comparison_metadata.comparison_universe,
    correlations.comparison_symbol AS sector_symbol,
    comparison_metadata.comparison_name AS sector_name,
    correlations.observations_3mo,
    correlations.corr_3mo,
    correlations.observations_1yr,
    correlations.corr_1yr,
    factor_performance.pct_change_1mo AS factor_return_1mo,
    factor_performance.pct_change_3mo AS factor_return_3mo,
    factor_performance.pct_change_1yr AS factor_return_1yr,
    comparison_performance.pct_change_1mo AS sector_return_1mo,
    comparison_performance.pct_change_3mo AS sector_return_3mo,
    comparison_performance.pct_change_1yr AS sector_return_1yr,
    ROUND(factor_performance.pct_change_3mo - comparison_performance.pct_change_3mo, 2) AS factor_sector_return_spread_3mo,
    ROUND(factor_performance.pct_change_1yr - comparison_performance.pct_change_1yr, 2) AS factor_sector_return_spread_1yr,
    factor_performance.std_diff_1yr AS factor_volatility_proxy_1yr,
    comparison_performance.std_diff_1yr AS sector_volatility_proxy_1yr
FROM rolling_correlations AS correlations
LEFT JOIN factor_metadata
    ON correlations.factor_symbol = factor_metadata.factor_symbol
LEFT JOIN comparison_metadata
    ON correlations.comparison_symbol = comparison_metadata.comparison_symbol
LEFT JOIN latest_factor_performance AS factor_performance
    ON correlations.factor_symbol = factor_performance.factor_symbol
LEFT JOIN latest_comparison_performance AS comparison_performance
    ON correlations.comparison_symbol = comparison_performance.comparison_symbol
