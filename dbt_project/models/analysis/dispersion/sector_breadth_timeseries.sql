{{
  config(
    description='Per-sector percentage of stocks above 200-day moving average over time, sampled weekly'
  )
}}

WITH sector_mapping AS (
    SELECT sector_mapping.*
    FROM (VALUES
        ('Information Technology', 'XLK', 'Technology'),
        ('Communication Services', 'XLC', 'Communication Services'),
        ('Consumer Discretionary', 'XLY', 'Consumer Discretionary'),
        ('Financials', 'XLF', 'Financial'),
        ('Industrials', 'XLI', 'Industrial'),
        ('Utilities', 'XLU', 'Utilities'),
        ('Consumer Staples', 'XLP', 'Consumer Staples'),
        ('Real Estate', 'XLRE', 'Real Estate'),
        ('Materials', 'XLB', 'Materials'),
        ('Energy', 'XLE', 'Energy'),
        ('Health Care', 'XLV', 'Health Care')
    ) AS sector_mapping (gics_sector, etf_symbol, sector_display_name)
),

-- Load 4 years of data: 3 years of output + ~1 year lookback for 200-day MA warm-up
stock_prices AS (
    SELECT
        symbol,
        date,
        adj_close AS price
    FROM {{ ref('stg_sp500_companies_prices') }}
    WHERE adj_close IS NOT NULL
        AND adj_close > 0
        AND date >= CURRENT_DATE - INTERVAL '4 years'
),

-- 200-day SMA per stock (computed on the full 4-year range)
stock_with_ma AS (
    SELECT
        symbol,
        date,
        price,
        AVG(price) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
        ) AS sma_200,
        COUNT(*) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
        ) AS ma_200_days_count
    FROM stock_prices
),

-- Flag above/below 200-day MA (only when enough data), trim to last 3 years
stock_ma_flags AS (
    SELECT
        symbol,
        date,
        CASE WHEN ma_200_days_count >= 200 AND price > sma_200 THEN 1 ELSE 0 END AS above_200_ma,
        CASE WHEN ma_200_days_count >= 200 THEN 1 ELSE 0 END AS has_valid_ma
    FROM stock_with_ma
    WHERE date >= CURRENT_DATE - INTERVAL '3 years'
),

-- Join with sector metadata
sector_join AS (
    SELECT
        smf.date,
        smf.above_200_ma,
        smf.has_valid_ma,
        c.sector AS gics_sector,
        sm.sector_display_name
    FROM stock_ma_flags smf
    INNER JOIN {{ source('staging', 'sp500_companies_raw') }} c
        ON smf.symbol = c.symbol
    INNER JOIN sector_mapping sm
        ON c.sector = sm.gics_sector
    WHERE smf.has_valid_ma = 1
),

-- Aggregate per sector per day
sector_daily_breadth AS (
    SELECT
        date,
        gics_sector,
        sector_display_name,
        COUNT(*) AS sector_stock_count,
        SUM(above_200_ma) AS stocks_above_200_ma,
        ROUND(SUM(above_200_ma) * 100.0 / COUNT(*), 1) AS pct_above_200_ma
    FROM sector_join
    GROUP BY date, gics_sector, sector_display_name
),

-- Downsample to weekly (Friday or last trading day of each week)
weekly_breadth AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY gics_sector, DATE_TRUNC('week', date)
            ORDER BY date DESC
        ) AS rn
    FROM sector_daily_breadth
)

SELECT
    date,
    gics_sector,
    sector_display_name,
    sector_stock_count,
    stocks_above_200_ma,
    pct_above_200_ma
FROM weekly_breadth
WHERE rn = 1
ORDER BY date, gics_sector
