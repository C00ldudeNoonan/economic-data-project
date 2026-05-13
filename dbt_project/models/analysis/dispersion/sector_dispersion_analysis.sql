{{
  config(
    description='Per-sector return dispersion metrics: std dev, spread, avg/median returns, leaders/laggards, and calendar-year trends'
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

-- Compute true trailing-12M returns directly from price data
-- (sp500_companies_summary uses mutually exclusive time buckets, so its
-- '1_year' period only covers the 6-to-12 month window, not a full year)
trailing_prices AS (
    SELECT
        p.symbol,
        p.date,
        p.adj_close,
        ROW_NUMBER() OVER (PARTITION BY p.symbol ORDER BY p.date ASC) AS rn_first,
        ROW_NUMBER() OVER (PARTITION BY p.symbol ORDER BY p.date DESC) AS rn_last
    FROM {{ ref('stg_sp500_companies_prices') }} p
    WHERE p.adj_close IS NOT NULL
        AND p.adj_close > 0
        AND p.date >= CURRENT_DATE - INTERVAL '1 year'
),

stock_trailing_returns AS (
    SELECT
        t_first.symbol,
        CASE WHEN t_first.adj_close > 0
            THEN ROUND(((t_last.adj_close - t_first.adj_close) / t_first.adj_close) * 100, 2)
        END AS return_1y
    FROM trailing_prices t_first
    INNER JOIN trailing_prices t_last
        ON t_first.symbol = t_last.symbol
    WHERE t_first.rn_first = 1 AND t_last.rn_last = 1
),

company_returns AS (
    SELECT
        c.symbol,
        c.sector AS gics_sector,
        sm.etf_symbol,
        sm.sector_display_name,
        str.return_1y
    FROM {{ source('staging', 'sp500_companies_raw') }} c
    INNER JOIN stock_trailing_returns str
        ON c.symbol = str.symbol
    INNER JOIN sector_mapping sm
        ON c.sector = sm.gics_sector
    WHERE str.return_1y IS NOT NULL
        AND c.symbol IS NOT NULL
),

-- Aggregate dispersion metrics per sector
sector_stats AS (
    SELECT
        gics_sector,
        etf_symbol,
        sector_display_name,
        COUNT(*) AS stock_count,
        ROUND(STDDEV_SAMP(return_1y), 2) AS intra_sector_std_dev,
        ROUND(MAX(return_1y) - MIN(return_1y), 2) AS best_worst_spread,
        ROUND(AVG(return_1y), 2) AS avg_return,
        ROUND(MEDIAN(return_1y), 2) AS median_return
    FROM company_returns
    GROUP BY gics_sector, etf_symbol, sector_display_name
),

-- Rank performers within each sector for leaders/laggards
ranked_performers AS (
    SELECT
        cr.*,
        ROW_NUMBER() OVER (
            PARTITION BY cr.gics_sector ORDER BY cr.return_1y DESC
        ) AS rank_best,
        ROW_NUMBER() OVER (
            PARTITION BY cr.gics_sector ORDER BY cr.return_1y ASC
        ) AS rank_worst
    FROM company_returns cr
),

-- Pivot top/bottom 2 performers per sector
leaders_laggards AS (
    SELECT
        gics_sector,
        MAX(CASE WHEN rank_best = 1 THEN symbol END) AS best_performer_symbol,
        MAX(CASE WHEN rank_best = 1 THEN return_1y END) AS best_performer_return,
        MAX(CASE WHEN rank_best = 2 THEN symbol END) AS second_best_symbol,
        MAX(CASE WHEN rank_best = 2 THEN return_1y END) AS second_best_return,
        MAX(CASE WHEN rank_worst = 1 THEN symbol END) AS worst_performer_symbol,
        MAX(CASE WHEN rank_worst = 1 THEN return_1y END) AS worst_performer_return,
        MAX(CASE WHEN rank_worst = 2 THEN symbol END) AS second_worst_symbol,
        MAX(CASE WHEN rank_worst = 2 THEN return_1y END) AS second_worst_return
    FROM ranked_performers
    GROUP BY gics_sector
),

-- Calendar-year returns from price data (2023 and 2024)
year_boundary_prices AS (
    SELECT
        p.symbol,
        c.sector AS gics_sector,
        EXTRACT(YEAR FROM p.date) AS yr,
        FIRST_VALUE(p.adj_close) OVER (
            PARTITION BY p.symbol, EXTRACT(YEAR FROM p.date)
            ORDER BY p.date ASC
        ) AS first_price,
        LAST_VALUE(p.adj_close) OVER (
            PARTITION BY p.symbol, EXTRACT(YEAR FROM p.date)
            ORDER BY p.date ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS last_price
    FROM {{ ref('stg_sp500_companies_prices') }} p
    INNER JOIN {{ source('staging', 'sp500_companies_raw') }} c
        ON p.symbol = c.symbol
    WHERE EXTRACT(YEAR FROM p.date) IN (2023, 2024)
        AND p.adj_close IS NOT NULL
        AND p.adj_close > 0
),

stock_annual_returns AS (
    SELECT DISTINCT
        symbol,
        gics_sector,
        yr,
        CASE WHEN first_price > 0
            THEN ROUND(((last_price - first_price) / first_price) * 100, 2)
        END AS annual_return
    FROM year_boundary_prices
),

calendar_year_returns AS (
    SELECT
        sm.gics_sector,
        ROUND(AVG(CASE WHEN sar.yr = 2023 THEN sar.annual_return END), 2) AS return_2023,
        ROUND(AVG(CASE WHEN sar.yr = 2024 THEN sar.annual_return END), 2) AS return_2024
    FROM stock_annual_returns sar
    INNER JOIN sector_mapping sm ON sar.gics_sector = sm.gics_sector
    GROUP BY sm.gics_sector
)

SELECT
    ss.sector_display_name,
    ss.gics_sector,
    ss.etf_symbol,
    ss.stock_count,
    ss.intra_sector_std_dev,
    ss.best_worst_spread,
    ss.avg_return,
    ss.median_return,
    ll.best_performer_symbol,
    ll.best_performer_return,
    ll.second_best_symbol,
    ll.second_best_return,
    ll.worst_performer_symbol,
    ll.worst_performer_return,
    ll.second_worst_symbol,
    ll.second_worst_return,
    cyr.return_2023,
    cyr.return_2024,
    ss.avg_return AS return_trailing_1y
FROM sector_stats ss
LEFT JOIN leaders_laggards ll ON ss.gics_sector = ll.gics_sector
LEFT JOIN calendar_year_returns cyr ON ss.gics_sector = cyr.gics_sector
ORDER BY ss.intra_sector_std_dev DESC
