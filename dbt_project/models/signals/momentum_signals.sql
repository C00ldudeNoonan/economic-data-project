{{ config(
    description='Momentum & trend-following signals from market return and price data'
) }}

/*
    Momentum Signals Model

    Calculates momentum and trend-following signals from existing market data:
    - TSMOM (Time-Series Momentum): 12mo return minus 1mo return
    - Dual Momentum: SPY vs ACWI relative + absolute momentum
    - Faber TAA: Count of assets above their 200-day SMA
    - Sector Rotation: Dispersion across 11 SPDR sector ETFs
    - Multi-Timeframe Trend Score: Sign of 1mo/3mo/6mo/12mo returns for SPY

    Produces one row per date for historical time series.

    Source tables: *_analysis_return (trailing returns), stg_* (daily prices for SMA)
*/

WITH spy_returns AS (
    SELECT
        date,
        pct_change_1mo,
        pct_change_3mo,
        pct_change_6mo,
        pct_change_1yr
    FROM {{ ref('major_indicies_analysis_return') }}
    WHERE symbol = 'SPY'
),

intl_returns AS (
    SELECT
        date,
        pct_change_1yr AS acwi_12m_return
    FROM {{ ref('global_markets_analysis_return') }}
    WHERE symbol = 'ACWI'
),

bond_returns AS (
    SELECT
        date,
        pct_change_1yr AS govt_12m_return
    FROM {{ ref('fixed_income_analysis_return') }}
    WHERE symbol = 'GOVT'
),

-- Sector rotation: compute per-date dispersion and top/bottom sectors
sector_returns AS (
    SELECT
        date,
        symbol,
        (COALESCE(pct_change_1mo, 0) + COALESCE(pct_change_3mo, 0)) / 2.0 AS avg_momentum
    FROM {{ ref('us_sector_analysis_return') }}
),

sector_stats AS (
    SELECT
        date,
        MAX(avg_momentum) - MIN(avg_momentum) AS dispersion,
        ARRAY_AGG(symbol ORDER BY avg_momentum DESC LIMIT 1)[OFFSET(0)] AS top_sector,
        ARRAY_AGG(symbol ORDER BY avg_momentum ASC LIMIT 1)[OFFSET(0)] AS bottom_sector
    FROM sector_returns
    GROUP BY date
),

-- 200-day SMA for each asset (daily data, joined by date)
spy_sma AS (
    SELECT
        date,
        CASE WHEN adj_close > AVG(adj_close) OVER (ORDER BY date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) THEN 1 ELSE 0 END AS above_sma
    FROM {{ ref('stg_major_indices') }}
    WHERE symbol = 'SPY'
      AND adj_close IS NOT NULL
),

acwi_sma AS (
    SELECT
        date,
        CASE WHEN adj_close > AVG(adj_close) OVER (ORDER BY date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) THEN 1 ELSE 0 END AS above_sma
    FROM {{ ref('stg_global_markets') }}
    WHERE symbol = 'ACWI'
      AND adj_close IS NOT NULL
),

govt_sma AS (
    SELECT
        date,
        CASE WHEN adj_close > AVG(adj_close) OVER (ORDER BY date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) THEN 1 ELSE 0 END AS above_sma
    FROM {{ ref('stg_fixed_income') }}
    WHERE symbol = 'GOVT'
      AND adj_close IS NOT NULL
),

xlre_sma AS (
    SELECT
        date,
        CASE WHEN adj_close > AVG(adj_close) OVER (ORDER BY date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) THEN 1 ELSE 0 END AS above_sma
    FROM {{ ref('stg_us_sectors') }}
    WHERE symbol = 'XLRE'
      AND adj_close IS NOT NULL
),

-- Faber TAA: count of assets above 200d SMA per date
faber AS (
    SELECT
        s.date,
        COALESCE(s.above_sma, 0) + COALESCE(a.above_sma, 0)
        + COALESCE(g.above_sma, 0) + COALESCE(x.above_sma, 0) AS invested_count
    FROM spy_sma AS s
    LEFT JOIN acwi_sma AS a ON s.date = a.date
    LEFT JOIN govt_sma AS g ON s.date = g.date
    LEFT JOIN xlre_sma AS x ON s.date = x.date
),

-- Join all data by date, anchored on spy_returns dates
final AS (
    SELECT
        spy.date,

        -- TSMOM
        ROUND(spy.pct_change_1yr - spy.pct_change_1mo, 4) AS tsmom_return,
        SIGN(spy.pct_change_1yr - spy.pct_change_1mo) AS tsmom_signal,

        -- Dual Momentum
        spy.pct_change_1yr AS spy_12m_return,
        intl.acwi_12m_return,
        bond.govt_12m_return,
        CASE
            WHEN spy.pct_change_1yr >= intl.acwi_12m_return AND spy.pct_change_1yr > 0 THEN 'equities'
            WHEN intl.acwi_12m_return > spy.pct_change_1yr AND intl.acwi_12m_return > 0 THEN 'international'
            WHEN GREATEST(spy.pct_change_1yr, intl.acwi_12m_return) <= 0 AND bond.govt_12m_return > 0 THEN 'bonds'
            ELSE 'cash'
        END AS dual_momentum_position,

        -- Faber TAA
        f.invested_count AS faber_invested_count,

        -- Sector Rotation
        ROUND(ss.dispersion * 100, 2) AS sector_dispersion,
        ss.top_sector,
        ss.bottom_sector,

        -- Multi-Timeframe Trend Score
        SIGN(spy.pct_change_1mo) + SIGN(spy.pct_change_3mo)
        + SIGN(spy.pct_change_6mo) + SIGN(spy.pct_change_1yr) AS trend_score,

        -- Status columns
        CASE
            WHEN SIGN(spy.pct_change_1yr - spy.pct_change_1mo) = -1 THEN 'high'
            WHEN SIGN(spy.pct_change_1yr - spy.pct_change_1mo) = 1 AND (spy.pct_change_1yr - spy.pct_change_1mo) > 0.20 THEN 'low'
            ELSE 'normal'
        END AS tsmom_status,

        CASE
            WHEN GREATEST(spy.pct_change_1yr, intl.acwi_12m_return) <= 0 AND bond.govt_12m_return <= 0 THEN 'high'
            WHEN GREATEST(spy.pct_change_1yr, intl.acwi_12m_return) <= 0 AND bond.govt_12m_return > 0 THEN 'medium'
            ELSE 'normal'
        END AS dual_momentum_status,

        CASE
            WHEN f.invested_count <= 1 THEN 'high'
            WHEN f.invested_count = 2 THEN 'medium'
            ELSE 'normal'
        END AS faber_taa_status,

        CASE
            WHEN ss.dispersion * 100 > 30 THEN 'high'
            WHEN ss.dispersion * 100 > 20 THEN 'medium'
            ELSE 'normal'
        END AS sector_rotation_status,

        CASE
            WHEN SIGN(spy.pct_change_1mo) + SIGN(spy.pct_change_3mo) + SIGN(spy.pct_change_6mo) + SIGN(spy.pct_change_1yr) <= -3 THEN 'high'
            WHEN SIGN(spy.pct_change_1mo) + SIGN(spy.pct_change_3mo) + SIGN(spy.pct_change_6mo) + SIGN(spy.pct_change_1yr) = -2 THEN 'medium'
            WHEN SIGN(spy.pct_change_1mo) + SIGN(spy.pct_change_3mo) + SIGN(spy.pct_change_6mo) + SIGN(spy.pct_change_1yr) = 4 THEN 'low'
            ELSE 'normal'
        END AS trend_score_status

    FROM spy_returns AS spy
    LEFT JOIN intl_returns AS intl ON spy.date = intl.date
    LEFT JOIN bond_returns AS bond ON spy.date = bond.date
    LEFT JOIN faber AS f ON spy.date = f.date
    LEFT JOIN sector_stats AS ss ON spy.date = ss.date
)

SELECT *
FROM final
WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR)
ORDER BY date DESC
