{{ config(
    description='Housing market signals: starts momentum, permits/starts ratio, mortgage stress, supply'
) }}

/*
    Housing Market Signals Model

    Calculates housing-related signals from FRED data:
    - Housing Starts Momentum: YoY rate of change in HOUST
    - Permits-to-Starts Ratio: PERMIT / HOUST pipeline indicator
    - Mortgage Rate Stress: 30Y mortgage rate level and trend
    - Housing Supply: MSACSR months of supply

    All source data already ingested via FRED.
*/

WITH housing_starts AS (
    SELECT
        DATE_TRUNC('month', date) AS month_date,
        MAX(literal) AS starts
    FROM {{ ref('stg_fred_series') }}
    WHERE
        series_code = 'HOUST'
        AND literal IS NOT NULL
    GROUP BY DATE_TRUNC('month', date)
),

building_permits AS (
    SELECT
        DATE_TRUNC('month', date) AS month_date,
        MAX(literal) AS permits
    FROM {{ ref('stg_fred_series') }}
    WHERE
        series_code = 'PERMIT'
        AND literal IS NOT NULL
    GROUP BY DATE_TRUNC('month', date)
),

mortgage_rate AS (
    SELECT
        date,
        literal AS rate_30y
    FROM {{ ref('stg_fred_series') }}
    WHERE
        series_code = 'MORTGAGE30US'
        AND literal IS NOT NULL
),

mortgage_monthly AS (
    SELECT
        DATE_TRUNC('month', date) AS month_date,
        AVG(rate_30y) AS avg_mortgage_rate,
        MAX(rate_30y) AS max_mortgage_rate
    FROM mortgage_rate
    GROUP BY DATE_TRUNC('month', date)
),

months_supply AS (
    SELECT
        DATE_TRUNC('month', date) AS month_date,
        MAX(literal) AS months_of_supply
    FROM {{ ref('stg_fred_series') }}
    WHERE
        series_code = 'MSACSR'
        AND literal IS NOT NULL
    GROUP BY DATE_TRUNC('month', date)
),

combined AS (
    SELECT
        COALESCE(hs.month_date, bp.month_date, mm.month_date, ms.month_date) AS date,
        hs.starts,
        bp.permits,
        mm.avg_mortgage_rate,
        ms.months_of_supply
    FROM housing_starts AS hs
    FULL OUTER JOIN building_permits AS bp ON hs.month_date = bp.month_date
    FULL OUTER JOIN mortgage_monthly AS mm ON COALESCE(hs.month_date, bp.month_date) = mm.month_date
    FULL OUTER JOIN months_supply AS ms ON COALESCE(hs.month_date, bp.month_date, mm.month_date) = ms.month_date
),

with_trends AS (
    SELECT
        *,
        -- YoY changes
        LAG(starts, 12) OVER (ORDER BY date) AS starts_12m_ago,
        LAG(starts, 3) OVER (ORDER BY date) AS starts_3m_ago,
        -- Permits-to-starts ratio
        ROUND(permits / NULLIF(starts, 0), 3) AS permits_starts_ratio,
        -- Mortgage rate trends
        LAG(avg_mortgage_rate, 3) OVER (ORDER BY date) AS mortgage_3m_ago,
        LAG(avg_mortgage_rate, 12) OVER (ORDER BY date) AS mortgage_12m_ago,
        -- Supply trends
        LAG(months_of_supply, 3) OVER (ORDER BY date) AS supply_3m_ago,
        -- 3-month moving averages
        AVG(starts) OVER (ORDER BY date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS starts_3m_avg,
        AVG(permits) OVER (ORDER BY date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS permits_3m_avg
    FROM combined
)

SELECT
    date,
    starts,
    permits,
    avg_mortgage_rate,
    months_of_supply,
    permits_starts_ratio,

    -- Housing starts YoY
    ROUND(((starts - starts_12m_ago) / NULLIF(starts_12m_ago, 0)) * 100, 2) AS starts_yoy_pct,
    -- Housing starts 3m change
    ROUND(((starts - starts_3m_ago) / NULLIF(starts_3m_ago, 0)) * 100, 2) AS starts_3m_pct,

    -- Mortgage rate changes
    ROUND(avg_mortgage_rate - mortgage_3m_ago, 2) AS mortgage_3m_change,
    ROUND(avg_mortgage_rate - mortgage_12m_ago, 2) AS mortgage_12m_change,

    -- Signal: Housing starts momentum
    CASE
        WHEN starts_12m_ago IS NOT NULL
            AND ((starts - starts_12m_ago) / NULLIF(starts_12m_ago, 0)) * 100 < -10 THEN 'high'
        WHEN starts_12m_ago IS NOT NULL
            AND ((starts - starts_12m_ago) / NULLIF(starts_12m_ago, 0)) * 100 < -5 THEN 'medium'
        WHEN starts_12m_ago IS NOT NULL
            AND ((starts - starts_12m_ago) / NULLIF(starts_12m_ago, 0)) * 100 > 10 THEN 'low'
        ELSE 'normal'
    END AS starts_momentum_status,

    -- Signal: Permits-to-starts pipeline
    CASE
        WHEN permits / NULLIF(starts, 0) < 0.9 THEN 'medium'
        WHEN permits / NULLIF(starts, 0) > 1.15 THEN 'low'
        ELSE 'normal'
    END AS permits_pipeline_status,

    -- Signal: Mortgage rate stress
    CASE
        WHEN avg_mortgage_rate > 7.5 THEN 'high'
        WHEN avg_mortgage_rate > 6.5 THEN 'medium'
        WHEN avg_mortgage_rate < 4.0 THEN 'low'
        ELSE 'normal'
    END AS mortgage_stress_status,

    -- Signal: Housing supply
    CASE
        WHEN months_of_supply > 7 THEN 'high'
        WHEN months_of_supply > 6 THEN 'medium'
        WHEN months_of_supply < 4 THEN 'medium'
        ELSE 'normal'
    END AS supply_status

FROM with_trends
WHERE date >= CURRENT_DATE - INTERVAL 3 YEAR
ORDER BY date DESC
