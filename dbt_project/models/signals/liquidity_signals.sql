{{ config(
    description='Liquidity signals: M2 growth rate, money supply trends, velocity, Fed balance sheet'
) }}

/*
    Liquidity Signals Model

    Calculates liquidity-related signals from FRED data:
    - M2 Growth Rate: YoY % change in M2 money supply
    - M1 Growth Rate: YoY % change in M1 (narrower measure)
    - Total Credit Growth: Business loans trend
    - M2 Velocity (M2V): Quarterly GDP/M2 ratio — declining velocity signals liquidity trap risk
    - Fed Balance Sheet (WALCL): Weekly total assets, aggregated monthly — tracks QT/QE
    - Reverse Repo (RRPONTSYD): Daily RRP facility usage, aggregated monthly
*/

WITH m2_data AS (
    SELECT
        DATE_TRUNC('month', date) AS month_date,
        MAX(literal) AS m2_level
    FROM {{ ref('stg_fred_series') }}
    WHERE
        series_code = 'M2SL'
        AND literal IS NOT NULL
    GROUP BY DATE_TRUNC('month', date)
),

m1_data AS (
    SELECT
        DATE_TRUNC('month', date) AS month_date,
        MAX(literal) AS m1_level
    FROM {{ ref('stg_fred_series') }}
    WHERE
        series_code = 'M1SL'
        AND literal IS NOT NULL
    GROUP BY DATE_TRUNC('month', date)
),

business_loans AS (
    SELECT
        DATE_TRUNC('month', date) AS month_date,
        MAX(literal) AS busloans
    FROM {{ ref('stg_fred_series') }}
    WHERE
        series_code = 'BUSLOANS'
        AND literal IS NOT NULL
    GROUP BY DATE_TRUNC('month', date)
),

total_credit AS (
    SELECT
        DATE_TRUNC('month', date) AS month_date,
        MAX(literal) AS total_consumer_credit
    FROM {{ ref('stg_fred_series') }}
    WHERE
        series_code = 'TOTALSL'
        AND literal IS NOT NULL
    GROUP BY DATE_TRUNC('month', date)
),

-- M2 Velocity (quarterly data)
velocity_data AS (
    SELECT
        date,
        literal AS money_velocity,
        LAG(literal, 4) OVER (ORDER BY date) AS velocity_1y_ago,
        ROW_NUMBER() OVER (ORDER BY date DESC) AS rn
    FROM {{ ref('stg_fred_series') }}
    WHERE
        series_code = 'M2V'
        AND literal IS NOT NULL
),

velocity_with_trend AS (
    SELECT
        money_velocity,
        CASE
            WHEN velocity_1y_ago IS NOT NULL AND velocity_1y_ago > 0
            THEN ((money_velocity - velocity_1y_ago) / velocity_1y_ago) * 100
            ELSE NULL
        END AS velocity_yoy_change
    FROM velocity_data
    WHERE rn = 1
),

-- Fed Balance Sheet (weekly -> monthly)
walcl_data AS (
    SELECT
        DATE_TRUNC('month', date) AS month_date,
        AVG(literal) AS walcl_avg
    FROM {{ ref('stg_fred_series') }}
    WHERE
        series_code = 'WALCL'
        AND literal IS NOT NULL
    GROUP BY DATE_TRUNC('month', date)
),

walcl_with_changes AS (
    SELECT
        month_date,
        walcl_avg,
        LAG(walcl_avg, 3) OVER (ORDER BY month_date) AS walcl_3m_ago,
        LAG(walcl_avg, 12) OVER (ORDER BY month_date) AS walcl_12m_ago
    FROM walcl_data
),

-- Reverse Repo (daily -> monthly)
rrp_data AS (
    SELECT
        DATE_TRUNC('month', date) AS month_date,
        AVG(literal) AS rrp_avg
    FROM {{ ref('stg_fred_series') }}
    WHERE
        series_code = 'RRPONTSYD'
        AND literal IS NOT NULL
    GROUP BY DATE_TRUNC('month', date)
),

rrp_with_changes AS (
    SELECT
        month_date,
        rrp_avg,
        LAG(rrp_avg, 3) OVER (ORDER BY month_date) AS rrp_3m_ago
    FROM rrp_data
),

combined AS (
    SELECT
        COALESCE(m2.month_date, m1.month_date, bl.month_date, tc.month_date) AS date,
        m2.m2_level,
        m1.m1_level,
        bl.busloans,
        tc.total_consumer_credit,
        w.walcl_avg,
        w.walcl_3m_ago,
        w.walcl_12m_ago,
        r.rrp_avg,
        r.rrp_3m_ago
    FROM m2_data AS m2
    FULL OUTER JOIN m1_data AS m1 ON m2.month_date = m1.month_date
    FULL OUTER JOIN business_loans AS bl ON COALESCE(m2.month_date, m1.month_date) = bl.month_date
    FULL OUTER JOIN total_credit AS tc ON COALESCE(m2.month_date, m1.month_date, bl.month_date) = tc.month_date
    LEFT JOIN walcl_with_changes AS w ON COALESCE(m2.month_date, m1.month_date) = w.month_date
    LEFT JOIN rrp_with_changes AS r ON COALESCE(m2.month_date, m1.month_date) = r.month_date
),

with_growth AS (
    SELECT
        *,
        LAG(m2_level, 12) OVER (ORDER BY date) AS m2_12m_ago,
        LAG(m2_level, 3) OVER (ORDER BY date) AS m2_3m_ago,
        LAG(m1_level, 12) OVER (ORDER BY date) AS m1_12m_ago,
        LAG(busloans, 12) OVER (ORDER BY date) AS busloans_12m_ago,
        LAG(total_consumer_credit, 12) OVER (ORDER BY date) AS credit_12m_ago
    FROM combined
)

SELECT
    wg.date,
    wg.m2_level,
    wg.m1_level,
    wg.busloans,
    wg.total_consumer_credit,

    -- M2 growth rates
    ROUND(((wg.m2_level / NULLIF(wg.m2_12m_ago, 0)) - 1) * 100, 2) AS m2_yoy_growth,
    ROUND(((wg.m2_level / NULLIF(wg.m2_3m_ago, 0)) - 1) * 400, 2) AS m2_3m_annualized,

    -- M1 growth rate
    ROUND(((wg.m1_level / NULLIF(wg.m1_12m_ago, 0)) - 1) * 100, 2) AS m1_yoy_growth,

    -- Business loans growth
    ROUND(((wg.busloans / NULLIF(wg.busloans_12m_ago, 0)) - 1) * 100, 2) AS busloans_yoy_growth,

    -- Consumer credit growth
    ROUND(((wg.total_consumer_credit / NULLIF(wg.credit_12m_ago, 0)) - 1) * 100, 2) AS consumer_credit_yoy_growth,

    -- Velocity (latest quarterly value, cross-joined)
    vt.money_velocity,
    ROUND(vt.velocity_yoy_change, 2) AS velocity_yoy_change,

    -- Fed balance sheet
    wg.walcl_avg,
    ROUND(((wg.walcl_avg / NULLIF(wg.walcl_3m_ago, 0)) - 1) * 100, 2) AS walcl_3m_pct_change,
    ROUND(((wg.walcl_avg / NULLIF(wg.walcl_12m_ago, 0)) - 1) * 100, 2) AS walcl_12m_pct_change,

    -- Reverse repo
    wg.rrp_avg,
    ROUND(((wg.rrp_avg / NULLIF(wg.rrp_3m_ago, 0)) - 1) * 100, 2) AS rrp_3m_pct_change,

    -- Signal: M2 growth rate
    CASE
        WHEN wg.m2_12m_ago IS NOT NULL AND ((wg.m2_level / NULLIF(wg.m2_12m_ago, 0)) - 1) * 100 < 0 THEN 'high'
        WHEN wg.m2_12m_ago IS NOT NULL AND ((wg.m2_level / NULLIF(wg.m2_12m_ago, 0)) - 1) * 100 < 4 THEN 'medium'
        WHEN wg.m2_12m_ago IS NOT NULL AND ((wg.m2_level / NULLIF(wg.m2_12m_ago, 0)) - 1) * 100 > 10 THEN 'low'
        ELSE 'normal'
    END AS m2_growth_status,

    -- Signal: Business loans growth
    CASE
        WHEN wg.busloans_12m_ago IS NOT NULL AND ((wg.busloans / NULLIF(wg.busloans_12m_ago, 0)) - 1) * 100 < -2 THEN 'high'
        WHEN wg.busloans_12m_ago IS NOT NULL AND ((wg.busloans / NULLIF(wg.busloans_12m_ago, 0)) - 1) * 100 < 0 THEN 'medium'
        ELSE 'normal'
    END AS busloans_growth_status,

    -- Signal: Consumer credit growth
    CASE
        WHEN wg.credit_12m_ago IS NOT NULL AND ((wg.total_consumer_credit / NULLIF(wg.credit_12m_ago, 0)) - 1) * 100 > 10 THEN 'medium'
        WHEN wg.credit_12m_ago IS NOT NULL AND ((wg.total_consumer_credit / NULLIF(wg.credit_12m_ago, 0)) - 1) * 100 < 0 THEN 'high'
        ELSE 'normal'
    END AS consumer_credit_status,

    -- Signal: Velocity declining (liquidity trap risk)
    CASE
        WHEN vt.velocity_yoy_change IS NOT NULL AND vt.velocity_yoy_change < -5 THEN 'high'
        WHEN vt.velocity_yoy_change IS NOT NULL AND vt.velocity_yoy_change < -2 THEN 'medium'
        WHEN vt.velocity_yoy_change IS NOT NULL AND vt.velocity_yoy_change < 0 THEN 'low'
        ELSE 'normal'
    END AS velocity_status,

    -- Signal: Fed balance sheet (QT pace)
    CASE
        WHEN wg.walcl_3m_ago IS NOT NULL AND ((wg.walcl_avg / NULLIF(wg.walcl_3m_ago, 0)) - 1) * 100 < -2 THEN 'medium'
        WHEN wg.walcl_3m_ago IS NOT NULL AND ((wg.walcl_avg / NULLIF(wg.walcl_3m_ago, 0)) - 1) * 100 < -1 THEN 'low'
        ELSE 'normal'
    END AS fed_balance_sheet_status

FROM with_growth wg
CROSS JOIN velocity_with_trend vt
WHERE wg.date >= CURRENT_DATE - INTERVAL '3 years'
ORDER BY wg.date DESC
