{{ config(
    description='Labor market signals: Job openings/unemployed ratio, claims trends'
) }}

/*
    Labor Market Signals Model

    Calculates labor-related signals from FRED data:
    - Job Openings to Unemployed Ratio: JTSJOL / UNEMPLOY (Fed's key tightness metric)
    - Initial Claims Trend: 4-week MA direction and level
    - Employment-Population Ratio trend

    Phase 1 uses only currently-ingested data.
    Sahm Rule (SAHMCURRENT) is now ingested and used directly.
    Hiring Rate (JTSHIR) and Quits/Layoffs will be added when those FRED series
    are ingested (see linked issue).
*/

WITH job_openings AS (
    SELECT
        DATE_TRUNC(date, MONTH) AS month_date,
        MAX(literal) AS job_openings
    FROM {{ ref('stg_fred_series') }}
    WHERE
        series_code = 'JTSJOL'
        AND literal IS NOT NULL
    GROUP BY DATE_TRUNC(date, MONTH)
),

unemployed AS (
    SELECT
        DATE_TRUNC(date, MONTH) AS month_date,
        MAX(literal) AS unemployed_count
    FROM {{ ref('stg_fred_series') }}
    WHERE
        series_code = 'UNEMPLOY'
        AND literal IS NOT NULL
    GROUP BY DATE_TRUNC(date, MONTH)
),

unemployment_rate AS (
    SELECT
        DATE_TRUNC(date, MONTH) AS month_date,
        MAX(literal) AS unrate
    FROM {{ ref('stg_fred_series') }}
    WHERE
        series_code = 'UNRATE'
        AND literal IS NOT NULL
    GROUP BY DATE_TRUNC(date, MONTH)
),

sahm_rule AS (
    SELECT
        DATE_TRUNC(date, MONTH) AS month_date,
        MAX(literal) AS sahm_rule
    FROM {{ ref('stg_fred_series') }}
    WHERE
        series_code = 'SAHMCURRENT'
        AND literal IS NOT NULL
    GROUP BY DATE_TRUNC(date, MONTH)
),

initial_claims AS (
    SELECT
        date,
        literal AS claims
    FROM {{ ref('stg_fred_series') }}
    WHERE
        series_code = 'ICSA'
        AND literal IS NOT NULL
),

claims_monthly AS (
    SELECT
        DATE_TRUNC(date, MONTH) AS month_date,
        AVG(claims) AS avg_monthly_claims,
        MAX(claims) AS max_monthly_claims,
        MIN(claims) AS min_monthly_claims
    FROM initial_claims
    GROUP BY DATE_TRUNC(date, MONTH)
),

emp_pop_ratio AS (
    SELECT
        DATE_TRUNC(date, MONTH) AS month_date,
        MAX(literal) AS emratio
    FROM {{ ref('stg_fred_series') }}
    WHERE
        series_code = 'EMRATIO'
        AND literal IS NOT NULL
    GROUP BY DATE_TRUNC(date, MONTH)
),

quits_rate AS (
    SELECT
        DATE_TRUNC(date, MONTH) AS month_date,
        MAX(literal) AS quits_rate
    FROM {{ ref('stg_fred_series') }}
    WHERE
        series_code = 'JTSQUR'
        AND literal IS NOT NULL
    GROUP BY DATE_TRUNC(date, MONTH)
),

combined AS (
    SELECT
        COALESCE(jo.month_date, u.month_date, ur.month_date, cm.month_date) AS date,
        jo.job_openings,
        u.unemployed_count,
        ROUND(jo.job_openings / NULLIF(u.unemployed_count, 0), 3) AS jo_unemployed_ratio,
        ur.unrate,
        cm.avg_monthly_claims,
        ep.emratio,
        qr.quits_rate,
        sr.sahm_rule
    FROM job_openings AS jo
    FULL OUTER JOIN unemployed AS u ON jo.month_date = u.month_date
    FULL OUTER JOIN unemployment_rate AS ur ON COALESCE(jo.month_date, u.month_date) = ur.month_date
    FULL OUTER JOIN claims_monthly AS cm ON COALESCE(jo.month_date, u.month_date, ur.month_date) = cm.month_date
    FULL OUTER JOIN emp_pop_ratio AS ep ON COALESCE(jo.month_date, u.month_date, ur.month_date, cm.month_date) = ep.month_date
    FULL OUTER JOIN quits_rate AS qr ON COALESCE(jo.month_date, u.month_date, ur.month_date, cm.month_date, ep.month_date) = qr.month_date
    FULL OUTER JOIN sahm_rule AS sr ON COALESCE(jo.month_date, u.month_date, ur.month_date, cm.month_date, ep.month_date, qr.month_date) = sr.month_date
),

with_trends AS (
    SELECT
        *,
        LAG(jo_unemployed_ratio, 3) OVER (ORDER BY date) AS jo_ratio_3m_ago,
        LAG(jo_unemployed_ratio, 6) OVER (ORDER BY date) AS jo_ratio_6m_ago,
        LAG(avg_monthly_claims, 3) OVER (ORDER BY date) AS claims_3m_ago,
        LAG(unrate, 3) OVER (ORDER BY date) AS unrate_3m_ago,
        LAG(quits_rate, 3) OVER (ORDER BY date) AS quits_rate_3m_ago
    FROM combined
)

SELECT
    date,
    job_openings,
    unemployed_count,
    jo_unemployed_ratio,
    unrate,
    avg_monthly_claims,
    emratio,
    quits_rate,

    -- Trends
    ROUND(jo_unemployed_ratio - jo_ratio_3m_ago, 3) AS jo_ratio_3m_change,
    ROUND(((avg_monthly_claims - claims_3m_ago) / NULLIF(claims_3m_ago, 0)) * 100, 2) AS claims_3m_pct_change,
    ROUND(unrate - unrate_3m_ago, 2) AS unrate_3m_change,
    ROUND(quits_rate - quits_rate_3m_ago, 2) AS quits_rate_3m_change,

    -- Sahm Rule indicator (direct from SAHMCURRENT)
    ROUND(sahm_rule, 2) AS sahm_approx,

    -- Signal: Job Openings / Unemployed ratio
    CASE
        WHEN jo_unemployed_ratio < 0.5 THEN 'high'
        WHEN jo_unemployed_ratio < 1.0 THEN 'medium'
        WHEN jo_unemployed_ratio > 1.5 THEN 'low'
        ELSE 'normal'
    END AS jo_ratio_status,

    -- Signal: Claims trend
    CASE
        WHEN claims_3m_ago IS NOT NULL AND ((avg_monthly_claims - claims_3m_ago) / NULLIF(claims_3m_ago, 0)) > 0.15 THEN 'high'
        WHEN claims_3m_ago IS NOT NULL AND ((avg_monthly_claims - claims_3m_ago) / NULLIF(claims_3m_ago, 0)) > 0.10 THEN 'medium'
        ELSE 'normal'
    END AS claims_trend_status,

    -- Signal: Sahm Rule (direct)
    CASE
        WHEN sahm_rule >= 0.50 THEN 'high'
        WHEN sahm_rule >= 0.30 THEN 'medium'
        ELSE 'normal'
    END AS sahm_approx_status,

    -- Signal: Quits rate declining (workers less confident)
    CASE
        WHEN quits_rate_3m_ago IS NOT NULL AND quits_rate - quits_rate_3m_ago < -0.5 THEN 'high'
        WHEN quits_rate_3m_ago IS NOT NULL AND quits_rate - quits_rate_3m_ago < -0.3 THEN 'medium'
        ELSE 'normal'
    END AS quits_trend_status

FROM with_trends
WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR)
ORDER BY date DESC
