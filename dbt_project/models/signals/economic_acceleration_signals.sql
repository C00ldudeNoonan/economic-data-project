{{ config(
    description='Economic acceleration (second derivative) signals: detects inflection points in PAYEMS, CPI, GDP'
) }}

/*
    Economic Acceleration Signals (Second Derivatives)

    Markets price inflection points, not levels. This model computes the second
    derivative (change in the rate of change) for key macro series:
    - Nonfarm Payrolls (PAYEMS): monthly, employment acceleration
    - CPI (CPIAUCSL): monthly, inflation acceleration
    - Real GDP (GDPC1): quarterly, growth acceleration

    Academic basis: Second derivatives catch turning points 3-6 months early.
    Consecutive negative acceleration in payrolls is a strong recession signal.
*/

WITH payems_raw AS (
    SELECT
        DATE_TRUNC('month', date) AS month_date,
        MAX(value) AS payems
    FROM {{ ref('stg_fred_series') }}
    WHERE series_code = 'PAYEMS'
      AND value IS NOT NULL
    GROUP BY DATE_TRUNC('month', date)
),

payems_derivatives AS (
    SELECT
        month_date,
        payems,
        -- First derivative: month-over-month percent change
        ROUND(((payems / NULLIF(LAG(payems, 1) OVER (ORDER BY month_date), 0)) - 1) * 100, 4) AS payems_mom_pct,
        -- Previous month's MoM for second derivative
        ROUND(((LAG(payems, 1) OVER (ORDER BY month_date) / NULLIF(LAG(payems, 2) OVER (ORDER BY month_date), 0)) - 1) * 100, 4) AS payems_mom_pct_prev
    FROM payems_raw
),

payems_accel AS (
    SELECT
        month_date,
        payems,
        payems_mom_pct,
        -- Second derivative: change in the rate of change
        ROUND(payems_mom_pct - COALESCE(payems_mom_pct_prev, payems_mom_pct), 4) AS payems_acceleration,
        -- Count consecutive negative acceleration months
        SUM(CASE WHEN (payems_mom_pct - COALESCE(payems_mom_pct_prev, payems_mom_pct)) < 0 THEN 0 ELSE 1 END)
            OVER (ORDER BY month_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS payems_accel_group
    FROM payems_derivatives
),

payems_consecutive AS (
    SELECT
        month_date,
        payems,
        payems_mom_pct,
        payems_acceleration,
        CASE
            WHEN payems_acceleration < 0
            THEN ROW_NUMBER() OVER (PARTITION BY payems_accel_group ORDER BY month_date)
            ELSE 0
        END AS payems_consecutive_negative
    FROM payems_accel
),

-- CPI acceleration (monthly)
cpi_raw AS (
    SELECT
        DATE_TRUNC('month', date) AS month_date,
        MAX(value) AS cpi
    FROM {{ ref('stg_fred_series') }}
    WHERE series_code = 'CPIAUCSL'
      AND value IS NOT NULL
    GROUP BY DATE_TRUNC('month', date)
),

cpi_derivatives AS (
    SELECT
        month_date,
        cpi,
        ROUND(((cpi / NULLIF(LAG(cpi, 1) OVER (ORDER BY month_date), 0)) - 1) * 100, 4) AS cpi_mom_pct,
        ROUND(((LAG(cpi, 1) OVER (ORDER BY month_date) / NULLIF(LAG(cpi, 2) OVER (ORDER BY month_date), 0)) - 1) * 100, 4) AS cpi_mom_pct_prev
    FROM cpi_raw
),

cpi_accel AS (
    SELECT
        month_date,
        cpi,
        cpi_mom_pct,
        ROUND(cpi_mom_pct - COALESCE(cpi_mom_pct_prev, cpi_mom_pct), 4) AS cpi_acceleration
    FROM cpi_derivatives
),

-- GDP acceleration (quarterly)
gdp_raw AS (
    SELECT
        date AS quarter_date,
        value AS gdp
    FROM {{ ref('stg_fred_series') }}
    WHERE series_code = 'GDPC1'
      AND value IS NOT NULL
),

gdp_derivatives AS (
    SELECT
        quarter_date,
        gdp,
        ROUND(((gdp / NULLIF(LAG(gdp, 1) OVER (ORDER BY quarter_date), 0)) - 1) * 100, 4) AS gdp_qoq_pct,
        ROUND(((LAG(gdp, 1) OVER (ORDER BY quarter_date) / NULLIF(LAG(gdp, 2) OVER (ORDER BY quarter_date), 0)) - 1) * 100, 4) AS gdp_qoq_pct_prev
    FROM gdp_raw
),

gdp_accel AS (
    SELECT
        quarter_date,
        gdp,
        gdp_qoq_pct,
        ROUND(gdp_qoq_pct - COALESCE(gdp_qoq_pct_prev, gdp_qoq_pct), 4) AS gdp_acceleration
    FROM gdp_derivatives
),

-- Combine: use monthly grain, forward-fill GDP quarterly into monthly
combined AS (
    SELECT
        p.month_date AS date,
        p.payems,
        p.payems_mom_pct,
        p.payems_acceleration,
        p.payems_consecutive_negative,
        c.cpi_mom_pct,
        c.cpi_acceleration,
        g.gdp_qoq_pct,
        g.gdp_acceleration
    FROM payems_consecutive p
    LEFT JOIN cpi_accel c ON p.month_date = c.month_date
    LEFT JOIN gdp_accel g ON DATE_TRUNC('quarter', p.month_date) = g.quarter_date
),

with_stats AS (
    SELECT
        *,
        -- Rolling z-scores (24-month window)
        (payems_acceleration - AVG(payems_acceleration) OVER (ORDER BY date ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING))
            / NULLIF(STDDEV(payems_acceleration) OVER (ORDER BY date ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING), 0)
            AS payems_accel_zscore,
        (cpi_acceleration - AVG(cpi_acceleration) OVER (ORDER BY date ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING))
            / NULLIF(STDDEV(cpi_acceleration) OVER (ORDER BY date ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING), 0)
            AS cpi_accel_zscore
    FROM combined
)

SELECT
    date,
    payems,
    ROUND(payems_mom_pct, 2) AS payems_mom_pct,
    ROUND(payems_acceleration, 4) AS payems_acceleration,
    payems_consecutive_negative,
    ROUND(payems_accel_zscore, 2) AS payems_accel_zscore,
    ROUND(cpi_mom_pct, 2) AS cpi_mom_pct,
    ROUND(cpi_acceleration, 4) AS cpi_acceleration,
    ROUND(cpi_accel_zscore, 2) AS cpi_accel_zscore,
    ROUND(gdp_qoq_pct, 2) AS gdp_qoq_pct,
    ROUND(gdp_acceleration, 4) AS gdp_acceleration,

    -- Composite: average of available z-scores (simple composite)
    ROUND(
        (COALESCE(payems_accel_zscore, 0) + COALESCE(cpi_accel_zscore, 0)) /
        (CASE WHEN payems_accel_zscore IS NOT NULL THEN 1 ELSE 0 END + CASE WHEN cpi_accel_zscore IS NOT NULL THEN 1 ELSE 0 END),
        2
    ) AS composite_accel_zscore,

    -- Payroll acceleration status (key recession signal)
    CASE
        WHEN payems_consecutive_negative >= 3 THEN 'high'
        WHEN payems_consecutive_negative >= 2 THEN 'medium'
        WHEN payems_acceleration < 0 THEN 'low'
        ELSE 'normal'
    END AS payems_accel_status,

    -- CPI acceleration status
    CASE
        WHEN cpi_acceleration > 0.1 THEN 'high'
        WHEN cpi_acceleration > 0 THEN 'medium'
        WHEN cpi_acceleration < -0.1 THEN 'low'
        ELSE 'normal'
    END AS cpi_accel_status,

    -- GDP acceleration status
    CASE
        WHEN gdp_acceleration < -0.5 THEN 'high'
        WHEN gdp_acceleration < -0.2 THEN 'medium'
        WHEN gdp_acceleration < 0 THEN 'low'
        ELSE 'normal'
    END AS gdp_accel_status

FROM with_stats
WHERE date >= CURRENT_DATE - INTERVAL '3 years'
ORDER BY date DESC
