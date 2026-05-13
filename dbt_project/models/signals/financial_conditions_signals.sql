{{ config(
    description='Financial conditions signals: NFCI, bank lending standards, leverage'
) }}

/*
    Financial Conditions Signals Model

    Calculates financial conditions signals from FRED data:
    - NFCI Level: Chicago Fed National Financial Conditions Index
    - Bank Lending Standards: Senior Loan Officer Survey (large + small firms)
    - NFCI Sub-indices: Credit, Leverage, Nonfinancial leverage

    Includes ANFCI, STLFSI4, and DRCCLACBS now that series are ingested.
*/

WITH nfci AS (
    SELECT
        date,
        literal AS nfci_value
    FROM {{ ref('stg_fred_series') }}
    WHERE
        series_code = 'NFCI'
        AND literal IS NOT NULL
),

anfci AS (
    SELECT
        date,
        literal AS anfci_value
    FROM {{ ref('stg_fred_series') }}
    WHERE
        series_code = 'ANFCI'
        AND literal IS NOT NULL
),

stl_fsi AS (
    SELECT
        date,
        literal AS stl_fsi_value
    FROM {{ ref('stg_fred_series') }}
    WHERE
        series_code = 'STLFSI4'
        AND literal IS NOT NULL
),

nfci_credit AS (
    SELECT
        date,
        literal AS nfci_credit
    FROM {{ ref('stg_fred_series') }}
    WHERE
        series_code = 'NFCICREDIT'
        AND literal IS NOT NULL
),

nfci_leverage AS (
    SELECT
        date,
        literal AS nfci_leverage
    FROM {{ ref('stg_fred_series') }}
    WHERE
        series_code = 'NFCINONFINLEVERAGE'
        AND literal IS NOT NULL
),

cc_delinquency AS (
    SELECT
        DATE_TRUNC('month', date) AS month_date,
        MAX(literal) AS cc_delinquency_rate
    FROM {{ ref('stg_fred_series') }}
    WHERE
        series_code = 'DRCCLACBS'
        AND literal IS NOT NULL
    GROUP BY DATE_TRUNC('month', date)
),

-- Bank lending standards (large and small firms)
lending_large AS (
    SELECT
        DATE_TRUNC('month', date) AS month_date,
        MAX(literal) AS lending_standards_large
    FROM {{ ref('stg_fred_series') }}
    WHERE
        series_code = 'DRTSCILM'
        AND literal IS NOT NULL
    GROUP BY DATE_TRUNC('month', date)
),

lending_small AS (
    SELECT
        DATE_TRUNC('month', date) AS month_date,
        MAX(literal) AS lending_standards_small
    FROM {{ ref('stg_fred_series') }}
    WHERE
        series_code = 'DRTSCIS'
        AND literal IS NOT NULL
    GROUP BY DATE_TRUNC('month', date)
),

-- Combine NFCI components (weekly data)
nfci_combined AS (
    SELECT
        n.date,
        n.nfci_value,
        nc.nfci_credit,
        nl.nfci_leverage,
        an.anfci_value,
        sf.stl_fsi_value,
        LAG(n.nfci_value, 4) OVER (ORDER BY n.date) AS nfci_4w_ago,
        LAG(n.nfci_value, 13) OVER (ORDER BY n.date) AS nfci_13w_ago,
        AVG(n.nfci_value) OVER (ORDER BY n.date ROWS BETWEEN 12 PRECEDING AND CURRENT ROW) AS nfci_13w_avg
    FROM nfci AS n
    LEFT JOIN nfci_credit AS nc ON n.date = nc.date
    LEFT JOIN nfci_leverage AS nl ON n.date = nl.date
    LEFT JOIN anfci AS an ON n.date = an.date
    LEFT JOIN stl_fsi AS sf ON n.date = sf.date
),

-- Combine lending standards (quarterly survey data)
lending_combined AS (
    SELECT
        COALESCE(ll.month_date, ls.month_date) AS date,
        ll.lending_standards_large,
        ls.lending_standards_small,
        ROUND((COALESCE(ll.lending_standards_large, 0) + COALESCE(ls.lending_standards_small, 0)) / 2.0, 2) AS lending_standards_avg,
        LAG(ll.lending_standards_large, 1) OVER (ORDER BY COALESCE(ll.month_date, ls.month_date)) AS lending_large_prev,
        LAG(ls.lending_standards_small, 1) OVER (ORDER BY COALESCE(ll.month_date, ls.month_date)) AS lending_small_prev
    FROM lending_large AS ll
    FULL OUTER JOIN lending_small AS ls ON ll.month_date = ls.month_date
),

-- Join everything at monthly grain
final AS (
    SELECT
        COALESCE(nc.date, lc.date, cd.month_date) AS date,
        nc.nfci_value,
        nc.nfci_credit,
        nc.nfci_leverage,
        nc.anfci_value,
        nc.stl_fsi_value,
        nc.nfci_4w_ago,
        nc.nfci_13w_ago,
        nc.nfci_13w_avg,
        ROUND(nc.nfci_value - nc.nfci_4w_ago, 4) AS nfci_4w_change,
        ROUND(nc.nfci_value - nc.nfci_13w_ago, 4) AS nfci_13w_change,
        lc.lending_standards_large,
        lc.lending_standards_small,
        lc.lending_standards_avg,
        cd.cc_delinquency_rate,
        -- Lending direction
        CASE
            WHEN lc.lending_large_prev IS NOT NULL
                THEN ROUND(lc.lending_standards_large - lc.lending_large_prev, 2)
        END AS lending_large_change,
        CASE
            WHEN lc.lending_small_prev IS NOT NULL
                THEN ROUND(lc.lending_standards_small - lc.lending_small_prev, 2)
    END AS lending_small_change
    FROM nfci_combined AS nc
    FULL OUTER JOIN lending_combined AS lc ON nc.date = lc.date
    FULL OUTER JOIN cc_delinquency AS cd
        ON DATE_TRUNC('month', COALESCE(nc.date, lc.date)) = cd.month_date
)

SELECT
    date,
    nfci_value,
    nfci_credit,
    nfci_leverage,
    anfci_value,
    stl_fsi_value,
    nfci_4w_change,
    nfci_13w_change,
    nfci_13w_avg,
    lending_standards_large,
    lending_standards_small,
    lending_standards_avg,
    cc_delinquency_rate,
    lending_large_change,
    lending_small_change,

    -- Signal: NFCI level
    CASE
        WHEN nfci_value > 1.0 THEN 'high'
        WHEN nfci_value > 0.5 THEN 'high'
        WHEN nfci_value > 0.0 THEN 'medium'
        WHEN nfci_value < -0.5 THEN 'low'
        ELSE 'normal'
    END AS nfci_status,

    -- Signal: NFCI trend (tightening or loosening)
    CASE
        WHEN nfci_13w_change > 0.3 THEN 'high'
        WHEN nfci_13w_change > 0.1 THEN 'medium'
        ELSE 'normal'
    END AS nfci_trend_status,

    -- Signal: Bank lending standards
    CASE
        WHEN lending_standards_avg > 30 THEN 'high'
        WHEN lending_standards_avg > 0 AND lending_large_change > 0 THEN 'medium'
        WHEN lending_standards_avg > 0 THEN 'low'
        ELSE 'normal'
    END AS lending_status

FROM final
WHERE date >= CURRENT_DATE - INTERVAL '3 years'
ORDER BY date DESC
