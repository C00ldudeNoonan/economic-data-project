{{ config(
    description='Credit market signals: HY OAS momentum, BBB-AAA spread, moving-average crossover, and trailing spread percentiles'
) }}

/*
    Credit Market Signals

    Uses FRED credit spread series to identify widening momentum, fallen-angel
    pressure, and historical high-yield spread regimes.
*/

WITH hy_oas AS (
    SELECT
        date,
        value AS hy_oas
    FROM {{ ref('stg_fred_series') }}
    WHERE
        series_code = 'BAMLH0A0HYM2'
        AND value IS NOT NULL
),

bbb_oas AS (
    SELECT
        date,
        value AS bbb_oas
    FROM {{ ref('stg_fred_series') }}
    WHERE
        series_code = 'BAMLC0A4CBBB'
        AND value IS NOT NULL
),

aaa_oas AS (
    SELECT
        date,
        value AS aaa_oas
    FROM {{ ref('stg_fred_series') }}
    WHERE
        series_code = 'BAMLC0A1CAAA'
        AND value IS NOT NULL
),

base AS (
    SELECT
        hy.date,
        hy.hy_oas,
        bbb.bbb_oas,
        aaa.aaa_oas,
        ROUND(bbb.bbb_oas - aaa.aaa_oas, 4) AS bbb_aaa_spread,
        ROUND(hy.hy_oas - LAG(hy.hy_oas, 20) OVER (ORDER BY hy.date), 4) AS hy_oas_20d_change,
        ROUND(hy.hy_oas - LAG(hy.hy_oas, 60) OVER (ORDER BY hy.date), 4) AS hy_oas_60d_change,
        AVG(hy.hy_oas) OVER (
            ORDER BY hy.date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS hy_oas_20d_avg,
        AVG(hy.hy_oas) OVER (
            ORDER BY hy.date
            ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
        ) AS hy_oas_60d_avg
    FROM hy_oas AS hy
    LEFT JOIN bbb_oas AS bbb ON hy.date = bbb.date
    LEFT JOIN aaa_oas AS aaa ON hy.date = aaa.date
),

with_momentum AS (
    SELECT
        *,
        AVG(hy_oas_20d_change) OVER (
            ORDER BY date
            ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
        ) AS hy_oas_20d_change_1y_avg,
        STDDEV_SAMP(hy_oas_20d_change) OVER (
            ORDER BY date
            ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
        ) AS hy_oas_20d_change_1y_std,
        LAG(hy_oas_20d_avg) OVER (ORDER BY date) AS hy_oas_20d_avg_prev,
        LAG(hy_oas_60d_avg) OVER (ORDER BY date) AS hy_oas_60d_avg_prev
    FROM base
),

with_percentiles AS (
    SELECT
        current_row.*,
        (
            SELECT SAFE_DIVIDE(COUNTIF(history.hy_oas <= current_row.hy_oas), COUNT(*))
            FROM hy_oas AS history
            WHERE
                history.date BETWEEN DATE_SUB(current_row.date, INTERVAL 5 YEAR)
                AND current_row.date
        ) AS hy_oas_percentile_5y,
        (
            SELECT SAFE_DIVIDE(COUNTIF(history.hy_oas <= current_row.hy_oas), COUNT(*))
            FROM hy_oas AS history
            WHERE
                history.date BETWEEN DATE_SUB(current_row.date, INTERVAL 10 YEAR)
                AND current_row.date
        ) AS hy_oas_percentile_10y
    FROM with_momentum AS current_row
)

SELECT
    date,
    ROUND(hy_oas, 4) AS hy_oas,
    ROUND(bbb_oas, 4) AS bbb_oas,
    ROUND(aaa_oas, 4) AS aaa_oas,
    bbb_aaa_spread,
    hy_oas_20d_change,
    hy_oas_60d_change,
    ROUND(hy_oas_20d_avg, 4) AS hy_oas_20d_avg,
    ROUND(hy_oas_60d_avg, 4) AS hy_oas_60d_avg,
    CASE
        WHEN hy_oas_20d_change_1y_std > 0 THEN
            ROUND(
                SAFE_DIVIDE(hy_oas_20d_change - hy_oas_20d_change_1y_avg, hy_oas_20d_change_1y_std),
                4
            )
    END AS hy_oas_20d_change_zscore,
    ROUND(hy_oas_percentile_5y, 4) AS hy_oas_percentile_5y,
    ROUND(hy_oas_percentile_10y, 4) AS hy_oas_percentile_10y,

    CASE
        WHEN hy_oas_20d_avg_prev <= hy_oas_60d_avg_prev
            AND hy_oas_20d_avg > hy_oas_60d_avg THEN 1
        ELSE 0
    END AS hy_oas_bearish_ma_cross_flag,

    CASE
        WHEN hy_oas >= 8.0 THEN 'crisis'
        WHEN hy_oas >= 6.0 THEN 'stress'
        WHEN hy_oas >= 4.5 THEN 'caution'
        WHEN hy_oas >= 3.0 THEN 'normal'
        ELSE 'complacent'
    END AS hy_oas_regime,

    CASE
        WHEN hy_oas_20d_change_1y_std > 0
            AND SAFE_DIVIDE(hy_oas_20d_change - hy_oas_20d_change_1y_avg, hy_oas_20d_change_1y_std) > 2.0 THEN 'high'
        WHEN hy_oas_20d_change >= 1.0 THEN 'medium'
        WHEN hy_oas_20d_change < -0.5 THEN 'low'
        ELSE 'normal'
    END AS hy_momentum_status,

    CASE
        WHEN bbb_aaa_spread > 2.0 THEN 'crisis'
        WHEN bbb_aaa_spread >= 1.0 THEN 'elevated'
        WHEN bbb_aaa_spread >= 0.5 THEN 'normal'
        WHEN bbb_aaa_spread IS NULL THEN NULL
        ELSE 'compressed'
    END AS bbb_aaa_spread_status,

    CASE
        WHEN hy_oas_percentile_10y >= 0.9 THEN 'high'
        WHEN hy_oas_percentile_10y >= 0.75 THEN 'medium'
        WHEN hy_oas_percentile_10y <= 0.1 THEN 'low'
        ELSE 'normal'
    END AS hy_percentile_status

FROM with_percentiles
WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR)
ORDER BY date DESC
