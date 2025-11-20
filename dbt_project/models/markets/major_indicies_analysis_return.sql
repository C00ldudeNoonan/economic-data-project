{{
  config(
    materialized='table',
    description='ETF daily data aggregated to monthly averages with forward-looking quarterly percentage changes'
  )
}}


WITH monthly_averages AS (
    SELECT
        symbol,
        exchange,
        EXTRACT(YEAR FROM date) AS year_val,
        EXTRACT(MONTH FROM date) AS month_val,
        CONCAT(
            EXTRACT(YEAR FROM date),
            '-',
            LPAD(EXTRACT(MONTH FROM date)::VARCHAR, 2, '0')
        ) AS year_month,
        MAKE_DATE(EXTRACT(YEAR FROM date), EXTRACT(MONTH FROM date), 1)
            AS month_date,

        -- Monthly averages for key price metrics
        ROUND(AVG(close), 4) AS avg_close,
        ROUND(AVG(open), 4) AS avg_open,
        ROUND(AVG(high), 4) AS avg_high,
        ROUND(AVG(low), 4) AS avg_low,
        ROUND(AVG(volume), 0) AS avg_volume

    FROM {{ ref('stg_major_indices') }}
    GROUP BY
        symbol,
        exchange,
        EXTRACT(YEAR FROM date),
        EXTRACT(MONTH FROM date)
),

quarterly_data AS (
    SELECT
        symbol,
        exchange,
        year_val,
        year_month,
        month_date,
        month_val,  -- Keep month_val for filtering

        -- Average the monthly averages within each quarter
        avg_close,

        avg_open,

        avg_high,

        avg_low,

        avg_volume,

        -- Keep monthly detail
        CASE
            WHEN month_val IN (1, 2, 3) THEN 1
            WHEN month_val IN (4, 5, 6) THEN 2
            WHEN month_val IN (7, 8, 9) THEN 3
            WHEN month_val IN (10, 11, 12) THEN 4
        END AS quarter_num,
        CONCAT(
            year_val, '-Q',
            CASE
                WHEN month_val IN (1, 2, 3) THEN 1
                WHEN month_val IN (4, 5, 6) THEN 2
                WHEN month_val IN (7, 8, 9) THEN 3
                WHEN month_val IN (10, 11, 12) THEN 4
            END
        ) AS year_quarter,
        AVG(avg_close) OVER (
            PARTITION BY
                symbol, exchange, year_val,
                CASE
                    WHEN month_val IN (1, 2, 3) THEN 1
                    WHEN month_val IN (4, 5, 6) THEN 2
                    WHEN month_val IN (7, 8, 9) THEN 3
                    WHEN month_val IN (10, 11, 12) THEN 4
                END
        ) AS quarterly_avg_close,
        AVG(avg_open) OVER (
            PARTITION BY
                symbol, exchange, year_val,
                CASE
                    WHEN month_val IN (1, 2, 3) THEN 1
                    WHEN month_val IN (4, 5, 6) THEN 2
                    WHEN month_val IN (7, 8, 9) THEN 3
                    WHEN month_val IN (10, 11, 12) THEN 4
                END
        ) AS quarterly_avg_open,
        AVG(avg_high) OVER (
            PARTITION BY
                symbol, exchange, year_val,
                CASE
                    WHEN month_val IN (1, 2, 3) THEN 1
                    WHEN month_val IN (4, 5, 6) THEN 2
                    WHEN month_val IN (7, 8, 9) THEN 3
                    WHEN month_val IN (10, 11, 12) THEN 4
                END
        ) AS quarterly_avg_high,
        AVG(avg_low) OVER (
            PARTITION BY
                symbol, exchange, year_val,
                CASE
                    WHEN month_val IN (1, 2, 3) THEN 1
                    WHEN month_val IN (4, 5, 6) THEN 2
                    WHEN month_val IN (7, 8, 9) THEN 3
                    WHEN month_val IN (10, 11, 12) THEN 4
                END
        ) AS quarterly_avg_low,
        AVG(avg_volume) OVER (
            PARTITION BY
                symbol, exchange, year_val,
                CASE
                    WHEN month_val IN (1, 2, 3) THEN 1
                    WHEN month_val IN (4, 5, 6) THEN 2
                    WHEN month_val IN (7, 8, 9) THEN 3
                    WHEN month_val IN (10, 11, 12) THEN 4
                END
        ) AS quarterly_avg_volume

    FROM monthly_averages
),

-- Get one row per quarter (last month of each quarter) for forward price calculations
quarterly_lookup AS (
    SELECT DISTINCT
        symbol,
        exchange,
        year_val,
        quarter_num,
        year_quarter,
        quarterly_avg_close
    FROM quarterly_data
    WHERE month_val IN (3, 6, 9, 12)  -- Last month of each quarter
),

-- Calculate forward quarterly prices using quarterly data only
quarterly_forward_prices AS (
    SELECT
        symbol,
        exchange,
        year_val,
        quarter_num,
        year_quarter,
        quarterly_avg_close,
        LEAD(quarterly_avg_close, 1) OVER (
            PARTITION BY symbol, exchange
            ORDER BY year_val, quarter_num
        ) AS close_q1_forward,
        LEAD(quarterly_avg_close, 2) OVER (
            PARTITION BY symbol, exchange
            ORDER BY year_val, quarter_num
        ) AS close_q2_forward,
        LEAD(quarterly_avg_close, 3) OVER (
            PARTITION BY symbol, exchange
            ORDER BY year_val, quarter_num
        ) AS close_q3_forward,
        LEAD(quarterly_avg_close, 4) OVER (
            PARTITION BY symbol, exchange
            ORDER BY year_val, quarter_num
        ) AS close_q4_forward
    FROM quarterly_lookup
),

-- Join forward prices back to monthly data
with_forward_quarters AS (
    SELECT
        qd.*,
        qfp.close_q1_forward,
        qfp.close_q2_forward,
        qfp.close_q3_forward,
        qfp.close_q4_forward
    FROM quarterly_data qd
    LEFT JOIN quarterly_forward_prices qfp
        ON qd.symbol = qfp.symbol
        AND qd.exchange = qfp.exchange
        AND qd.year_val = qfp.year_val
        AND qd.quarter_num = qfp.quarter_num
)

SELECT
    symbol,
    exchange,
    year_month,
    month_date,
    year_quarter,
    quarter_num,
    year_val,

    -- Monthly averages
    avg_close AS monthly_avg_close,
    avg_open AS monthly_avg_open,
    avg_high AS monthly_avg_high,
    avg_low AS monthly_avg_low,
    avg_volume AS monthly_avg_volume,

    -- Quarterly averages
    ROUND(quarterly_avg_close, 4) AS quarterly_avg_close,
    ROUND(quarterly_avg_open, 4) AS quarterly_avg_open,
    ROUND(quarterly_avg_high, 4) AS quarterly_avg_high,
    ROUND(quarterly_avg_low, 4) AS quarterly_avg_low,
    ROUND(quarterly_avg_volume, 0) AS quarterly_avg_volume,

    -- Forward quarterly percentage changes (with NULL handling)
    CASE
        WHEN close_q1_forward IS NOT NULL AND quarterly_avg_close > 0
        THEN ROUND((close_q1_forward - quarterly_avg_close) / quarterly_avg_close * 100, 2)
        ELSE NULL
    END AS pct_change_q1_forward,

    CASE
        WHEN close_q2_forward IS NOT NULL AND quarterly_avg_close > 0
        THEN ROUND((close_q2_forward - quarterly_avg_close) / quarterly_avg_close * 100, 2)
        ELSE NULL
    END AS pct_change_q2_forward,

    CASE
        WHEN close_q3_forward IS NOT NULL AND quarterly_avg_close > 0
        THEN ROUND((close_q3_forward - quarterly_avg_close) / quarterly_avg_close * 100, 2)
        ELSE NULL
    END AS pct_change_q3_forward,

    CASE
        WHEN close_q4_forward IS NOT NULL AND quarterly_avg_close > 0
        THEN ROUND((close_q4_forward - quarterly_avg_close) / quarterly_avg_close * 100, 2)
        ELSE NULL
    END AS pct_change_q4_forward

FROM with_forward_quarters
ORDER BY symbol, exchange, month_date
