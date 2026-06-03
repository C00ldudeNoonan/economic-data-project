WITH raw_series AS (
    SELECT
        series_name,
        date,
        value
    FROM {{ ref('stg_fred_series') }}
    WHERE
        series_name IN (
            '30 year Mortgage Rate',
            'Median Sales Price of Houses Sold for the United States'
        )
),

rate AS (
    SELECT
        series_name,
        DATE_TRUNC(date, MONTH) AS date,
        AVG(CAST(value AS FLOAT64)) AS mortgage_rate
    FROM raw_series
    WHERE series_name = '30 year Mortgage Rate'
    GROUP BY
        series_name,
        DATE_TRUNC(date, MONTH)
),

price AS (
    SELECT
        series_name,
        date,
        CAST(value AS FLOAT64) AS median_price_no_down_payment,
        CAST(value AS FLOAT64) * 0.8 AS median_price_20_pct_down_payment
    FROM raw_series
    WHERE
        series_name = 'Median Sales Price of Houses Sold for the United States'
)

SELECT
    rate.date,
    median_price_no_down_payment,
    median_price_20_pct_down_payment,
    mortgage_rate,
    round(
        median_price_no_down_payment
        * (mortgage_rate / 12 / 100 * power(1 + mortgage_rate / 12 / 100, 360))
        / (power(1 + mortgage_rate / 12 / 100, 360) - 1),
        2
    ) AS monthly_payment_no_down_payment,
    round(
        median_price_20_pct_down_payment
        * (mortgage_rate / 12 / 100 * power(1 + mortgage_rate / 12 / 100, 360))
        / (power(1 + mortgage_rate / 12 / 100, 360) - 1),
        2
    ) AS monthly_payment_20_pct_down_payment
FROM rate
INNER JOIN price ON rate.date = price.date
ORDER BY rate.date ASC
