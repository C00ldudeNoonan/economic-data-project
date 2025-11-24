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
        strftime(date, '%Y-%m-01') AS date,
        avg(value::DOUBLE) AS mortgage_rate
    FROM raw_series
    WHERE series_name = '30 year Mortgage Rate'
    GROUP BY
        series_name,
        strftime(date, '%Y-%m-01')
),

price AS (
    SELECT
        series_name,
        date,
        value::DOUBLE AS median_price_no_down_payment,
        value::DOUBLE * 0.8 AS median_price_20_pct_down_payment
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

