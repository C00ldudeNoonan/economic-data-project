WITH raw_series as (
  select 
    series_name,
    date,
    value
  FROM {{ ref('stg_fred_series') }}
  WHERE series_name in ('30 year Mortgage Rate', 'Median Sales Price of Houses Sold for the United States')
),
rate as (
  select
  series_name,
  strftime(date, '%Y-%m-01') as date,
  avg(value::DOUBLE) as mortgage_rate
  from raw_series
  where series_name = '30 year Mortgage Rate'
  group by
  series_name,
  strftime(date, '%Y-%m-01')
  ),
price as (
  select
    series_name,
    date,
    value::DOUBLE as median_price_no_down_payment,
    value::DOUBLE * 0.8 as median_price_20_pct_down_payment
  from raw_series
  where series_name = 'Median Sales Price of Houses Sold for the United States'
)
SELECT
  date,
  median_price_no_down_payment,
  median_price_20_pct_down_payment,
  mortgage_rate,
  ROUND(
    median_price_no_down_payment * 
    (mortgage_rate / 12 / 100 * POWER(1 + mortgage_rate / 12 / 100, 360 )) /
    (POWER(1 + mortgage_rate / 12 / 100, 360 ) - 1)
  , 2) AS monthly_payment_no_down_payment,
    ROUND(
    median_price_20_pct_down_payment * 
    (mortgage_rate / 12 / 100 * POWER(1 + mortgage_rate / 12 / 100, 360 )) /
    (POWER(1 + mortgage_rate / 12 / 100, 360 ) - 1)
  , 2) AS monthly_payment_no_down_payment
FROM rate
JOIN price USING (date)
ORDER BY date asc


-- median mortagate payment