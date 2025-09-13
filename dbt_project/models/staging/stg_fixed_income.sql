{{ config(
    materialized='view'
) }}

SELECT
    open,
    high,
    low,
    close,
    volume,
    adj_high,
    adj_low,
    adj_close,
    adj_open,
    adj_volume,
    split_factor,
    dividend,
    name,
    exchange_code,
    asset_type,
    price_currency,
    symbol,
    exchange,
    cast(date AS date) AS date
FROM {{ source('staging', 'fixed_income_etfs_raw') }}
