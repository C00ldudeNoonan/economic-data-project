{{ config(materialized='table') }}

SELECT
    'stock' AS asset_class,
    symbol || '|' || exchange AS asset_id,
    date,
    symbol,
    exchange,
    CAST(NULL AS STRING) AS commodity_name,
    CAST(NULL AS STRING) AS commodity_unit,
    current_price,
    std_diff_1yr
FROM {{ ref('sp500_companies_analysis_return') }}

UNION ALL

SELECT
    'sector_etf' AS asset_class,
    symbol || '|' || exchange AS asset_id,
    date,
    symbol,
    exchange,
    CAST(NULL AS STRING) AS commodity_name,
    CAST(NULL AS STRING) AS commodity_unit,
    current_price,
    std_diff_1yr
FROM {{ ref('us_sector_analysis_return') }}

UNION ALL

SELECT
    'commodity' AS asset_class,
    commodity_name || '|' || commodity_unit AS asset_id,
    date,
    CAST(NULL AS STRING) AS symbol,
    CAST(NULL AS STRING) AS exchange,
    commodity_name,
    commodity_unit,
    current_price,
    std_diff_1yr
FROM {{ ref('input_commodities_analysis_return') }}
