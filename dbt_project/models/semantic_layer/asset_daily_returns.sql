WITH stocks AS (
    SELECT
        CONCAT('stock:', exchange, ':', symbol) AS asset_key,
        'stock' AS asset_class,
        symbol AS asset_id,
        symbol AS asset_name,
        symbol,
        symbol AS stock_symbol,
        CAST(NULL AS STRING) AS sector_etf_symbol,
        CAST(NULL AS STRING) AS commodity_name,
        CAST(NULL AS STRING) AS commodity_unit,
        exchange,
        date AS trade_date,
        current_price,
        std_diff_1yr,
        pct_change_1yr
    FROM {{ ref('sp500_companies_analysis_return') }}
),

sector_etfs AS (
    SELECT
        CONCAT('sector_etf:', exchange, ':', symbol) AS asset_key,
        'sector_etf' AS asset_class,
        symbol AS asset_id,
        symbol AS asset_name,
        symbol,
        CAST(NULL AS STRING) AS stock_symbol,
        symbol AS sector_etf_symbol,
        CAST(NULL AS STRING) AS commodity_name,
        CAST(NULL AS STRING) AS commodity_unit,
        exchange,
        date AS trade_date,
        current_price,
        std_diff_1yr,
        pct_change_1yr
    FROM {{ ref('us_sector_analysis_return') }}
),

commodities AS (
    SELECT
        CONCAT('commodity:', commodity_name, ':', commodity_unit) AS asset_key,
        'commodity' AS asset_class,
        commodity_name AS asset_id,
        commodity_name AS asset_name,
        CAST(NULL AS STRING) AS symbol,
        CAST(NULL AS STRING) AS stock_symbol,
        CAST(NULL AS STRING) AS sector_etf_symbol,
        commodity_name,
        commodity_unit,
        CAST(NULL AS STRING) AS exchange,
        date AS trade_date,
        current_price,
        std_diff_1yr,
        pct_change_1yr
    FROM {{ ref('input_commodities_analysis_return') }}
),

unioned_assets AS (
    SELECT
        asset_key,
        asset_class,
        asset_id,
        asset_name,
        symbol,
        stock_symbol,
        sector_etf_symbol,
        commodity_name,
        commodity_unit,
        exchange,
        trade_date,
        current_price,
        std_diff_1yr,
        pct_change_1yr
    FROM stocks

    UNION ALL

    SELECT
        asset_key,
        asset_class,
        asset_id,
        asset_name,
        symbol,
        stock_symbol,
        sector_etf_symbol,
        commodity_name,
        commodity_unit,
        exchange,
        trade_date,
        current_price,
        std_diff_1yr,
        pct_change_1yr
    FROM sector_etfs

    UNION ALL

    SELECT
        asset_key,
        asset_class,
        asset_id,
        asset_name,
        symbol,
        stock_symbol,
        sector_etf_symbol,
        commodity_name,
        commodity_unit,
        exchange,
        trade_date,
        current_price,
        std_diff_1yr,
        pct_change_1yr
    FROM commodities
)

SELECT
    asset_key,
    asset_class,
    asset_id,
    asset_name,
    symbol,
    stock_symbol,
    sector_etf_symbol,
    commodity_name,
    commodity_unit,
    exchange,
    trade_date,
    current_price,
    std_diff_1yr,
    pct_change_1yr
FROM unioned_assets
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY asset_key, trade_date
    ORDER BY current_price DESC
) = 1
