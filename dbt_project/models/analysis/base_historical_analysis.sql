{{
  config(
    description='Comprehensive historical analysis combining market return data across all asset categories with economic indicators from FRED'
  )
}}

WITH return_data AS (
    SELECT
        symbol,
        exchange,
        date,
        current_price,
        current_volume,
        pct_change_1mo,
        pct_change_3mo,
        pct_change_6mo,
        pct_change_9mo,
        pct_change_1yr,
        high_1yr,
        low_1yr,
        std_diff_1yr,
        'currency' AS category
    FROM {{ ref('currency_analysis_return') }}

    UNION ALL

    SELECT
        symbol,
        exchange,
        date,
        current_price,
        current_volume,
        pct_change_1mo,
        pct_change_3mo,
        pct_change_6mo,
        pct_change_9mo,
        pct_change_1yr,
        high_1yr,
        low_1yr,
        std_diff_1yr,
        'fixed_income' AS category
    FROM {{ ref('fixed_income_analysis_return') }}

    UNION ALL

    SELECT
        symbol,
        exchange,
        date,
        current_price,
        current_volume,
        pct_change_1mo,
        pct_change_3mo,
        pct_change_6mo,
        pct_change_9mo,
        pct_change_1yr,
        high_1yr,
        low_1yr,
        std_diff_1yr,
        'global_markets' AS category
    FROM {{ ref('global_markets_analysis_return') }}

    UNION ALL

    SELECT
        symbol,
        exchange,
        date,
        current_price,
        current_volume,
        pct_change_1mo,
        pct_change_3mo,
        pct_change_6mo,
        pct_change_9mo,
        pct_change_1yr,
        high_1yr,
        low_1yr,
        std_diff_1yr,
        'major_indices' AS category
    FROM {{ ref('major_indices_analysis_return') }}

    UNION ALL

    SELECT
        symbol,
        exchange,
        date,
        current_price,
        current_volume,
        pct_change_1mo,
        pct_change_3mo,
        pct_change_6mo,
        pct_change_9mo,
        pct_change_1yr,
        high_1yr,
        low_1yr,
        std_diff_1yr,
        'sector' AS category
    FROM {{ ref('us_sector_analysis_return') }}

)

SELECT
    rt.symbol,
    rt.exchange,
    rt.date,
    rt.current_price,
    rt.current_volume,
    rt.pct_change_1mo,
    rt.pct_change_3mo,
    rt.pct_change_6mo,
    rt.pct_change_9mo,
    rt.pct_change_1yr,
    rt.high_1yr,
    rt.low_1yr,
    rt.std_diff_1yr,
    rt.category,
    fr.series_name,
    fr.value,
    fr.period_diff
FROM return_data AS rt
LEFT JOIN {{ ref('fred_monthly_diff') }} AS fr
    ON rt.date = CAST(fr.date AS date)
