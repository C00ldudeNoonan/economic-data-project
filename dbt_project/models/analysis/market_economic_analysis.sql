{{
  config(
    materialized='table',
    description='Minimal test version - joining market returns with economic indicators'
  )
}}


WITH economic_data AS (
    SELECT
        year_month,
        series_code,
        series_name,
        avg_value AS economic_value,
        pct_change_period AS economic_change_pct,
        data_source AS economic_data_source,
        -- Create month_date for joining
        CASE
            WHEN year_month ~ '^\d{4}-\d{1,2}$'
                THEN
                    MAKE_DATE(
                        CAST(SPLIT_PART(year_month, '-', 1) AS INTEGER),
                        CAST(SPLIT_PART(year_month, '-', 2) AS INTEGER),
                        1
                    )
        END AS month_date
    FROM {{ ref('fred_quarterly_roc') }}
),

economic_indicators_pivoted AS (
    SELECT
        year_month,
        month_date,

        -- GDP indicators
        MAX(
            CASE
                WHEN
                    series_code LIKE '%GDP%'
                    OR series_name ILIKE '%gross domestic product%'
                    THEN economic_value
            END
        ) AS gdp_value,
        MAX(
            CASE
                WHEN
                    series_code LIKE '%GDP%'
                    OR series_name ILIKE '%gross domestic product%'
                    THEN economic_change_pct
            END
        ) AS gdp_change_pct,

        -- Inflation indicators (CPI)
        MAX(
            CASE
                WHEN
                    series_code LIKE '%CPI%'
                    OR series_name ILIKE '%consumer price%'
                    THEN economic_value
            END
        ) AS cpi_value,
        MAX(
            CASE
                WHEN
                    series_code LIKE '%CPI%'
                    OR series_name ILIKE '%consumer price%'
                    THEN economic_change_pct
            END
        ) AS cpi_change_pct,

        -- Interest rate indicators  
        MAX(
            CASE
                WHEN
                    series_name ILIKE '%interest%' OR series_name ILIKE '%rate%'
                    THEN economic_value
            END
        ) AS interest_rate_value,
        MAX(
            CASE
                WHEN
                    series_name ILIKE '%interest%' OR series_name ILIKE '%rate%'
                    THEN economic_change_pct
            END
        ) AS interest_rate_change_pct

    FROM economic_data
    WHERE month_date IS NOT NULL
    GROUP BY year_month, month_date
)

-- Start with just the economic data to test
SELECT
    year_month,
    month_date,
    gdp_value,
    gdp_change_pct,
    cpi_value,
    cpi_change_pct,
    interest_rate_value,
    interest_rate_change_pct,

    -- Economic regime classification
    CASE
        WHEN cpi_change_pct > 2 THEN 'HIGH_INFLATION'
        WHEN cpi_change_pct BETWEEN 0 AND 2 THEN 'MODERATE_INFLATION'
        WHEN cpi_change_pct < 0 THEN 'DEFLATION'
        ELSE 'UNKNOWN'
    END AS inflation_regime

FROM economic_indicators_pivoted
ORDER BY month_date
