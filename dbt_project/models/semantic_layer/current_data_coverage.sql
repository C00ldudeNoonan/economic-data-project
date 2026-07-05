{{ config(materialized='table') }}

WITH source_specs AS (
    SELECT *
    FROM UNNEST([
        STRUCT('sp500_companies_prices_raw' AS source_name, 'markets' AS source_domain, 'daily_market_prices' AS grain, 31 AS lookback_days, 5 AS freshness_warn_days, 10 AS freshness_error_days),
        STRUCT('us_sector_etfs_raw' AS source_name, 'markets' AS source_domain, 'daily_market_prices' AS grain, 31 AS lookback_days, 5 AS freshness_warn_days, 10 AS freshness_error_days),
        STRUCT('currency_etfs_raw' AS source_name, 'markets' AS source_domain, 'daily_market_prices' AS grain, 31 AS lookback_days, 5 AS freshness_warn_days, 10 AS freshness_error_days),
        STRUCT('commodity_etfs_raw' AS source_name, 'markets' AS source_domain, 'daily_market_prices' AS grain, 31 AS lookback_days, 5 AS freshness_warn_days, 10 AS freshness_error_days),
        STRUCT('major_indices_raw' AS source_name, 'markets' AS source_domain, 'daily_market_prices' AS grain, 31 AS lookback_days, 5 AS freshness_warn_days, 10 AS freshness_error_days),
        STRUCT('fixed_income_etfs_raw' AS source_name, 'markets' AS source_domain, 'daily_market_prices' AS grain, 31 AS lookback_days, 5 AS freshness_warn_days, 10 AS freshness_error_days),
        STRUCT('global_markets_raw' AS source_name, 'markets' AS source_domain, 'daily_market_prices' AS grain, 31 AS lookback_days, 5 AS freshness_warn_days, 10 AS freshness_error_days),
        STRUCT('energy_commodities_raw' AS source_name, 'commodities' AS source_domain, 'daily_commodity_prices' AS grain, 31 AS lookback_days, 5 AS freshness_warn_days, 10 AS freshness_error_days),
        STRUCT('input_commodities_raw' AS source_name, 'commodities' AS source_domain, 'daily_commodity_prices' AS grain, 31 AS lookback_days, 5 AS freshness_warn_days, 10 AS freshness_error_days),
        STRUCT('agriculture_commodities_raw' AS source_name, 'commodities' AS source_domain, 'daily_commodity_prices' AS grain, 31 AS lookback_days, 5 AS freshness_warn_days, 10 AS freshness_error_days),
        STRUCT('fred_raw' AS source_name, 'government' AS source_domain, 'economic_series' AS grain, 93 AS lookback_days, 45 AS freshness_warn_days, 75 AS freshness_error_days)
    ])
),

raw_source_observations AS (
    SELECT
        'sp500_companies_prices_raw' AS source_name,
        symbol AS entity_id,
        date AS observation_date
    FROM {{ ref('stg_sp500_companies_prices') }}
    WHERE symbol IS NOT NULL AND date IS NOT NULL

    UNION ALL

    SELECT
        'us_sector_etfs_raw' AS source_name,
        symbol AS entity_id,
        date AS observation_date
    FROM {{ ref('stg_us_sectors') }}
    WHERE symbol IS NOT NULL AND date IS NOT NULL

    UNION ALL

    SELECT
        'currency_etfs_raw' AS source_name,
        symbol AS entity_id,
        date AS observation_date
    FROM {{ ref('stg_currency') }}
    WHERE symbol IS NOT NULL AND date IS NOT NULL

    UNION ALL

    SELECT
        'commodity_etfs_raw' AS source_name,
        symbol AS entity_id,
        date AS observation_date
    FROM {{ ref('stg_commodity_etfs') }}
    WHERE symbol IS NOT NULL AND date IS NOT NULL

    UNION ALL

    SELECT
        'major_indices_raw' AS source_name,
        symbol AS entity_id,
        date AS observation_date
    FROM {{ ref('stg_major_indices') }}
    WHERE symbol IS NOT NULL AND date IS NOT NULL

    UNION ALL

    SELECT
        'fixed_income_etfs_raw' AS source_name,
        symbol AS entity_id,
        date AS observation_date
    FROM {{ ref('stg_fixed_income') }}
    WHERE symbol IS NOT NULL AND date IS NOT NULL

    UNION ALL

    SELECT
        'global_markets_raw' AS source_name,
        symbol AS entity_id,
        date AS observation_date
    FROM {{ ref('stg_global_markets') }}
    WHERE symbol IS NOT NULL AND date IS NOT NULL

    UNION ALL

    SELECT
        'energy_commodities_raw' AS source_name,
        CONCAT(commodity_name, ':', commodity_unit) AS entity_id,
        date AS observation_date
    FROM {{ ref('stg_energy_commodities') }}
    WHERE commodity_name IS NOT NULL AND date IS NOT NULL

    UNION ALL

    SELECT
        'input_commodities_raw' AS source_name,
        CONCAT(commodity_name, ':', commodity_unit) AS entity_id,
        date AS observation_date
    FROM {{ ref('stg_input_commodities') }}
    WHERE commodity_name IS NOT NULL AND date IS NOT NULL

    UNION ALL

    SELECT
        'agriculture_commodities_raw' AS source_name,
        CONCAT(commodity_name, ':', commodity_unit) AS entity_id,
        date AS observation_date
    FROM {{ ref('stg_agriculture_commodities') }}
    WHERE commodity_name IS NOT NULL AND date IS NOT NULL

    UNION ALL

    SELECT
        'fred_raw' AS source_name,
        series_code AS entity_id,
        date AS observation_date
    FROM {{ ref('stg_fred_series') }}
    WHERE series_code IS NOT NULL AND date IS NOT NULL
),

source_observations AS (
    SELECT
        source_name,
        entity_id,
        observation_date
    FROM raw_source_observations
    WHERE observation_date <= CURRENT_DATE()
),

expected_entities AS (
    SELECT
        source_name,
        COUNT(DISTINCT entity_id) AS expected_entity_count
    FROM source_observations
    GROUP BY source_name
),

latest_observations AS (
    SELECT
        source_name,
        MAX(observation_date) AS coverage_date
    FROM source_observations
    GROUP BY source_name
),

windowed_observations AS (
    SELECT
        observations.source_name,
        observations.entity_id,
        observations.observation_date
    FROM source_observations AS observations
    INNER JOIN latest_observations AS latest
        ON observations.source_name = latest.source_name
    INNER JOIN source_specs AS specs
        ON observations.source_name = specs.source_name
    WHERE observations.observation_date >= DATE_SUB(
        latest.coverage_date,
        INTERVAL specs.lookback_days DAY
    )
),

coverage_counts AS (
    SELECT
        source_name,
        COUNT(*) AS observed_row_count,
        COUNT(DISTINCT entity_id) AS observed_entity_count
    FROM windowed_observations
    GROUP BY source_name
)

SELECT
    CONCAT(specs.source_name, ':', CAST(latest.coverage_date AS STRING)) AS coverage_id,
    specs.source_name,
    specs.source_domain,
    specs.grain,
    latest.coverage_date,
    DATE_SUB(latest.coverage_date, INTERVAL specs.lookback_days DAY) AS coverage_window_start,
    specs.lookback_days,
    expected.expected_entity_count,
    COALESCE(counts.observed_entity_count, 0) AS observed_entity_count,
    expected.expected_entity_count - COALESCE(counts.observed_entity_count, 0) AS missing_entity_count,
    COALESCE(counts.observed_row_count, 0) AS observed_row_count,
    SAFE_DIVIDE(
        COALESCE(counts.observed_entity_count, 0),
        NULLIF(expected.expected_entity_count, 0)
    ) AS coverage_pct,
    DATE_DIFF(CURRENT_DATE(), latest.coverage_date, DAY) AS freshness_lag_days,
    specs.freshness_warn_days,
    specs.freshness_error_days,
    CASE
        WHEN expected.expected_entity_count = 0 THEN 'no_expected_entities'
        WHEN DATE_DIFF(CURRENT_DATE(), latest.coverage_date, DAY) > specs.freshness_error_days THEN 'stale'
        WHEN SAFE_DIVIDE(COALESCE(counts.observed_entity_count, 0), NULLIF(expected.expected_entity_count, 0)) < 0.80 THEN 'coverage_gap'
        WHEN DATE_DIFF(CURRENT_DATE(), latest.coverage_date, DAY) > specs.freshness_warn_days THEN 'lagging'
        WHEN SAFE_DIVIDE(COALESCE(counts.observed_entity_count, 0), NULLIF(expected.expected_entity_count, 0)) < 0.98 THEN 'partial'
        ELSE 'healthy'
    END AS coverage_status,
    CURRENT_TIMESTAMP() AS generated_at
FROM source_specs AS specs
LEFT JOIN latest_observations AS latest
    ON specs.source_name = latest.source_name
LEFT JOIN expected_entities AS expected
    ON specs.source_name = expected.source_name
LEFT JOIN coverage_counts AS counts
    ON specs.source_name = counts.source_name
