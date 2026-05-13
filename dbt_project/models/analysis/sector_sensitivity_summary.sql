{{
  config(
    description='Aggregated sector sensitivity scores with ranked indicators by predictive power for each sector'
  )
}}

WITH base_sensitivity AS (
    SELECT * FROM {{ ref('sector_indicator_sensitivity') }}
),

-- Rank indicators by sensitivity score within each sector
ranked_by_sector AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY symbol
            ORDER BY sensitivity_score DESC
        ) AS rank_in_sector,
        PERCENT_RANK() OVER (
            PARTITION BY symbol
            ORDER BY sensitivity_score
        ) AS percentile_in_sector
    FROM base_sensitivity
),

-- Get top indicators for each sector
top_indicators_per_sector AS (
    SELECT
        symbol,
        sector_name,
        series_code,
        series_name,
        indicator_category,
        observation_count,
        corr_1mo_contemp,
        corr_3mo_contemp,
        best_lag_months,
        best_lag_correlation_abs,
        avg_return_indicator_up,
        avg_return_indicator_down,
        return_spread,
        sensitivity_score,
        rank_in_sector,
        percentile_in_sector,
        CASE
            WHEN corr_1mo_contemp > 0 THEN 'Positive'
            WHEN corr_1mo_contemp < 0 THEN 'Negative'
            ELSE 'Neutral'
        END AS correlation_direction,
        CASE
            WHEN ABS(corr_1mo_contemp) >= 0.5 THEN 'Strong'
            WHEN ABS(corr_1mo_contemp) >= 0.3 THEN 'Moderate'
            WHEN ABS(corr_1mo_contemp) >= 0.1 THEN 'Weak'
            ELSE 'Negligible'
        END AS correlation_strength
    FROM ranked_by_sector
),

-- Aggregate statistics per sector
sector_summary AS (
    SELECT
        symbol,
        sector_name,
        COUNT(*) AS total_indicators_analyzed,
        ROUND(AVG(sensitivity_score), 2) AS avg_sensitivity_score,
        ROUND(MAX(sensitivity_score), 2) AS max_sensitivity_score,
        ROUND(MIN(sensitivity_score), 2) AS min_sensitivity_score,
        COUNT(CASE WHEN sensitivity_score >= 20 THEN 1 END) AS high_sensitivity_count,
        COUNT(CASE WHEN correlation_direction = 'Positive' THEN 1 END) AS positive_correlation_count,
        COUNT(CASE WHEN correlation_direction = 'Negative' THEN 1 END) AS negative_correlation_count,
        ROUND(AVG(ABS(corr_1mo_contemp)), 4) AS avg_abs_correlation,
        -- Most sensitive indicator for this sector
        MAX(CASE WHEN rank_in_sector = 1 THEN series_name END) AS top_indicator_name,
        MAX(CASE WHEN rank_in_sector = 1 THEN series_code END) AS top_indicator_code,
        MAX(CASE WHEN rank_in_sector = 1 THEN sensitivity_score END) AS top_indicator_score
    FROM top_indicators_per_sector
    GROUP BY symbol, sector_name
),

-- Aggregate statistics per indicator category
category_summary AS (
    SELECT
        indicator_category,
        symbol,
        sector_name,
        COUNT(*) AS indicators_in_category,
        ROUND(AVG(sensitivity_score), 2) AS avg_category_sensitivity,
        ROUND(AVG(corr_1mo_contemp), 4) AS avg_category_correlation
    FROM top_indicators_per_sector
    GROUP BY indicator_category, symbol, sector_name
)

-- Final output: detailed sensitivity records with rankings
SELECT
    t.symbol,
    t.sector_name,
    t.series_code,
    t.series_name,
    t.indicator_category,
    t.observation_count,
    t.corr_1mo_contemp,
    t.corr_3mo_contemp,
    t.best_lag_months,
    t.best_lag_correlation_abs,
    t.avg_return_indicator_up,
    t.avg_return_indicator_down,
    t.return_spread,
    t.sensitivity_score,
    t.rank_in_sector,
    ROUND(t.percentile_in_sector * 100, 1) AS percentile_in_sector,
    t.correlation_direction,
    t.correlation_strength,

    -- Sector-level context
    s.total_indicators_analyzed,
    s.avg_sensitivity_score AS sector_avg_sensitivity,
    s.high_sensitivity_count AS sector_high_sensitivity_count,

    -- Category-level context
    c.avg_category_sensitivity,
    c.avg_category_correlation,

    -- Is this a top indicator for this sector?
    CASE WHEN t.rank_in_sector <= 10 THEN TRUE ELSE FALSE END AS is_top_10_for_sector,
    CASE WHEN t.rank_in_sector <= 5 THEN TRUE ELSE FALSE END AS is_top_5_for_sector,

    -- Predictive power flag
    CASE
        WHEN t.best_lag_correlation_abs >= 0.2 AND t.best_lag_months BETWEEN 1 AND 3
        THEN TRUE
        ELSE FALSE
    END AS has_predictive_power

FROM top_indicators_per_sector t
LEFT JOIN sector_summary s ON t.symbol = s.symbol
LEFT JOIN category_summary c
    ON t.symbol = c.symbol
    AND t.indicator_category = c.indicator_category
ORDER BY t.symbol, t.rank_in_sector
