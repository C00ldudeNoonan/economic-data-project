{{
  config(
    description='Event study analysis measuring market response to economic indicator releases and surprises'
  )
}}

-- Indicator Market Response Analysis
-- Calculates how sectors respond to economic indicator releases and surprise moves

WITH sector_names AS (
    SELECT sector_names.*
    FROM (VALUES
        ('XLK', 'Technology'),
        ('XLC', 'Communication Services'),
        ('XLY', 'Consumer Discretionary'),
        ('XLF', 'Financial'),
        ('XLI', 'Industrial'),
        ('XLU', 'Utilities'),
        ('XLP', 'Consumer Staples'),
        ('XLRE', 'Real Estate'),
        ('XLB', 'Materials'),
        ('XLE', 'Energy'),
        ('XLV', 'Health Care')
    ) AS sector_names (symbol, sector_name)
),

-- Get monthly sector returns
sector_monthly AS (
    SELECT
        symbol,
        DATE_TRUNC('month', date) AS month_date,
        LAST_VALUE(pct_change_1mo) OVER (
            PARTITION BY symbol, DATE_TRUNC('month', date)
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS monthly_return,
        ROW_NUMBER() OVER (
            PARTITION BY symbol, DATE_TRUNC('month', date)
            ORDER BY date DESC
        ) AS rn
    FROM {{ ref('us_sector_analysis_return') }}
    WHERE symbol IN ('XLK', 'XLC', 'XLY', 'XLF', 'XLI', 'XLU', 'XLP', 'XLRE', 'XLB', 'XLE', 'XLV')
),

sector_returns AS (
    SELECT symbol, month_date, monthly_return
    FROM sector_monthly
    WHERE rn = 1
),

-- Get monthly FRED indicator values with changes and rolling stats
indicator_monthly AS (
    SELECT
        series_code,
        series_name,
        DATE_TRUNC('month', date) AS month_date,
        value,
        -- Month-over-month change
        value - LAG(value) OVER (PARTITION BY series_code ORDER BY date) AS mom_change,
        -- Percentage change
        CASE
            WHEN LAG(value) OVER (PARTITION BY series_code ORDER BY date) IS NOT NULL
                AND LAG(value) OVER (PARTITION BY series_code ORDER BY date) != 0
            THEN ((value - LAG(value) OVER (PARTITION BY series_code ORDER BY date))
                  / ABS(LAG(value) OVER (PARTITION BY series_code ORDER BY date))) * 100
        END AS mom_pct_change,
        -- Rolling 12-month average change (as trend/expected)
        AVG(value - LAG(value) OVER (PARTITION BY series_code ORDER BY date)) OVER (
            PARTITION BY series_code
            ORDER BY date
            ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING
        ) AS avg_12mo_change,
        -- Rolling standard deviation of changes
        STDDEV(value - LAG(value) OVER (PARTITION BY series_code ORDER BY date)) OVER (
            PARTITION BY series_code
            ORDER BY date
            ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING
        ) AS std_12mo_change
    FROM {{ ref('fred_monthly_diff') }}
),

-- Calculate indicator surprises (deviation from trend)
indicator_surprises AS (
    SELECT
        series_code,
        series_name,
        month_date,
        value,
        mom_change,
        mom_pct_change,
        avg_12mo_change,
        std_12mo_change,
        -- Surprise = actual change minus expected (rolling average) change
        mom_change - COALESCE(avg_12mo_change, 0) AS surprise_value,
        -- Normalized surprise (z-score)
        CASE
            WHEN std_12mo_change > 0.0001
            THEN (mom_change - COALESCE(avg_12mo_change, 0)) / std_12mo_change
            ELSE 0
        END AS surprise_zscore,
        -- Categorize surprise magnitude
        CASE
            WHEN std_12mo_change > 0.0001 THEN
                CASE
                    WHEN (mom_change - COALESCE(avg_12mo_change, 0)) / std_12mo_change > 2 THEN 'Large Beat'
                    WHEN (mom_change - COALESCE(avg_12mo_change, 0)) / std_12mo_change > 1 THEN 'Beat'
                    WHEN (mom_change - COALESCE(avg_12mo_change, 0)) / std_12mo_change > -1 THEN 'In Line'
                    WHEN (mom_change - COALESCE(avg_12mo_change, 0)) / std_12mo_change > -2 THEN 'Miss'
                    ELSE 'Large Miss'
                END
            ELSE 'In Line'
        END AS surprise_category,
        -- Direction
        CASE
            WHEN mom_change > 0 THEN 'Rising'
            WHEN mom_change < 0 THEN 'Falling'
            ELSE 'Flat'
        END AS indicator_direction
    FROM indicator_monthly
    WHERE mom_change IS NOT NULL
),

-- Join sectors with indicators for event study
sector_indicator_events AS (
    SELECT
        sr.symbol,
        sn.sector_name,
        sr.month_date,
        sr.monthly_return,
        isp.series_code,
        isp.series_name,
        fsm.category AS indicator_category,
        isp.mom_change,
        isp.mom_pct_change,
        isp.surprise_value,
        isp.surprise_zscore,
        isp.surprise_category,
        isp.indicator_direction
    FROM sector_returns sr
    CROSS JOIN indicator_surprises isp
    LEFT JOIN sector_names sn ON sr.symbol = sn.symbol
    LEFT JOIN {{ ref('fred_series_mapping') }} fsm ON isp.series_code = fsm.code
    WHERE sr.month_date = isp.month_date
        AND sr.monthly_return IS NOT NULL
        AND isp.surprise_zscore IS NOT NULL
),

-- Calculate event study statistics by sector and indicator
event_study_stats AS (
    SELECT
        symbol,
        sector_name,
        series_code,
        series_name,
        indicator_category,
        COUNT(*) AS n_events,

        -- Overall response
        ROUND(AVG(monthly_return), 4) AS avg_return_all_events,
        ROUND(STDDEV(monthly_return), 4) AS return_volatility,

        -- Response by surprise category
        ROUND(AVG(CASE WHEN surprise_category = 'Large Beat' THEN monthly_return END), 4) AS avg_return_large_beat,
        ROUND(AVG(CASE WHEN surprise_category = 'Beat' THEN monthly_return END), 4) AS avg_return_beat,
        ROUND(AVG(CASE WHEN surprise_category = 'In Line' THEN monthly_return END), 4) AS avg_return_inline,
        ROUND(AVG(CASE WHEN surprise_category = 'Miss' THEN monthly_return END), 4) AS avg_return_miss,
        ROUND(AVG(CASE WHEN surprise_category = 'Large Miss' THEN monthly_return END), 4) AS avg_return_large_miss,

        -- Count by category
        COUNT(CASE WHEN surprise_category = 'Large Beat' THEN 1 END) AS n_large_beat,
        COUNT(CASE WHEN surprise_category = 'Beat' THEN 1 END) AS n_beat,
        COUNT(CASE WHEN surprise_category = 'In Line' THEN 1 END) AS n_inline,
        COUNT(CASE WHEN surprise_category = 'Miss' THEN 1 END) AS n_miss,
        COUNT(CASE WHEN surprise_category = 'Large Miss' THEN 1 END) AS n_large_miss,

        -- Response by indicator direction
        ROUND(AVG(CASE WHEN indicator_direction = 'Rising' THEN monthly_return END), 4) AS avg_return_indicator_rising,
        ROUND(AVG(CASE WHEN indicator_direction = 'Falling' THEN monthly_return END), 4) AS avg_return_indicator_falling,
        COUNT(CASE WHEN indicator_direction = 'Rising' THEN 1 END) AS n_rising,
        COUNT(CASE WHEN indicator_direction = 'Falling' THEN 1 END) AS n_falling,

        -- Win rates by category
        ROUND(
            COUNT(CASE WHEN surprise_category IN ('Beat', 'Large Beat') AND monthly_return > 0 THEN 1 END) * 100.0 /
            NULLIF(COUNT(CASE WHEN surprise_category IN ('Beat', 'Large Beat') THEN 1 END), 0),
            1
        ) AS win_rate_on_beat,
        ROUND(
            COUNT(CASE WHEN surprise_category IN ('Miss', 'Large Miss') AND monthly_return > 0 THEN 1 END) * 100.0 /
            NULLIF(COUNT(CASE WHEN surprise_category IN ('Miss', 'Large Miss') THEN 1 END), 0),
            1
        ) AS win_rate_on_miss,

        -- Correlation with surprise magnitude
        ROUND(CORR(surprise_zscore, monthly_return), 4) AS surprise_correlation

    FROM sector_indicator_events
    GROUP BY symbol, sector_name, series_code, series_name, indicator_category
    HAVING COUNT(*) >= 24  -- At least 2 years of data
)

SELECT
    symbol,
    sector_name,
    series_code,
    series_name,
    indicator_category,
    n_events,

    -- Overall stats
    avg_return_all_events,
    return_volatility,

    -- Response by surprise
    avg_return_large_beat,
    avg_return_beat,
    avg_return_inline,
    avg_return_miss,
    avg_return_large_miss,
    n_large_beat,
    n_beat,
    n_inline,
    n_miss,
    n_large_miss,

    -- Response by direction
    avg_return_indicator_rising,
    avg_return_indicator_falling,
    n_rising,
    n_falling,

    -- Beat/Miss spread (how much better sector does on beat vs miss)
    ROUND(
        COALESCE(avg_return_beat, avg_return_large_beat, 0) -
        COALESCE(avg_return_miss, avg_return_large_miss, 0),
        4
    ) AS beat_miss_spread,

    -- Win rates
    win_rate_on_beat,
    win_rate_on_miss,

    -- Surprise sensitivity
    surprise_correlation,

    -- Response strength score (combines spread and correlation)
    ROUND(
        (ABS(COALESCE(surprise_correlation, 0)) * 50) +
        (ABS(COALESCE(avg_return_beat, 0) - COALESCE(avg_return_miss, 0)) * 10),
        2
    ) AS response_strength_score,

    -- Is responsive to surprises?
    CASE
        WHEN ABS(COALESCE(surprise_correlation, 0)) >= 0.2
            OR ABS(COALESCE(avg_return_beat, 0) - COALESCE(avg_return_miss, 0)) >= 1.0
        THEN TRUE
        ELSE FALSE
    END AS is_surprise_responsive,

    -- Response classification
    CASE
        WHEN surprise_correlation > 0.15 THEN 'Pro-cyclical'
        WHEN surprise_correlation < -0.15 THEN 'Counter-cyclical'
        ELSE 'Neutral'
    END AS response_type

FROM event_study_stats
ORDER BY symbol, response_strength_score DESC
