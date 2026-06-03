{{ config(
    description='Custom diffusion index: breadth of improving economic indicators across employment, production, housing, trade'
) }}

/*
    Custom Diffusion Index

    Selects key FRED series across major economic categories. For each series,
    scores 1 if the 3-month change is positive (improving), 0 otherwise.
    Diffusion = sum(improving) / total × 100.

    Thresholds:
    - Above 50: majority of indicators improving (expansion)
    - Below 50: majority deteriorating (contraction risk)
    - Below 30: recession-like breadth

    A narrow expansion (low diffusion while headline GDP positive) signals fragility.
*/

WITH series_monthly AS (
    SELECT
        DATE_TRUNC('month', date) AS month_date,
        series_code,
        MAX(value) AS val
    FROM {{ ref('stg_fred_series') }}
    WHERE series_code IN (
        -- Employment (5)
        'PAYEMS',       -- Nonfarm payrolls
        'CIVPART',      -- Labor force participation
        'JTSJOL',       -- Job openings
        'EMRATIO',      -- Employment-population ratio
        'JTSHIR',       -- Hires
        -- Production & Output (4)
        'INDPRO',       -- Industrial production
        'TCU',          -- Capacity utilization
        'RSXFS',        -- Retail sales
        'PCEC96',       -- Real consumer spending
        -- Housing (3)
        'HOUST',        -- Housing starts
        'PERMIT',       -- Building permits
        'CSUSHPISA',    -- Home prices
        -- Trade & FX (2)
        'EXPGS',        -- Exports
        'BOPGSTB',      -- Trade balance (inverted: less negative = improving)
        -- Sentiment & Leading (3)
        'UMCSENT',      -- Consumer sentiment
        'IPMAN',        -- Manufacturing production
        'NEWORDER',     -- Manufacturers' new orders
        -- Credit & Monetary (3)
        'M2SL',         -- M2 money supply
        'BUSLOANS',     -- Business loans
        'PI'            -- Personal income
    )
    AND value IS NOT NULL
    GROUP BY DATE_TRUNC('month', date), series_code
),

with_changes AS (
    SELECT
        month_date,
        series_code,
        val,
        LAG(val, 3) OVER (PARTITION BY series_code ORDER BY month_date) AS val_3m_ago,
        -- For trade balance (BOPGSTB), less negative = improving, so normal direction works
        CASE
            WHEN series_code = 'ICSA'
            THEN -1  -- Inverted: fewer claims = improving
            ELSE 1
        END AS direction
    FROM series_monthly
),

scored AS (
    SELECT
        month_date,
        series_code,
        val,
        val_3m_ago,
        CASE
            WHEN val_3m_ago IS NULL THEN NULL
            WHEN (val - val_3m_ago) * direction > 0 THEN 1
            ELSE 0
        END AS is_improving
    FROM with_changes
),

monthly_diffusion AS (
    SELECT
        month_date AS date,
        COUNT(*) FILTER (WHERE is_improving IS NOT NULL) AS total_count,
        COALESCE(SUM(is_improving), 0) AS improving_count,
        ROUND(
            COALESCE(SUM(is_improving), 0) * 100.0 / NULLIF(COUNT(*) FILTER (WHERE is_improving IS NOT NULL), 0),
            1
        ) AS diffusion_pct
    FROM scored
    GROUP BY month_date
),

with_stats AS (
    SELECT
        *,
        -- Trend
        LAG(diffusion_pct, 1) OVER (ORDER BY date) AS diffusion_prev_month,
        LAG(diffusion_pct, 3) OVER (ORDER BY date) AS diffusion_3m_ago,
        AVG(diffusion_pct) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS diffusion_6m_avg,
        -- Rolling z-score (24-month window)
        (diffusion_pct - AVG(diffusion_pct) OVER (ORDER BY date ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING))
            / NULLIF(STDDEV(diffusion_pct) OVER (ORDER BY date ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING), 0)
            AS diffusion_zscore,
        -- Consecutive months below 50
        SUM(CASE WHEN diffusion_pct >= 50 THEN 1 ELSE 0 END)
            OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS above_50_group
    FROM monthly_diffusion
)

SELECT
    date,
    total_count,
    improving_count,
    diffusion_pct,
    ROUND(diffusion_6m_avg, 1) AS diffusion_6m_avg,
    ROUND(diffusion_zscore, 2) AS diffusion_zscore,

    -- Momentum
    ROUND(diffusion_pct - COALESCE(diffusion_prev_month, diffusion_pct), 1) AS diffusion_mom_change,
    ROUND(diffusion_pct - COALESCE(diffusion_3m_ago, diffusion_pct), 1) AS diffusion_3m_change,

    -- Breadth trend
    CASE
        WHEN diffusion_pct >= 50 AND diffusion_pct > COALESCE(diffusion_prev_month, 0) THEN 'broadening'
        WHEN diffusion_pct >= 50 THEN 'stable'
        WHEN diffusion_pct < 50 AND diffusion_pct < COALESCE(diffusion_prev_month, 100) THEN 'narrowing'
        ELSE 'recovering'
    END AS breadth_trend,

    -- Signal status
    CASE
        WHEN diffusion_pct < 30 THEN 'high'
        WHEN diffusion_pct < 40 THEN 'medium'
        WHEN diffusion_pct < 50 THEN 'low'
        ELSE 'normal'
    END AS diffusion_status

FROM with_stats
WHERE date >= CURRENT_DATE - INTERVAL 3 YEAR
ORDER BY date DESC
