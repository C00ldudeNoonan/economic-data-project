{{ config(
    description='Net liquidity indicator: Fed Total Assets minus Treasury General Account minus Reverse Repo'
) }}

/*
    Net Liquidity Indicator

    Construction: Net Liquidity = WALCL - WTREGEN - RRPONTSYD
    - WALCL: Fed total assets (balance sheet size)
    - WTREGEN: Treasury General Account balance (cash sitting at Fed, unavailable to markets)
    - RRPONTSYD: Reverse repo facility usage (cash parked at Fed by money market funds)

    This proxy for money available to financial markets correlates with risk asset
    performance. The RRP draining toward zero from ~$2.5T peak represents a regime
    shift — further QT directly drains bank reserves.

    Data grain: Weekly (WALCL is weekly, others daily → aggregated to weekly)
    Fallback: If WTREGEN is not yet ingested, computes 2-component version (WALCL - RRPONTSYD)
*/

WITH walcl_weekly AS (
    SELECT
        DATE_TRUNC('week', date) AS week_date,
        AVG(literal) AS walcl
    FROM {{ ref('stg_fred_series') }}
    WHERE series_code = 'WALCL'
      AND literal IS NOT NULL
    GROUP BY DATE_TRUNC('week', date)
),

wtregen_weekly AS (
    SELECT
        DATE_TRUNC('week', date) AS week_date,
        AVG(literal) AS wtregen
    FROM {{ ref('stg_fred_series') }}
    WHERE series_code = 'WTREGEN'
      AND literal IS NOT NULL
    GROUP BY DATE_TRUNC('week', date)
),

rrp_weekly AS (
    SELECT
        DATE_TRUNC('week', date) AS week_date,
        AVG(literal) AS rrpontsyd
    FROM {{ ref('stg_fred_series') }}
    WHERE series_code = 'RRPONTSYD'
      AND literal IS NOT NULL
    GROUP BY DATE_TRUNC('week', date)
),

combined AS (
    SELECT
        w.week_date AS date,
        w.walcl,
        wt.wtregen,
        r.rrpontsyd,
        -- Net liquidity: 3-component if WTREGEN available, else 2-component
        w.walcl - COALESCE(wt.wtregen, 0) - COALESCE(r.rrpontsyd, 0) AS net_liquidity,
        CASE WHEN wt.wtregen IS NOT NULL THEN 3 ELSE 2 END AS component_count
    FROM walcl_weekly w
    LEFT JOIN wtregen_weekly wt ON w.week_date = wt.week_date
    LEFT JOIN rrp_weekly r ON w.week_date = r.week_date
),

with_trends AS (
    SELECT
        *,
        -- Moving averages
        AVG(net_liquidity) OVER (ORDER BY date ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS net_liquidity_4w_avg,
        AVG(net_liquidity) OVER (ORDER BY date ROWS BETWEEN 12 PRECEDING AND CURRENT ROW) AS net_liquidity_13w_avg,
        -- Lagged values for rate of change
        LAG(net_liquidity, 4) OVER (ORDER BY date) AS net_liquidity_4w_ago,
        LAG(net_liquidity, 13) OVER (ORDER BY date) AS net_liquidity_13w_ago,
        LAG(net_liquidity, 52) OVER (ORDER BY date) AS net_liquidity_52w_ago,
        -- Rolling stats for z-score (52-week window)
        AVG(net_liquidity) OVER (ORDER BY date ROWS BETWEEN 52 PRECEDING AND 1 PRECEDING) AS nl_52w_mean,
        STDDEV(net_liquidity) OVER (ORDER BY date ROWS BETWEEN 52 PRECEDING AND 1 PRECEDING) AS nl_52w_std
    FROM combined
)

SELECT
    date,
    ROUND(walcl, 2) AS walcl,
    ROUND(wtregen, 2) AS wtregen,
    ROUND(rrpontsyd, 2) AS rrpontsyd,
    ROUND(net_liquidity, 2) AS net_liquidity,
    component_count,
    ROUND(net_liquidity_4w_avg, 2) AS net_liquidity_4w_avg,
    ROUND(net_liquidity_13w_avg, 2) AS net_liquidity_13w_avg,

    -- Rate of change
    ROUND(((net_liquidity / NULLIF(net_liquidity_4w_ago, 0)) - 1) * 100, 2) AS net_liquidity_4w_pct_change,
    ROUND(((net_liquidity / NULLIF(net_liquidity_13w_ago, 0)) - 1) * 100, 2) AS net_liquidity_13w_pct_change,
    ROUND(((net_liquidity / NULLIF(net_liquidity_52w_ago, 0)) - 1) * 100, 2) AS net_liquidity_52w_pct_change,

    -- Z-score
    ROUND((net_liquidity - nl_52w_mean) / NULLIF(nl_52w_std, 0), 2) AS net_liquidity_zscore,

    -- Trend direction
    CASE
        WHEN net_liquidity > net_liquidity_4w_avg AND net_liquidity_4w_avg > net_liquidity_13w_avg THEN 'expanding'
        WHEN net_liquidity < net_liquidity_4w_avg AND net_liquidity_4w_avg < net_liquidity_13w_avg THEN 'contracting'
        ELSE 'mixed'
    END AS net_liquidity_trend,

    -- Signal status
    CASE
        WHEN (net_liquidity - nl_52w_mean) / NULLIF(nl_52w_std, 0) < -2 THEN 'high'
        WHEN (net_liquidity - nl_52w_mean) / NULLIF(nl_52w_std, 0) < -1 THEN 'medium'
        WHEN net_liquidity < net_liquidity_4w_avg AND net_liquidity_4w_avg < net_liquidity_13w_avg THEN 'low'
        ELSE 'normal'
    END AS net_liquidity_status,

    -- RRP depletion signal (approaching zero = regime shift)
    CASE
        WHEN rrpontsyd IS NOT NULL AND rrpontsyd < 50000 THEN 'high'
        WHEN rrpontsyd IS NOT NULL AND rrpontsyd < 200000 THEN 'medium'
        WHEN rrpontsyd IS NOT NULL AND rrpontsyd < 500000 THEN 'low'
        ELSE 'normal'
    END AS rrp_depletion_status

FROM with_trends
WHERE date >= CURRENT_DATE - INTERVAL 3 YEAR
ORDER BY date DESC
