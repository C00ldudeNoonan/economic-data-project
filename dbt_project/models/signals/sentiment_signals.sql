{{ config(
    description='Sentiment signals: confidence/sentiment divergence, manufacturing activity'
) }}

/*
    Sentiment Signals Model

    Calculates sentiment-related signals from FRED data:
    - Consumer Confidence vs Sentiment Divergence: OECD confidence vs UMich sentiment
    - Manufacturing Production Level and Trend (IPMAN)
    - Manufacturing New Orders vs Prices (stagflation proxy)

    Note: ISM diffusion indices were removed from FRED in 2016.
    We use FRED-available manufacturing proxies instead.
*/

WITH consumer_sentiment AS (
    SELECT
        DATE_TRUNC('month', date) AS month_date,
        MAX(literal) AS umcsent
    FROM {{ ref('stg_fred_series') }}
    WHERE
        series_code = 'UMCSENT'
        AND literal IS NOT NULL
    GROUP BY DATE_TRUNC('month', date)
),

consumer_confidence AS (
    SELECT
        DATE_TRUNC('month', date) AS month_date,
        MAX(literal) AS confidence
    FROM {{ ref('stg_fred_series') }}
    WHERE
        series_code = 'CSCICP03USM665S'
        AND literal IS NOT NULL
    GROUP BY DATE_TRUNC('month', date)
),

mfg_production AS (
    SELECT
        DATE_TRUNC('month', date) AS month_date,
        MAX(literal) AS ipman
    FROM {{ ref('stg_fred_series') }}
    WHERE
        series_code = 'IPMAN'
        AND literal IS NOT NULL
    GROUP BY DATE_TRUNC('month', date)
),

mfg_new_orders AS (
    SELECT
        DATE_TRUNC('month', date) AS month_date,
        MAX(literal) AS new_orders
    FROM {{ ref('stg_fred_series') }}
    WHERE
        series_code = 'NEWORDER'
        AND literal IS NOT NULL
    GROUP BY DATE_TRUNC('month', date)
),

mfg_prices AS (
    SELECT
        DATE_TRUNC('month', date) AS month_date,
        MAX(literal) AS prices
    FROM {{ ref('stg_fred_series') }}
    WHERE
        series_code = 'PCUOMFG'
        AND literal IS NOT NULL
    GROUP BY DATE_TRUNC('month', date)
),

mfg_employment AS (
    SELECT
        DATE_TRUNC('month', date) AS month_date,
        MAX(literal) AS employment
    FROM {{ ref('stg_fred_series') }}
    WHERE
        series_code = 'MANEMP'
        AND literal IS NOT NULL
    GROUP BY DATE_TRUNC('month', date)
),

mfg_inventories AS (
    SELECT
        DATE_TRUNC('month', date) AS month_date,
        MAX(literal) AS inventories
    FROM {{ ref('stg_fred_series') }}
    WHERE
        series_code = 'MNFCTRMPCIMSA'
        AND literal IS NOT NULL
    GROUP BY DATE_TRUNC('month', date)
),

combined AS (
    SELECT
        COALESCE(cs.month_date, cc.month_date, mp.month_date) AS date,
        cs.umcsent,
        cc.confidence,
        mp.ipman,
        mo.new_orders,
        mpr.prices,
        me.employment,
        mi.inventories
    FROM consumer_sentiment AS cs
    FULL OUTER JOIN consumer_confidence AS cc ON cs.month_date = cc.month_date
    FULL OUTER JOIN mfg_production AS mp ON COALESCE(cs.month_date, cc.month_date) = mp.month_date
    FULL OUTER JOIN mfg_new_orders AS mo ON COALESCE(cs.month_date, cc.month_date, mp.month_date) = mo.month_date
    FULL OUTER JOIN mfg_prices AS mpr ON COALESCE(cs.month_date, cc.month_date, mp.month_date, mo.month_date) = mpr.month_date
    FULL OUTER JOIN mfg_employment AS me ON COALESCE(cs.month_date, cc.month_date, mp.month_date, mo.month_date, mpr.month_date) = me.month_date
    FULL OUTER JOIN mfg_inventories AS mi ON COALESCE(cs.month_date, cc.month_date, mp.month_date, mo.month_date, mpr.month_date, me.month_date) = mi.month_date
),

with_stats AS (
    SELECT
        *,
        -- Z-scores for divergence calculation (using rolling 24-month stats)
        AVG(umcsent) OVER (ORDER BY date ROWS BETWEEN 23 PRECEDING AND CURRENT ROW) AS umcsent_24m_avg,
        STDDEV(umcsent) OVER (ORDER BY date ROWS BETWEEN 23 PRECEDING AND CURRENT ROW) AS umcsent_24m_std,
        AVG(confidence) OVER (ORDER BY date ROWS BETWEEN 23 PRECEDING AND CURRENT ROW) AS confidence_24m_avg,
        STDDEV(confidence) OVER (ORDER BY date ROWS BETWEEN 23 PRECEDING AND CURRENT ROW) AS confidence_24m_std,
        -- Manufacturing production YoY % change
        LAG(ipman, 12) OVER (ORDER BY date) AS ipman_12m_ago,
        LAG(ipman, 3) OVER (ORDER BY date) AS ipman_3m_ago,
        LAG(ipman, 6) OVER (ORDER BY date) AS ipman_6m_ago,
        -- New orders YoY % change
        LAG(new_orders, 12) OVER (ORDER BY date) AS new_orders_12m_ago,
        LAG(new_orders, 1) OVER (ORDER BY date) AS new_orders_prev,
        -- Prices YoY % change
        LAG(prices, 12) OVER (ORDER BY date) AS prices_12m_ago,
        -- Inventories YoY % change
        LAG(inventories, 12) OVER (ORDER BY date) AS inventories_12m_ago
    FROM combined
),

with_yoy AS (
    SELECT
        *,
        -- YoY percent changes
        ROUND(100.0 * (ipman - ipman_12m_ago) / NULLIF(ipman_12m_ago, 0), 2) AS ipman_yoy_pct,
        ROUND(100.0 * (new_orders - new_orders_12m_ago) / NULLIF(new_orders_12m_ago, 0), 2) AS new_orders_yoy_pct,
        ROUND(100.0 * (prices - prices_12m_ago) / NULLIF(prices_12m_ago, 0), 2) AS prices_yoy_pct,
        ROUND(100.0 * (inventories - inventories_12m_ago) / NULLIF(inventories_12m_ago, 0), 2) AS inventories_yoy_pct,
        -- Z-scores
        ROUND((umcsent - umcsent_24m_avg) / NULLIF(umcsent_24m_std, 0), 2) AS umcsent_zscore,
        ROUND((confidence - confidence_24m_avg) / NULLIF(confidence_24m_std, 0), 2) AS confidence_zscore,
        ROUND(
            ((confidence - confidence_24m_avg) / NULLIF(confidence_24m_std, 0))
            - ((umcsent - umcsent_24m_avg) / NULLIF(umcsent_24m_std, 0)),
            2
        ) AS confidence_sentiment_divergence
    FROM with_stats
)

SELECT
    date,
    umcsent,
    confidence,
    ipman,
    new_orders,
    prices AS mfg_prices,
    employment AS mfg_employment,
    inventories AS mfg_inventories,
    umcsent_zscore,
    confidence_zscore,
    confidence_sentiment_divergence,
    ipman_yoy_pct,
    new_orders_yoy_pct,
    prices_yoy_pct,
    inventories_yoy_pct,

    -- Manufacturing production trend
    ROUND(100.0 * (ipman - ipman_3m_ago) / NULLIF(ipman_3m_ago, 0), 2) AS ipman_3m_change_pct,
    ROUND(100.0 * (ipman - ipman_6m_ago) / NULLIF(ipman_6m_ago, 0), 2) AS ipman_6m_change_pct,

    -- Signal: Confidence/Sentiment divergence
    CASE
        WHEN ABS(confidence_sentiment_divergence) > 1.5 THEN 'high'
        WHEN ABS(confidence_sentiment_divergence) > 1.0 THEN 'medium'
        ELSE 'normal'
    END AS divergence_status,

    -- Signal: Manufacturing production (YoY decline = contraction)
    CASE
        WHEN ipman_yoy_pct < -5 THEN 'high'
        WHEN ipman_yoy_pct < -2 THEN 'medium'
        WHEN ipman_yoy_pct > 5 THEN 'low'
        ELSE 'normal'
    END AS mfg_production_status,

    -- Signal: Stagflation proxy (falling orders + rising prices)
    CASE
        WHEN new_orders_yoy_pct < -5 AND prices_yoy_pct > 5 THEN 'high'
        WHEN new_orders_yoy_pct < 0 AND prices_yoy_pct > 3 THEN 'medium'
        ELSE 'normal'
    END AS stagflation_status,

    -- Signal: New orders trend
    CASE
        WHEN new_orders_yoy_pct < -10 THEN 'high'
        WHEN new_orders_yoy_pct < -5 AND new_orders_prev IS NOT NULL AND new_orders < new_orders_prev THEN 'medium'
        ELSE 'normal'
    END AS new_orders_status,

    -- Signal: Orders vs Inventories (demand-supply balance)
    CASE
        WHEN new_orders_yoy_pct < -5 AND inventories_yoy_pct > 5 THEN 'high'
        WHEN new_orders_yoy_pct < 0 AND inventories_yoy_pct > 0 THEN 'medium'
        WHEN new_orders_yoy_pct < inventories_yoy_pct THEN 'low'
        ELSE 'normal'
    END AS orders_inventories_status

FROM with_yoy
WHERE date >= CURRENT_DATE - INTERVAL 3 YEAR
ORDER BY date DESC
