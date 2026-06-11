/*
    Cross-Asset Divergence Signals

    Builds relational signals across equities, credit, FX, commodities, and rates.
*/

WITH spy_prices AS (
    SELECT
        date,
        adj_close AS spy_close
    FROM {{ ref('stg_major_indices') }}
    WHERE symbol = 'SPY'
      AND adj_close IS NOT NULL
      AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR)
),

spy_indicators AS (
    SELECT
        date,
        spy_close,
        AVG(spy_close) OVER (
            ORDER BY date
            ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
        ) AS spy_sma_50,
        AVG(spy_close) OVER (
            ORDER BY date
            ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
        ) AS spy_sma_200,
        MAX(spy_close) OVER (
            ORDER BY date
            ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
        ) AS spy_high_252d
    FROM spy_prices
),

hyg_prices AS (
    SELECT
        date,
        adj_close AS hyg_close
    FROM {{ ref('stg_fixed_income') }}
    WHERE symbol = 'HYG'
      AND adj_close IS NOT NULL
      AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR)
),

hyg_indicators AS (
    SELECT
        date,
        hyg_close,
        AVG(hyg_close) OVER (
            ORDER BY date
            ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
        ) AS hyg_sma_50
    FROM hyg_prices
),

hy_spread_data AS (
    SELECT
        date,
        value AS hy_spread
    FROM {{ ref('stg_fred_series') }}
    WHERE series_code = 'BAMLH0A0HYM2'
      AND value IS NOT NULL
      AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR)
),

hy_spread_indicators AS (
    SELECT
        date,
        hy_spread,
        hy_spread - LAG(hy_spread, 20) OVER (ORDER BY date) AS hy_spread_20d_change
    FROM hy_spread_data
),

hy_equity_divergence AS (
    SELECT
        s.date,
        s.spy_close,
        s.spy_sma_50,
        s.spy_sma_200,
        s.spy_high_252d,
        h.hyg_close,
        h.hyg_sma_50,
        hs.hy_spread,
        hs.hy_spread_20d_change,
        CASE
            WHEN h.hyg_close < h.hyg_sma_50
                AND s.spy_close > s.spy_sma_50 THEN 1
            ELSE 0
        END AS hy_equity_divergence_flag,
        CASE
            WHEN hs.hy_spread_20d_change > 0
                AND s.spy_close >= s.spy_high_252d THEN 1
            ELSE 0
        END AS hy_spread_divergence_flag
    FROM spy_indicators s
    LEFT JOIN hyg_indicators h ON s.date = h.date
    LEFT JOIN hy_spread_indicators hs ON s.date = hs.date
),

spy_returns AS (
    SELECT
        date,
        spy_close,
        (SAFE_DIVIDE(spy_close, LAG(spy_close) OVER (ORDER BY date)) - 1.0) AS spy_return
    FROM spy_prices
),

govt_prices AS (
    SELECT
        date,
        adj_close AS govt_close
    FROM {{ ref('stg_fixed_income') }}
    WHERE symbol = 'GOVT'
      AND adj_close IS NOT NULL
      AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR)
),

govt_returns AS (
    SELECT
        date,
        govt_close,
        (SAFE_DIVIDE(govt_close, LAG(govt_close) OVER (ORDER BY date)) - 1.0) AS govt_return
    FROM govt_prices
),

stock_bond_corr AS (
    SELECT
        s.date,
        CORR(s.spy_return, g.govt_return) OVER (
            ORDER BY s.date
            ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
        ) AS stock_bond_corr_252d
    FROM spy_returns s
    INNER JOIN govt_returns g
        ON s.date = g.date
    WHERE s.spy_return IS NOT NULL
      AND g.govt_return IS NOT NULL
),

xlp_prices AS (
    SELECT
        date,
        adj_close AS xlp_close
    FROM {{ ref('stg_us_sectors') }}
    WHERE symbol = 'XLP'
      AND adj_close IS NOT NULL
      AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR)
),

xly_prices AS (
    SELECT
        date,
        adj_close AS xly_close
    FROM {{ ref('stg_us_sectors') }}
    WHERE symbol = 'XLY'
      AND adj_close IS NOT NULL
      AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR)
),

xlp_xly_ratio_data AS (
    SELECT
        xlp.date,
        xlp.xlp_close,
        xly.xly_close,
        CASE
            WHEN xly.xly_close > 0 THEN SAFE_DIVIDE(xlp.xlp_close, xly.xly_close)
        END AS xlp_xly_ratio
    FROM xlp_prices xlp
    INNER JOIN xly_prices xly
        ON xlp.date = xly.date
),

xlp_xly_indicators AS (
    SELECT
        date,
        xlp_xly_ratio,
        AVG(xlp_xly_ratio) OVER (
            ORDER BY date
            ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
        ) AS xlp_xly_sma_50,
        AVG(xlp_xly_ratio) OVER (
            ORDER BY date
            ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
        ) AS xlp_xly_sma_200
    FROM xlp_xly_ratio_data
),

gold_prices AS (
    SELECT
        date,
        price AS gold_price
    FROM {{ ref('stg_input_commodities') }}
    WHERE commodity_name = 'gold'
      AND price IS NOT NULL
      AND price > 0
      AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR)
),

copper_prices AS (
    SELECT
        date,
        price AS copper_price
    FROM {{ ref('stg_input_commodities') }}
    WHERE commodity_name = 'copper'
      AND price IS NOT NULL
      AND price > 0
      AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR)
),

real_yields AS (
    SELECT
        date,
        value AS real_yield_10y
    FROM {{ ref('stg_fred_series') }}
    WHERE series_code = 'DFII10'
      AND value IS NOT NULL
      AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR)
),

gold_real_base AS (
    SELECT
        g.date,
        g.gold_price,
        r.real_yield_10y
    FROM gold_prices g
    INNER JOIN real_yields r
        ON g.date = r.date
),

gold_real_regression AS (
    SELECT
        date,
        gold_price,
        real_yield_10y,
        AVG(real_yield_10y) OVER w AS avg_real_yield,
        AVG(gold_price) OVER w AS avg_gold_price,
        AVG(real_yield_10y * gold_price) OVER w AS avg_xy,
        AVG(real_yield_10y * real_yield_10y) OVER w AS avg_x2
    FROM gold_real_base
    WINDOW w AS (
        ORDER BY date
        ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
    )
),

gold_real_residual AS (
    SELECT
        date,
        gold_price,
        real_yield_10y,
        CASE
            WHEN (avg_x2 - (avg_real_yield * avg_real_yield)) <> 0 THEN
                SAFE_DIVIDE(
                    avg_xy - (avg_real_yield * avg_gold_price),
                    avg_x2 - (avg_real_yield * avg_real_yield)
                )
        END AS beta,
        CASE
            WHEN (avg_x2 - (avg_real_yield * avg_real_yield)) <> 0 THEN
                avg_gold_price
                - (
                    SAFE_DIVIDE(
                        avg_xy - (avg_real_yield * avg_gold_price),
                        avg_x2 - (avg_real_yield * avg_real_yield)
                    )
                ) * avg_real_yield
        END AS alpha
    FROM gold_real_regression
),

gold_real_zscore AS (
    SELECT
        date,
        gold_price,
        real_yield_10y,
        CASE
            WHEN beta IS NOT NULL AND alpha IS NOT NULL THEN
                gold_price - (alpha + beta * real_yield_10y)
        END AS gold_real_residual,
        AVG(
            CASE
                WHEN beta IS NOT NULL AND alpha IS NOT NULL THEN
                    gold_price - (alpha + beta * real_yield_10y)
            END
        ) OVER w AS residual_avg,
        STDDEV_SAMP(
            CASE
                WHEN beta IS NOT NULL AND alpha IS NOT NULL THEN
                    gold_price - (alpha + beta * real_yield_10y)
            END
        ) OVER w AS residual_std
    FROM gold_real_residual
    WINDOW w AS (
        ORDER BY date
        ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
    )
),

iwm_prices AS (
    SELECT
        date,
        adj_close AS iwm_close
    FROM {{ ref('stg_major_indices') }}
    WHERE symbol = 'IWM'
      AND adj_close IS NOT NULL
      AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR)
),

rsp_prices AS (
    SELECT
        date,
        adj_close AS rsp_close
    FROM {{ ref('stg_major_indices') }}
    WHERE symbol = 'RSP'
      AND adj_close IS NOT NULL
      AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR)
),

iwm_spy_ratio_data AS (
    SELECT
        s.date,
        CASE
            WHEN s.spy_close > 0 THEN SAFE_DIVIDE(i.iwm_close, s.spy_close)
        END AS iwm_spy_ratio
    FROM spy_prices s
    INNER JOIN iwm_prices i
        ON s.date = i.date
),

iwm_spy_indicators AS (
    SELECT
        date,
        iwm_spy_ratio,
        AVG(iwm_spy_ratio) OVER (
            ORDER BY date
            ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
        ) AS iwm_spy_sma_50,
        AVG(iwm_spy_ratio) OVER (
            ORDER BY date
            ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
        ) AS iwm_spy_sma_200
    FROM iwm_spy_ratio_data
),

rsp_spy_ratio_data AS (
    SELECT
        s.date,
        CASE
            WHEN s.spy_close > 0 THEN SAFE_DIVIDE(r.rsp_close, s.spy_close)
        END AS rsp_spy_ratio
    FROM spy_prices s
    INNER JOIN rsp_prices r
        ON s.date = r.date
),

rsp_spy_indicators AS (
    SELECT
        date,
        rsp_spy_ratio,
        AVG(rsp_spy_ratio) OVER (
            ORDER BY date
            ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
        ) AS rsp_spy_sma_50,
        AVG(rsp_spy_ratio) OVER (
            ORDER BY date
            ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
        ) AS rsp_spy_sma_200
    FROM rsp_spy_ratio_data
),

copper_gold_base AS (
    SELECT
        g.date,
        g.gold_price,
        c.copper_price,
        CASE
            WHEN g.gold_price > 0 THEN SAFE_DIVIDE(c.copper_price, g.gold_price) * 1000
        END AS copper_gold_ratio
    FROM gold_prices g
    INNER JOIN copper_prices c
        ON g.date = c.date
),

treasury_yields AS (
    SELECT
        SAFE_CAST(date AS DATE) AS date,
        bc_10year AS treasury_10y_yield
    FROM {{ ref('stg_treasury_yields') }}
    WHERE bc_10year IS NOT NULL
      AND SAFE_CAST(date AS DATE) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR)
),

copper_gold_yield_corr AS (
    SELECT
        c.date,
        c.copper_gold_ratio,
        t.treasury_10y_yield,
        CORR(c.copper_gold_ratio, t.treasury_10y_yield) OVER (
            ORDER BY c.date
            ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
        ) AS copper_gold_yield_corr_252d
    FROM copper_gold_base c
    INNER JOIN treasury_yields t
        ON c.date = t.date
),

fxa_prices AS (
    SELECT
        date,
        adj_close AS fxa_close
    FROM {{ ref('stg_currency') }}
    WHERE symbol = 'FXA'
      AND adj_close IS NOT NULL
      AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR)
),

fxa_spy_ratio_data AS (
    SELECT
        s.date,
        CASE
            WHEN s.spy_close > 0 THEN SAFE_DIVIDE(f.fxa_close, s.spy_close)
        END AS fxa_spy_ratio
    FROM spy_prices s
    INNER JOIN fxa_prices f
        ON s.date = f.date
),

fxa_spy_indicators AS (
    SELECT
        date,
        fxa_spy_ratio,
        AVG(fxa_spy_ratio) OVER (
            ORDER BY date
            ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
        ) AS fxa_spy_sma_50
    FROM fxa_spy_ratio_data
),

dia_prices AS (
    SELECT
        date,
        adj_close AS dia_close
    FROM {{ ref('stg_major_indices') }}
    WHERE symbol = 'DIA'
      AND adj_close IS NOT NULL
      AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR)
),

iyt_prices AS (
    SELECT
        date,
        adj_close AS iyt_close
    FROM {{ ref('stg_major_indices') }}
    WHERE symbol = 'IYT'
      AND adj_close IS NOT NULL
      AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR)
),

dow_theory AS (
    SELECT
        d.date,
        d.dia_close,
        i.iyt_close,
        MAX(d.dia_close) OVER (
            ORDER BY d.date
            ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
        ) AS dia_high_252d,
        MAX(i.iyt_close) OVER (
            ORDER BY i.date
            ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
        ) AS iyt_high_252d
    FROM dia_prices d
    INNER JOIN iyt_prices i
        ON d.date = i.date
),

soxx_prices AS (
    SELECT
        date,
        adj_close AS soxx_close
    FROM {{ ref('stg_major_indices') }}
    WHERE symbol = 'SOXX'
      AND adj_close IS NOT NULL
      AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR)
),

soxx_spy_ratio_data AS (
    SELECT
        s.date,
        CASE
            WHEN s.spy_close > 0 THEN SAFE_DIVIDE(x.soxx_close, s.spy_close)
        END AS soxx_spy_ratio
    FROM spy_prices s
    INNER JOIN soxx_prices x
        ON s.date = x.date
),

soxx_spy_indicators AS (
    SELECT
        date,
        soxx_spy_ratio,
        AVG(soxx_spy_ratio) OVER (
            ORDER BY date
            ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
        ) AS soxx_spy_sma_200
    FROM soxx_spy_ratio_data
)

SELECT
    h.date,
    h.spy_close,
    h.spy_sma_50,
    h.spy_sma_200,
    h.spy_high_252d,
    h.hyg_close,
    h.hyg_sma_50,
    h.hy_spread,
    h.hy_spread_20d_change,
    h.hy_equity_divergence_flag,
    h.hy_spread_divergence_flag,

    sb.stock_bond_corr_252d,
    CASE
        WHEN sb.stock_bond_corr_252d > 0 THEN 'positive'
        WHEN sb.stock_bond_corr_252d IS NULL THEN NULL
        ELSE 'negative'
    END AS stock_bond_corr_regime,

    xlp.xlp_xly_ratio,
    xlp.xlp_xly_sma_50,
    xlp.xlp_xly_sma_200,
    CASE
        WHEN xlp.xlp_xly_ratio > xlp.xlp_xly_sma_50
            AND xlp.xlp_xly_sma_50 > xlp.xlp_xly_sma_200 THEN 1
        ELSE 0
    END AS defensive_ratio_uptrend_flag,

    gr.gold_price,
    gr.real_yield_10y,
    gr.gold_real_residual,
    CASE
        WHEN gr.residual_std > 0 THEN SAFE_DIVIDE(gr.gold_real_residual - gr.residual_avg, gr.residual_std)
    END AS gold_real_residual_zscore,

    iwm.iwm_spy_ratio,
    iwm.iwm_spy_sma_50,
    iwm.iwm_spy_sma_200,

    rsp.rsp_spy_ratio,
    rsp.rsp_spy_sma_50,
    rsp.rsp_spy_sma_200,

    cg.copper_gold_ratio,
    cg.treasury_10y_yield,
    cg.copper_gold_yield_corr_252d,

    fxa.fxa_spy_ratio,
    fxa.fxa_spy_sma_50,
    CASE
        WHEN fxa.fxa_spy_ratio < fxa.fxa_spy_sma_50
            AND h.spy_close > h.spy_sma_50 THEN 1
        ELSE 0
    END AS aud_risk_divergence_flag,

    dow.dia_close,
    dow.iyt_close,
    dow.dia_high_252d,
    dow.iyt_high_252d,
    CASE
        WHEN dow.dia_close >= dow.dia_high_252d
            AND dow.iyt_close < dow.iyt_high_252d * 0.98 THEN 1
        ELSE 0
    END AS dow_non_confirmation_flag,

    soxx.soxx_spy_ratio,
    soxx.soxx_spy_sma_200,
    CASE
        WHEN soxx.soxx_spy_ratio < soxx.soxx_spy_sma_200
            AND h.spy_close >= h.spy_high_252d * 0.98 THEN 1
        ELSE 0
    END AS semis_divergence_flag

FROM hy_equity_divergence h
LEFT JOIN stock_bond_corr sb ON h.date = sb.date
LEFT JOIN xlp_xly_indicators xlp ON h.date = xlp.date
LEFT JOIN gold_real_zscore gr ON h.date = gr.date
LEFT JOIN iwm_spy_indicators iwm ON h.date = iwm.date
LEFT JOIN rsp_spy_indicators rsp ON h.date = rsp.date
LEFT JOIN copper_gold_yield_corr cg ON h.date = cg.date
LEFT JOIN fxa_spy_indicators fxa ON h.date = fxa.date
LEFT JOIN dow_theory dow ON h.date = dow.date
LEFT JOIN soxx_spy_indicators soxx ON h.date = soxx.date

WHERE h.date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR)
ORDER BY h.date DESC
