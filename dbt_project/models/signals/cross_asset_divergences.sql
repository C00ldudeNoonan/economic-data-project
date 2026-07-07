/*
    Cross-Asset Divergence Signals

    Final fan-in across smaller cross-asset signal components.
*/

{% set as_of_date = var('as_of_date', 'CURRENT_DATE()') %}

SELECT
    credit.date,
    credit.spy_close,
    credit.spy_sma_50,
    credit.spy_sma_200,
    credit.spy_high_252d,
    credit.hyg_close,
    credit.hyg_sma_50,
    credit.hy_spread,
    credit.hy_spread_20d_change,
    credit.hy_equity_divergence_flag,
    credit.hy_spread_divergence_flag,
    credit.stock_bond_corr_252d,
    credit.stock_bond_corr_regime,
    risk.xlp_xly_ratio,
    risk.xlp_xly_sma_50,
    risk.xlp_xly_sma_200,
    risk.defensive_ratio_uptrend_flag,
    commodities.gold_price,
    commodities.real_yield_10y,
    commodities.gold_real_residual,
    commodities.gold_real_residual_zscore,
    breadth.iwm_spy_ratio,
    breadth.iwm_spy_sma_50,
    breadth.iwm_spy_sma_200,
    breadth.rsp_spy_ratio,
    breadth.rsp_spy_sma_50,
    breadth.rsp_spy_sma_200,
    commodities.copper_gold_ratio,
    commodities.treasury_10y_yield,
    commodities.copper_gold_yield_corr_252d,
    risk.fxa_spy_ratio,
    risk.fxa_spy_sma_50,
    risk.aud_risk_divergence_flag,
    confirmation.dia_close,
    confirmation.iyt_close,
    confirmation.dia_high_252d,
    confirmation.iyt_high_252d,
    confirmation.dow_non_confirmation_flag,
    confirmation.soxx_spy_ratio,
    confirmation.soxx_spy_sma_200,
    confirmation.semis_divergence_flag
FROM {{ ref('int_cross_asset_credit_signals') }} AS credit
LEFT JOIN {{ ref('int_cross_asset_risk_confirmation_signals') }} AS risk
    ON credit.date = risk.date
LEFT JOIN {{ ref('int_cross_asset_commodity_signals') }} AS commodities
    ON credit.date = commodities.date
LEFT JOIN {{ ref('int_cross_asset_breadth_signals') }} AS breadth
    ON credit.date = breadth.date
LEFT JOIN {{ ref('int_cross_asset_confirmation_signals') }} AS confirmation
    ON credit.date = confirmation.date
WHERE credit.date >= DATE_SUB({{ as_of_date }}, INTERVAL 3 YEAR)
ORDER BY credit.date DESC
