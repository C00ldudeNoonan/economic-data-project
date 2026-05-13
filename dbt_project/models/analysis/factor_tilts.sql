{{ config(
    materialized='view',
    description='Maps economic regimes to recommended factor tilts (value, momentum, quality, low vol, size)'
) }}

/*
    Factor Tilt Mapping

    Provides a monthly time series of recommended factor tilts based on
    the existing economic regime classification.

    Regimes: Expansion, Slowdown, Contraction, Recovery
*/

WITH regime_history AS (
    SELECT
        month_date,
        regime
    FROM {{ ref('economic_regime_classification') }}
),

regime_mapping AS (
    SELECT
        t.*
    FROM (
        VALUES
            ('Expansion', 'Neutral', 'Overweight', 'Neutral', 'Underweight', 'Neutral',
             'Momentum tends to lead in sustained expansions.'),
            ('Slowdown', 'Overweight', 'Neutral', 'Overweight', 'Neutral', 'Neutral',
             'Value and financial strength tend to outperform late-cycle.'),
            ('Contraction', 'Neutral', 'Underweight', 'Overweight', 'Overweight', 'Underweight',
             'Quality and low volatility typically hold up best in recessions.'),
            ('Recovery', 'Overweight', 'Neutral', 'Neutral', 'Underweight', 'Overweight',
             'Early recoveries favor value and size as risk appetite returns.')
    ) AS t (
        regime,
        value_tilt,
        momentum_tilt,
        quality_tilt,
        low_vol_tilt,
        size_tilt,
        notes
    )
)

SELECT
    rh.month_date,
    rh.regime,
    rm.value_tilt,
    rm.momentum_tilt,
    rm.quality_tilt,
    rm.low_vol_tilt,
    rm.size_tilt,
    rm.notes
FROM regime_history rh
LEFT JOIN regime_mapping rm
    ON rh.regime = rm.regime
WHERE rh.regime IS NOT NULL
ORDER BY rh.month_date
