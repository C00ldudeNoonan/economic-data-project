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
    SELECT *
    FROM UNNEST([
        STRUCT(
            'Expansion' AS regime,
            'Neutral' AS value_tilt,
            'Overweight' AS momentum_tilt,
            'Neutral' AS quality_tilt,
            'Underweight' AS low_vol_tilt,
            'Neutral' AS size_tilt,
            'Momentum tends to lead in sustained expansions.' AS notes
        ),
        STRUCT('Slowdown', 'Overweight', 'Neutral', 'Overweight', 'Neutral', 'Neutral', 'Value and financial strength tend to outperform late-cycle.'),
        STRUCT('Contraction', 'Neutral', 'Underweight', 'Overweight', 'Overweight', 'Underweight', 'Quality and low volatility typically hold up best in recessions.'),
        STRUCT('Recovery', 'Overweight', 'Neutral', 'Neutral', 'Underweight', 'Overweight', 'Early recoveries favor value and size as risk appetite returns.')
    ])
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
