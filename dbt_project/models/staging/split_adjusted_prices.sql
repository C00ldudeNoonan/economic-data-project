{{ config(materialized='table') }}

-- Computes split-adjusted prices for individual stocks.
-- For each symbol, calculates a cumulative split adjustment factor = product of all
-- split factors occurring AFTER that date. Historical prices are divided by this factor
-- to make them comparable to post-split prices.

{% set split_eligible = split_eligible_tables() %}

{% for entry in split_eligible %}

SELECT
    source_table,
    symbol,
    date,
    exchange,
    name,
    asset_type,
    open,
    high,
    low,
    close,
    volume,
    adj_close,
    adj_open,
    adj_high,
    adj_low,
    adj_volume,
    raw_split_factor,
    dividend,
    cumulative_split_factor,
    close / cumulative_split_factor AS split_adj_close,
    open / cumulative_split_factor AS split_adj_open,
    high / cumulative_split_factor AS split_adj_high,
    low / cumulative_split_factor AS split_adj_low,
    volume * cumulative_split_factor AS split_adj_volume
FROM (
    SELECT
        '{{ entry.raw }}' AS source_table,
        p.symbol,
        CAST(p.date AS DATE) AS date,
        p.exchange,
        p.name,
        p.asset_type,
        p.open,
        p.high,
        p.low,
        p.close,
        p.volume,
        p.adj_close,
        p.adj_open,
        p.adj_high,
        p.adj_low,
        p.adj_volume,
        p.split_factor AS raw_split_factor,
        p.dividend,
        -- Cumulative split factor: product of all future splits for this symbol
        -- EXP(SUM(LN(x))) = product of x values; COALESCE to 1.0 when no future splits
        COALESCE(
            EXP(
                SUM(LN(s.split_factor)) OVER (
                    PARTITION BY p.symbol
                    ORDER BY CAST(p.date AS DATE)
                    ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING
                )
            ),
            1.0
        ) AS cumulative_split_factor
    FROM {{ ref(entry.stg) }} AS p
    LEFT JOIN (
        SELECT symbol, date, split_factor
        FROM {{ ref('corporate_actions') }}
        WHERE source_table = '{{ entry.raw }}'
          AND action_type = 'split'
          AND split_factor != 1
    ) AS s
        ON p.symbol = s.symbol
        AND CAST(p.date AS DATE) = s.date
) AS adjusted

{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
