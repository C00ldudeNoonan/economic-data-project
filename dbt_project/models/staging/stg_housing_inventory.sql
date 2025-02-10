{{ config(
    materialized='view'
) }}

SELECT
    inv.data_type_code,
    inv.time_slot_id,
    inv.seasonally_adj,
    inv.category_code,
    inv.cell_value,
    inv.error_data,
    inv.time,
    inv.us,
    map.series_name,
    map.plot_groupings
FROM {{ source('bls_data', 'housing_inventory_raw') }} inv
LEFT JOIN {{ ref('fred_series_mapping') }} map
    on inv.data_type_code = map.code