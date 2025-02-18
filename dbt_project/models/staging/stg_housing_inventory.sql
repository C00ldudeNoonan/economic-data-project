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
    map.plot_grouping
FROM {{ source('staging', 'housing_inventory_raw') }} AS inv
LEFT JOIN {{ ref('housing_inventory_mapping') }} AS map
    ON inv.data_type_code = map.code
