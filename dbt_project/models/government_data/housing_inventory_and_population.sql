{{ config(
    materialized='table'
) }}


With hs As (
    Select
        time_date,
        cast(series_value As float) As number_of_households,
        extract(Year From time_date) As year
    From {{ ref('housing_inventory') }}
    Where
        category_code = 'TTLHH'
        And series_value <> '.'
)



Select
    series_name,
    cast(series_value As float) As series_value,
    cast((Case
        When
            right(housing_inventory.time, 2) = 'Q1'
            Then cast ((left(housing_inventory.time, 4) || '-01-01') As date)
        When
            right(housing_inventory.time, 2) = 'Q2'
            Then cast ((left(housing_inventory.time, 4) || '-04-01') As date)
        When
            right(housing_inventory.time, 2) = 'Q3'
            Then cast ((left(housing_inventory.time, 4) || '-07-01') As date)
        When
            right(housing_inventory.time, 2) = 'Q4'
            Then cast ((left(housing_inventory.time, 4) || '-10-01') As date)
    End) As date) As time_date,
    hs.number_of_households,
    extract(Year From cast((Case
        When
            right(housing_inventory.time, 2) = 'Q1'
            Then cast ((left(housing_inventory.time, 4) || '-01-01') As date)
        When
            right(housing_inventory.time, 2) = 'Q2'
            Then cast ((left(housing_inventory.time, 4) || '-04-01') As date)
        When
            right(housing_inventory.time, 2) = 'Q3'
            Then cast ((left(housing_inventory.time, 4) || '-07-01') As date)
        When
            right(housing_inventory.time, 2) = 'Q4'
            Then cast ((left(housing_inventory.time, 4) || '-10-01') As date)
    End) As date)) As year
From {{ ref('housing_inventory') }}
Left Join hs
    On extract(Year From cast((Case
        When
            right(housing_inventory.time, 2) = 'Q1'
            Then cast ((left(housing_inventory.time, 4) || '-01-01') As date)
        When
            right(housing_inventory.time, 2) = 'Q2'
            Then cast ((left(housing_inventory.time, 4) || '-04-01') As date)
        When
            right(housing_inventory.time, 2) = 'Q3'
            Then cast ((left(housing_inventory.time, 4) || '-07-01') As date)
        When
            right(housing_inventory.time, 2) = 'Q4'
            Then cast ((left(housing_inventory.time, 4) || '-10-01') As date)
    End) As date)) = hs.year

Where
    error_data = 'no'
    And category_code = 'ESTIMATE'
    And series_name In (
        'Renter Occupied Units',
        'Owner Occupied Units',
        'Total Vacant Housing Units'
    )
