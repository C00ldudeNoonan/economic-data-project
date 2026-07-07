{{
    config(
        enabled=var('enable_legacy_misspelled_market_models', false)
    )
}}

select * from {{ ref('major_indices_summary') }}
