{{
    config(
        enabled=var('enable_legacy_staging_alias_models', false)
    )
}}

select * from {{ ref('stg_split_adjusted_prices') }}
