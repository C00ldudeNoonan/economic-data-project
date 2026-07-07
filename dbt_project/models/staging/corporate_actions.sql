{{
    config(
        enabled=var('enable_legacy_staging_alias_models', false)
    )
}}

select * from {{ ref('stg_corporate_actions') }}
