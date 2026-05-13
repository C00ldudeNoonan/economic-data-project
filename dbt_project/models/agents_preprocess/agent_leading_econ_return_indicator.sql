{{
    config(
        tags=['agents_preprocess']
    )
}}

select * from {{ ref('leading_econ_return_indicator') }}
