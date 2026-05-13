{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='append_new_columns'
    )
}}

with source as (
    select * from {{ source('raw_telemetry', 'telemetry_events_raw') }}
    {% if is_incremental() %}
    where replicated_at > (select max(replicated_at) from {{ this }})
    {% endif %}
),

parsed as (
    select
        -- Primary identifiers
        id,
        event_type,
        session_id,
        user_id,

        -- Timestamps
        properties as raw_properties,
        created_at,
        replicated_at,

        -- Parse JSON properties (using DuckDB's JSON functions)
        -- Navigation properties
        to_timestamp(timestamp_ms / 1000.0) as event_timestamp,
        date_trunc('day', to_timestamp(timestamp_ms / 1000.0)) as event_date,
        date_trunc('hour', to_timestamp(timestamp_ms / 1000.0)) as event_hour,

        -- Feature usage properties
        try_cast(json_extract_string(properties, '$.page_url') as varchar) as page_url,
        try_cast(json_extract_string(properties, '$.page_title') as varchar) as page_title,
        try_cast(json_extract_string(properties, '$.referrer') as varchar) as referrer,
        try_cast(json_extract_string(properties, '$.feature_name') as varchar) as feature_name,

        -- Error properties
        try_cast(json_extract_string(properties, '$.query_text') as varchar) as query_text,
        try_cast(json_extract_string(properties, '$.chart_type') as varchar) as chart_type,
        try_cast(json_extract_string(properties, '$.export_format') as varchar) as export_format,

        -- Performance properties (Web Vitals)
        try_cast(json_extract_string(properties, '$.error_message') as varchar) as error_message,
        try_cast(json_extract_string(properties, '$.error_stack') as varchar) as error_stack,
        try_cast(json_extract_string(properties, '$.error_code') as varchar) as error_code,

        -- Auth properties
        try_cast(json_extract_string(properties, '$.metric_name') as varchar) as metric_name,
        try_cast(json_extract_string(properties, '$.metric_value') as double) as metric_value,

        -- Device/Browser context
        try_cast(json_extract_string(properties, '$.metric_rating') as varchar) as metric_rating,
        try_cast(json_extract_string(properties, '$.auth_method') as varchar) as auth_method,
        try_cast(json_extract_string(properties, '$.auth_provider') as varchar) as auth_provider,
        try_cast(json_extract_string(properties, '$.user_agent') as varchar) as user_agent,
        try_cast(json_extract_string(properties, '$.screen_width') as integer) as screen_width,

        -- Keep full properties JSON for ad-hoc analysis
        try_cast(json_extract_string(properties, '$.screen_height') as integer) as screen_height,

        -- Metadata
        try_cast(json_extract_string(properties, '$.viewport_width') as integer) as viewport_width,
        try_cast(json_extract_string(properties, '$.viewport_height') as integer) as viewport_height

    from source
),

final as (
    select
        *,

        -- Derived flags
        coalesce(event_type in ('api_error', 'app_error'), false) as is_error_event,

        coalesce(event_type in ('sign_in', 'sign_up', 'sign_out'), false) as is_auth_event,

        coalesce(event_type = 'page_view', false) as is_navigation_event,

        coalesce(event_type in ('chat_query', 'chart_export', 'filter_applied', 'example_question_click'), false) as is_feature_usage_event,

        coalesce(event_type = 'web_vitals', false) as is_performance_event,

        -- Extract page path from full URL
        case
            when page_url is not null
                then
                    regexp_extract(page_url, '^https?://[^/]+(/.*)$', 1)
        end as page_path,

        -- Extract domain from URL
        case
            when page_url is not null
                then
                    regexp_extract(page_url, '^https?://([^/]+)', 1)
        end as page_domain

    from parsed
)

select * from final
