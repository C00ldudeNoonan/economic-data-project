{{
    config(
        tags=['staging', 'fomc', 'summaries']
    )
}}

with source as (
    select * from {{ source('staging', 'fomc_meeting_summaries') }}
),

cleaned as (
    select
        summary_id,
        meeting_date,
        summary_type,
        summary_text,
        key_decisions,
        dissenting_views,
        economic_outlook_summary,
        policy_rationale,
        notable_quotes,
        generated_by,
        generation_date,
        created_at,
        -- Extract year and quarter
        extract(year from meeting_date) as year,
        extract(quarter from meeting_date) as quarter,
        -- Count key decisions and quotes
        array_length(key_decisions) as num_key_decisions,
        array_length(dissenting_views) as num_dissenting_views,
        array_length(notable_quotes) as num_notable_quotes,
        -- Calculate summary length
        length(summary_text) as summary_length
    from source
    where meeting_date is not null
)

select * from cleaned
