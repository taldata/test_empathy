{{ config(materialized='view') }}

with base as (
    select
        user_id,
        lower(email) as email,
        upper(trim(country_code)) as country_code,
        try_to_timestamp_ntz(signup_ts) as signup_ts,
        upper(trim(marketing_channel)) as marketing_channel,
        {{ is_deleted('is_deleted') }} as is_deleted_flag,
        try_to_timestamp_ntz(_ingested_at) as _ingested_at
    from {{ source('raw', 'users') }}
),

filtered as (
    select *
    from base
    where not is_deleted_flag
),

deduplicated as (
    select *,
           row_number() over (
               partition by user_id
               order by _ingested_at desc
           ) as record_rank
    from filtered
)

select
    user_id,
    email,
    country_code,
    coalesce(nullif(country_code, ''), 'UNKNOWN') as country_code_clean,
    signup_ts,
    to_date(signup_ts) as signup_date,
    marketing_channel,
    _ingested_at
from deduplicated
qualify record_rank = 1
