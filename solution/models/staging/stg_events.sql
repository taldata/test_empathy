{{ config(materialized='view') }}

{% set allowed_event_types = ['SIGNUP', 'LOGIN', 'PURCHASE', 'LOGOUT'] %}

with source_events as (
    select
        event_id,
        try_to_timestamp_ntz(event_ts) as event_ts,
        user_id,
        upper(trim(event_type)) as raw_event_type,
        try_parse_json(event_attributes) as event_attributes,
        upper(trim(source_app)) as source_app,
        {{ is_deleted('is_deleted') }} as is_deleted_flag,
        try_to_timestamp_ntz(_ingested_at) as _ingested_at
    from {{ source('raw', 'events') }}
),

filtered as (
    select *
    from source_events
    where not is_deleted_flag
),

typed as (
    select
        event_id,
        event_ts,
        user_id,
        case
            when raw_event_type in ( {% for event_type in allowed_event_types %}'{{ event_type }}'{% if not loop.last %}, {% endif %}{% endfor %} ) then raw_event_type
            else 'UNKNOWN'
        end as event_type,
        event_attributes,
        event_attributes:price::number(38, 2) as price,
        event_attributes:currency::string as currency,
        source_app,
        _ingested_at
    from filtered
),

deduplicated as (
    select *,
           row_number() over (
               partition by event_id
               order by _ingested_at desc
           ) as record_rank
    from typed
)

select
    event_id,
    event_ts,
    to_date(event_ts) as event_date,
    user_id,
    event_type,
    source_app,
    price,
    currency,
    _ingested_at
from deduplicated
qualify record_rank = 1
