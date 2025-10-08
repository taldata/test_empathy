{{ config(materialized='table') }}

with users as (
    select
        user_id,
        email,
        country_code_clean,
        signup_ts,
        signup_date,
        marketing_channel,
        _ingested_at
    from {{ ref('stg_users') }}
),

event_bounds as (
    select
        user_id,
        min(event_ts) as first_event_ts,
        max(event_ts) as last_event_ts
    from {{ ref('stg_events') }}
    group by 1
)

select
    {{ dbt_utils.generate_surrogate_key(['users.user_id']) }} as user_sk,
    users.user_id,
    users.email,
    users.country_code_clean as country_code,
    users.signup_date,
    event_bounds.first_event_ts,
    event_bounds.last_event_ts,
    users.marketing_channel,
    users.signup_ts,
    users._ingested_at
from users
left join event_bounds
  on users.user_id = event_bounds.user_id
