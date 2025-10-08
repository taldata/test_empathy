{{ config(
    materialized='incremental',
    unique_key='event_id',
    incremental_strategy='delete+insert'
) }}

{% set incremental_lookback_days = 1 %}

with events as (
    select
        event_id,
        event_ts,
        event_date,
        user_id,
        event_type,
        source_app,
        price,
        currency,
        _ingested_at
    from {{ ref('stg_events') }}
    {% if is_incremental() %}
        where event_ts >= dateadd(day, -{{ incremental_lookback_days }}, current_timestamp)
    {% endif %}
),

users as (
    select
        user_sk,
        user_id
    from {{ ref('dim_user') }}
),

enriched as (
    select
        {{ dbt_utils.generate_surrogate_key(['events.event_id']) }} as event_sk,
        events.event_id,
        users.user_sk,
        events.user_id,
        events.event_ts,
        events.event_date,
        events.event_type,
        events.source_app,
        events.price,
        events.currency,
        events.event_type = 'PURCHASE' as is_purchase,
        events._ingested_at
    from events
    inner join users
      on events.user_id = users.user_id
)

select *
from enriched
