with invalid_events as (
    select event_id, event_type
    from {{ ref('stg_events') }}
    where event_type not in ('SIGNUP', 'LOGIN', 'PURCHASE', 'LOGOUT', 'UNKNOWN')
)

select *
from invalid_events
