{{ config(
    materialized='incremental',
    unique_key='event_id',
    incremental_strategy='delete+insert'
) }}

-- TODO: Build the Gold fact_events model.
--
-- GRAIN: One row per event_id (deduplication already handled in staging)
--
-- KEY FIELDS:
--  * event_sk: Surrogate key generated from event_id
--  * event_id: Natural business key from source
--  * user_sk: Foreign key to dim_user via user_id join (INNER JOIN to exclude events of deleted users)
--
-- EVENT ATTRIBUTES (already extracted in staging layer):
--  * price: Numeric field parsed from JSON event_attributes (Purchase events: 12.5, 9.99, 100.0)
--  * currency: String field parsed from JSON event_attributes (USD, EUR, etc.)
--  * event_type: Normalized to uppercase in staging (SIGNUP, LOGIN, PURCHASE, LOGOUT, UNKNOWN)
--  * event_date: Already extracted from event_ts in staging
--  * source_app: Source application (web, ios, android)
--
-- DERIVED METRICS:
--  * is_purchase: Boolean flag indicating if event_type = 'PURCHASE'
--
-- FILTERING (already applied in staging):
--  * Soft-deleted events excluded via is_deleted macro
--  * Duplicates resolved using QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY _ingested_at DESC) = 1
--
-- MATERIALIZATION: Incremental with:
--  * unique_key='event_id'
--  * incremental_strategy='delete+insert'
--  * Lookback window: Filter events from last 1 day on incremental runs to handle late-arriving data
--    (event_ts >= dateadd(day, -1, current_timestamp))
--
-- EXPECTED OUTPUT: 10 events total
--  * Excluded: EVT-0006 (deleted), EVT-0010 (deleted), EVT-0011 (user USR-006 deleted → INNER JOIN filters it)
--  * Purchase events: EVT-0003, EVT-0008, EVT-0012, EVT-0013 (4 total with is_purchase=true)

select *
from {{ ref('stg_events') }}
