{{ config(materialized='incremental', unique_key='event_id') }}

-- TODO: Build the Gold fact_events model.
-- Requirements:
--  * Reference {{ ref('stg_events') }} and {{ ref('dim_user') }}.
--  * Extract structured purchase metrics (price, currency) from parsed attributes.
--  * Apply the is_deleted macro to drop soft-deleted records.
--  * Deduplicate late-arriving duplicates using QUALIFY or similar window pattern.
--  * Join to dim_user to pull the surrogate key.
--  * Flag purchase events and ensure the grain is one row per event_id.
--  * For incremental runs, handle late-arriving data using event timestamps.

select *
from {{ ref('stg_events') }}
