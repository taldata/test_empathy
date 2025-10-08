{{ config(materialized='view') }}

-- TODO: Build the staging layer for raw.events.
-- Requirements:
--  * Reference {{ source('raw', 'events') }}.
--  * Parse event_attributes JSON into structured columns via Snowflake functions.
--  * Normalize event_type casing and trim whitespace.
--  * Filter soft deletes using the shared is_deleted macro once implemented.
--  * Deduplicate on business keys using QUALIFY ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...).
--  * Produce clean column names suitable for downstream models.

with source_events as (
    select * from {{ source('raw', 'events') }}
)

-- Add additional CTEs here

select *
from source_events
