

-- TODO: Build the staging layer for raw.events.
-- Requirements:
--  * Reference EMPATHY_DATABASE.DBT_ETL.raw_events.
--  * Parse event_attributes JSON into structured columns via Snowflake functions.
--  * Normalize event_type casing and trim whitespace.
--  * Filter soft deletes using the shared is_deleted macro once implemented.
--  * Deduplicate on business keys using QUALIFY ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...).
--  * Produce clean column names suitable for downstream models.

with source_events as (
    select * from EMPATHY_DATABASE.DBT_ETL.raw_events
)

-- Add additional CTEs here

select *
from source_events