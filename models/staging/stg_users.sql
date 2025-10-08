{{ config(materialized='view') }}

-- TODO: Build the staging layer for raw.users.
-- Requirements:
--  * Reference {{ source('raw', 'users') }}.
--  * Normalize casing for country codes and marketing channels.
--  * Filter soft deletes via the shared is_deleted macro once implemented.
--  * Derive additional helpful columns (e.g., signup_date) for downstream use.
--  * Ensure a single active row per user_id.

with source_users as (
    select * from {{ source('raw', 'users') }}
)

select *
from source_users
