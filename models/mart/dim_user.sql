{{ config(materialized='table') }}

-- TODO: Build the Gold dim_user model.
-- Requirements:
--  * Reference {{ ref('stg_users') }} and {{ ref('fact_events') }} once implemented.
--  * Derive surrogate keys, first/last event timestamps, and apply country coalescing.
--  * Exclude soft-deleted users via the shared macro.
--  * Ensure the grain is one row per active user.

select *
from {{ ref('stg_users') }}
