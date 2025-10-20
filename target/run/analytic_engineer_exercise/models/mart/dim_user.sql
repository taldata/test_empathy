
  
    

create or replace transient table EMPATHY_DATABASE.DBT_ETL.dim_user
    
    
    
    as (

-- TODO: Build the Gold dim_user model.
-- 
-- GRAIN: One row per active user (soft-deleted users already filtered in staging)
--
-- KEY FIELDS:
--  * user_sk: Surrogate key generated from user_id using hash function (e.g., dbt_utils.generate_surrogate_key())
--  * user_id: Natural business key from source
--  * email, marketing_channel: Direct passthrough from EMPATHY_DATABASE.DBT_ETL.stg_users
--
-- DERIVED FIELDS:
--  * country_code: Use country_code_clean from staging (nulls/empty strings → 'UNKNOWN')
--  * signup_date: Already extracted in staging from signup_ts
--  * first_event_ts: MIN event timestamp across all user events (aggregate from EMPATHY_DATABASE.DBT_ETL.stg_events)
--  * last_event_ts: MAX event timestamp across all user events (aggregate from EMPATHY_DATABASE.DBT_ETL.stg_events)
--
-- MATERIALIZATION: Table (full refresh)
--
-- EXPECTED OUTPUT: 7 active users (USR-001 through USR-005, USR-007, USR-008)

select *
from EMPATHY_DATABASE.DBT_ETL.stg_users
    )
;


  