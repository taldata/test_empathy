begin;
    
        
        
        delete from EMPATHY_DATABASE.DBT_ETL.fact_events as DBT_INTERNAL_DEST
        where (event_id) in (
            select distinct event_id
            from EMPATHY_DATABASE.DBT_ETL.fact_events__dbt_tmp as DBT_INTERNAL_SOURCE
        );

    

    insert into EMPATHY_DATABASE.DBT_ETL.fact_events ("EVENT_ID", "EVENT_TS", "USER_ID", "EVENT_TYPE", "EVENT_ATTRIBUTES", "SOURCE_APP", "IS_DELETED", "_INGESTED_AT")
    (
        select "EVENT_ID", "EVENT_TS", "USER_ID", "EVENT_TYPE", "EVENT_ATTRIBUTES", "SOURCE_APP", "IS_DELETED", "_INGESTED_AT"
        from EMPATHY_DATABASE.DBT_ETL.fact_events__dbt_tmp
    );
    commit;