# Analytic Engineer Hourly Exercise

## Context
You are joining as an analytic engineer and will model a small event dataset using the medallion architecture (Bronze → Silver → Gold) with dbt. You receive Raw layer extracts already loaded into Snowflake as `raw.events` and `raw.users` (see seeds in `seeds/`). Your task is to transform them into curated Gold models `mart.dim_user` and `mart.fact_events` applying best practices.

## Deliverables
- Bronze sources declared via `sources.yml`.
- Silver staging models in `models/staging/` that:
  - Clean and conform column types.
  - Filter soft-deleted records via the shared macro.
  - Deduplicate events using a Snowflake `QUALIFY ROW_NUMBER() OVER (PARTITION BY ...)` window.
- Gold models in `models/mart/`: `dim_user` and `fact_events` implementing the business logic detailed below.
- Macro `macros/is_deleted.sql` returning a standardized predicate to filter soft deletes.
- dbt tests defined in `models/schema_tests.yml`, including a custom data test validating allowed event types.
- Documentation (`description` fields) for sources and models.

## Business Logic Requirements

### `mart.dim_user`
- **Grain**: One row per active user (soft-deleted users already filtered in staging via `is_deleted` macro).
- **Key fields**:
  - `user_sk`: Surrogate key generated from `user_id` using hash function (e.g., `dbt_utils.generate_surrogate_key()`).
  - `user_id`: Natural business key from source.
  - `email`, `marketing_channel`: Direct passthrough from staging.
- **Derived fields**:
  - `country_code`: Use cleaned `country_code_clean` from staging (nulls/empty strings coalesced to 'UNKNOWN').
  - `signup_date`: Already extracted in staging from `signup_ts` timestamp.
  - `first_event_ts`: MIN event timestamp across all user events (aggregate from `stg_events`).
  - `last_event_ts`: MAX event timestamp across all user events (aggregate from `stg_events`).
- **Materialization**: Table (full refresh).

### `mart.fact_events`
- **Grain**: One row per event_id (deduplication already handled in staging).
- **Key fields**:
  - `event_sk`: Surrogate key generated from `event_id`.
  - `event_id`: Natural business key from source.
  - `user_sk`: Foreign key to `dim_user` via `user_id` join.
- **Event attributes** (extracted in staging layer):
  - `price`: Numeric field parsed from JSON `event_attributes` (Purchase events have values like 12.5, 9.99, 100.0).
  - `currency`: String field parsed from JSON `event_attributes` (USD, EUR, etc.).
  - `event_type`: Normalized to uppercase in staging (values: SIGNUP, LOGIN, PURCHASE, LOGOUT, UNKNOWN for invalid types).
  - `event_date`: Already extracted from `event_ts` in staging.
  - `source_app`: Source application (web, ios, android).
- **Derived metrics**:
  - `is_purchase`: Boolean flag indicating if `event_type = 'PURCHASE'`.
- **Filtering** (applied in staging):
  - Soft-deleted events excluded via `is_deleted` macro.
  - Duplicates resolved using `QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY _ingested_at DESC) = 1`.
- **Materialization**: Incremental with:
  - `unique_key='event_id'`.
  - Incremental strategy: `delete+insert`.
  - Lookback window: Filter events from last 1 day on incremental runs (`event_ts >= dateadd(day, -1, current_timestamp)`) to handle late-arriving data.

### Data Examples from Seeds
**Raw Users** (`seeds/raw_users.csv`):
- USR-003: `country_code` is empty → cleaned to 'UNKNOWN' in staging
- USR-006: `is_deleted=true` → filtered out in staging
- USR-008: `country_code` is empty, `marketing_channel='Affiliate'`

**Raw Events** (`seeds/raw_events.csv`):
- EVT-0003: Purchase event with `{"price": 12.5, "currency": "USD"}` → parsed to columns
- EVT-0006: `is_deleted=true` → excluded in staging
- EVT-0009: `event_type='LOGIN'` (inconsistent casing) → normalized to 'LOGIN'
- EVT-0010/EVT-0011: Both for USR-006 at same timestamp, but different event_ids and deleted status
- EVT-0012/EVT-0013: Duplicate Purchase events (same user, timestamp, attributes) → both kept (different event_id = different business keys)
- EVT-0008: Late-arriving event (`event_ts=2025-05-31` but `_ingested_at=2025-06-01`) → handled by lookback window in incremental runs

**Expected Results**:
- `dim_user`: 7 active users (USR-001 through USR-005, USR-007, USR-008; USR-006 excluded as deleted)
- `fact_events`: 10 events total
  - Excluded: EVT-0006 (deleted), EVT-0010 (deleted), EVT-0011 (user USR-006 deleted → INNER JOIN filters it)
  - Included: EVT-0001, EVT-0002, EVT-0003, EVT-0004, EVT-0005, EVT-0007, EVT-0008, EVT-0009, EVT-0012, EVT-0013
- Purchase events: EVT-0003, EVT-0008, EVT-0012, EVT-0013 (4 total with `is_purchase=true`)

## Workflow Expectations
1. Inspect the raw tables using `dbt seed` (or load into Snowflake manually).
2. Build staging models in `models/staging/` using dbt CTE patterns and Jinja where appropriate.
3. Use incremental materializations where they make sense (e.g., `fact_events`).
4. Reference the shared macro for soft delete logic across models.
5. Implement schema/data tests and run `dbt test`.
6. Generate lineage (`dbt docs generate` recommended) and be ready to discuss design choices.

## Getting Started
1. Copy `profiles.yml.example` to `~/.dbt/profiles.yml` and adjust Snowflake credentials (or configure an env-specific profile).
2. Run `dbt deps` to ensure packages are installed (none required by default).
3. Load seeds:
   ```bash
   dbt seed
   ```
4. Develop staging and mart models:
   ```bash
   dbt run --select staging
   dbt run --select mart
   ```
5. Execute tests:
   ```bash
   dbt test
   ```

## Notes for Interview Discussion
- Be ready to explain your approach to handling late arriving data, soft deletes, and surrogate keys.
- Consider future scalability (partitioning, incremental strategies, role-based access).
- Optional: include snapshots or exposures if relevant.
