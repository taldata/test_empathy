# Analytic Engineer Hourly Exercise

## Context
You are joining as an analytic engineer and will model a small event dataset using the medallion architecture (Bronze → Silver → Gold) with dbt. You receive Raw layer extracts already loaded into Snowflake as `raw.events` and `raw.users` (see seeds in `seeds/`). Your task is to transform them into curated Gold models `mart.dim_user` and `mart.fact_events` applying best practices.

## Deliverables
- Bronze sources declared via `sources.yml`.
- Silver staging models in `models/staging/` that:
  - Clean and conform column types.
  - Filter soft-deleted records via the shared macro.
  - Deduplicate events using a Snowflake `QUALIFY ROW_NUMBER() OVER (PARTITION BY ...)` window.
- Gold models in `models/mart/`: `dim_user` and `fact_events` implementing the provided business logic.
- Macro `macros/is_deleted.sql` returning a standardized predicate to filter soft deletes.
- dbt tests defined in `models/schema_tests.yml`, including a custom data test validating allowed event types.
- Documentation (`description` fields) for sources and models.

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
