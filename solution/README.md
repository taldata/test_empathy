# Analytic Engineer Exercise — Solution

This directory contains a reference implementation of the medallion-pattern dbt project. It fulfills the requirements outlined in the candidate exercise:

- **Bronze**: `raw.events` and `raw.users` are loaded via dbt seeds.
- **Silver**: `models/staging/stg_events.sql` and `models/staging/stg_users.sql` conform, deduplicate, and document the data.
- **Gold**: `models/mart/dim_user.sql` and `models/mart/fact_events.sql` deliver curated models with surrogate keys and business metrics.
- **Macros**: `macros/is_deleted.sql` standardizes soft-delete filtering and is used across staging and mart layers.
- **Tests**: Schema tests plus a custom data test ensure model quality.

## Running the project
1. Install dependencies (none required beyond dbt core packages):
   ```bash
   dbt deps
   ```
2. Seed the raw layer:
   ```bash
   dbt seed
   ```
3. Build the project:
   ```bash
   dbt run
   ```
4. Execute tests:
   ```bash
   dbt test
   ```
5. (Optional) Generate documentation:
   ```bash
   dbt docs generate
   ```

## Notes
- Incremental logic in `fact_events` supports late-arriving events by backfilling a one-day window.
- The solution avoids package dependencies; surrogate keys leverage built-in functions.
- Use `profiles.yml.example` as a template for configuring Snowflake credentials.
