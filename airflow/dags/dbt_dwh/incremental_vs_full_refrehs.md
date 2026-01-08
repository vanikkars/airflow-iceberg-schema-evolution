
##  Incremental Run (Default)

  Command:
```bash
  dbt run
  dbt run --select stg_ecomm_audit_log_dml
```

###  What happens in the macro:
```sql
  {%- set existing_relation = 
  adapter.get_relation(target_relation.database, target_relation.schema,
   target_relation.identifier) -%}

  {% if not existing_relation %}
    {# TABLE DOESN'T EXIST - FIRST RUN #}
    CREATE TABLE {{ target_relation }} AS SELECT * FROM ({{ sql }})

  {% else %}
    {# TABLE EXISTS - INCREMENTAL RUN #}
    MERGE INTO {{ target_relation }} t
    USING ({{ sql }}) s
    ON t.{{ unique_key }} = s.{{ unique_key }}

    WHEN MATCHED THEN UPDATE SET ...
    WHEN NOT MATCHED THEN INSERT ...
  {% endif %}
```

###  On incremental runs:

  1. Check if table exists - Yes, it already exists
  2. Execute MERGE statement:
    - Match rows by unique_key
    - UPDATE matched rows with new data
    - INSERT new rows that don't exist

###  Example:

  Initial data:
```

  order_id | amount | status
  ---------|--------|-------
  1        | 100    | pending
  2        | 200    | shipped

  Source data (from incremental query):
  order_id | amount | status
  ---------|--------|-------
  1        | 150    | confirmed  (UPDATE)
  3        | 300    | pending    (INSERT)

  Result after MERGE:
  order_id | amount | status
  ---------|--------|-------
  1        | 150    | confirmed  ← UPDATED
  2        | 200    | shipped    ← UNCHANGED
  3        | 300    | pending    ← INSERTED
```

  ---
##  Full Refresh

  Command:
```bash
  dbt run --full-refresh
  dbt run --full-refresh --select stg_ecomm_audit_log_dml
```

###  What happens in the macro:

  When you use --full-refresh, dbt forces a full reload by:
  1. Dropping the existing table (or marking it for rebuild)
  2. Treating the model as if the table doesn't exist
  3. Creating a new table with all data from scratch

  {% if not existing_relation %}
    {# TABLE DOESN'T EXIST (or was dropped) - FULL REFRESH #}
    CREATE TABLE {{ target_relation }} AS SELECT * FROM ({{ sql }})

  {% else %}
    {# ... incremental logic ... #}
  {% endif %}

  Full refresh executes:
  CREATE TABLE staging.stg_ecomm_audit_log_dml AS
  SELECT * FROM (
    -- Entire model SQL without incremental filters
  )

###  Example:
```
  Before full refresh:
  order_id | amount | status
  ---------|--------|-------
  1        | 100    | pending
  2        | 200    | shipped

  After full refresh (with all source data):
  order_id | amount | status
  ---------|--------|-------
  1        | 150    | confirmed
  2        | 200    | shipped
  3        | 300    | pending
  4        | 400    | draft
  5        | 500    | cancelled
```

  ---
  Key Differences
```
  | Aspect        | Incremental Run              | Full Refresh                     |
  |---------------|------------------------------|----------------------------------|
  | Command       | dbt run                      | dbt run --full-refresh           |
  | Table Exists? | Yes                          | No (dropped first)               |
  | SQL Generated | MERGE INTO                   | CREATE TABLE AS  SELECT          |
  | Data Scope    | Only new/changed data        | All data from scratch            |
  | Performance   | ⚡ Fast (only delta)          | 🐢 Slow (full  reload)           |
  | When to Use   | Daily runs, normal pipelines | Schema changes, data corrections |
```

  ---
  In Your Models

  Look at your model SQL:
```sql
  {{ config(
      materialized='trino_incremental_upsert',
      unique_key='audit_event_id'
  ) }}

  with src as (
      select ...
      from {{ source('src', 'ecomm_audit_log_dml') }}
      where 1=1
      {% if is_incremental() %}
        and ingested_at > (
            select coalesce(max(ingested_at), timestamp '1970-01-01
  00:00:00')
            from {{ this }}
        )
      {% endif %}
  )

  The {% if is_incremental() %} block:
  - On incremental run: Filters to only NEW data (since last run)
  - On full refresh: Ignores the filter, processes ALL data
```

  Incremental query:
```sql
  SELECT ... FROM landing.ecomm_audit_log_dml
  WHERE ingested_at > (last max ingested_at from staging table)

  Full refresh query:
  SELECT ... FROM landing.ecomm_audit_log_dml
  -- No WHERE clause, gets everything
```

  ---
##  Example Scenario

  Day 1 - First Run (No table exists):
  ```bash
  dbt run --select stg_ecomm_audit_log_dml
  ```
  → Creates table with 1000 records

  Day 2 - Incremental Run (100 new records):
  ```bash
  dbt run --select stg_ecomm_audit_log_dml
  ```
  → MERGE statement:
  - Processes only 100 new records
  - Updates any matching records
  - Table now has 1000 + new inserts

  Day 3 - Data Error, Full Refresh:
  ```bash
  dbt run --full-refresh --select stg_ecomm_audit_log_dml
  ```
  → Drops and recreates table with ALL records from source (1000+)

  ---
##  Performance Impact

  Incremental Run:
  - ⚡ Processes only delta (e.g., 100 rows)
  - ✅ Fast and efficient
  - ✅ Cheaper (fewer Iceberg operations)

  Full Refresh:
  - 🐢 Processes all data (e.g., 100,000 rows)
  - ⚠️ Slower
  - ⚠️ More expensive (full table rewrite)

  Best Practice:
  - Use incremental runs for daily/scheduled pipelines
  - Use full refresh only when needed (schema changes, data corrections,
   migrations)