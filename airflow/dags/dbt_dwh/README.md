# dbt Data Warehouse Project for Iceberg Lakehouse

## Overview

This is a dbt project that processes CDC (Change Data Capture) audit logs from PostgreSQL into an Iceberg data warehouse using Trino as the query engine.

**Data Flow:**
```
landing (raw audit logs)
  ↓
staging (deduplicated, flattened)
  ↓
marts (current state, SCD2 history)
```

The project expects data to be loaded into the `landing` schema, which is then incrementally processed through `staging` and `marts` layers for analytics.

---

## Materializations

### `trino_incremental_upsert`

A custom incremental materialization optimized for Trino + Iceberg catalogs.

**File:** `macros/trino_upsert.sql`

**Purpose:** Implements a simple UPSERT (INSERT + UPDATE) strategy for incremental models without soft-delete logic.

**How it Works:**

1. **First Run:** Creates the target table with `CREATE TABLE AS SELECT`
2. **Incremental Runs:** Uses Trino's MERGE statement:
   - **MATCH** (row exists): UPDATE all columns except unique_key
   - **NO MATCH** (new row): INSERT all columns

**Configuration:**

```sql
{{ config(
    materialized='trino_incremental_upsert',
    unique_key='column_name',           # Single or composite key
    on_schema_change='append_new_columns'
) }}
```

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `unique_key` | `string` or `list` | ✅ Yes | Column(s) to match on for MERGE |
| `on_schema_change` | `string` | ❌ No | How to handle schema changes: `fail`, `append_new_columns`, `sync_all_columns` |

**Examples:**

Single unique key:
```sql
{{ config(
    materialized='trino_incremental_upsert',
    unique_key='order_id'
) }}
```

Composite unique key:
```sql
{{ config(
    materialized='trino_incremental_upsert',
    unique_key=['order_id', 'source_id']
) }}
```

**Macro Naming:**

- **Materialization name** (public API): `trino_incremental_upsert`
- **Implementation macro** (internal): `trino__get_upsert_sql`

The `trino__` prefix is dbt's adapter-specific naming convention that allows different database adapters (Trino, PostgreSQL, Snowflake, etc.) to have their own implementations.

**Generated MERGE SQL Example:**

```sql
MERGE INTO target_table t
USING (source_query) s
ON t.order_id = s.order_id

WHEN MATCHED THEN UPDATE SET
  created_at = s.created_at,
  updated_at = s.updated_at,
  status = s.status

WHEN NOT MATCHED THEN INSERT (
  order_id, created_at, updated_at, status
) VALUES (
  s.order_id, s.created_at, s.updated_at, s.status
);
```

**Use Cases:**

- ✅ Incremental models with simple upsert logic
- ✅ Single or composite primary keys
- ✅ Any number of columns (dynamically generated)
- ✅ Works with Iceberg partitioned tables

**When to Use:**

- You want simple INSERT + UPDATE behavior
- You don't need soft-delete support
- Your source data is already deduplicated

**When NOT to Use:**

- You need soft-delete support (use `trino_incremental_always_merge` instead)
- You need timestamp-based update logic (use `trino_merge_with_delete` instead)

---

## Models Using `trino_incremental_upsert`

### 1. `stg_ecomm_audit_log_dml`

Deduplicates raw audit events by `audit_event_id`, keeping the latest record.

```sql
{{ config(
    materialized='trino_incremental_upsert',
    unique_key='audit_event_id',
    on_schema_change='append_new_columns',
    partitioned_by=['day(ingested_at)']
) }}
```

### 2. `stg_ecomm_audit_log_dml_orders_flattened`

Flattens JSON from audit logs, extracts order-specific columns.

```sql
{{ config(
    materialized='trino_incremental_upsert',
    unique_key='audit_event_id',
    on_schema_change='append_new_columns',
    partitioned_by=['day(ingested_at)']
) }}
```

---

## Alternative Materializations

### `trino_incremental_always_merge` (Deprecated)

Complex materialization with soft-delete support. Use `trino_incremental_upsert` instead for simpler logic.

### `trino_merge_with_delete`

Implements soft-delete logic for audit operations:
- Deletes rows marked with `audit_operation = 'D'`
- Updates rows when matched
- Inserts new rows

---

## Running dbt

### Basic Commands

```bash
# Run all models
dbt run

# Run specific model
dbt run --select stg_ecomm_audit_log_dml

# Run only staging models
dbt run --select staging

# Full refresh (recreate tables)
dbt run --full-refresh

# Test data contracts
dbt test
```

### Incremental Run vs Full Refresh

> 📖 **See [incremental_vs_full_refrehs.md](./incremental_vs_full_refrehs.md) for detailed documentation.**

The `trino_incremental_upsert` materialization behaves differently depending on whether it's an incremental run or a full refresh.

#### Incremental Run (Default)

**Command:** `dbt run`

**What happens:**
1. Checks if the target table exists
2. If it exists: Executes a **MERGE statement**
   - **MATCH** (row exists by unique_key): UPDATE all columns with new data
   - **NO MATCH** (new row): INSERT new rows
3. Only processes **new or changed data** (filtered by incremental logic)

**Query Example:**
```sql
WITH src AS (
  SELECT ... FROM landing.ecomm_audit_log_dml
  WHERE ingested_at > (SELECT MAX(ingested_at) FROM staging.stg_ecomm_audit_log_dml)
)
MERGE INTO staging.stg_ecomm_audit_log_dml t
USING src s
ON t.audit_event_id = s.audit_event_id
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...
```

**Performance:** ⚡ **Fast** - only processes delta (new/changed records)

**Use Case:** Daily scheduled runs, normal pipeline execution

#### Full Refresh

**Command:** `dbt run --full-refresh`

**What happens:**
1. Drops the existing table
2. Creates a new table from scratch
3. Processes **ALL data** (ignores incremental filters)
4. Executes `CREATE TABLE AS SELECT`

**Query Example:**
```sql
CREATE TABLE staging.stg_ecomm_audit_log_dml AS
SELECT ... FROM landing.ecomm_audit_log_dml
-- No WHERE clause, gets ALL data
```

**Performance:** 🐢 **Slow** - recreates entire table

**Use Cases:**
- Schema changes (adding/removing columns)
- Data corrections or backfill
- Bug fixes in model logic
- Migration to new table structure

### Example Scenario

**Day 1 - First Run (Table doesn't exist):**
```bash
dbt run --select stg_ecomm_audit_log_dml
```
- Creates new table with 1,000 records

**Day 2 - Incremental Run (100 new records):**
```bash
dbt run --select stg_ecomm_audit_log_dml
```
- MERGE statement processes only 100 new records
- Updates matching records (by audit_event_id)
- Table now has 1,000+ records

**Day 3 - Data Error, Full Refresh:**
```bash
dbt run --full-refresh --select stg_ecomm_audit_log_dml
```
- Drops existing table
- Recreates with ALL source data
- Ensures data consistency

### Comparison Table

| Aspect | Incremental Run | Full Refresh |
|--------|-----------------|--------------|
| **Command** | `dbt run` | `dbt run --full-refresh` |
| **Table Exists?** | Yes | Dropped first |
| **SQL Operation** | MERGE INTO | CREATE TABLE AS SELECT |
| **Data Scope** | Only new/changed data | All data from scratch |
| **Performance** | ⚡ Fast | 🐢 Slow |
| **When to Use** | Daily runs, normal pipelines | Schema changes, corrections |

### Best Practices

- **Use incremental runs** for scheduled daily pipelines (faster, cheaper)
- **Use full refresh** only when necessary (schema changes, data corrections)
- **Monitor table sizes** - ensure incremental logic is working correctly
- **Test before production** - run with `--full-refresh` in dev first

---

## Project Structure

```
dbt_dwh/
├── models/
│   ├── staging/ecomm/
│   │   ├── stg_ecomm_audit_log_dml.sql
│   │   └── stg_ecomm_audit_log_dml_orders_flattened.sql
│   ├── marts/ecomm/
│   │   └── (future mart models)
│   ├── sources.yml
│   └── schema.yml (data contracts)
├── macros/
│   ├── trino_upsert.sql              # Simple UPSERT materialization
│   ├── trino_incremental_always_merge.sql  # Complex merge with delete
│   └── trino_merge_with_delete.sql   # Soft-delete support
├── seeds/
│   └── (reference data)
└── dbt_project.yml
```

---

## Resources

- [dbt Documentation](https://docs.getdbt.com/docs/introduction)
- [Trino Documentation](https://trino.io/docs/)
- [Apache Iceberg](https://iceberg.apache.org/)
- [dbt Community Slack](https://community.getdbt.com/)
