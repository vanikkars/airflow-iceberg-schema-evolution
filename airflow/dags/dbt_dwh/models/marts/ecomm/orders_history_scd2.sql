{{- config(
    materialized='incremental',
    unique_key=['order_id', 'valid_from'],
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) -}}

/*
  SCD Type 2: Full history of all order changes (INCREMENTAL)

  Tracks every version of each order with temporal validity:
  - valid_from: When this version became active (audit_timestamp)
  - valid_to: When this version was superseded (NULL = current)
  - is_current: Boolean flag for the latest version
  - is_deleted: Boolean flag for deleted records

  Incremental Strategy:
  - Identifies order_ids with new changes since last run
  - Rebuilds complete history for only those orders (from all staging data)
  - Keeps existing records for orders without new changes
  - Uses MERGE to replace old versions with updated temporal ranges

  Use cases:
  - Historical analysis: "What was order 123 on 2024-01-15?"
  - Audit trail: "Show me all changes to order 456"
  - Time-travel queries: "How many active orders on any given date?"
*/

WITH
{% if is_incremental() %}
-- Step 1: Identify orders with new changes (incremental only)
new_changes AS (
    SELECT DISTINCT order_id
    FROM {{ ref('stg_ecomm_audit_log_dml_orders_flattened') }}
    WHERE ingested_at > (SELECT COALESCE(MAX(ingested_at), TIMESTAMP '1970-01-01 00:00:00') FROM {{ this }})
),
{% endif %}

-- Step 2: Get ALL history for affected orders (not just new changes)
all_changes_for_affected_orders AS (
    SELECT
        ingested_at,
        source_file,
        audit_event_id,
        audit_operation,
        audit_timestamp,
        tbl_schema,
        tbl_name,
        order_id,
        order_timestamp,
        created_at,
        updated_at,
        order_sum,
        description
    FROM {{ ref('stg_ecomm_audit_log_dml_orders_flattened') }}
    {% if is_incremental() %}
    WHERE order_id IN (SELECT order_id FROM new_changes)
    {% endif %}
),

-- Step 3: Deduplicate by audit_event_id (keep latest ingested version)
deduped_changes AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY audit_event_id
            ORDER BY ingested_at DESC
        ) as rn
    FROM all_changes_for_affected_orders
),

-- Step 4: Calculate temporal validity ranges using window functions
temporal_history AS (
    SELECT
        ingested_at,
        source_file,
        audit_event_id,
        audit_operation,
        audit_timestamp as valid_from,
        LEAD(audit_timestamp) OVER (
            PARTITION BY order_id
            ORDER BY audit_timestamp, audit_event_id
        ) as valid_to,
        CASE
            WHEN LEAD(audit_timestamp) OVER (
                PARTITION BY order_id
                ORDER BY audit_timestamp, audit_event_id
            ) IS NULL THEN TRUE
            ELSE FALSE
        END as is_current,
        CASE
            WHEN audit_operation = 'D' THEN TRUE
            ELSE FALSE
        END as is_deleted,
        tbl_schema,
        tbl_name,
        order_id,
        order_timestamp,
        created_at,
        updated_at,
        order_sum,
        description
    FROM deduped_changes
    WHERE rn = 1
)

-- Step 5: Return results
{% if is_incremental() %}

-- Incremental: Only return history for orders with new changes
-- dbt will MERGE these, replacing old versions with updated temporal ranges
SELECT
    ingested_at,
    source_file,
    audit_event_id,
    audit_operation,
    valid_from,
    valid_to,
    is_current,
    is_deleted,
    tbl_schema,
    tbl_name,
    order_id,
    order_timestamp,
    created_at,
    updated_at,
    order_sum,
    description
FROM temporal_history

{% else %}

-- Full refresh: Return all history
SELECT
    ingested_at,
    source_file,
    audit_event_id,
    audit_operation,
    valid_from,
    valid_to,
    is_current,
    is_deleted,
    tbl_schema,
    tbl_name,
    order_id,
    order_timestamp,
    created_at,
    updated_at,
    order_sum,
    description
FROM temporal_history

{% endif %}
