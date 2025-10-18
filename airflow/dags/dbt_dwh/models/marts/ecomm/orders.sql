{{- config(
    materialized='trino_incremental_always_merge',
    unique_key='order_id',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) -}}

/*
  Marts: Current state table for orders

  Contains the latest version of each order (excludes deletes).
  All type casting is done in staging - this model uses clean typed columns.
*/

WITH new_changes AS (
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
        WHERE ingested_at > (SELECT COALESCE(MAX(ingested_at), TIMESTAMP '1970-01-01 00:00:00') FROM {{ this }})
    {% endif %}
),
latest_changes AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY audit_timestamp DESC, audit_event_id DESC
        ) AS rn
    FROM new_changes
)
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
FROM latest_changes
WHERE rn = 1
