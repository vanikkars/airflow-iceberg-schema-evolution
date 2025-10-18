{{- config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    post_hook=[
        "DELETE FROM {{ this }} WHERE order_id IN (SELECT CAST(order_id AS INTEGER) FROM {{ ref('stg_ecomm_audit_log_dml_orders_flattened') }} WHERE audit_operation = 'D' {% if is_incremental() %}AND ingested_at > (SELECT COALESCE(MAX(ingested_at), TIMESTAMP '1970-01-01 00:00:00') FROM {{ this }}){% endif %})"
    ]
) -}}

/*
  Current state table for orders using standard dbt incremental
  - Uses MERGE for insert/update operations
  - Uses post-hook to DELETE records marked with audit_operation = 'D'
  - Keeps only the latest change per order_id based on audit_timestamp
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
        CAST(order_id AS INTEGER) as order_id,
        CAST(order_timestamp AS TIMESTAMP) as order_timestamp,
        CAST(created_at AS TIMESTAMP) as created_at,
        CAST(updated_at AS TIMESTAMP) as updated_at,
        CAST(order_sum AS DOUBLE) as order_sum,
        description
    FROM {{ ref('stg_ecomm_audit_log_dml_orders_flattened') }}
    {% if is_incremental() %}
        WHERE ingested_at > (SELECT COALESCE(MAX(ingested_at), TIMESTAMP '1970-01-01 00:00:00') FROM {{ this }})
    {% endif %}
),
latest_changes AS (
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
        description,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY audit_timestamp DESC, audit_event_id DESC) AS rn
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
  AND audit_operation <> 'D'  -- Don't insert/update deleted records
