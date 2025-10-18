{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='order_id',
    on_schema_change='sync_all_columns'
) }}

WITH source_data AS (
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
    WHERE audit_operation IN ('I', 'U', 'D')
    {% if is_incremental() %}
        -- Filter for new records since the last run
        AND ingested_at > (SELECT MAX(ingested_at) FROM {{ this }})
    {% endif %}
),

ranked AS (
    SELECT
        *,
        -- Rank records for each order to find the latest one
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY ingested_at DESC, audit_timestamp DESC) as rn,
        -- Count non-delete operations to avoid inserting a record that was only ever deleted
        SUM(CASE WHEN audit_operation IN ('I', 'U') THEN 1 ELSE 0 END) OVER (PARTITION BY order_id) as non_delete_ops
    FROM source_data
),

latest_records AS (
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
    FROM ranked
    -- Select the latest version for each order_id
    WHERE rn = 1
      -- Exclude records where the only operation is a delete
      AND (
          (audit_operation = 'D' AND non_delete_ops > 0)
          OR audit_operation IN ('I', 'U')
      )
)

SELECT * FROM latest_records
