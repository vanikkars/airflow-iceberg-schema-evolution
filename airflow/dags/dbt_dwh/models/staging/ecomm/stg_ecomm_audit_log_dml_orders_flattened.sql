{{- config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['audit_event_id'],
    on_schema_change='append_new_columns',
    partitioned_by=['day(ingested_at)']
) -}}

/*
  Staging: Orders flattened from JSON with proper type casting

  Responsibilities:
  - Extract JSON fields from raw_data
  - Cast to proper data types (marts will use these typed columns)
  - Filter for orders table only
  - Incremental based on ingested_at
*/

with src as (
    select
        ingested_at,
        source_file,
        audit_event_id,
        audit_operation,
        audit_timestamp,
        tbl_schema,
        tbl_name,
        try(json_parse(raw_data)) as j
    from {{ ref('stg_ecomm_audit_log_dml') }}
    where tbl_schema = 'public'
      and tbl_name = 'orders'
      {% if is_incremental() %}
        and ingested_at > (
            select coalesce(max(ingested_at), timestamp '1970-01-01 00:00:00')
            from {{ this }}
        )
      {% endif %}
)
select
    -- Metadata columns
    ingested_at,
    source_file,
    audit_event_id,
    audit_operation,
    audit_timestamp,
    tbl_schema,
    tbl_name,

    -- Business columns with proper type casting
    cast(json_extract_scalar(j, '$.order_id') as integer) as order_id,
    cast(json_extract_scalar(j, '$.order_timestamp') as timestamp) as order_timestamp,
    cast(json_extract_scalar(j, '$.created_at') as timestamp) as created_at,
    cast(json_extract_scalar(j, '$.updated_at') as timestamp) as updated_at,
    cast(json_extract_scalar(j, '$.sum') as double) as order_sum,
    json_extract_scalar(j, '$.description') as description
from src
where j is not null
