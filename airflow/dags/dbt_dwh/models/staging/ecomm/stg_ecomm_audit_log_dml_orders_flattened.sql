{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['audit_event_id'],
    on_schema_change='append_new_columns'
) }}

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
    ingested_at,
    source_file,
    audit_event_id,
    audit_operation,
    audit_timestamp,
    tbl_schema,
    tbl_name,
    json_extract_scalar(j, '$.order_id')            as order_id,
    json_extract_scalar(j, '$.order_timestamp')     as order_timestamp,
    json_extract_scalar(j, '$.created_at')          as created_at,
    json_extract_scalar(j, '$.updated_at')          as updated_at,
    json_extract_scalar(j, '$.sum')                 as order_sum,
    json_extract_scalar(j, '$.description')         as description
from src
where j is not null
