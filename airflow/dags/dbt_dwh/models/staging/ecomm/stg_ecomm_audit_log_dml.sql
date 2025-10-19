{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['audit_event_id'],
    on_schema_change='append_new_columns',
    partitioned_by=['day(ingested_at)']
) }}

with src as (
    select
        ingested_at,
        source_file,
        cast(audit_event_id as bigint) as audit_event_id,
        audit_operation,
        audit_timestamp,
        tbl_schema,
        tbl_name,
        raw_data,
        row_number() over (
            partition by audit_event_id order by audit_timestamp desc
        ) as rn
    from {{ source('src', 'ecomm_audit_log_dml') }}
    where 1=1
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
    raw_data
from src
where rn = 1
