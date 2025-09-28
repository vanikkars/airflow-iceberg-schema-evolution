{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['booking_id','customer_id'],
    on_schema_change = 'append_new_columns'
) }}

with src as (
    select
        created_at,
        source_file,
        raw_data,
        try(json_parse(raw_data)) as j
    from {{ source('src', 'rides_raw') }}
)
select
    created_at,
    source_file,
    cast(json_extract_scalar(j, '$.booking_date') as date)          as booking_date,
    json_extract_scalar(j, '$.booking_time')                        as booking_time,
    json_extract_scalar(j, '$.booking_id')                          as booking_id,
    json_extract_scalar(j, '$.booking_status')                      as booking_status,
    json_extract_scalar(j, '$.customer_id')                         as customer_id,
    json_extract_scalar(j, '$.vehicle_type')                        as vehicle_type,
    json_extract_scalar(j, '$.pickup_location')                     as pickup_location,
    json_extract_scalar(j, '$.drop_location')                       as drop_location,
    cast(json_extract_scalar(j, '$.avg_vtat') as decimal(16,2))     as avg_vtat,
    cast(json_extract_scalar(j, '$.avg_ctat') as decimal(16,2))     as avg_ctat,
    cast(json_extract_scalar(j, '$.cancelled_rides_by_customer') as integer) as cancelled_rides_by_customer,
    json_extract_scalar(j, '$.reason_for_cancelling_by_customer')   as reason_for_cancelling_by_customer,
    cast(json_extract_scalar(j, '$.cancelled_rides_by_driver') as integer)   as cancelled_rides_by_driver,
    json_extract_scalar(j, '$.driver_cancellation_reason')          as driver_cancellation_reason,
    cast(json_extract_scalar(j, '$.incomplete_rides') as integer)   as incomplete_rides,
    json_extract_scalar(j, '$.incomplete_rides_reason')             as incomplete_rides_reason,
    cast(json_extract_scalar(j, '$.booking_value') as decimal(24,4)) as booking_value,
    cast(json_extract_scalar(j, '$.ride_distance') as decimal(16,2)) as ride_distance,
    cast(json_extract_scalar(j, '$.driver_ratings') as decimal(8,2)) as driver_ratings,
    cast(json_extract_scalar(j, '$.customer_rating') as decimal(8,2)) as customer_rating,
    json_extract_scalar(j, '$.payment_method')                      as payment_method
from src
where j is not null
{% if is_incremental() %}
  and created_at > (
      select coalesce(max(created_at), timestamp '1970-01-01 00:00:00')
      from {{ this }}
  )
{% endif %}