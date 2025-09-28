{{ config(
    materialized='incremental',
    unique_key = ['booking_id','customer_id'],
    incremental_strategy='merge',
    on_schema_change = 'append_new_columns'
   )
}}

select
    booking_date,
    booking_time,
    booking_id,
    booking_status,
    customer_id,
    vehicle_type,
    pickup_location,
    drop_location,
    avg_vtat,
    avg_ctat,
    cancelled_rides_by_customer,
    reason_for_cancelling_by_customer,
    cancelled_rides_by_driver,
    driver_cancellation_reason,
    incomplete_rides,
    incomplete_rides_reason,
    booking_value,
    ride_distance,
    driver_ratings,
    customer_rating,
    payment_method,
    created_at,
    source_file
from {{ ref('stg_uber_rides') }}
{% if is_incremental() %}
where created_at > (
    select coalesce(max(created_at), timestamp '1900-01-01')
    from {{ this }}
)
{% endif %}