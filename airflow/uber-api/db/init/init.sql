COPY ride_bookings(
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
    payment_method
)
FROM '/docker-entrypoint-initdb.d/ncr_ride_bookings.csv'
DELIMITER ','
CSV HEADER
NULL 'null';