select
  booking_date as ride_date,
  count(*) as rides_count,
  avg(ride_distance) as avg_distance_km,
  max(ride_distance) as max_distance_km,
  min(ride_distance) as min_distance_km
from {{ ref('uber_rides') }}
group by 1
order by 1