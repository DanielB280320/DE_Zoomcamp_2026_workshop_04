SELECT 
    pickup_zone, 
    DATE_TRUNC(pickup_datetime, MONTH) AS Revenue_Month,
    trip_type AS service_type,
    SUM(passenger_count) AS total_passengers,
    SUM(trip_distance) AS total_distance,
    SUM(fare_amount) AS total_fare_amount,
    SUM(extra) AS total_extra,
    SUM(mta_tax) AS total_mta_tax,
    SUM(tip_amount) AS total_tip_amount,
    SUM(tolls_amount) AS total_tolls_amount,
    SUM(ehail_fee) AS total_ehail_fee,
    SUM(improvement_surcharge) AS total_improvement_surcharge,
    SUM(total_amount) AS total_revenue_per_month,
    -- Additional metrics
    COUNT(trip_id) AS total_trips,
    AVG(trip_distance) AS avg_trip_distance,
    AVG(passenger_count) AS avg_passenger_count,

FROM {{ref('fact_trips')}}
GROUP BY pickup_zone, Revenue_Month, service_type
ORDER BY Revenue_Month DESC
