--WITH enriched_tripdata AS (
SELECT 
    T.trip_id,
    T.vendor_id,
    T.pickup_datetime,
    T.dropoff_datetime,
    DATE_DIFF(T.dropoff_datetime, T.pickup_datetime, MINUTE) AS trip_duration_minutes,
    T.store_and_fwd_flag,
    T.ratecode_id,
    T.pickup_location_id,
    PZ.borough AS pickup_borough,
    PZ.zone AS pickup_zone,
    T.dropoff_location_id,
    DZ.borough AS dropoff_borough,
    DZ.zone AS dropoff_zone,
    T.passenger_count,
    T.trip_distance,
    T.fare_amount,
    T.extra,
    T.mta_tax,
    T.tip_amount,
    T.tolls_amount,
    T.ehail_fee,
    T.improvement_surcharge,
    T.total_amount,
    T.payment_type,
    T.payment_description,
    T.trip_type,
    T.congestion_surcharge, 
    T.service_type

FROM {{ref('tripdata_consolidated')}} T
LEFT JOIN {{ref('dim_zones')}} PZ
    ON T.pickup_location_id = PZ.location_id
LEFT JOIN {{ref('dim_zones')}} DZ
    ON T.dropoff_location_id = DZ.location_id
