SELECT 
    CAST(vendorid AS INT) AS vendor_id,
    CAST(tpep_pickup_datetime AS TIMESTAMP) AS pickup_datetime,
    CAST(tpep_dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,
    CAST(store_and_fwd_flag AS STRING) AS store_and_fwd_flag,
    CAST(ratecodeid AS INT) AS ratecode_id,
    CAST(pulocationid AS INT) AS pickup_location_id,
    CAST(dolocationid AS INT) AS dropoff_location_id,
    CAST(passenger_count AS INT) AS passenger_count,
    CAST(trip_distance AS NUMERIC) AS trip_distance,
    CAST(fare_amount AS NUMERIC) AS fare_amount,
    CAST(extra AS NUMERIC) AS extra,
    CAST(mta_tax AS NUMERIC) AS mta_tax,
    CAST(tip_amount AS NUMERIC) AS tip_amount,
    CAST(tolls_amount AS NUMERIC) AS tolls_amount,
    NULL AS ehail_fee, 
    CAST(improvement_surcharge AS NUMERIC) AS improvement_surcharge,
    CAST(total_amount AS NUMERIC) AS total_amount,
    CAST(payment_type AS INT) AS payment_type,
    1 AS trip_type,
    CAST(congestion_surcharge AS NUMERIC) AS congestion_surcharge

FROM {{source('ny_taxi_trips_raw', 'yellow_tripdata')}}
WHERE vendorid IS NOT NULL