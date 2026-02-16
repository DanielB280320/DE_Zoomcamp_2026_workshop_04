SELECT 
    CAST(dispatching_base_num AS STRING) AS dispatching_base_num,
    CAST(pickup_datetime AS TIMESTAMP) AS pickup_datetime,
    CAST(dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,
    CAST(PULocationID AS INT) AS pickup_location_id,
    CAST(DOLocationID AS INT) AS dropoff_location_id,
    CAST(SR_Flag AS INT) AS SR_Flag, 
    CAST(Affiliated_base_number AS STRING) AS Affiliated_base_number

FROM {{source('ny_taxi_trips_raw', 'fhv_tripdata')}}
WHERE dispatching_base_num IS NOT NULL