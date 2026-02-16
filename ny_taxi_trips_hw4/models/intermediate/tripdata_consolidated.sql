WITH green_union_yellow AS (
    SELECT
        vendor_id,
        pickup_datetime,
        dropoff_datetime,
        store_and_fwd_flag,
        ratecode_id,
        pickup_location_id,
        dropoff_location_id,
        passenger_count,
        trip_distance,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        ehail_fee, 
        improvement_surcharge,
        total_amount,
        payment_type,
        trip_type, 
        congestion_surcharge, 
        'Green' AS service_type

    FROM {{ref('stg_green_tripdata')}}

    UNION ALL

    SELECT 
        vendor_id,
        pickup_datetime,
        dropoff_datetime,
        store_and_fwd_flag,
        ratecode_id,
        pickup_location_id,
        dropoff_location_id,
        passenger_count,
        trip_distance,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        NULL AS ehail_fee, 
        improvement_surcharge,
        total_amount,
        payment_type,
        1 AS trip_type,
        congestion_surcharge, 
        'yellow' AS service_type

    FROM {{ref('stg_yellow_tripdata')}}
)

SELECT
    {{dbt_utils.generate_surrogate_key(
        ['U.vendor_id', 'U.pickup_location_id', 'U.ratecode_id']
        )
    }} AS trip_id,
    U.vendor_id,
    U.pickup_datetime,
    U.dropoff_datetime,
    U.store_and_fwd_flag,
    U.ratecode_id,
    U.pickup_location_id,
    U.dropoff_location_id,
    U.passenger_count,
    U.trip_distance,
    U.fare_amount,
    U.extra,
    U.mta_tax,
    U.tip_amount,
    U.tolls_amount,
    U.ehail_fee, 
    U.improvement_surcharge,
    U.total_amount,
    U.payment_type,
    P.description AS payment_description, 
    U.trip_type, 
    U.congestion_surcharge, 
    U.service_type

FROM green_union_yellow U
LEFT JOIN {{ref('payment_type_lookup')}} P
    ON U.payment_type = P.payment_type

QUALIFY ROW_NUMBER() OVER (PARTITION BY U.vendor_id, U.pickup_datetime,
    U.pickup_location_id, U.service_type ORDER BY U.pickup_datetime) = 1
