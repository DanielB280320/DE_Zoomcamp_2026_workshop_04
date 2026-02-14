WITH green_union_yellow AS (
    SELECT * 
    FROM {{ref('stg_green_tripdata')}}

    UNION ALL

    SELECT *
    FROM {{ref('stg_yellow_tripdata')}}
)

SELECT
    {{dbt_utils.generate_surrogate_key(
        ['U.vendor_id', 'U.pickup_location_id', 'U.ratecode_id']
    )}} AS trip_id,
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
    U.congestion_surcharge

FROM green_union_yellow U
LEFT JOIN {{ref('payment_type_lookup')}} P
    ON U.payment_type = P.payment_type

QUALIFY ROW_NUMBER() OVER (PARTITION BY U.pickup_datetime,
    U.dropoff_datetime, U.trip_distance, U.tip_amount ORDER BY U.pickup_datetime) = 1 