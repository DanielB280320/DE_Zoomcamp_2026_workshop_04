SELECT 
    locationid AS location_id,
    borough,
    zone,
    service_zone
    
FROM {{ref('taxi_zone_lookup')}}