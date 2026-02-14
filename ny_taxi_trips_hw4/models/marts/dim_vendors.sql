SELECT
    DISTINCT vendor_id,
    {{get_vendor_names('vendor_id')}} AS vendor_names
    
FROM {{ref('tripdata_consolidated')}}