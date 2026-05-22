{{ config(
    materialized = 'table'
)}}

-- Real NYC TLC borough/zone lookup, loaded from seeds/taxi_zone_lookup.csv.

WITH pickup_zones AS (
    SELECT DISTINCT PULocationID AS location_id
    FROM {{ source('nyc_taxi', 'yellow_tripdata') }}
),

dropoff_zones AS (
    SELECT DISTINCT DOLocationID AS location_id
    FROM {{ source('nyc_taxi', 'yellow_tripdata') }}
),

all_zones AS (
    SELECT location_id FROM pickup_zones
    UNION DISTINCT
    SELECT location_id FROM dropoff_zones
)

SELECT
    z.location_id,
    COALESCE(l.Borough, 'Unknown') AS borough,
    COALESCE(l.Zone,    CONCAT('Zone ', CAST(z.location_id AS STRING))) AS zone,
    l.service_zone
FROM all_zones z
LEFT JOIN {{ ref('taxi_zone_lookup') }} l
       ON z.location_id = l.LocationID
