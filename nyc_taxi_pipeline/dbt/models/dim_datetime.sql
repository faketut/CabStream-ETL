{{ config(
    materialized = 'table',
    partition_by = {
      "field": "datetime_timestamp",
      "data_type": "timestamp",
      "granularity": "day"
    }
)}}

WITH bounds AS (
    SELECT
        MIN(DATE(tpep_pickup_datetime))  AS min_d,
        MAX(DATE(tpep_dropoff_datetime)) AS max_d
    FROM {{ source('nyc_taxi', 'yellow_tripdata') }}
),

datetime_spine AS (
    SELECT day AS date
    FROM bounds,
         UNNEST(GENERATE_DATE_ARRAY(min_d, max_d)) AS day
),

hours AS (
    SELECT hour FROM UNNEST(GENERATE_ARRAY(0, 23)) AS hour
),

datetime_expanded AS (
    SELECT date, hour
    FROM datetime_spine
    CROSS JOIN hours
)

SELECT
    FORMAT_TIMESTAMP('%Y%m%d%H', TIMESTAMP(date) + INTERVAL hour HOUR) AS datetime_id,
    hour,
    EXTRACT(DAY   FROM date) AS day,
    EXTRACT(MONTH FROM date) AS month,
    EXTRACT(YEAR  FROM date) AS year,
    EXTRACT(DAYOFWEEK FROM date) AS weekday,
    EXTRACT(DAYOFWEEK FROM date) IN (1, 7) AS is_weekend,
    TIMESTAMP(date) + INTERVAL hour HOUR AS datetime_timestamp
FROM datetime_expanded
