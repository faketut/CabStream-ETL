{{ config(
    materialized = 'table',
    partition_by = {
      "field": "pickup_date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by = ["pickup_hour"]
)}}

-- Hourly aggregation; reads only from fact_trips (no dim_datetime join needed).
SELECT
    pickup_date,
    EXTRACT(YEAR  FROM pickup_date) AS year,
    EXTRACT(MONTH FROM pickup_date) AS month,
    EXTRACT(DAY   FROM pickup_date) AS day,
    pickup_hour,
    is_weekend,
    COUNT(*)            AS trip_count,
    AVG(trip_distance)  AS avg_distance,
    AVG(fare_amount)    AS avg_fare,
    AVG(tip_amount)     AS avg_tip,
    AVG(total_amount)   AS avg_total,
    SUM(total_amount)   AS total_revenue
FROM {{ ref('fact_trips') }}
WHERE pickup_date >= DATE_SUB(
    CURRENT_DATE(), INTERVAL {{ var('lookback_days', 90) }} DAY
)
GROUP BY pickup_date, pickup_hour, is_weekend
ORDER BY pickup_date, pickup_hour
