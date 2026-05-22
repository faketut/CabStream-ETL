{{ config(
    materialized = 'table',
    partition_by = {
      "field": "pickup_date",
      "data_type": "date",
      "granularity": "day"
    }
)}}

-- Daily aggregation for the temporal dashboard tile.
SELECT
    pickup_date,
    COUNT(*)              AS trip_count,
    AVG(trip_distance)    AS avg_distance,
    AVG(fare_amount)      AS avg_fare,
    AVG(tip_amount)       AS avg_tip,
    AVG(total_amount)     AS avg_total,
    SUM(total_amount)     AS total_revenue
FROM {{ ref('fact_trips') }}
WHERE pickup_date >= DATE_SUB(
    CURRENT_DATE(), INTERVAL {{ var('lookback_days', 90) }} DAY
)
GROUP BY pickup_date
ORDER BY pickup_date
