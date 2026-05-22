{{ config(
    materialized = 'incremental',
    unique_key = 'trip_id',
    incremental_strategy = 'merge',
    partition_by = {
      "field": "pickup_date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by = ["pickup_location_id"]
)}}

SELECT
    -- Deterministic, collision-resistant trip ID.
    TO_HEX(SHA256(CONCAT(
        CAST(VendorID AS STRING),               '|',
        CAST(tpep_pickup_datetime AS STRING),   '|',
        CAST(tpep_dropoff_datetime AS STRING),  '|',
        CAST(PULocationID AS STRING),           '|',
        CAST(DOLocationID AS STRING),           '|',
        CAST(total_amount AS STRING),           '|',
        CAST(trip_distance AS STRING)
    ))) AS trip_id,

    -- Dimension surrogate keys.
    FORMAT_TIMESTAMP('%Y%m%d%H', tpep_pickup_datetime)  AS pickup_datetime_id,
    FORMAT_TIMESTAMP('%Y%m%d%H', tpep_dropoff_datetime) AS dropoff_datetime_id,
    PULocationID AS pickup_location_id,
    DOLocationID AS dropoff_location_id,

    -- Trip measures.
    passenger_count,
    trip_distance,
    fare_amount,
    tip_amount,
    total_amount,

    -- Derived columns used by marts (avoids joining dim_datetime downstream).
    DATE(tpep_pickup_datetime)               AS pickup_date,
    EXTRACT(HOUR  FROM tpep_pickup_datetime) AS pickup_hour,
    EXTRACT(DAYOFWEEK FROM tpep_pickup_datetime) IN (1, 7) AS is_weekend,
    TIMESTAMP_DIFF(tpep_dropoff_datetime, tpep_pickup_datetime, MINUTE) AS trip_duration_minutes

FROM
    {{ source('nyc_taxi', 'yellow_tripdata') }}
WHERE
    tpep_pickup_datetime IS NOT NULL
    AND tpep_dropoff_datetime IS NOT NULL
    AND tpep_dropoff_datetime > tpep_pickup_datetime
    AND trip_distance > 0
    AND total_amount > 0

{% if is_incremental() %}
    AND tpep_pickup_datetime > (
        SELECT COALESCE(MAX(pickup_date), DATE '1900-01-01') FROM {{ this }}
    )
{% endif %}
