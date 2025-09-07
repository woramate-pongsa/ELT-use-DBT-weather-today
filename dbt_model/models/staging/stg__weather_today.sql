{{ config(materialized='view') }}

SELECT
    CAST(province AS STRING) AS province,
    CAST(station_name AS STRING) AS station_name,
    ROUND(COALESCE(CAST(latitude AS FLOAT), 0), 2) AS latitude,
    ROUND(COALESCE(CAST(longitude AS FLOAT), 0), 2)AS longitude,
    CAST(date_time AS TIMESTAMP) AS date_time,
    ROUND(COALESCE(CAST(temperature AS FLOAT), 0), 2) AS temperature,
    ROUND(COALESCE(CAST(max_temperature AS FLOAT), 0), 2) AS max_temperature,
    ROUND(COALESCE(CAST(min_temperature AS FLOAT), 0), 2) AS min_temperature,
    ROUND(COALESCE(CAST(wind_speed AS FLOAT), 0), 2) AS wind_speed,
    CURRENT_TIMESTAMP() AS loaded_at
FROM 
    {{ ref('raw') }}