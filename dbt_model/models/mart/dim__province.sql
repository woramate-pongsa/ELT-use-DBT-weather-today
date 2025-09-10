{{ config(materialized='view') }}

SELECT
    province,
    station_name,
    latitude,
    longitude
FROM 
    {{ source('weather_today','stg__weather_today') }}
WHERE 
    date_time = '2025-08-07 07:00:00 UTC'