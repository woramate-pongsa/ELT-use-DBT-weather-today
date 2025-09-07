{{ config(materialized='view') }}

SELECT
    province,
    station_name,
    latitude,
    longitude
FROM 
    {{ source(('my_project'), 'weather_today_data') }}
WHERE 
    date_time = '2025-08-07 07:00:00 UTC'