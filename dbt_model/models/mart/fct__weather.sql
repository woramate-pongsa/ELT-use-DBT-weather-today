{{ config(materialized='view') }}

SELECT
    province,
    station_name,
    year_month,
    avg_temperature,
    max_temperature,
    max_wind_speed
FROM 
    {{ source('weather_today', 'stg__weather_today') }}