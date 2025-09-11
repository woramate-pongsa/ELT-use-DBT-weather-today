{{ config(materialized='table') }}

SELECT
    province,
    station_name,
    date_time,
    year_month,
    avg_temperature,
    max_temperature,
    min_temperature,
    max_wind_speed,
FROM 
    {{ ref('int__weather_today') }}