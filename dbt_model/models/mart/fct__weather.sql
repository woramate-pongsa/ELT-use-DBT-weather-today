{{ config(materialized='view') }}

SELECT
    province,
    station_name,
    year_month,
    avg_temperature,
    max_temperature,
    max_wind_speed
FROM 
    {{ ref('int__weather_today_province') }}