{{ config(materialized='view') }}

SELECT
    province,
    station_name,
    DATE(date_time) AS date_time,
    FORMAT_DATE('%Y-%m', DATE(date_time)) AS year_month,
    ROUND(AVG(temperature), 2) AS avg_temperature,
    ROUND(MAX(max_temperature), 2) AS max_temperature,
    ROUND(MIN(min_temperature), 2) AS min_temperature,
    ROUND(MAX(wind_speed), 2) AS max_wind_speed
FROM 
    {{ source(('my_project'), 'weather_today_data') }}
GROUP BY 
    province,
    station_name,
    date_time, 
    year_month