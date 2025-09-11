{{ config(materialized='table') }}

SELECT
    province,
    station_name,
    latitude,
    longitude
FROM 
    {{ ref('int__weather_today') }}