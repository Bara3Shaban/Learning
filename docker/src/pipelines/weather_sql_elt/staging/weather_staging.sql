CREATE OR REPLACE TABLE `{{ params.project_id }}.staging.weather`
PARTITION BY DATE(observation_ts)
CLUSTER BY city AS
SELECT 
    'Amman' AS city,
    SAFE_CAST(JSON_EXTRACT_SCALAR(payload.latitude) AS FLOAT64) AS latitude,
    SAFE_CAST(JSON_EXTRACT_SCALAR(payload.longitude) AS FLOAT64) AS longitude,

    TIMESTAMP(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M',JSON_EXTRACT_SCALAR(payload.current_weather.time)))as observation_ts,
    
    SAFE_CAST(JSON_EXTRACT_SCALAR(payload.current_weather.temperature) AS FLOAT64) AS temprature_c,
    SAFE_CAST(JSON_EXTRACT_SCALAR(payload.current_weather.windspeed) AS FLOAT64) AS wind_speed,
    SAFE_CAST(JSON_EXTRACT_SCALAR(payload.current_weather.winddirection) AS FLOAT64) AS wind_direction,
    SAFE_CAST(JSON_EXTRACT_SCALAR(payload.current_weather.weathercode) AS FLOAT64) AS weather_code,

    ingestion_ts,
    batch_id
    source_system
    
FROM`learning-486315.raw.weather`

WHERE payload.current_weather IS NOT NULL;