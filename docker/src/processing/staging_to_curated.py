from datetime import datetime
from curated.weather_fact import WeatherFacts


def staging_to_curated(staging:dict,city:dict)->WeatherFacts:
    event_time = staging["event_time"]
    return WeatherFacts(
        event_time = event_time,
        city_id = city["city_id"],
        city_name = city["city_name"],
        latitude = staging["latitude"],
        longitude = staging["longitude"],

        temperautre_c = staging["temperautre_c"],
        windspeed_kmh = staging["windspeed_kmh"],
        winddirection_deg = staging["winddirection_deg"],
        weather_code = staging["weather_code"],
        is_day = staging["is_day"],

        elevation_m = staging["elevation_m"],
        source ="open-meteo",
        ingested_at = staging["ingested_at"],
        dt = event_time.date(),
    )