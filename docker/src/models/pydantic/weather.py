from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field

class CurrentWeatherUnits(BaseModel):
    time: str
    interval: str
    temperature:str
    windspeed:str
    winddirection:str
    is_day:str
    weathercode:str

class CurrentWeather(BaseModel):
    time:datetime
    interval: int
    temperature:float
    windspeed:float
    winddirection:int
    is_day:int= Field(ge=0,le=1)
    weathercode: int

class WeatherResponse(BaseModel):
    latitude: float
    longitude: float
    generationtime_ms: float
    utc_offset_seconds: int
    timezone: str
    timezone_abbreviation: str
    elevation: float
    current_weather_units: CurrentWeatherUnits
    current_weather: CurrentWeather

    def to_staging_dict(self) ->dict:
        return{
            "event_time":self.current_weather.time,
            "latitude":self.latitude,
            "longitude":self.longitude,
            "temperature_c":self.current_weather.temperature,
            "winddirection_deg":self.current_weather.winddirection,
            "weather_code":self.current_weather.weathercode,
            "is_day":self.current_weather.is_day,
            "timezone":self.timezone,
            "elevation":self.elevation,
            "ingested_at":datetime.utcnow(),
        }
    

