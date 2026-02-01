from datetime import datetime
from pydantic import BaseModel

class WeatherFacts(BaseModel):
    event_time: datetime
    city_id:str
    city_name:str
    latitude:float
    longitude:float

    temperautre_c:float
    windspeed_kmh:float
    winddirection_deg:int
    weather_code:int
    is_day:bool

    elevation_m:float
    source:str
    ingested_at:datetime
    dt:datetime


