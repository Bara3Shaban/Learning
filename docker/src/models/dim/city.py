from pydantic import BaseModel

class CityDim(BaseModel):
    city_id:str
    city_name:str
    latitude:float
    longitude:float
    elevation_m:float
    country:str | None = None