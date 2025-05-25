
from pydantic import BaseModel
from typing_extensions import TypedDict
from dataclasses import dataclass, field
from typing import Dict, Any, Optional, Union, Literal
import pandas as pd
import json


class Config:
    def __init__(self, config_path: str):
        self.config_path = config_path
        self._load_config()

    def _load_config(self):
        with open(self.config_path, 'r', encoding='utf-8') as f:
            config_data = json.load(f)
            self.model_id = config_data.get('model_id', None)  

    def __repr__(self):
        return f"Config(model_id={self.model_id})"
    

class TQuery(TypedDict):
    request_id: str
    features: Dict[str, Union[str, int, bool, float]]


class TResponse(TypedDict):
    request_id: str
    result: float
    status: str


@dataclass
class Features:
    okrug: Literal[
        'САО', 'НАО', 'ЮЗАО', 'ЮВАО', 'ЗелАО',
        'ВАО', 'ЗАО', 'СВАО', 'ЮАО', 'ЦАО',
        'ТАО', 'СЗАО'
    ]
    roomsCount: int = 2
    ceilingHeight: float = 2.85
    totalArea: float = 50.0
    floorNumber: int = 5
    floorsCount: int = 9
    cargoLiftsCount: int = 1
    houseMaterialType_brick: bool = False
    houseMaterialType_monolith: bool = False
    houseMaterialType_monolithBrick: bool = False
    houseMaterialType_none: bool = False
    houseMaterialType_panel: bool = False   

    def get_df(self):
        data = {
            'roomsCount': [self.roomsCount],
            'ceilingHeight': [self.ceilingHeight],
            'totalArea': [self.totalArea],
            'floorNumber': [self.floorNumber],
            'floorsCount': [self.floorsCount],
            'cargoLiftsCount': [self.cargoLiftsCount],
            'houseMaterialType_brick': [self.houseMaterialType_brick],
            'houseMaterialType_monolith': [self.houseMaterialType_monolith],
            'houseMaterialType_monolithBrick': [self.houseMaterialType_monolithBrick],
            'houseMaterialType_none': [self.houseMaterialType_none],
            'houseMaterialType_panel': [self.houseMaterialType_panel]
        }
        return pd.DataFrame(data)


@dataclass
class RedisData:
    area: Literal[
        'САО', 'НАО', 'ЮЗАО', 'ЮВАО', 'ЗелАО',
        'ВАО', 'ЗАО', 'СВАО', 'ЮАО', 'ЦАО',
        'ТАО', 'СЗАО'
    ]
    area_km2: float = field(default=0.0)
    area_percentage: float = field(default=0.0)
    rank_area: int = field(default=0)
    population_2024: int = field(default=0)
    population_percentage: float = field(default=0.0)
    rank_population: int = field(default=0)
    density_2024: float = field(default=0.0)
    rank_density: int = field(default=0)

    def get_df(self):
        data = {
            'Area_percentage': [self.area_percentage],
            'Rank_Area': [self.rank_area],
            'Population_2024': [self.population_2024],
            'Population_percentage': [self.population_percentage],
            'Rank_Population': [self.rank_population],
            'Density_2024': [self.density_2024],
            'Rank_Density': [self.rank_density],
        }
        return pd.DataFrame(data)





