from .ml_space import ModelXGBoost
from dataclasses import dataclass, field
from typing import Dict, Any
from .db_manager import RedisOkrugFetcher
from .models import Features, RedisData
import pandas as pd


class PipelineModel:
    def __init__(self):

        self.redisfetch = RedisOkrugFetcher()
        self.model = ModelXGBoost('./app/config.json')

    def failure_responce(self, request_id, message):
        return {
            "request_id": request_id,
            "message": message, 
            "status": "fail"
        }

    def process(self, query: Dict[str, Any]): 

        
        if 'request_id' not in query or 'features' not in query or 'okrug' not in query['features']:
            self.failure_responce(
                query.get("request_id", "unknown"), 
                "Error: request_id, features, and okrug are required."
            )
        
        features_data = query['features']
        request_id = query['request_id']
        try:
            features = Features(
                okrug = features_data['okrug'],
                roomsCount=features_data.get('roomsCount', 2),
                ceilingHeight=features_data.get('ceilingHeight', 2.85),
                totalArea=features_data.get('totalArea', 50.0),
                floorNumber=features_data.get('floorNumber', 5),
                floorsCount=features_data.get('floorsCount', 9),
                cargoLiftsCount=features_data.get('cargoLiftsCount', 1),
                houseMaterialType_brick=features_data.get('houseMaterialType_brick', False),
                houseMaterialType_monolith=features_data.get('houseMaterialType_monolith', False),
                houseMaterialType_monolithBrick=features_data.get('houseMaterialType_monolithBrick', False),
                houseMaterialType_none=features_data.get('houseMaterialType_none', False),
                houseMaterialType_panel=features_data.get('houseMaterialType_panel', False),
            )

            rfeatures = self.redisfetch.fetch_okrug_data(features.okrug)
            df_features = Features.get_df(features)
            df_redis = RedisData.get_df(rfeatures)
            data = pd.concat([df_features, df_redis], axis=1)
        
            res = self.model.predict(data)
            float(res[0]) 

            return {
                "request_id": request_id,
                "result":  float(res[0]), 
                "status": "done"
            }
        except Exception as err:
            self.failure_responce(
                request_id,
                f"ErrorType: {type(err).__name__}. Err: {err}"
            )
