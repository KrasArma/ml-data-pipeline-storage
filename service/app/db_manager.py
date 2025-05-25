from .models import RedisData
import redis
import os
from typing import Optional
from .log_conf import logger


class RedisOkrugFetcher:
    def __init__(self):
        self.redis_client = redis.Redis(
            host=os.getenv('REDIS_HOST'), 
            port=int(os.getenv('REDIS_PORT')), 
            db=0
        )
        logger.info("Redis client initialized")

    def fetch_okrug_data(self, okrug: str) -> Optional[RedisData]:
        key = f"wiki:{okrug}"
        data = self.redis_client.hgetall(key)

        if not data:
            logger.warning(f"No data found for {key}.")
            return None

        okrug_data = RedisData(
            area=data.get(b'area', b'').decode('utf-8'),
            area_km2=float(data.get(b'area_km2', b'0').decode('utf-8')),
            area_percentage=float(data.get(b'area_percentage', b'0').decode('utf-8')),
            rank_area=int(data.get(b'rank_area', b'0').decode('utf-8')),
            population_2024=int(data.get(b'population_2024', b'0').decode('utf-8')),
            population_percentage=float(data.get(b'population_percentage', b'0').decode('utf-8')),
            rank_population=int(data.get(b'rank_population', b'0').decode('utf-8')),
            density_2024=float(data.get(b'density_2024', b'0').decode('utf-8')),
            rank_density=int(data.get(b'rank_density', b'0').decode('utf-8'))
        )

        logger.info(f"Data for {okrug}: {okrug_data}")
        return okrug_data