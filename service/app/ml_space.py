
import mlflow
import pandas as pd
from .models import Config


class ModelXGBoost:
    def __init__(self, config: str):
        self.config = Config(config)
        self.model_id = self.config.model_id  
        self.logged_model = f'runs:/{self.model_id}/xgboost_model'
        self.loaded_model = mlflow.pyfunc.load_model(self.logged_model)

    def predict(self, data: pd.DataFrame): 
        return self.loaded_model.predict(data)
