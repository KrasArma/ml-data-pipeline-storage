import mlflow

from models import Config


class ModelXGBoost:
    def __ini__(self, config: str):
        self.config = Config(config)
    
logged_model = 'runs:/0cc1f208149c4326b218e3e3449fd7a4/xgboost_model'

# Load model as a PyFuncModel.
loaded_model = mlflow.pyfunc.load_model(logged_model)

# Predict on a Pandas DataFrame.
import pandas as pd
loaded_model.predict(pd.DataFrame('data'))

class MLModel:
    def __init__(self, model_uri: str):
        self.model = mlflow.pyfunc.load_model(model_uri)

    def predict(self, data):
        return self.model.predict(data)