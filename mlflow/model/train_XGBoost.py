import pandas as pd
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.preprocessing import StandardScaler
from xgboost import XGBRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.pipeline import Pipeline
import mlflow
import mlflow.sklearn
from mlflow.models import infer_signature

mlflow.set_tracking_uri(uri="http://mlflow:5000")  
mlflow.set_experiment("XGBoost Regression by MLFlow Pipeline")

df = pd.read_csv('all_data_prepared.csv')

x = df.drop(['price'], axis=1)
y = df['price']
x = x.fillna(0)
x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.25, random_state=123)

pipeline = Pipeline([
    ('scaler', StandardScaler()),
    ('xgb', XGBRegressor(objective='reg:squarederror', random_state=123))
])

param_grid = {
    'xgb__n_estimators': [100, 200],
    'xgb__max_depth': [3, 5, 7],
    'xgb__learning_rate': [0.01, 0.1],
    'xgb__subsample': [0.8, 1.0],
    'xgb__colsample_bytree': [0.8, 1.0],
    'xgb__reg_alpha': [0, 0.1, 1],  
    'xgb__reg_lambda': [1, 1.5, 2]
}

with mlflow.start_run():
    grid_search = GridSearchCV(pipeline, param_grid, scoring='neg_mean_squared_error', cv=3, n_jobs=-1)
    grid_search.fit(x_train, y_train)

    best_model = grid_search.best_estimator_

    prediction = best_model.predict(x_test)

    mae = mean_absolute_error(y_test, prediction)
    mse = mean_squared_error(y_test, prediction)
    r2 = r2_score(y_test, prediction)

    mlflow.log_params(grid_search.best_params_)
    mlflow.log_metric("MAE", mae)
    mlflow.log_metric("MSE", mse)
    mlflow.log_metric("R2", r2)

    signature = infer_signature(x_train, best_model.predict(x_train))
    mlflow.sklearn.log_model(
        sk_model=best_model,
        artifact_path="xgboost_model",
        signature=signature,
        input_example=x_train,
        registered_model_name="XGBoost_Regression_Model"
    )

    print("Best params:")
    print(grid_search.best_params_)
    print(f'XGBoost MAE: {mae}')
    print(f'XGBoost MSE: {mse}')
    print(f'XGBoost R²: {r2}')
