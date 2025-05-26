from fastapi import FastAPI, HTTPException
from .model_pipeline import PipelineModel  
import logging
from .log_conf import logger 
from .models import TQuery, TResponse
import uvicorn

from prometheus_fastapi_instrumentator import Instrumentator

app = FastAPI()
Instrumentator().instrument(app).expose(app)

pipeline_model = PipelineModel()

@app.get("/routes")
async def get_routes():
    return [{"path": route.path, "name": route.name} for route in app.routes]


@app.post("/process", response_model=TResponse)
async def process_request(query: TQuery):
    logger.info(f"Received request: {query}")

    try:
        result = pipeline_model.process(query)
        return result

    except Exception as e:
        logger.error(f"Error processing request: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    
    uvicorn.run(app, host="0.0.0.0", port=5051)