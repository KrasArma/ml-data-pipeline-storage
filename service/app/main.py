from fastapi import FastAPI
from app.services import process_request
from app.schemas import RequestSchema, ResponseSchema

app = FastAPI()

@app.post("/process", response_model=ResponseSchema)
async def process(request: RequestSchema):
    return await process_request(request)