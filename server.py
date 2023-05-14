import logging
import uvicorn
from fastapi import FastAPI, Body, Request
from fastapi.responses import FileResponse, Response, PlainTextResponse
import requests
from starlette.datastructures import Address
from sensor import SensorReading
from datastore import DataStore, Format
from datetime import datetime
from typing import Annotated, Optional
from pathlib import Path
from contextlib import asynccontextmanager

ARCHIVE_PATH = Path("archive/data.parquet")
datastore = DataStore(ARCHIVE_PATH)


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    datastore.archive_data()


app = FastAPI(lifespan=lifespan)
app.proxy: Optional[Address] = None


def client_and_endpoint_url(client: Address, endpoint: str):
    return f"http://{client.host}:{client.port}{endpoint}"


@app.post("/")
async def add_reading(reading: SensorReading):
    logging.log(logging.INFO, reading)
    if app.proxy is not None:
        requests.post(client_and_endpoint_url(app.proxy, "/"), json=reading)
    datastore.add_reading(reading)
    return reading


@app.get("/")
async def tail():
    return str(datastore.tail())


@app.post("/archive/parquet/")
def send_data_since(timestamp: Annotated[datetime, Body()]) -> Response:
    parquet_bytes = datastore.serialize_archive_since_timestamp(timestamp)
    return Response(parquet_bytes)


@app.get("/archive/parquet/")
def send_archive() -> Response:
    parquet_bytes = datastore.serialize_archive()
    return Response(parquet_bytes)


@app.post("/archive/json/")
def send_data_since(timestamp: Annotated[datetime, Body()]):
    json = datastore.serialize_archive_since_timestamp(timestamp, Format.JSON)
    return json


@app.get("/archive/json/")
def send_archive():
    json = datastore.serialize_archive(format=Format.JSON)
    return json


@app.post("/register_proxy")
def register_proxy_server(request: Request):
    app.proxy = request.client


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    main()
