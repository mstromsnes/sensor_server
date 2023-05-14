import logging
import uvicorn
from fastapi import FastAPI, Body
from fastapi.responses import FileResponse, Response, PlainTextResponse
from sensor import SensorReading
from datastore import DataStore, Format
from datetime import datetime
from typing import Annotated
from pathlib import Path
from contextlib import asynccontextmanager

ARCHIVE_PATH = Path("archive/data.parquet")
datastore = DataStore(ARCHIVE_PATH)


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    datastore.archive_data()


app = FastAPI(lifespan=lifespan)


@app.post("/")
async def add_reading(reading: SensorReading):
    logging.log(logging.INFO, reading)
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


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    main()
