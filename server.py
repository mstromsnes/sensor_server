import logging
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Annotated, Union

import pandas as pd
import uvicorn
from fastapi import (BackgroundTasks, Body, Depends, FastAPI, Request,
                     WebSocket, WebSocketDisconnect)
from fastapi.responses import PlainTextResponse, Response
from fastapi_utils.tasks import repeat_every

import remotereader
from datastore import DataStore
from forwarding import ForwardingManager
from publisher import Publisher
from sensor import SensorReading
from sensordata import SensorData


def dev_mode():
    return Path("dev").exists()


PROXY = "http://192.168.4.141:8000" if dev_mode() else None

DATASTORE = DataStore(proxy=PROXY)
PUBLISHER = Publisher()
FORWARDER = ForwardingManager()


def get_datastore():
    return DATASTORE


def get_publisher():
    return PUBLISHER


def get_forwarder():
    return FORWARDER


DataStoreDep = Annotated[DataStore, Depends(get_datastore)]
PublisherDep = Annotated[Publisher, Depends(get_publisher)]
ForwarderDep = Annotated[ForwardingManager, Depends(get_forwarder)]


def archive_data():
    get_datastore().archive_data()


@repeat_every(seconds=60 * 60 * 6, wait_first=True)
def archive_task():
    archive_data()


@asynccontextmanager
async def lifespan(app: FastAPI):
    if dev_mode():
        remotereader.register_as_forwarding_server(PROXY)
    await archive_task()
    yield
    if dev_mode():
        remotereader.remove_as_forwarding_server(PROXY)
    archive_data()


app = FastAPI(lifespan=lifespan)


@app.post("/")
async def add_reading(
    reading: SensorReading,
    background_tasks: BackgroundTasks,
    datastore: DataStoreDep,
    publisher: PublisherDep,
    forwarder: ForwarderDep,
):
    background_tasks.add_task(publisher.broadcast, reading)
    background_tasks.add_task(forwarder.broadcast, reading)
    datastore.add_reading(reading)


@app.get("/")
async def tail(datastore: DataStoreDep):
    return str(datastore.tail())


@app.post("/archive/parquet/")
def send_data_since_parquet(
    timestamp: Annotated[datetime, Body()], datastore: DataStoreDep
) -> Response:
    coerced_timestamp = pd.Timestamp(timestamp)
    parquet_bytes = datastore.serialize_archive(
        timestamp=coerced_timestamp, format=SensorData.Parquet
    )
    return Response(parquet_bytes)


@app.get("/archive/parquet/")
def send_archive_parquet(datastore: DataStoreDep) -> Response:
    parquet_bytes = datastore.serialize_archive()
    return Response(parquet_bytes)


@app.post("/archive/json/")
def send_data_since_json(
    timestamp: Annotated[datetime, Body()], datastore: DataStoreDep
):
    coerced_timestamp = pd.Timestamp(timestamp)
    json = datastore.serialize_archive(
        timestamp=coerced_timestamp, format=SensorData.JSON
    )
    return json


@app.get("/archive/json/")
def send_archive_json(datastore: DataStoreDep):
    json = datastore.serialize_archive(format=SensorData.JSON)
    return json


def client_port_endpoint_url(client: str, port: Union[str, int], endpoint: str):
    return f"http://{client}:{port}{endpoint}"


@app.post("/register_forwarding_server/")
def register_forwarding_server(request: Request, forwarder: ForwarderDep):
    if request.client is not None:
        host, port = request.client
        forwarder.register_forwarding_endpoint(
            client_port_endpoint_url(host, port, "/")
        )


@app.delete("/register_forwarding_server/")
def delete_forwarding_server(request: Request, forwarder: ForwarderDep):
    if request.client is not None:
        host, port = request.client
        forwarder.remove_forwarding_endpoint(client_port_endpoint_url(host, port, "/"))


@app.get("/helloworld/")
def hello() -> PlainTextResponse:
    return PlainTextResponse("Hello World!")


@app.websocket("/")
async def handle_subscriber(websocket: WebSocket, publisher: PublisherDep):
    await publisher.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        publisher.disconnect(websocket)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    main()
