import logging
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Annotated, Union, Optional

import pandas as pd
import uvicorn
from fastapi import (
    BackgroundTasks,
    Body,
    Depends,
    FastAPI,
    Request,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.responses import PlainTextResponse, Response
from fastapi_utils.tasks import repeat_every
from fastapi.exceptions import HTTPException

import remotereader
from datastore import DataStore
from forwarding import ForwardingManager
from publisher import Publisher
from sensor import SensorReading
from sensordata import SensorData

from format import SerializationFormat


def dev_mode():
    return Path("dev").exists()


if __name__ == "__main__":
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
        assert PROXY is not None
        remotereader.register_as_forwarding_server(PROXY)
    await archive_task()
    yield
    if dev_mode():
        assert PROXY is not None
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


@app.get("/archive/parquet/")
def send_full_archive_parquet(datastore: DataStoreDep) -> Response:
    parquet_bytes = datastore.serialize_archive(format=SensorData.Parquet)
    return Response(parquet_bytes)


@app.post("/archive/parquet/")
async def send_data_in_interval_parquet(
    datastore: DataStoreDep,
    start: Union[pd.Timestamp, None] = None,
    end: Union[pd.Timestamp, None] = None,
) -> Response:
    parquet_bytes = get_serialized_archive(datastore, start, end, SensorData.Parquet)
    return Response(parquet_bytes)


@app.get("/archive/json/")
def send_full_archive_json(datastore: DataStoreDep):
    json = datastore.serialize_archive(format=SensorData.JSON)
    return json


@app.post("/archive/json/")
async def send_data_in_interval_json(
    datastore: DataStoreDep,
    start: Union[pd.Timestamp, None] = None,
    end: Union[pd.Timestamp, None] = None,
):
    json = get_serialized_archive(datastore, start, end, format=SensorData.JSON)
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


def get_serialized_archive(
    datastore: DataStore,
    start: Optional[pd.Timestamp],
    end: Optional[pd.Timestamp],
    format: SerializationFormat,
):
    if start is not None and end is not None and start > end:
        raise HTTPException(
            422, "Ending timestamp must be later than starting timestamp"
        )
    serialized_result = datastore.serialize_archive(start=start, end=end, format=format)
    return serialized_result


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    main()
