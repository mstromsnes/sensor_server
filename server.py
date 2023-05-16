import logging
import uvicorn
from fastapi import (
    FastAPI,
    Body,
    Request,
    BackgroundTasks,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.responses import Response, PlainTextResponse
from publisher import Publisher
from sensor import SensorReading
from datastore import DataStore, Format
from datetime import datetime
from typing import Annotated, Union
from pathlib import Path
from forwarding import ForwardingManager
from contextlib import asynccontextmanager

ARCHIVE_PATH = Path("archive/data.parquet")
datastore = DataStore(parquet_file=ARCHIVE_PATH)
publisher = Publisher()
forwarder = ForwardingManager()


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    datastore.archive_data(ARCHIVE_PATH)


app = FastAPI(lifespan=lifespan)


@app.post("/")
async def add_reading(reading: SensorReading, background_tasks: BackgroundTasks):
    background_tasks.add_task(publisher.broadcast, reading)
    background_tasks.add_task(forwarder.broadcast, reading)
    datastore.add_reading(reading)


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


def client_port_endpoint_url(client: str, port: Union[str, int], endpoint: str):
    return f"http://{client}:{port}{endpoint}"


@app.post("/register_forwarding_server/")
def register_forwarding_server(request: Request):
    if request.client is not None:
        host, port = request.client
        forwarder.register_forwarding_endpoint(
            client_port_endpoint_url(host, port, "/")
        )


@app.delete("/register_forwarding_server/")
def delete_forwarding_server(request: Request):
    if request.client is not None:
        host, port = request.client
        forwarder.remove_forwarding_endpoint(client_port_endpoint_url(host, port, "/"))


@app.get("/helloworld/")
def hello() -> PlainTextResponse:
    return PlainTextResponse("Hello World!")


@app.websocket("/")
async def handle_subscriber(websocket: WebSocket):
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
