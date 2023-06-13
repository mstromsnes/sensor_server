import pandas as pd
from sensor import SensorReading
from datastore import SensorData, DataStore, ParquetManager, SensorReadingQueue
from pathlib import Path
from format import Format
from publisher import Publisher
from forwarding import ForwardingManager
from server import app, get_datastore, get_forwarder, get_publisher
from fastapi.testclient import TestClient
import datetime
import pytest

TIME_ISO_FORMAT = "2023-05-17"

timestamp = pd.Timestamp.fromisoformat(TIME_ISO_FORMAT)
delta = pd.Timedelta(seconds=1)


def reading_generator():
    i = 0
    timestamp = datetime.datetime.now()
    recipe = {
        "sensor_type": "temperature",
        "sensor": "DHT11",
        "timestamp": str(timestamp),
        "reading": 20.0,
        "unit": "C",
    }
    while True:
        i += 1
        recipe["timestamp"] = str(timestamp + datetime.timedelta(seconds=i))
        yield SensorReading(**recipe)


@pytest.fixture
def archive_timestamp():
    return pd.Timestamp.fromisoformat(TIME_ISO_FORMAT)


def generate_from_delta(num_delta: int):
    return [
        SensorReading(
            sensor_type="temperature",
            sensor="PI_CPU",
            timestamp=(timestamp + delta * num_delta).isoformat(),
            reading=34.0,
            unit="C",
        ),
        SensorReading(
            sensor_type="temperature",
            sensor="DHT11",
            timestamp=(timestamp + delta * num_delta).isoformat(),
            reading=34.0,
            unit="C",
        ),
        SensorReading(
            sensor_type="temperature",
            sensor="DS18B20",
            timestamp=(timestamp + delta * num_delta).isoformat(),
            reading=34.0,
            unit="C",
        ),
        SensorReading(
            sensor_type="humidity",
            sensor="DHT11",
            timestamp=(timestamp + delta * num_delta).isoformat(),
            reading=34.0,
            unit="%",
        ),
    ]


@pytest.fixture
def readings() -> list[SensorReading]:
    """360 instances of SensorReading and in a single list.
    Each group of 4 readings is offset by 1 second"""
    return [reading for num in range(360) for reading in generate_from_delta(num)]


@pytest.fixture
def dataframe(readings: list[SensorReading]) -> pd.DataFrame:
    return SensorData.make_dataframe_from_list_of_readings(readings)


@pytest.fixture
def serialized_parquet_frame(dataframe: pd.DataFrame):
    return Format.Parquet.serialize(dataframe)


@pytest.fixture
def serialized_json_frame(dataframe: pd.DataFrame):
    return Format.JSON.serialize(dataframe)


@pytest.fixture
def archive_file(dataframe: pd.DataFrame, tmp_path):
    path = tmp_path / "test.parquet"
    Format.Parquet.write(dataframe, path)
    return path


@pytest.fixture
def queue():
    return SensorReadingQueue(maxlen=10)


@pytest.fixture
def filled_queue(queue: SensorReadingQueue):
    readings = reading_generator()
    for _ in range(10):
        queue.add_reading(next(readings), lambda: ...)


@pytest.fixture
def parquet_manager():
    return ParquetManager(Path("archive/test/"))


@pytest.fixture
def datastore(parquet_manager: ParquetManager, queue: SensorReadingQueue):
    return DataStore(manager=parquet_manager, queue=queue)


@pytest.fixture
def publisher():
    return Publisher()


@pytest.fixture
def forwarder():
    return ForwardingManager()


@pytest.fixture
def client(datastore, publisher, forwarder) -> TestClient:
    app.dependency_overrides[get_datastore] = lambda: datastore
    app.dependency_overrides[get_publisher] = lambda: publisher
    app.dependency_overrides[get_forwarder] = lambda: forwarder

    return TestClient(app)
