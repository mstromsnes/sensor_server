import pandas as pd
from sensor import SensorReading
from datastore import SensorData, DataStore
from format import Format
from publisher import Publisher
from forwarding import ForwardingManager
from server import app, get_datastore, get_forwarder, get_publisher
from fastapi.testclient import TestClient
import pytest

TIME_ISO_FORMAT = "2023-05-17"

timestamp = pd.Timestamp.fromisoformat(TIME_ISO_FORMAT)
delta = pd.Timedelta(seconds=1)


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
def dataframe(readings: list[SensorReading]):
    return SensorData.make_dataframe_from_list_of_readings(readings)


@pytest.fixture
def serialized_parquet_frame(dataframe: pd.DataFrame):
    return Format.Parquet.serialize(dataframe)


@pytest.fixture
def serialized_json_frame(dataframe: pd.DataFrame):
    return Format.JSON.serialize(dataframe)


