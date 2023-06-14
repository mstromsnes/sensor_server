from pathlib import Path

import hypothesis
import hypothesis.strategies
import pytest

from datastore import DataStore, SensorReadingQueue
from forwarding import ForwardingManager
from parquetmanager import DictBackend, ParquetManager
from publisher import Publisher
from tests.strats import sensor_data, sensor_data_with_recent_data, sensor_reading


def create_sensor_data():
    frame = sensor_data().example()
    return frame


def queue(maxlen: int) -> SensorReadingQueue:
    return SensorReadingQueue(maxlen=maxlen)


def filled_queue(maxlen: int = 10):
    full_queue = queue(maxlen)
    readings = hypothesis.strategies.data()
    for _ in range(maxlen):
        reading = readings.draw(sensor_reading())
        full_queue.add_reading(next(reading), lambda: ...)
    return full_queue


@pytest.fixture(scope="module")
def parquet_manager():
    manager = ParquetManager(DictBackend(Path("archive/")))
    data = sensor_data().example()
    manager.save_dataframe(data)
    return manager


@pytest.fixture(scope="module")
def parquet_manager_with_recent_data():
    manager = ParquetManager(DictBackend(Path("archive/")))
    data = sensor_data_with_recent_data().example()
    manager.save_dataframe(data)
    return manager


@pytest.fixture
def empty_parquet_manager():
    return ParquetManager(DictBackend(Path("archive/")))


@pytest.fixture
def read_only_datastore(parquet_manager: ParquetManager) -> DataStore:
    return DataStore(manager=parquet_manager, queue=queue(20))


def datastore(parquet_manager: ParquetManager, maxlen: int) -> DataStore:
    return DataStore(manager=parquet_manager, queue=queue(maxlen))


@pytest.fixture
def publisher():
    return Publisher()


@pytest.fixture
def forwarder():
    return ForwardingManager()
