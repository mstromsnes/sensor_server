from format import Format
from datastore import DataStore
from sensor import SensorReading, SensorData
import datetime
import pandas as pd
from .conftest import reading_generator


def test_timestamp(datastore: DataStore, archive_timestamp):
    timedelta = pd.Timedelta(seconds=60)
    archive = datastore.serialize_archive(
        timestamp=archive_timestamp + timedelta, format=Format.Parquet
    )
    assert len(archive) > 0


def test_adding_data(datastore: DataStore):
    readings = reading_generator()
    readings_to_add = [next(readings) for _ in range(200)]
    for reading in readings_to_add:
        datastore.add_reading(reading)
    timestamp = pd.Timestamp(readings_to_add[0].timestamp)
    expected = SensorData.make_dataframe_from_list_of_readings(readings_to_add)
    actual = datastore.get_archive_since(timestamp)
    assert expected.equals(actual)
