from format import Format
from datastore import DataStore
from sensor import SensorReading
import datetime
import pandas as pd


def test_timestamp(datastore: DataStore, archive_timestamp):
    timedelta = pd.Timedelta(seconds=60)
    archive = datastore.serialize_archive(
        timestamp=archive_timestamp + timedelta, format=Format.Parquet
    )
    assert len(archive) > 0


def test_adding_data(datastore: DataStore):
    for i in range(10000):
        reading = SensorReading(
            sensor_type="temperature",
            sensor="DHT11",
            timestamp=str(datetime.datetime.now()),
            reading=20.0,
            unit="C",
        )
        datastore.add_reading(reading)
