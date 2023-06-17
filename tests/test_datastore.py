from pathlib import Path

import pandas as pd
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

import tests.conftest
from datastore import DataStore
from parquetmanager import DictBackend, ParquetManager
from sensordata import SensorData
from sensor import SensorReading
from tests.strats import MAX_TIMESTAMP, MIN_TIMESTAMP, sensor_reading


@given(
    timestamp=st.datetimes(
        min_value=MIN_TIMESTAMP,
        max_value=MAX_TIMESTAMP,
    )
)
@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_timestamp(timestamp, read_only_datastore: DataStore):
    archive = read_only_datastore.serialize_archive(
        start=timestamp, format=SensorData.Parquet
    )
    assert isinstance(archive, bytes)
    assert len(archive) > 0


@given(readings=st.lists(sensor_reading(), min_size=1))
@settings(suppress_health_check=[HealthCheck.large_base_example], deadline=None)
def test_added_data_is_saved_and_can_be_recalled(
    readings: list[SensorReading],
):
    parquet_manager = ParquetManager(DictBackend(Path("archive/")))
    datastore = tests.conftest.datastore(parquet_manager, 10)
    readings.sort(key=lambda x: pd.Timestamp(x.timestamp))
    for reading in readings:
        datastore.add_reading(reading)
    expected = SensorData.make_dataframe_from_list_of_readings(readings).sort_index()
    timestamp = expected.reset_index()["timestamp"].min()
    actual = datastore.get_archive_since(timestamp, None).sort_index()
    result = expected.equals(actual)
    assert result
