import datetime
from pathlib import Path

import pandas as pd
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

import tests.conftest
from format import Format
from parquetmanager import DictBackend, ParquetManager
from sensor import SensorData
from tests.strats import sensor_reading, MIN_TIMESTAMP, MAX_TIMESTAMP


@given(
    timestamp=st.datetimes(
        min_value=MIN_TIMESTAMP,
        max_value=MAX_TIMESTAMP,
    )
)
@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_timestamp(timestamp, read_only_datastore):
    timedelta = pd.Timedelta(seconds=60)
    archive = read_only_datastore.serialize_archive(
        timestamp=timestamp + timedelta, format=Format.Parquet
    )
    assert len(archive) > 0


@given(readings=st.data())
@settings(suppress_health_check=[HealthCheck.large_base_example], deadline=None)
def test_added_data_is_saved_and_can_be_recalled(
    readings: st.DataObject,
):
    parquet_manager = ParquetManager(DictBackend(Path("archive/")))
    datastore = tests.conftest.datastore(parquet_manager, 10)
    readings_to_add = [readings.draw(sensor_reading()) for _ in range(20)]
    readings_to_add.sort(key=lambda x: pd.Timestamp(x.timestamp))
    for reading in readings_to_add:
        datastore.add_reading(reading)
    expected = SensorData.make_dataframe_from_list_of_readings(
        readings_to_add
    ).sort_index()
    timestamp = expected.reset_index().timestamp.min()
    actual = datastore.get_archive_since(timestamp).sort_index()
    result = expected.equals(actual)
    assert result
