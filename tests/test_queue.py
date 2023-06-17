from functools import partial

import hypothesis.strategies as st
from hypothesis import given
from pandas import Timestamp

from datastore import SensorReadingQueue
from sensordata import SensorData
from sensor import SensorReading
from tests.strats import sensor_reading

from .conftest import queue


@given(
    readings=st.data(),
    queue=st.builds(partial(queue, 10)),
)
def test_filling_queue_calls_callback(
    readings: st.DataObject, queue: SensorReadingQueue
):
    callback_called = False

    def callback(*_):
        nonlocal callback_called
        callback_called = True

    queue.register_on_full_callback(callback)
    for _ in range(10):
        queue.add_reading(readings.draw(sensor_reading()))

    assert callback_called


@given(
    readings=st.data(),
    queue=st.builds(partial(queue, 10)),
)
def test_filling_queue_callback_has_queue_content(
    readings: st.DataObject, queue: SensorReadingQueue
):
    expected = [readings.draw(sensor_reading()) for _ in range(10)]
    received = None

    def callback(got):
        nonlocal received
        received = got

    queue.register_on_full_callback(callback)
    [queue.add_reading(reading) for reading in expected]

    assert expected == received


@given(
    readings=st.lists(sensor_reading(), min_size=5),
    queue=st.builds(partial(queue, 10)),
)
def test_queue_get_data_in_interval(
    readings: list[SensorReading], queue: SensorReadingQueue
):
    queue.register_on_full_callback(lambda x: ...)
    readings.sort(key=lambda x: Timestamp(x.timestamp))
    [queue.add_reading(reading) for reading in readings]
    start_timestamp = Timestamp(readings[-4].timestamp)
    end_timestamp = Timestamp(readings[-2].timestamp)
    dataframe = SensorData.make_dataframe_from_list_of_readings(readings)
    idx = (slice(None), slice(None), slice(start_timestamp, end_timestamp))
    expected = dataframe.sort_index().loc[idx, :]
    actual = queue.get_data_in_interval(
        start=start_timestamp, end=end_timestamp
    ).sort_index()
    assert expected.equals(actual)
