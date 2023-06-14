from datastore import SensorReadingQueue
from .conftest import queue
from tests.strats import sensor_reading
from hypothesis import given
import hypothesis.strategies as st
from functools import partial


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
