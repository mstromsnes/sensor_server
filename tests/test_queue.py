import pytest
from datastore import SensorReadingQueue
from sensor import SensorReading
from tests.conftest import reading_generator
import datetime


def test_filling_queue_calls_callback(queue: SensorReadingQueue):
    readings = reading_generator()
    callback_called = False

    def callback(*args):
        nonlocal callback_called
        callback_called = True

    for _ in range(10):
        queue.add_reading(next(readings), callback)

    assert callback_called


def test_filling_queue_callback_has_queue_content(queue: SensorReadingQueue):
    readings = reading_generator()
    expected = [next(readings) for _ in range(10)]
    received = None

    def callback(got):
        nonlocal received
        received = got

    [queue.add_reading(reading, callback) for reading in expected]

    assert expected == received
