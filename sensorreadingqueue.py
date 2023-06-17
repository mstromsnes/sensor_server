import itertools
from collections import deque
from typing import Callable, Optional

import pandas as pd

from sensor import SensorReading
from sensordata import SensorData


class SensorReadingQueue:
    """A queue that handles the incoming data before it's appended to the dataframe.

    Appending to the dataframe is expensive, and we want to avoid doing it. A common usecase is to request the most recent data, which should be in the queue.
    This implements this queue as a deque, which will fill up to maxlen size, then call the callback that should merge the queue into the dataframe.

    The queue stays at maxlen size, adding new data to the front of the queue and popping old data off the back. Every maxlen readings added it calls the callback to
    merge the queue with the dataframe. As it is never destroyed and reconstructed, the latest data will always be available, and can be requested from here
    rather than concatenating and indexing into the large dataframe."""

    def __init__(self, maxlen):
        self._maxlen = maxlen
        self._queue: deque[SensorReading] = deque(maxlen=maxlen)
        self._counter = 0

    def register_on_full_callback(self, callback: Callable):
        self._on_full_callback = callback

    def add_reading(self, reading: SensorReading):
        self._queue.append(reading)
        self._counter += 1
        if self._counter == self._maxlen:
            self._on_full_callback(self.get_unsynced_queue())
            self._counter = 0

    def contains(self, timestamp: pd.Timestamp):
        return self._queue and timestamp >= pd.Timestamp(self._queue[0].timestamp)

    def get_since_timestamp(self, timestamp: pd.Timestamp) -> pd.DataFrame:
        if not self.contains(timestamp):
            return SensorData.construct_empty_dataframe()
        it = reversed(self._queue)
        for i, reading in enumerate(it):
            if timestamp <= pd.Timestamp(reading.timestamp):
                continue
        list_of_readings = list(
            itertools.islice(self._queue, -i - 1 + len(self._queue), len(self._queue))
        )
        return SensorData.make_dataframe_from_list_of_readings(list_of_readings)

    def get_unsynced_queue(self):
        if len(self._queue) < self._maxlen:
            return list(itertools.islice(self._queue, 0, self._counter))
        else:
            return list(
                itertools.islice(
                    self._queue, self._maxlen - self._counter, self._maxlen
                )
            )

    def reset_counter(self):
        if len(self._queue) < self._maxlen:
            self._counter = 0
        else:
            self._counter = len(self._queue)
