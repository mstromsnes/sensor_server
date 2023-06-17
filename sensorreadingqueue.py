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

    def might_contain_data_newer_than_timestamp(
        self, timestamp: Optional[pd.Timestamp]
    ):
        return self._queue and (
            timestamp is None or pd.Timestamp(self._queue[-1].timestamp) >= timestamp
        )

    def might_contain_data_older_than_timestamp(
        self, timestamp: Optional[pd.Timestamp]
    ):
        return self._queue and (
            timestamp is None or timestamp >= pd.Timestamp(self._queue[0].timestamp)
        )

    def get_data_in_interval(
        self, start: Optional[pd.Timestamp], end: Optional[pd.Timestamp]
    ) -> pd.DataFrame:
        # if there isn't any data more recent than start, we can just return empty
        # if there isn't any data older than end, we can just return empty
        if not self.might_contain_data_newer_than_timestamp(
            start
        ) or not self.might_contain_data_older_than_timestamp(end):
            return SensorData.construct_empty_dataframe()
        if start is None:
            start_idx = 0
        else:
            start_idx = self._find_first_entry_older_than_timestamp(start)
        if end is None:
            end_idx = len(self._queue)
        else:
            end_idx = self._find_last_entry_newer_than_timestamp(end)
        list_of_readings = list(itertools.islice(self._queue, start_idx, end_idx))
        return SensorData.make_dataframe_from_list_of_readings(list_of_readings)

    def _find_first_entry_older_than_timestamp(self, timestamp: pd.Timestamp):
        # Most lookups will be for data near the end of the queue, so start looking from the end
        it = reversed(self._queue)
        for i, reading in enumerate(it):
            # if the newest timestamp is more recent than the one we're looking for, keep going backwards into the list
            if timestamp <= pd.Timestamp(reading.timestamp):
                continue
            # Once we find a timestamp older than the one we're looking for, stop and return the index as if we looked from 0.
            return len(self._queue) - i
        # If we never find a timestamp older than the one we're looking for, return 0 indicating the whole queue is newer than what we're looking for
        return 0

    def _find_last_entry_newer_than_timestamp(self, timestamp: pd.Timestamp):
        for i, reading in enumerate(self._queue):
            if pd.Timestamp(reading.timestamp) <= timestamp:
                continue
            return i
        return len(self._queue)

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
