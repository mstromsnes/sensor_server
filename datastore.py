import logging
from typing import Optional, Sequence

import pandas as pd
import pandera as pa
from pandera.errors import SchemaError

import remotereader
from format import Format
from parquetmanager import ParquetManager
from sensor import SensorData, SensorReading
from sensorreadingqueue import SensorReadingQueue

log = logging.getLogger("datastore")


class DataStore:
    def __init__(
        self,
        *,
        manager=ParquetManager(),
        proxy=None,
        queue=SensorReadingQueue(maxlen=10000),
    ):
        self.parquet_manager = manager
        self.proxy = proxy
        self._dataframe = self._load_archive()
        self._reload_dataframe = False
        self._queue = queue
        self._queue.register_on_full_callback(self._update_from_queue)

    @property
    @pa.check_types
    def dataframe(self) -> pd.DataFrame:
        if self._reload_dataframe or self._dataframe.size == 0:
            self._dataframe = self._load_archive()
            self._reload_dataframe = False
        self._dataframe = self._merge_queue_with_dataframe(
            self._dataframe, self._queue.get_unsynced_queue()
        )
        self._clear_pending_queue()
        return self._dataframe

    def archive_data(self):
        self.parquet_manager.save_dataframe(self.dataframe)
        self._reload_dataframe = True

    def serialize_archive(
        self,
        *,
        timestamp: Optional[pd.Timestamp] = None,
        format: Format = Format.Parquet,
    ) -> bytes:
        """If no timestamp is provided, returns all data currently in memory, as decided by ParquetManager or Proxy.
        If a timestamp is provided, provides all data from the time given to now, possibly loading more data from storage if necessary.
        """
        if timestamp is None:
            return format.serialize(self.dataframe)
        df = self.get_archive_since(timestamp)
        return format.serialize(df)

    def get_archive_since(self, timestamp: Optional[pd.Timestamp]):
        # Slices to all entries of the highest level index; SensorType, all entries of the next level index; Sensor, and then to all values at timestamp and later
        idx = (slice(None), slice(None), slice(timestamp, None))
        try:
            df = self._queue.get_since_timestamp(timestamp)
        except KeyError:
            if (
                self.parquet_manager is not None
                and timestamp < ParquetManager.earliest_date()
            ):
                df = self.parquet_manager.get_historic_data(timestamp, None)
                df = SensorData.repair_dataframe(df).sort_index()
            else:
                df = self.dataframe
            df = df.loc[idx, :]
        return df

    def add_reading(self, reading: SensorReading):
        self._write_reading_to_queue(reading)

    def tail(self) -> pd.DataFrame:
        return self.dataframe.tail()

    def _clear_pending_queue(self):
        """Clear the queue of readings that will be added to the dataframe in the next update"""
        self._queue.reset_counter()

    def _load_archive(self) -> pd.DataFrame:
        if self.proxy is not None:
            return self._download_archive_from_proxy(self.proxy)
        elif self.parquet_manager is not None:
            return self._load_dataframe_from_file()
        else:
            return SensorData.construct_empty_dataframe()

    def _load_dataframe_from_file(self) -> pd.DataFrame:
        return self.parquet_manager.load_dataframe().sort_index()

    def _download_archive_from_proxy(self, proxy) -> pd.DataFrame:
        archive, format = remotereader.download_archive(proxy)
        return format.load(archive)

    @staticmethod
    def _load_dataframe_from_queue(queue) -> pd.DataFrame:
        return SensorData.make_dataframe_from_list_of_readings(queue)

    @classmethod
    def _merge_queue_with_dataframe(cls, old_dataframe, queue) -> pd.DataFrame:
        new_dataframe = cls._load_dataframe_from_queue(queue)
        if new_dataframe.size == 0:
            return old_dataframe
        try:
            SensorData.validate(new_dataframe)
        except SchemaError:
            new_dataframe = SensorData.construct_empty_dataframe()
            log.error("Invalid Dataframe constructed from queue", exc_info=True)
            log.error(f"{queue=}")
        return pd.concat([old_dataframe, new_dataframe]).sort_index()

    def _update_from_queue(self, queue: Sequence[SensorReading]):
        self._dataframe = self._merge_queue_with_dataframe(self._dataframe, queue)

    def _write_reading_to_queue(self, reading: SensorReading):
        self._queue.add_reading(reading)
