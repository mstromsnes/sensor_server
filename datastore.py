import logging
from typing import Optional, Sequence

import pandas as pd
import pandera as pa
from pandera.errors import SchemaError

import remotereader
from format import SerializationFormat, SerializedDataFrame
from parquetmanager import ParquetManager
from sensor import SensorReading
from sensordata import SensorData
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
        start: Optional[pd.Timestamp] = None,
        end: Optional[pd.Timestamp] = None,
        format: SerializationFormat = SensorData.Parquet,
    ) -> SerializedDataFrame:
        """If no timestamp is provided, returns all data currently in memory, as decided by ParquetManager or Proxy.
        If a timestamp is provided, provides all data from the time given to now, possibly loading more data from storage if necessary.
        """
        if start is None and end is None:
            return format.serialize(self.dataframe)
        df = self.get_archive_since(start, end)
        return format.serialize(df)

    def get_archive_since(
        self, start: Optional[pd.Timestamp], end: Optional[pd.Timestamp]
    ) -> pd.DataFrame:
        # Slices to all entries of the highest level index; SensorType, all entries of the next level index; Sensor, and then to all values at timestamp and later
        quick_df = self._get_fast_data(start, end)
        in_memory_df = self._get_in_memory_data(start, end)
        slow_df = self._get_archived_data(start, end)
        df = self._merge_archives([slow_df, in_memory_df, quick_df])
        return df

    def _get_fast_data(
        self, start: Optional[pd.Timestamp], end: Optional[pd.Timestamp]
    ) -> pd.DataFrame:
        return self._queue.get_data_in_interval(start, end)

    def _get_in_memory_data(
        self, start: Optional[pd.Timestamp], end: Optional[pd.Timestamp]
    ) -> pd.DataFrame:
            return SensorData.construct_empty_dataframe()
        idx = (slice(None), slice(None), slice(start, end))
        return self._dataframe.loc[idx, :]

    def _get_archived_data(
        self, start: Optional[pd.Timestamp], end: Optional[pd.Timestamp]
    ) -> pd.DataFrame:
        if (
            self.parquet_manager is not None
            and timestamp < ParquetManager.earliest_date()
        ):
            return self.parquet_manager.get_historic_data(start, end)
        return SensorData.construct_empty_dataframe()

    def _merge_archives(self, dataframes: list[pd.DataFrame]) -> pd.DataFrame:
        frame_map = map(lambda df: df.reset_index(), dataframes)
        return SensorData.repair_dataframe(pd.concat(frame_map))

    def add_reading(self, reading: SensorReading):
        self._write_reading_to_queue(reading)

    def tail(self) -> pd.DataFrame:
        return self.dataframe.tail()

    def _clear_pending_queue(self):
        """Clear the queue of readings that will be added to the dataframe in the next update"""
        self._queue.reset_counter()

    def _load_archive(self) -> pd.DataFrame:
        if self.proxy is not None:
            return self._download_archive_from_proxy(
                self.proxy, "archive/parquet/", SensorData.Parquet
            )
        elif self.parquet_manager is not None:
            return self._load_dataframe_from_file()
        else:
            return SensorData.construct_empty_dataframe()

    def _load_dataframe_from_file(self) -> pd.DataFrame:
        return self.parquet_manager.load_dataframe().sort_index()

    def _download_archive_from_proxy(
        self, proxy: str, endpoint: str, format: SerializationFormat
    ) -> pd.DataFrame:
        url = proxy + endpoint
        archive = remotereader.download_archive(url, format)
        return format.load(archive)

    @staticmethod
    def _load_dataframe_from_queue(queue) -> pd.DataFrame:
        return SensorData.make_dataframe_from_list_of_readings(queue)

    @classmethod
    def _merge_queue_with_dataframe(cls, old_dataframe, queue) -> pd.DataFrame:
        new_dataframe = cls._load_dataframe_from_queue(queue)
        if new_dataframe.empty:
            return old_dataframe
        try:
            SensorData.Model.validate(new_dataframe)
        except SchemaError:
            new_dataframe = SensorData.construct_empty_dataframe()
            log.error("Invalid Dataframe constructed from queue", exc_info=True)
            log.error(f"{queue=}")
        return pd.concat([old_dataframe, new_dataframe]).sort_index()

    def _update_from_queue(self, queue: Sequence[SensorReading]):
        self._dataframe = self._merge_queue_with_dataframe(self._dataframe, queue)

    def _write_reading_to_queue(self, reading: SensorReading):
        self._queue.add_reading(reading)
