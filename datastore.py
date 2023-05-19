import pandas as pd
from pathlib import Path
from typing import Optional
from format import Format
import pandera as pa
from pandera.errors import SchemaError
import remotereader
import logging
from sensor import SensorReading, SensorData

log = logging.getLogger("datastore")


class DataStore:
    def __init__(self, *, parquet_file=None, proxy=None):
        self._dataframe = SensorData.construct_empty_dataframe()
        self.parquet_file = parquet_file
        self.proxy = proxy
        self._create_pending_queue()

    def _can_load_dataframe(self) -> bool:
        return self.proxy is not None or self.parquet_file is not None

    @property
    @pa.check_types
    def dataframe(self) -> pd.DataFrame:
        if self._dataframe.size == 0 and self._can_load_dataframe():
            self._dataframe = self._load_archive()
        self._dataframe = self._merge_queue_with_dataframe(self._dataframe, self._queue)
        self._clear_pending_queue()
        return self._dataframe

    def archive_data(self, parquet_file: Path):
        Format.Parquet.write(self.dataframe, parquet_file)

    def serialize_archive(
        self,
        *,
        timestamp: Optional[pd.Timestamp] = None,
        format: Format = Format.Parquet,
    ) -> bytes:
        # Slices to all entries of the highest level index, SensorType, all entries of the next level index, Sensor and then to all values at timestamp and later
        # If timestamp is None, this just slices to the whole dataframe, so the behaviour doesn't branch whether we provide a timestamp or not
        if timestamp is not None:
            idx = (slice(None), slice(None), slice(timestamp, None))
            df = self.dataframe.loc[idx, :]
        else:
            df = self.dataframe
        return format.serialize(df)

    def add_reading(self, reading: SensorReading):
        self._write_reading_to_queue(reading)

    def tail(self) -> pd.DataFrame:
        return self.dataframe.tail()

    def _create_pending_queue(self):
        """Create the queue of readings that will be added to the dataframe in the next update"""
        self._queue: list[SensorReading] = list()

    def _clear_pending_queue(self):
        """Clear the queue of readings that will be added to the dataframe in the next update"""
        self._create_pending_queue()

    def _load_archive(self) -> pd.DataFrame:
        if self.proxy is not None:
            return self._download_archive_from_proxy(self.proxy)
        elif self.parquet_file is not None:
            return self._load_dataframe_from_file(self.parquet_file)
        else:
            return SensorData.construct_empty_dataframe()

    def _load_dataframe_from_file(self, parquet_file: Path) -> pd.DataFrame:
        try:
            return Format.Parquet.load(parquet_file).sort_index()
        except Exception as err:
            log.log(logging.DEBUG, f"{parquet_file=} failed to load", err)
            return SensorData.construct_empty_dataframe()

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

    def _write_reading_to_queue(self, reading: SensorReading):
        self._queue.append(reading)
        if len(self._queue) > 1000:
            # Force a write
            self.dataframe
