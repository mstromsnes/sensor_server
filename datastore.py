import pandas as pd
from pathlib import Path
from format import Format
import pandera as pa
from pandera.typing import DataFrame, Series, Index
import remotereader
import logging
from sensor import SensorReading, SensorData

log = logging.getLogger("datastore")


class DataStore:
    def __init__(self, *, parquet_file=None, proxy=None):
        self._dataframe = None
        self.parquet_file = parquet_file
        self.proxy = proxy
        self._create_pending_queue()

    def load_archive(self):
        if self.proxy is not None:
            self._dataframe = self._download_archive_from_proxy(self.proxy)
        elif self.parquet_file is not None:
            self._dataframe = self._load_dataframe_from_file(self.parquet_file)

    @pa.check_types
    def _download_archive_from_proxy(self, proxy) -> DataFrame[SensorData]:
        archive = remotereader.download_archive(proxy)
        return pd.read_parquet(archive)

    def archive_data(self, parquet_file: Path):
        self._update_dataframe()
        self._dataframe.to_parquet(parquet_file)

    def serialize_archive_since_timestamp(
        self, timestamp: pa.DateTime, format: Format = Format.Parquet
    ) -> bytes:
        self._update_dataframe()
        df = self._dataframe.loc[(slice(None), slice(None), slice(timestamp, None)), :]
        SensorData.validate(df)

        if format == Format.Parquet:
            return self._get_bytes_from_fastparquet(df)
        elif format == Format.JSON:
            return df.to_json(orient="table")

    def serialize_archive(self, format: Format = Format.Parquet):
        self._update_dataframe()
        if format == Format.Parquet:
            return self._get_bytes_from_fastparquet(self._dataframe)
        elif format == Format.JSON:
            return self._dataframe.to_json(orient="table")

    def add_reading(self, reading: SensorReading):
        self._write_reading_to_queue(reading)

    def tail(self):
        self._update_dataframe()
        return self._dataframe.tail()

    def _create_pending_queue(self):
        self._queue: list[SensorReading] = list()

    @pa.check_types
    def _load_dataframe_from_file(
        self, parquet_file: Path
    ) -> Optional[DataFrame[SensorData]]:
        try:
            return pd.read_parquet(parquet_file).sort_index()
        except Exception:
            logging.log(logging.DEBUG, f"{parquet_file=} failed to load", Exception)
            return None

    @pa.check_types
    def _load_dataframe_from_queue(self) -> DataFrame[SensorData]:
        df = pd.DataFrame(map(lambda r: r.dict(), self._queue))
        df = df.set_index(SensorReading._indexes, drop=True)
        return df

    @pa.check_types
    def _merge_queue_with_dataframe(self) -> DataFrame[SensorData]:
        new_dataframe = self._load_dataframe_from_queue()
        new_dataframe = pd.concat([self._dataframe, new_dataframe]).sort_index()
        return new_dataframe

    @pa.check_types
    def _create_empty_dataframe(self) -> DataFrame[SensorData]:
        return SensorData.example(size=0)

    def _write_reading_to_queue(self, reading: SensorReading):
        self._queue.append(reading)
        if len(self._queue) > 1000:
            self._update_dataframe()

    def _update_dataframe(self):
        if self._dataframe is None:
            self.load_archive()
        if self._queue:
            self._dataframe = self._merge_queue_with_dataframe()
            self._create_pending_queue()  # Clear out the buffer

    @staticmethod
    def _get_bytes_from_fastparquet(df: pd.DataFrame) -> bytes:
        """Fastparquet version 2023.4.0 closes the file/buffer itself after its done. For a file this is ok. For a buffer this clears out the buffer, deleting the data.
        When passing path=None to to_parquet() Pandas is supposed to return the bytes. But it does it the same way I tried. It uses a BytesIO buffer that gets closed,
        then tries to return the bytes from the buffer, which aren't there. Code from pandas own documentation doesn't work because of this bug.

        This is a workaround to prevent fastparquet from closing the buffer.
        If using pyarrow we don't need to do this, but pyarrow doesn't install on the RPi easily.
        """
        buffer = BytesIO()
        actual_close = buffer.close
        buffer.close = lambda: ...
        df.to_parquet(buffer)
        contents = buffer.getvalue()
        actual_close()
        return contents
