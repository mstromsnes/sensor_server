import pandas as pd
from pathlib import Path
from typing import Union
import pandera as pa
from pandera.typing import DataFrame, Series, Index
import logging
from sensor import Sensor, SensorType, Unit, SensorReading


class SensorData(pa.DataFrameModel):
    timestamp: Index[pa.DateTime] = pa.Field(check_name=True)
    sensor: Index[str] = pa.Field(is_in_enum=Sensor)
    sensor_type: Index[str] = pa.Field(
        is_in_enum=SensorType,
    )

    reading: Series[float]
    unit: Series[str] = pa.Field(is_in_enum=Unit)


class DataStore:
    def __init__(self, parquet_file: Union[Path, None] = None):
        if parquet_file is not None:
            self._dataframe = self._load_dataframe_from_parquet(parquet_file)
        else:
            self._dataframe = self._create_empty_dataframe()
        self._create_pending_queue()

    def _create_pending_queue(self):
        self._queue: list[SensorReading] = list()

    @pa.check_types
    def _load_dataframe_from_parquet(self, parquet_file: Path) -> DataFrame[SensorData]:
        try:
            return pd.read_parquet(parquet_file)
        except Exception:
            logging.log(logging.DEBUG, f"{parquet_file=} failed to load", Exception)
            return self._create_empty_dataframe()

    @pa.check_types
    def _load_dataframe_from_queue(self) -> DataFrame[SensorData]:
        df = pd.DataFrame(map(lambda r: r.dict(), self._queue))
        df = df.set_index(SensorReading._indexes, drop=True)
        return df

    @pa.check_types
    def _merge_queue_with_dataframe(self) -> DataFrame[SensorData]:
        new_dataframe = self._load_dataframe_from_queue()
        return pd.concat([self._dataframe, new_dataframe])

    @pa.check_types
    def _create_empty_dataframe(self) -> DataFrame[SensorData]:
        return SensorData.example(size=0)

    def _write_reading_to_queue(self, reading: SensorReading):
        self._queue.append(reading)

    def add_reading(self, reading: SensorReading):
        self._write_reading_to_queue(reading)

    def _update_dataframe(self):
        if self._queue:
            self._dataframe = self._merge_queue_with_dataframe()
            SensorData.validate(self._dataframe)
            self._create_pending_queue()  # Clear out the buffer

    def tail(self):
        self._update_dataframe()
        return self._dataframe.tail()
