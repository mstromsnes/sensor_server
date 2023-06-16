from io import BytesIO
from pathlib import Path
from typing import Union

import pandas as pd
import pandera as pa
from pandera.typing import Index, Series

from format import JSONFormat, ParquetFormat, SerializedDataFrame, make_type_error
from sensor import Sensor, SensorReading, SensorType, Unit, make_dtype_kwargs


class ParquetFormatSensorData(ParquetFormat):
    @staticmethod
    def serialize(df: pd.DataFrame) -> bytes:
        """Fastparquet version 2023.4.0 closes the file/buffer itself after its done. For a file this is ok. For a buffer this clears out the buffer, deleting the data.
        When passing path=None to to_parquet() Pandas is supposed to return the bytes. But it does it the same way I tried. It uses a BytesIO buffer that gets closed,
        then tries to return the bytes from the buffer, which aren't there. Code from pandas own documentation doesn't work because of this bug.

        This is a workaround to prevent fastparquet from closing the buffer.
        If using pyarrow we don't need to do this, but pyarrow doesn't install on the RPi easily.
        """
        buffer = BytesIO()
        actual_close = buffer.close
        buffer.close = lambda: None
        df = df.reset_index()
        df.to_parquet(buffer)
        contents = buffer.getvalue()
        actual_close()
        return contents

    @staticmethod
    def write(df: pd.DataFrame, path: Path) -> None:
        df = df.reset_index()
        df.to_parquet(path)

    @staticmethod
    def load(archive: Union[SerializedDataFrame, Path]) -> pd.DataFrame:
        if isinstance(archive, BytesIO):
            pass
        elif isinstance(archive, Path):
            pass
        elif isinstance(archive, bytes):
            archive = BytesIO(archive)
        elif isinstance(archive, str):
            raise make_type_error("str", "parquet")
        else:
            raise TypeError("Unsupported type")
        df = pd.read_parquet(archive)
        return SensorData.repair_dataframe(df)

    @staticmethod
    def endpoint() -> str:
        return "parquet/"


class JSONFormatSensorData(JSONFormat):
    @staticmethod
    def serialize(df: pd.DataFrame) -> str:
        return df.to_json(orient="table")

    @staticmethod
    def write(df: pd.DataFrame, path: Path) -> None:
        df.to_json(path, orient="table")

    @staticmethod
    def load(archive: Union[SerializedDataFrame, Path]) -> pd.DataFrame:
        if isinstance(archive, str):
            pass
        elif isinstance(archive, Path):
            pass
        elif isinstance(archive, bytes):
            raise make_type_error("bytes", "JSON")
        elif isinstance(archive, BytesIO):
            raise make_type_error("BytesIO", "JSON")
        else:
            raise TypeError("Unsupported type")
        return pd.read_json(archive, orient="table")

    @staticmethod
    def endpoint() -> str:
        return "json/"


class SensorDataModel(pa.DataFrameModel):
    sensor_type: Index[pd.CategoricalDtype] = pa.Field(
        dtype_kwargs=make_dtype_kwargs(SensorType), isin=SensorType.values()
    )
    sensor: Index[pd.CategoricalDtype] = pa.Field(
        dtype_kwargs=make_dtype_kwargs(Sensor), isin=Sensor.values()
    )
    timestamp: Index[pa.DateTime]

    reading: Series[float]
    unit: Series[pd.CategoricalDtype] = pa.Field(
        dtype_kwargs=make_dtype_kwargs(Unit), isin=Unit.values()
    )


class SensorData:
    Model = SensorDataModel

    Parquet = ParquetFormatSensorData
    JSON = JSONFormatSensorData

    @classmethod
    def repair_dataframe(cls, df) -> pd.DataFrame:
        """The dataframe multiindex doesn't save correctly in parquet. The categorical types are dropped. To fix this, the index is first reset before saving, making them regular columns.
        The columns do preserve the categorical types."""
        df = cls._convert_columns(df)
        df = df.set_index(SensorReading._indexes, drop=True)
        return df

    @staticmethod
    def _convert_columns(df: pd.DataFrame) -> pd.DataFrame:
        """Convert columns to appropriate dtypes."""
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df["sensor_type"] = pd.Categorical(
            df["sensor_type"], **make_dtype_kwargs(SensorType)
        )
        df["sensor"] = pd.Categorical(df["sensor"], **make_dtype_kwargs(Sensor))
        df["unit"] = pd.Categorical(df["unit"], **make_dtype_kwargs(Unit))
        return df

    @classmethod
    def make_dataframe_from_list_of_readings(
        cls,
        readings: list[SensorReading],
    ) -> pd.DataFrame:
        if readings:
            df = pd.DataFrame(map(lambda r: r.dict(), readings))
            df = cls.repair_dataframe(df)
        else:
            df = SensorData.construct_empty_dataframe()
        return df

    @classmethod
    def construct_empty_dataframe(cls) -> pd.DataFrame:
        dict_base = {key: [] for key in cls.Model._collect_fields().keys()}
        df = pd.DataFrame(dict_base)
        df = cls._convert_columns(df)
        df = df.set_index(SensorReading._indexes, drop=True)
        return df
