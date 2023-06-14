from enum import Enum
from typing import Sequence

import pandas as pd
import pandera as pa
from pandera.typing import Index, Series
from pydantic import BaseModel, validator

SENSOR_COMBINATIONS = [
    ("temperature", "DHT11"),
    ("temperature", "DS18B20"),
    ("temperature", "PI_CPU"),
    ("humidity", "DHT11"),
]


class MemberStrEnum(Enum):
    """A workaround to get valid str values from the enum. Python 3.12 will allow us to test for values directly"""

    @classmethod
    def values(cls) -> list[str]:
        return [member.value for member in cls]


class Sensor(MemberStrEnum):
    DHT11 = "DHT11"
    PITEMP = "PI_CPU"
    DS18B20 = "DS18B20"


class SensorType(MemberStrEnum):
    Temperature = "temperature"
    Humidity = "humidity"


class Unit(MemberStrEnum):
    Celsius = "C"
    RelativeHumidity = "%"


class SensorReading(BaseModel):
    sensor_type: str
    sensor: str
    timestamp: str

    reading: float
    unit: str

    @validator("sensor")
    @classmethod
    def is_in_sensor(cls, v):
        if v not in Sensor.values():
            raise ValueError("Not a legitimate Sensor value")
        return v

    @validator("unit")
    @classmethod
    def is_in_unit(cls, v):
        if v not in Unit.values():
            raise ValueError("Not a legitimate Unit value")
        return v

    @validator("sensor_type")
    @classmethod
    def is_in_sensor_type(cls, v):
        if v not in SensorType.values():
            raise ValueError("Not a legitimate SensorType value")
        return v

    _indexes = ["sensor_type", "sensor", "timestamp"]
    _columns = ["reading", "unit"]

    def index(self):
        t = tuple(value for key, value in self if key in self._indexes)
        n = tuple(key for key, value in self if key in self._indexes)
        return pd.MultiIndex.from_tuples([t], names=n)

    def columns(self):
        return {key: value for key, value in self if key not in self._indexes}


def make_dtype_kwargs(enum):
    return {"categories": enum.values(), "ordered": False}


class SensorData(pa.DataFrameModel):
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

    @classmethod
    def repair_dataframe(cls, df: pd.DataFrame) -> pd.DataFrame:
        """The dataframe multiindex doesn't save correctly in parquet. The categorical types are dropped. To fix this, the index is first reset before saving, making them regular columns.
        The columns do preserve the categorical types."""
        df = df.reset_index()
        try:
            df = df.drop("index", axis=1)
        except KeyError:
            pass
        df = df.drop_duplicates(["sensor_type", "sensor", "timestamp"])
        df = cls._convert_columns(df)
        df = df.set_index(SensorReading._indexes, drop=True)
        return df

    @staticmethod
    def _convert_columns(df: pd.DataFrame) -> pd.DataFrame:
        df["timestamp"] = pd.to_datetime(df["timestamp"], format="%Y-%m-%d %H:%M:%S.%f")
        df["sensor_type"] = pd.Categorical(
            df["sensor_type"], **make_dtype_kwargs(SensorType)
        )
        df["sensor"] = pd.Categorical(df["sensor"], **make_dtype_kwargs(Sensor))
        df["unit"] = pd.Categorical(df["unit"], **make_dtype_kwargs(Unit))
        return df

    @classmethod
    def make_dataframe_from_list_of_readings(
        cls,
        readings: Sequence[SensorReading],
    ) -> pd.DataFrame:
        if readings:
            df = pd.DataFrame(map(lambda r: r.dict(), readings))
            df = cls.repair_dataframe(df)
        else:
            df = SensorData.construct_empty_dataframe()
        return df

    @classmethod
    def construct_empty_dataframe(cls) -> pd.DataFrame:
        dict_base = {key: [] for key in cls._collect_fields().keys()}
        df = pd.DataFrame(dict_base)
        df = cls._convert_columns(df)
        df = df.set_index(SensorReading._indexes, drop=True)
        return df
