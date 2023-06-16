from enum import Enum

import pandas as pd
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


def make_dtype_kwargs(enum: MemberStrEnum):
    return {"categories": enum.values(), "ordered": False}


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
