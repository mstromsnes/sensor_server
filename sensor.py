from enum import Enum
from pydantic import BaseModel
from datetime import datetime

class Sensor(Enum):
    DHT11 = "DHT11"
    PITEMP = "PI_CPU"
    DS18B20 = "DS18B20"

class SensorType(Enum):
    Temperature = "temperature"
    Humidity = "humidity"

class Unit(Enum):
    Celsius = "C"
    RelativeHumidity = "%"

class SensorReading(BaseModel):
    timestamp: datetime
    reading: float
    sensor_type: SensorType
    sensor: Sensor
    unit: Unit
