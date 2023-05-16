import pandera as pa
import pandas as pd
from sensor import SensorReading
from datastore import SensorData

now = pd.Timestamp.now()
delta = pd.Timedelta(seconds=1)


def generate_from_delta(num_delta: int):
    return [
        SensorReading(
            sensor_type="temperature",
            sensor="PI_CPU",
            timestamp=(now + delta * num_delta).isoformat(),
            reading=34.0,
            unit="C",
        ),
        SensorReading(
            sensor_type="temperature",
            sensor="DHT11",
            timestamp=(now + delta * num_delta).isoformat(),
            reading=34.0,
            unit="C",
        ),
        SensorReading(
            sensor_type="temperature",
            sensor="DS18B20",
            timestamp=(now + delta * num_delta).isoformat(),
            reading=34.0,
            unit="C",
        ),
        SensorReading(
            sensor_type="humidity",
            sensor="DHT11",
            timestamp=(now + delta * num_delta).isoformat(),
            reading=34.0,
            unit="%",
        ),
    ]


readings: list[SensorReading] = [
    reading for num in range(360) for reading in generate_from_delta(num)
]

df = SensorData.make_dataframe_from_list_of_readings(readings)
SensorData.validate(df)
df.to_parquet("archive/test.parquet")
