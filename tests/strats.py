import pandas as pd
from hypothesis import strategies as st

from parquetmanager import start_of_week_timestamp
from sensor import SENSOR_COMBINATIONS, SensorReading
from sensordata import SensorData

MIN_TIMESTAMP = pd.Timestamp(year=2023, month=5, day=17)
MAX_TIMESTAMP = pd.Timestamp(year=2024, month=5, day=17)


@st.composite
def sensor_reading(draw):
    """A hypothesis strategy to generate valid SensorReading objects between 17th May 2023 to 17th May 2024"""

    def gen_unit(sensor_type: str):
        if sensor_type == "temperature":
            return "C"
        if sensor_type == "humidity":
            return "%"
        raise KeyError(f"Invalid {sensor_type=}")

    def gen_reading(draw, sensor_type: str, unit: str):
        if sensor_type == "temperature" and unit == "C":
            min_value = -273.15
            max_value = 1000.0
        if sensor_type == "humidity" and unit == "%":
            min_value = 0.0
            max_value = 100.0
        return draw(st.floats(min_value, max_value))

    sensor_type, sensor = draw(st.sampled_from(SENSOR_COMBINATIONS))
    timestamp: pd.Timestamp = draw(
        st.datetimes(
            min_value=MIN_TIMESTAMP,
            max_value=MAX_TIMESTAMP,
        )
    )
    unit = gen_unit(sensor_type)

    reading = gen_reading(draw, sensor_type, unit)
    return SensorReading(
        sensor_type=sensor_type,
        sensor=sensor,
        timestamp=timestamp.strftime("%Y-%m-%d %H:%M:%S.%f"),
        reading=reading,
        unit=unit,
    )


@st.composite
def sensor_data(draw: st.DrawFn):
    list_of_readings = [draw(sensor_reading()) for _ in range(30)]
    return SensorData.make_dataframe_from_list_of_readings(
        list_of_readings
    ).sort_index()


@st.composite
def sensor_data_with_recent_data(draw: st.DrawFn):
    def has_recent_data(df: pd.DataFrame):
        start_of_week = df.reset_index()["timestamp"].apply(start_of_week_timestamp)
        current_time = pd.Timestamp.now()
        return (start_of_week == start_of_week_timestamp(current_time)).any() or (
            start_of_week
            == start_of_week_timestamp(current_time - pd.Timedelta(days=7))
        ).any()

    frame = draw(sensor_data().filter(has_recent_data))
    return frame
