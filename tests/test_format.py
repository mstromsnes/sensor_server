from hypothesis import given
from tests.strats import sensor_data
from sensordata import SensorData
from format import SerializedDataFrame


@given(dataframe=sensor_data())
def test_parquet_serialization(dataframe):
    parquet_bytes = SensorData.Parquet.serialize(dataframe)
    assert len(parquet_bytes) > 0


# def test_parquet_deserialization(serialized_parquet_frame: SerializedDataFrame):
#     frame = SensorData.Parquet.load(serialized_parquet_frame)
#     assert frame.size > 0


@given(dataframe=sensor_data())
def test_json_serialization(dataframe):
    json_str = SensorData.JSON.serialize(dataframe)
    assert len(json_str) > 0


# def test_json_deserialization(serialized_json_frame: SerializedDataFrame):
#     assert isinstance(serialized_json_frame, str)
#     frame = SensorData.JSON.load(serialized_json_frame)
#     assert frame.size > 0
