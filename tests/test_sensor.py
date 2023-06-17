import pandas as pd
import pytest
from hypothesis import given
from pandera.errors import SchemaError

from sensor import SensorReading
from sensordata import SensorData
from tests.strats import sensor_data

TEST_ARCHIVE_FILE = "archive/test.parquet"


def replace_index(
    df: pd.DataFrame, indexes: list[str], from_: str, to: str
) -> pd.DataFrame:
    df = df.reset_index()
    df = df.replace(from_, to)
    df = df.set_index(indexes, drop=True)
    return df


@given(dataframe=sensor_data())
def test_wrong_category_sensortype(dataframe: pd.DataFrame):
    if dataframe.empty:
        return  # Replacing the index is a noop on an empty dataframe, so we can't catch the missed validation
    dataframe = replace_index(dataframe, SensorReading._indexes, "temperature", "t")
    with pytest.raises(SchemaError):
        SensorData.Model.validate(dataframe)


@given(dataframe=sensor_data())
def test_wrong_category_sensor(dataframe: pd.DataFrame):
    if dataframe.empty:
        return  # Replacing the index is a noop on an empty dataframe, so we can't catch the missed validation
    dataframe = replace_index(dataframe, SensorReading._indexes, "DHT11", "dht11")
    with pytest.raises(SchemaError):
        SensorData.Model.validate(dataframe)


@given(dataframe=sensor_data())
def test_wrong_category_unit(dataframe: pd.DataFrame):
    if dataframe.empty:
        return  # Replacing the index is a noop on an empty dataframe, so we can't catch the missed validation
    dataframe = dataframe.replace("C", "K")
    with pytest.raises(SchemaError):
        SensorData.Model.validate(dataframe)


@given(dataframe=sensor_data())
def test_serialize_and_deserialize_maintains_dtypes(dataframe):
    ser = SensorData.Parquet.serialize(dataframe)
    df = SensorData.Parquet.load(ser)
    SensorData.Model.validate(df)


def test_make_empty_dataframe():
    empty_df = SensorData.construct_empty_dataframe()
    assert empty_df.size == 0


@given(dataframe=sensor_data())
@pytest.mark.parametrize("column", SensorReading._columns)
def test_missing_column(dataframe: pd.DataFrame, column):
    dataframe = dataframe.drop(columns=column)
    with pytest.raises(SchemaError):
        SensorData.Model.validate(dataframe)
