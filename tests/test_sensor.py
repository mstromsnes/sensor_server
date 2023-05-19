from sensor import SensorData
from pandera.errors import SchemaError
from sensor import SensorReading
import pytest
import pandas as pd
from format import Format

TEST_ARCHIVE_FILE = "archive/test.parquet"


def replace_index(
    df: pd.DataFrame, indexes: list[str], from_: str, to: str
) -> pd.DataFrame:
    df = df.reset_index()
    df = df.replace(from_, to)
    df = df.set_index(indexes, drop=True)
    return df


def test_wrong_category_sensortype(dataframe):
    dataframe = replace_index(dataframe, SensorReading._indexes, "temperature", "t")
    with pytest.raises(SchemaError):
        SensorData.validate(dataframe)


def test_wrong_category_sensor(dataframe):
    dataframe = replace_index(dataframe, SensorReading._indexes, "DHT11", "dht11")
    with pytest.raises(SchemaError):
        SensorData.validate(dataframe)


def test_wrong_category_unit(dataframe: pd.DataFrame):
    dataframe = dataframe.replace("C", "K")
    with pytest.raises(SchemaError):
        SensorData.validate(dataframe)


def test_serialize_and_deserialize_maintains_dtypes(dataframe):
    ser = Format.Parquet.serialize(dataframe)
    df = Format.Parquet.load(ser)
    SensorData.validate(df)


def test_make_empty_dataframe():
    empty_df = SensorData.construct_empty_dataframe()
    assert empty_df.size == 0


@pytest.mark.parametrize("column", SensorReading._columns)
def test_missing_column(dataframe: pd.DataFrame, column):
    dataframe = dataframe.drop(columns=column)
    with pytest.raises(SchemaError):
        SensorData.validate(dataframe)
