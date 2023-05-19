from format import Format, FormatAnnotation
import pandas as pd


def test_parquet_serialization(dataframe):
    parquet_bytes = Format.Parquet.serialize(dataframe)
    assert len(parquet_bytes) > 0


def test_parquet_deserialization(serialized_parquet_frame: pd.DataFrame):
    frame = Format.Parquet.load(serialized_parquet_frame)
    assert frame.size > 0


def test_json_serialization(dataframe):
    json_str = Format.JSON.serialize(dataframe)
    assert len(json_str) > 0


def test_json_deserialization(serialized_json_frame: pd.DataFrame):
    frame = Format.JSON.load(serialized_json_frame)
    assert frame.size > 0


def test_all_formats_are_fully_annotated():
    # Test that no exception is raised for any defined format
    for key in FormatAnnotation.__required_keys__:
        for format in Format:
            format._get_metadata(key)
