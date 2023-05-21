import pandas as pd
from pandas import DataFrame
from io import BytesIO
from typing import Union, Callable
from pathlib import Path
from typing import Protocol, runtime_checkable

SerializedDataFrame = Union[str, bytes, BytesIO]

SerializeType = Callable[[pd.DataFrame], SerializedDataFrame]
DeserializeType = Callable[[SerializedDataFrame], DataFrame]
WriteFnType = Callable[[pd.DataFrame, Path], None]


def make_type_error(from_type: str, to_type: str) -> TypeError:
    return TypeError(f"Cannot load {to_type} from {from_type}")


class SerializationFormat(Protocol):
    """Serialization format handler protocol that should be implemented separately for each schema the server handles."""

    @staticmethod
    def serialize(df: pd.DataFrame) -> SerializedDataFrame:
        """Consumes an in-memory dataframe and outputs a serialized form"""
        ...

    @staticmethod
    def write(df: pd.DataFrame, path: Path) -> None:
        """Consumes an in-memory dataframe and writes it to file."""
        ...

    @staticmethod
    def load(archive: Union[SerializedDataFrame, Path]) -> pd.DataFrame:
        """Takes an in-memory serialized dataframe or path representing a file serialized dataframe and returns the dataframe."""
        ...

    @staticmethod
    def endpoint() -> str:
        """Returns the endpoint associated with this format/schema"""
        ...


# Below are more specialized formats that know more about what types they return and consume.
# The formats are implemented near the Schema that holds


@runtime_checkable
class ParquetFormat(SerializationFormat, Protocol):
    """Serialization handler for the Parquet format. Each schema that supports parquet should implement this protocol to allow serialization and loading."""

    @staticmethod
    def serialize(df: pd.DataFrame) -> bytes:
        ...

    @staticmethod
    def write(df: pd.DataFrame, path: Path) -> None:
        ...

    @staticmethod
    def load(archive: Union[SerializedDataFrame, Path]) -> DataFrame:
        ...

    @staticmethod
    def endpoint() -> str:
        ...


@runtime_checkable
class JSONFormat(SerializationFormat, Protocol):
    """Serialization handler for the JSON format. Each schema that supports JSON should implement this protocol to allow serialization and loading."""

    @staticmethod
    def serialize(df: pd.DataFrame) -> str:
        ...

    @staticmethod
    def write(df: pd.DataFrame, path: Path) -> None:
        ...

    @staticmethod
    def load(archive: Union[str, Path]) -> DataFrame:
        ...

    @staticmethod
    def endpoint() -> str:
        ...
