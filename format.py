import pandas as pd
from io import BytesIO
from typing import Union, Callable, Annotated, TypedDict, Any, get_type_hints, TypeVar
from sensor import SensorData
from enum import Enum, auto
from pathlib import Path


def get_parquet(
    df: pd.DataFrame,
) -> bytes:
    """Fastparquet version 2023.4.0 closes the file/buffer itself after its done. For a file this is ok. For a buffer this clears out the buffer, deleting the data.
    When passing path=None to to_parquet() Pandas is supposed to return the bytes. But it does it the same way I tried. It uses a BytesIO buffer that gets closed,
    then tries to return the bytes from the buffer, which aren't there. Code from pandas own documentation doesn't work because of this bug.

    This is a workaround to prevent fastparquet from closing the buffer.
    If using pyarrow we don't need to do this, but pyarrow doesn't install on the RPi easily.
    """
    buffer = BytesIO()
    actual_close = buffer.close
    buffer.close = lambda: None
    df = df.reset_index()
    df.to_parquet(buffer)
    contents = buffer.getvalue()
    actual_close()
    return contents


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    df = df.reset_index()
    df.to_parquet(path)


def get_json(df: pd.DataFrame) -> str:
    return df.to_json(orient="table")


def write_json(df: pd.DataFrame, path: Path) -> None:
    df.to_json(path, orient="table")


def read_json(json: str) -> pd.DataFrame:
    return pd.read_json(json, orient="table")


def read_parquet(archive: Union[BytesIO, bytes]) -> pd.DataFrame:
    if isinstance(archive, bytes):
        archive = BytesIO(archive)
    df = pd.read_parquet(archive)
    return SensorData.repair_dataframe(df)


SerializedDataFrameType = TypeVar("SerializedDataFrameType", str, bytes, BytesIO)

SerializeType = Callable[[pd.DataFrame], SerializedDataFrameType]
DeserializeType = Callable[[SerializedDataFrameType], pd.DataFrame]
WriteFnType = Callable[[pd.DataFrame, Path], None]


class FormatAnnotation(TypedDict):
    serialize: SerializeType
    deserialize: DeserializeType
    write: WriteFnType
    endpoint: str


class MissingAnnotationError(Exception):
    def __init__(self, type):
        self.message = f"{type=} does not have a type annotation. It should be annotated with functions to serialize, deserialize and write the archive to disk as well as an API endpoint."
        super().__init__(self.message)


ParquetIO = Annotated[
    Any,
    FormatAnnotation(
        serialize=get_parquet,
        deserialize=read_parquet,
        write=write_parquet,
        endpoint="parquet/",
    ),
]

JSONIO = Annotated[
    Any,
    FormatAnnotation(
        serialize=get_json, deserialize=read_json, write=write_json, endpoint="json/"
    ),
]


class Format(Enum):
    Parquet: ParquetIO = auto()
    JSON: JSONIO = auto()

    def _missing_annotation_error(self):
        raise MissingAnnotationError(self)

    def _get_metadata(self, key: str) -> Any:
        try:
            annotations = get_type_hints(self, include_extras=True)[self.name]
            metadata = annotations.__metadata__[0][key]
        except (KeyError, ValueError, TypeError):
            raise MissingAnnotationError(self)
        return metadata

    def serialize(self, df) -> SerializedDataFrameType:
        """Serializes the dataframe using the serialize function attached to the format."""
        output_fn = self._get_metadata("serialize")
        return output_fn(df)

    def load(self, archive) -> pd.DataFrame:
        """Deserializes the dataframe using the deserialize function attached to the format."""
        input_fn = self._get_metadata("deserialize")
        return input_fn(archive)

    def write(self, archive, path):
        write_fn = self._get_metadata("write")
        write_fn(archive, path)

    def endpoint(self) -> str:
        """API endpoint for fastapi"""
        endpoint = self._get_metadata("endpoint")
        return endpoint
