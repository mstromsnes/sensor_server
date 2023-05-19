from format import Format
from datastore import DataStore
import pandas as pd


def test_timestamp(datastore: DataStore, archive_timestamp):
    timedelta = pd.Timedelta(seconds=60)
    archive = datastore.serialize_archive(
        timestamp=archive_timestamp + timedelta, format=Format.Parquet
    )
    assert len(archive) > 0
