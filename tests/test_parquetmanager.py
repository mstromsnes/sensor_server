from datastore import ParquetManager
import datetime


def test_load_frame(parquet_manager: ParquetManager):
    frame = parquet_manager.load_dataframe()
    assert frame.size


def test_load_historic_frame(parquet_manager: ParquetManager):
    frame = parquet_manager.get_historic_data(datetime.datetime(2023, 1, 1), None)
    assert frame.size


def test_future_data(parquet_manager: ParquetManager):
    frame = parquet_manager.get_historic_data(
        datetime.datetime.today() + datetime.timedelta(days=7), None
    )
    assert not frame.size


def test_historic_greater_than_base(parquet_manager: ParquetManager):
    base_frame = parquet_manager.load_dataframe()
    historic_frame = parquet_manager.get_historic_data(
        datetime.datetime(2023, 1, 1), None
    )
    assert historic_frame.size > base_frame.size
