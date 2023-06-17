import pandas as pd
from hypothesis import given, settings

from datastore import ParquetManager
from tests.strats import MAX_TIMESTAMP, MIN_TIMESTAMP, sensor_data


def test_load_frame(parquet_manager_with_recent_data: ParquetManager):
    frame = parquet_manager_with_recent_data.load_dataframe()
    assert frame.size


def test_load_historic_frame(parquet_manager: ParquetManager):
    frame = parquet_manager.get_historic_data(
        MIN_TIMESTAMP - pd.Timedelta(days=1), MAX_TIMESTAMP + pd.Timedelta(days=1)
    )
    assert frame.size


def test_future_data(parquet_manager: ParquetManager):
    frame = parquet_manager.get_historic_data(
        MAX_TIMESTAMP + pd.Timedelta(days=1), MAX_TIMESTAMP + pd.Timedelta(days=365 * 4)
    )
    assert not frame.size


@given(sensor_data=sensor_data())
@settings(max_examples=10)
def test_new_data_doesnt_overwrite_old_data(
    parquet_manager: ParquetManager, sensor_data: pd.DataFrame
):
    pre_historic = MIN_TIMESTAMP - pd.Timedelta(days=1)
    old_data = parquet_manager.get_historic_data(pre_historic, None)
    new_data = sensor_data
    new_data = new_data.drop(new_data.isin(old_data), errors="ignore")  # type: ignore
    parquet_manager.save_dataframe(new_data)
    full_dataset = parquet_manager.get_historic_data(pre_historic, None)
    assert old_data.isin(full_dataset).all().all()
