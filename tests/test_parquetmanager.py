from datastore import ParquetManager
import datetime
from tests.strats import sensor_data, MIN_TIMESTAMP, MAX_TIMESTAMP
import pandas as pd
from hypothesis import given


def test_load_frame(parquet_manager_with_recent_data: ParquetManager):
    frame = parquet_manager_with_recent_data.load_dataframe()
    assert frame.size


def test_load_historic_frame(parquet_manager: ParquetManager):
    frame = parquet_manager.get_historic_data(
        MIN_TIMESTAMP - pd.Timedelta(days=1), None
    )
    assert frame.size


def test_future_data(parquet_manager: ParquetManager):
    frame = parquet_manager.get_historic_data(
        MAX_TIMESTAMP + pd.Timedelta(days=1), None
    )
    assert not frame.size


@given(sensor_data=sensor_data())
def test_new_data_doesnt_overwrite_old_data(
    parquet_manager: ParquetManager, sensor_data: pd.DataFrame
):
    pre_historic = MIN_TIMESTAMP - pd.Timedelta(days=1)
    old_data = parquet_manager.get_historic_data(pre_historic, None)
    new_data = sensor_data
    new_data = new_data.drop(new_data.isin(old_data), errors="ignore")
    parquet_manager.save_dataframe(new_data)
    full_dataset = parquet_manager.get_historic_data(pre_historic, None)
    try:
        assert old_data.isin(full_dataset).all().all()
    except:
        assert False
