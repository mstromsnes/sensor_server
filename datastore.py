import pandas as pd
from pathlib import Path
from typing import Optional
from format import Format
import pandera as pa
from pandera.errors import SchemaError
import remotereader
import logging
from sensor import SensorReading, SensorData
import datetime

log = logging.getLogger("datastore")


def start_of_week(year: int, week: int, *args):
    """Gets the date that starts given the iso week given"""
    jan_fourth = datetime.datetime(
        year, 1, 4
    )  # Jan 4th is always in the first ISO week of the year
    start_of_week_one = jan_fourth - datetime.timedelta(days=jan_fourth.weekday())
    start_of_week = start_of_week_one + datetime.timedelta(days=(week - 1) * 7)
    return start_of_week


def end_of_week(year: int, week: int, *args):
    """Gets the date starting the next week after the one given.

    This isn't simply start_of_week(..., week+1) as the next week can be the first in the next year.
    """
    return start_of_week(year, week) + datetime.timedelta(days=7)


class ParquetManager:
    def __init__(self, archive_folder: Path = Path("archive/")):
        self.archive_folder = archive_folder

    def load_dataframe(self) -> pd.DataFrame:
        last_week, today = self._get_recent_weeks()
        last_week_df = self._load_single_week(*last_week)
        current_week_df = self._load_single_week(*today)
        return pd.concat([last_week_df, current_week_df])

    def _load_single_week(self, year: int, week: int, *args) -> pd.DataFrame:
        parquet_file = self._archive_path_for_week(year, week)
        try:
            return Format.Parquet.load(parquet_file)
        except Exception as err:
            log.log(logging.DEBUG, f"{parquet_file=} failed to load", err)
            return SensorData.construct_empty_dataframe()

    def save_dataframe(self, dataframe: pd.DataFrame):
        last_week, today = self._get_recent_weeks()
        dataframe = dataframe.reset_index().set_index("timestamp").sort_index()
        last_week_df = dataframe.loc[
            slice(
                start_of_week(*last_week),
                end_of_week(*last_week),
            )
        ]
        current_week_df = dataframe.loc[
            slice(
                start_of_week(today.year, today.week),
                end_of_week(today.year, today.week),
            )
        ]
        Format.Parquet.write(last_week_df, self._archive_path_for_week(*last_week))
        Format.Parquet.write(current_week_df, self._archive_path_for_week(*today))

    @staticmethod
    def _get_recent_weeks():
        last_week = (datetime.date.today() - datetime.timedelta(days=7)).isocalendar()
        today = datetime.date.today().isocalendar()
        return last_week, today

    def _archive_path_for_week(self, year: int, week: int, *args) -> Path:
        return self.archive_folder / f"{year}-W{week:02}.parquet"

    @classmethod
    def earliest_date(cls) -> datetime.datetime:
        last_week, _ = cls._get_recent_weeks()
        return start_of_week(*last_week)

    def get_historic_data(
        self, start: datetime.datetime, end: Optional[datetime.datetime]
    ):
        """The default behaviour is to only load the last two weeks of data. If data earlier than that is requested
        this function should be called to load it. It takes a beginning and optional end.

        If the start and end point are in the same week, it simply loads that parquet file and returns it.
        If they are in the same year but not the same week, it loads all the files from start week to end week inclusive, concatenate them
        and returns.

        If they are in different years it loads the files from the given start date to the final week of the year, concates them and then concates them
        with a recursive call with the start_date of Jan 4 the next year.
        """
        start_iso = start.isocalendar()
        if end is None:
            end = datetime.datetime.today()
        if start > end:
            return SensorData.construct_empty_dataframe()
        end_iso = end.isocalendar()
        if start_iso.year == end_iso.year:
            if start_iso.week == end_iso.week:
                return self._load_single_week(*start_iso)
            else:
                return pd.concat(
                    [
                        self._load_single_week(start_iso.year, week)
                        for week in range(start_iso.week, end_iso.week + 1)
                    ]
                )
        else:
            year = start_iso.year
            final_week = (
                datetime.datetime(year=year, month=12, day=28).isocalendar().week
            )  # Dec 28 is ALWAYS in the last ISO week of the year
            frame = pd.concat(
                [
                    self._load_single_week(year, week)
                    for week in range(start_iso.week, final_week + 1)
                ]
            )
            new_year = datetime.datetime(
                year=year + 1, month=1, day=4
            )  # Jan 4 is ALWAYS in the first ISO week of the year
            return pd.concat([self.get_historic_data(new_year, end), frame])


class DataStore:
    def __init__(self, *, manager=ParquetManager(), proxy=None):
        self._dataframe = SensorData.construct_empty_dataframe()
        self.parquet_manager = manager
        self.proxy = proxy
        self._reload_dataframe = False
        self._create_pending_queue()

    def _can_load_dataframe(self) -> bool:
        return self.proxy is not None or self.parquet_manager is not None

    @property
    @pa.check_types
    def dataframe(self) -> pd.DataFrame:
        if self._reload_dataframe or (
            self._dataframe.size == 0 and self._can_load_dataframe()
        ):
            self._dataframe = self._load_archive()
            self._reload_dataframe = False
        self._dataframe = self._merge_queue_with_dataframe(self._dataframe, self._queue)
        self._clear_pending_queue()
        return self._dataframe

    def archive_data(self):
        self.parquet_manager.save_dataframe(self.dataframe)
        self._reload_dataframe = True

    def serialize_archive(
        self,
        *,
        timestamp: Optional[pd.Timestamp] = None,
        format: Format = Format.Parquet,
    ) -> bytes:
        """If no timestamp is provided, returns all data currently in memory, as decided by ParquetManager or Proxy.
        If a timestamp is provided, provides all data from the time given to now, possibly loading more data from storage if necessary.
        """
        if timestamp is None:
            return format.serialize(self.dataframe)
        # Slices to all entries of the highest level index; SensorType, all entries of the next level index; Sensor, and then to all values at timestamp and later
        idx = (slice(None), slice(None), slice(timestamp, None))
        if (
            self.parquet_manager is not None
            and timestamp < ParquetManager.earliest_date()
        ):
            df = self.parquet_manager.get_historic_data(timestamp, None)
            df = SensorData.repair_dataframe(df).sort_index()
        else:
            df = self.dataframe
        df = df.loc[idx, :]
        return format.serialize(df)

    def add_reading(self, reading: SensorReading):
        self._write_reading_to_queue(reading)

    def tail(self) -> pd.DataFrame:
        return self.dataframe.tail()

    def _create_pending_queue(self):
        """Create the queue of readings that will be added to the dataframe in the next update"""
        self._queue: list[SensorReading] = list()

    def _clear_pending_queue(self):
        """Clear the queue of readings that will be added to the dataframe in the next update"""
        self._create_pending_queue()

    def _load_archive(self) -> pd.DataFrame:
        if self.proxy is not None:
            return self._download_archive_from_proxy(self.proxy)
        elif self.parquet_manager is not None:
            return self._load_dataframe_from_file()
        else:
            return SensorData.construct_empty_dataframe()

    def _load_dataframe_from_file(self) -> pd.DataFrame:
        return self.parquet_manager.load_dataframe().sort_index()

    def _download_archive_from_proxy(self, proxy) -> pd.DataFrame:
        archive, format = remotereader.download_archive(proxy)
        return format.load(archive)

    @staticmethod
    def _load_dataframe_from_queue(queue) -> pd.DataFrame:
        return SensorData.make_dataframe_from_list_of_readings(queue)

    @classmethod
    def _merge_queue_with_dataframe(cls, old_dataframe, queue) -> pd.DataFrame:
        new_dataframe = cls._load_dataframe_from_queue(queue)
        if new_dataframe.size == 0:
            return old_dataframe
        try:
            SensorData.validate(new_dataframe)
        except SchemaError:
            new_dataframe = SensorData.construct_empty_dataframe()
            log.error("Invalid Dataframe constructed from queue", exc_info=True)
            log.error(f"{queue=}")
        return pd.concat([old_dataframe, new_dataframe]).sort_index()

    def _write_reading_to_queue(self, reading: SensorReading):
        self._queue.append(reading)
        if len(self._queue) > 1000:
            # Force a write
            self.dataframe
