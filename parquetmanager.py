import datetime
from abc import ABC, abstractclassmethod, abstractmethod
from pathlib import Path
from typing import Optional

import pandas as pd

from sensordata import SensorData


def start_of_week_timestamp(timestamp: pd.Timestamp):
    return start_of_week(*timestamp.isocalendar())


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


class AbstractParquetManager(ABC):
    @abstractmethod
    def load_dataframe(self):
        pass

    @abstractmethod
    def save_dataframe(self, dataframe: pd.DataFrame):
        pass

    @abstractclassmethod
    def earliest_date(cls) -> datetime.datetime:
        pass

    @abstractmethod
    def get_historic_data(
        self, start: datetime.datetime, end: Optional[datetime.datetime]
    ) -> pd.DataFrame:
        pass


class ParquetBackend:
    def __init__(self, archive_folder: Path = Path("archive/")):
        self.archive_folder = archive_folder

    def load_dataframe(self, parquet_file: Path) -> pd.DataFrame:
        try:
            return SensorData.Parquet.load(
                self.archive_folder / parquet_file.with_suffix(".parquet")
            )
        except Exception:
            return SensorData.construct_empty_dataframe()

    def save_dataframe(self, dataframe: pd.DataFrame, parquet_file: Path):
        SensorData.Parquet.write(dataframe, self.archive_folder / parquet_file)


class DictBackend:
    def __init__(self, _: Optional[Path] = None):
        self.archive = dict()

    def load_dataframe(self, parquet_file: Path) -> pd.DataFrame:
        try:
            return self.archive[parquet_file]
        except KeyError:
            return SensorData.construct_empty_dataframe()

    def save_dataframe(self, dataframe: pd.DataFrame, parquet_file: Path):
        self.archive[parquet_file] = dataframe


class ParquetManager(AbstractParquetManager):
    def __init__(self, backend=ParquetBackend(Path("archive/"))):
        self.backend = backend

    def load_dataframe(self) -> pd.DataFrame:
        last_week, today = self._get_recent_weeks()
        last_week_df = self._load_single_week(*last_week)
        current_week_df = self._load_single_week(*today)
        return pd.concat([last_week_df, current_week_df])

    def _load_single_week(self, year: int, week: int, *args) -> pd.DataFrame:
        parquet_file = self._archive_path_for_week(year, week)
        return self.backend.load_dataframe(parquet_file)

    def save_dataframe(self, dataframe: pd.DataFrame):
        """Merges the data given with the data"""
        week_starting_points = self._get_dataframe_week_ranges(dataframe)
        for week in week_starting_points:
            idx = (slice(None), slice(None), slice(week, week + pd.Timedelta(days=7)))
            self._save_dataframe_by_week(dataframe.loc[idx, :], week)

    def _save_dataframe_by_week(self, dataframe, week_starting_point: pd.Timestamp):
        isoformat = week_starting_point.isocalendar()
        old_data = self._load_single_week(*isoformat)
        df = SensorData.repair_dataframe(pd.concat([old_data, dataframe])).sort_index()
        self.backend.save_dataframe(df, self._archive_path_for_week(*isoformat))

    @staticmethod
    def _get_dataframe_week_ranges(df: pd.DataFrame) -> list[pd.Timestamp]:
        df = df.reset_index()
        week_series = (
            df.timestamp.apply(start_of_week_timestamp)
            .drop_duplicates()
            .sort_values(ignore_index=True)
        )
        return week_series.to_list()

    @staticmethod
    def _get_recent_weeks():
        last_week = (datetime.date.today() - datetime.timedelta(days=7)).isocalendar()
        today = datetime.date.today().isocalendar()
        return last_week, today

    def _archive_path_for_week(self, year: int, week: int, *args) -> Path:
        return Path(f"{year}-W{week:02}.parquet")

    @classmethod
    def earliest_date(cls) -> datetime.datetime:
        last_week, _ = cls._get_recent_weeks()
        return start_of_week(*last_week)

    def get_historic_data(
        self, start: Optional[datetime.datetime], end: Optional[datetime.datetime]
    ) -> pd.DataFrame:
        """The default behaviour is to only load the last two weeks of data. If data earlier than that is requested
        this function should be called to load it. It takes a beginning and optional end.

        If the start and end point are in the same week, it simply loads that parquet file and returns it.
        If they are in the same year but not the same week, it loads all the files from start week to end week inclusive, concatenate them
        and returns.

        If they are in different years it loads the files from the given start date to the final week of the year, concates them and then concates them
        with a recursive call with the start_date of Jan 4 the next year.
        """
        df = self._load_old_data(start, end)
        return SensorData.repair_dataframe(df).sort_index()

    def _load_old_data(
        self, start: Optional[datetime.datetime], end: Optional[datetime.datetime]
    ) -> pd.DataFrame:
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
