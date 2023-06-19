from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional, Sequence, Iterable

import pandas as pd

from sensordata import SensorData


def start_of_week_timestamp(timestamp: pd.Timestamp):
    return start_of_week(*timestamp.isocalendar())


def start_of_week(year: int, week: int, *args) -> pd.Timestamp:
    """Gets the date that starts given the iso week given"""
    jan_fourth = pd.Timestamp(
        year, 1, 4
    )  # Jan 4th is always in the first ISO week of the year
    start_of_week_one = jan_fourth - pd.Timedelta(days=jan_fourth.weekday())
    start_of_week = start_of_week_one + pd.Timedelta(days=(week - 1) * 7)
    return start_of_week


def end_of_week(year: int, week: int, *args):
    """Gets the date starting the next week after the one given.

    This isn't simply start_of_week(..., week+1) as the next week can be the first in the next year.
    """
    return start_of_week(year, week) + pd.Timedelta(days=7)


class AbstractParquetManager(ABC):
    @property
    @abstractmethod
    def empty(self) -> bool:
        pass

    @abstractmethod
    def load_dataframe(self) -> pd.DataFrame:
        pass

    @abstractmethod
    def save_dataframe(self, dataframe: pd.DataFrame):
        pass

    @abstractmethod
    def get_historic_data(
        self, start: Optional[pd.Timestamp], end: Optional[pd.Timestamp]
    ) -> pd.DataFrame:
        pass

    @abstractmethod
    def oldest_date(self) -> pd.Timestamp:
        pass

    @abstractmethod
    def latest_date(self) -> pd.Timestamp:
        pass


class ArchiveBackend(ABC):
    @abstractmethod
    def __init__(self, _):
        ...

    @property
    @abstractmethod
    def empty(self) -> bool:
        ...

    @abstractmethod
    def load_dataframe(self, key) -> pd.DataFrame:
        ...

    @abstractmethod
    def save_dataframe(self, dataframe: pd.DataFrame, key) -> None:
        ...

    @abstractmethod
    def archive_names(self) -> Sequence[str]:
        ...


class ParquetBackend(ArchiveBackend):
    def __init__(self, archive_folder: Path = Path("archive/")):
        self.archive_folder = archive_folder

    @property
    def empty(self) -> bool:
        return not bool(list(self.archive_folder.glob("*")))

    def load_dataframe(self, parquet_file: Path) -> pd.DataFrame:
        try:
            return SensorData.Parquet.load(
                self.archive_folder / parquet_file.with_suffix(".parquet")
            )
        except Exception:
            return SensorData.construct_empty_dataframe()

    def save_dataframe(self, dataframe: pd.DataFrame, parquet_file: Path):
        SensorData.Parquet.write(
            dataframe, self.archive_folder / parquet_file.with_suffix(".parquet")
        )

    def archive_names(self) -> Iterable[str]:
        return map(lambda path: path.stem, self.archive_folder.glob("*"))


class DictBackend(ArchiveBackend):
    def __init__(self, _: Optional[Path] = None):
        self.archive = dict()

    @property
    def empty(self) -> bool:
        return not bool(self.archive)

    def load_dataframe(self, parquet_file: Path) -> pd.DataFrame:
        try:
            return self.archive[parquet_file]
        except KeyError:
            return SensorData.construct_empty_dataframe()

    def save_dataframe(self, dataframe: pd.DataFrame, archive_name: str):
        self.archive[archive_name] = dataframe

    def archive_names(self):
        return self.archive.keys()


class ParquetManager(AbstractParquetManager):
    def __init__(self, backend: ArchiveBackend = ParquetBackend(Path("archive/"))):
        self.backend = backend

    @property
    def empty(self):
        return self.backend.empty

    def load_dataframe(self) -> pd.DataFrame:
        last_week, today = self._get_recent_weeks()
        last_week_df = self._load_single_week(*last_week)
        current_week_df = self._load_single_week(*today)
        return pd.concat([last_week_df, current_week_df])

    def _load_single_week(self, year: int, week: int, *args) -> pd.DataFrame:
        parquet_file = Path(self._archive_name_for_week(year, week))
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
        self.backend.save_dataframe(df, Path(self._archive_name_for_week(*isoformat)))

    @staticmethod
    def _get_dataframe_week_ranges(df: pd.DataFrame) -> list[pd.Timestamp]:
        df = df.reset_index()
        week_series = (
            df["timestamp"]
            .apply(start_of_week_timestamp)
            .drop_duplicates()
            .sort_values(ignore_index=True)
        )
        return week_series.to_list()

    @staticmethod
    def _get_recent_weeks():
        last_week = (pd.Timestamp.today() - pd.Timedelta(days=7)).isocalendar()
        today = pd.Timestamp.today().isocalendar()
        return last_week, today

    def _archive_name_for_week(self, year: int, week: int, *args) -> str:
        return f"{year}-W{week:02}"

    @classmethod
    def _name_to_timestamp(cls, name: str) -> pd.Timestamp:
        return start_of_week(*map(int, name.split("-W")))

    def oldest_date(self) -> pd.Timestamp:
        assert not self.empty
        return min(map(self._name_to_timestamp, self.backend.archive_names()))

    def latest_date(self) -> pd.Timestamp:
        assert not self.empty
        return max(
            map(self._name_to_timestamp, self.backend.archive_names())
        ) + pd.Timedelta(days=7)

    def get_historic_data(
        self, start: Optional[pd.Timestamp], end: Optional[pd.Timestamp]
    ) -> pd.DataFrame:
        """The default behaviour is to only load the last two weeks of data. If data earlier than that is requested
        this function should be called to load it. It takes a beginning and optional end.

        If the start and end point are in the same week, it simply loads that parquet file and returns it.
        If they are in the same year but not the same week, it loads all the files from start week to end week inclusive, concatenate them
        and returns.

        If they are in different years it loads the files from the given start date to the final week of the year, concates them and then concates them
        with a recursive call with the start_date of Jan 4 the next year.
        """
        if self.backend.empty:
            return SensorData.construct_empty_dataframe()
        df = self._load_old_data(start, end)
        return SensorData.repair_dataframe(df).sort_index()

    def _load_old_data(
        self, start: Optional[pd.Timestamp], end: Optional[pd.Timestamp]
    ) -> pd.DataFrame:
        if start is None:
            start = self.oldest_date()
        if end is None:
            end = self.latest_date()
        return self._load_recursively(start, end)

    def _load_recursively(self, start: pd.Timestamp, end: pd.Timestamp):
        if start > end:
            return SensorData.construct_empty_dataframe()

        start_iso = start.isocalendar()
        end_iso = end.isocalendar()
        if start_iso.year != end_iso.year:  # type: ignore
            year = start_iso.year  # type: ignore
            final_week = (
                pd.Timestamp(year=year, month=12, day=28).isocalendar().week  # type: ignore
            )  # Dec 28 is ALWAYS in the last ISO week of the year
            frame = pd.concat(
                [
                    self._load_single_week(year, week)
                    for week in range(start_iso.week, final_week + 1)  # type: ignore
                ]
            )
            new_year = pd.Timestamp(
                year=year + 1, month=1, day=4
            )  # Jan 4 is ALWAYS in the first ISO week of the year
            return pd.concat([self._load_recursively(new_year, end), frame])

        if start_iso.week == end_iso.week:  # type: ignore
            return self._load_single_week(*start_iso)
        return pd.concat(
            [
                self._load_single_week(start_iso.year, week)  # type: ignore
                for week in range(start_iso.week, end_iso.week + 1)  # type: ignore
            ]
        )
