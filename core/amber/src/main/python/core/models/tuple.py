from __future__ import annotations

from dataclasses import dataclass
from pandas import Series


@dataclass
class Tuple:
    """
    Python representation of the Texera.Tuple, as a pandas.Series.
    """

    _internal_storage: Series

    @staticmethod
    def from_series(series: Series) -> Tuple:
        return Tuple(_internal_storage=series)

    def as_series(self) -> Series:
        return self._internal_storage


@dataclass
class InputExhausted:
    pass
