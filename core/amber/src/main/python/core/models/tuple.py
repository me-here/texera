from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from pandas import Series


class ITuple(ABC):
    pass


@dataclass
class Tuple(ITuple):
    """
    Python representation of the Texera.Tuple, as a pandas.Series.
    """

    _internal_storage: Series

    @staticmethod
    def from_series(series: Series) -> Tuple:
        return Tuple(_internal_storage=series)

    def as_series(self) -> Series:
        return self._internal_storage

    @staticmethod
    def new_builder() -> Tuple.Builder:
        return Tuple.Builder()

    class Builder:
        def __init__(self):
            pass


@dataclass
class InputExhausted:
    pass
