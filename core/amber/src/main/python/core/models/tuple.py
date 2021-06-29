import pandas
from abc import ABC
from dataclasses import dataclass


class ITuple(ABC):
    pass


@dataclass
class Tuple(ITuple):
    """
    Python representation of the Texera.Tuple, as a pandas.Series.
    """

    def __init__(self, row: pandas.Series):
        self.row = row


@dataclass
class InputExhausted:
    pass
