import datetime
import typing
from dataclasses import dataclass
from typing import Any, List, Mapping, TypeVar

import pandas

AttributeType = TypeVar('AttributeType', int, float, str, datetime.datetime)

TupleLike = TypeVar('TupleLike', pandas.Series, List[typing.Tuple[str, AttributeType]], Mapping[str, AttributeType])


@dataclass
class InputExhausted:
    pass


class ArrowTableTupleProvider:
    def __init__(self, table):
        self.table = table
        self.current_idx = 0
        self.current_chunk = 0

    def __iter__(self):
        return self

    def __next__(self):
        self.current_idx += 1
        if self.current_idx > self.table.column(0).chunks[self.current_chunk].length():
            self.current_idx = 0
            self.current_chunk += 1
            if self.current_chunk > self.table.column(0).num_chunks:
                raise StopIteration

        def field_accessor(field_name):
            return self.table.column(field_name).chunks[self.current_chunk][self.current_idx]

        return field_accessor


class Tuple:
    """
    Lazy-Tuple implementation.
    """

    def __init__(self, input_field_names=None, output_data=None):
        self.input_field_accessor = None
        self.input_field_names = [] if input_field_names is None else input_field_names
        if output_data is None:
            self.output_fields = {}
        elif isinstance(output_data, List):
            self.output_fields = dict(output_data)
        elif isinstance(output_data, Tuple):
            self.output_fields = output_data.as_dict().copy()
        else:
            self.output_fields = output_data

    def __getitem__(self, item):
        if item not in self.output_fields:
            # evaluate the field now
            self.output_fields[item] = self.input_field_accessor(item).as_py()
        return self.output_fields[item]

    def __setitem__(self, key, value):
        self.output_fields[key] = value

    def as_series(self) -> pandas.Series:
        return pandas.Series(self.as_dict())

    def as_dict(self) -> Mapping[str, Any]:
        # evaluate all the fields now
        for field in self.input_field_names:
            if field not in self.output_fields:
                self.output_fields[field] = self.input_field_accessor(field).as_py()
        return self.output_fields

    def as_key_value_pairs(self) -> List[typing.Tuple[str, Any]]:
        return list(self.as_dict().items())

    def __str__(self) -> str:
        return f"Tuple[{str(self.as_dict()).strip('{').strip('}')}]"

    __repr__ = __str__

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Tuple):
            return False
        else:
            return pandas.Series.__eq__(self.as_series(), other.as_series()).all()

    def __ne__(self, other) -> bool:
        return not self.__eq__(other)

    def reset(self, field_accessor):
        self.output_fields.clear()
        self.fully_evaluated = False
        self.input_field_accessor = field_accessor
