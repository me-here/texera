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
        chunk_idx = self.current_chunk
        tuple_idx = self.current_idx

        def field_accessor(field_name):
            return self.table.column(field_name).chunks[chunk_idx][tuple_idx]

        self.current_idx += 1
        if self.current_idx >= len(self.table.column(0).chunks[self.current_chunk]):
            self.current_idx = 0
            self.current_chunk += 1
            if self.current_chunk >= self.table.column(0).num_chunks:
                raise StopIteration

        return field_accessor


class Tuple:
    """
    Lazy-Tuple implementation.
    """

    def __init__(self, input_field_names):
        self.input_field_accessor = None
        self.input_field_names = input_field_names
        self.output_fields = {}

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

    def to_output_tuple(self, output_field_names):
        return tuple(self[i] for i in output_field_names)

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
        self.input_field_accessor = field_accessor


class OutputTuple:
    def __init__(self, tuple_like, output_field_names):
        if isinstance(tuple_like, Tuple):
            self.data = tuple_like.to_output_tuple(output_field_names)
        else:
            if isinstance(tuple_like, List):
                field_dict = dict(tuple_like)
            else:
                field_dict = tuple_like
            self.data = (field_dict[i] if i in field_dict else None for i in output_field_names)

    def get_fields(self, indices):
        return (self.data[i] for i in indices)

    def __iter__(self):
        return iter(self.data)
