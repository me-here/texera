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
        if self.current_idx >= len(self.table.column(0).chunks[self.current_chunk]):
            self.current_idx = 0
            self.current_chunk += 1
            if self.current_chunk >= self.table.column(0).num_chunks:
                raise StopIteration

        chunk_idx = self.current_chunk
        tuple_idx = self.current_idx

        def field_accessor(field_name):
            return self.table.column(field_name).chunks[chunk_idx][tuple_idx]

        self.current_idx += 1
        return field_accessor


class Tuple:
    """
    Lazy-Tuple implementation.
    """

    def __init__(self, field_data: TupleLike = None, field_names=None, field_accessor=None):
        """
        Construct a lazy-tuple with given data
        :param field_data: tuple-like data that is already in memory
        :param field_names: all field names that can be fetched
        :param field_accessor: a lambda function that fetches a field with given field name
        """
        self.field_accessor = field_accessor
        self.field_names = field_names
        if field_data is None:
            self.field_data = {}
        else:
            self.field_data = dict(field_data)

    def __getitem__(self, item):
        """
        Get a field with given name. If it's not present, fetch it from accessor.
        :param item: field name
        :return: field value
        """
        if item not in self.field_data:
            # evaluate the field now
            self.field_data[item] = self.field_accessor(item).as_py()
        return self.field_data[item]

    def __setitem__(self, key, value):
        """
        Set a field with given value.
        :param key: field name
        :param value: field value
        """
        self.field_data[key] = value

    def as_series(self) -> pandas.Series:
        return pandas.Series(self.as_dict())

    def as_dict(self) -> Mapping[str, Any]:
        """
        Return a dictionary reference of this tuple.
        Fields will be fetched from accessor if absent.
        :return: dict with all the fields
        """
        # evaluate all the fields now
        if self.field_names is not None:
            for field in self.field_names:
                if field not in self.field_data:
                    self.field_data[field] = self.field_accessor(field).as_py()
        return self.field_data

    def as_key_value_pairs(self) -> List[typing.Tuple[str, Any]]:
        return list(self.as_dict().items())

    def to_values(self, output_field_names=None):
        if output_field_names is None:
            if self.field_names is None:
                return tuple(self.field_data.values())
            else:
                return tuple(self[i] for i in self.field_names)
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
        """
        Reset the tuple with given field accessor
        to avoid unnecessary tuple object creation.
        Field values will be cleared but field names(schema)
        will not be reset in this operation.
        :param field_accessor: new accessor
        """
        self.field_data.clear()
        self.field_accessor = field_accessor


class ImmutableTuple:
    """
    Container of pure data values. Only used after user returns
    a modified tuple or tuple-like.
    """
    def __init__(self, tuple_like, output_field_names):
        """
        Create a ImmutableTuple from tuple-like objects or tuple.
        :param tuple_like: data object
        :param output_field_names: output schema
        """
        if isinstance(tuple_like, Tuple):
            self.data = tuple_like.to_values(output_field_names)
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
