from typing import Optional, Iterator

from core.models import Tuple


class DataBreakpointManager:
    def __init__(self):
        self._tuple_to_trace = None
        self._iterator_to_trace = None
        self._breakpoints = []
        # self.set_break(lambda tuple_: "name" in tuple_ and tuple_['name'] == 'yicong huang')
        self.set_break()

    def trace_tuple(self, tuple_: Tuple):
        self._tuple_to_trace = tuple_

    def trace_iterator(self, iterator: Iterator[Optional[Tuple]]):
        self._iterator_to_trace = iterator

    def set_break(self, condition=lambda tuple_: True):
        self._breakpoints.append(condition)

    def check_break(self) -> bool:
        for condition in self._breakpoints:
            if condition(self._tuple_to_trace):
                return True
        return False
