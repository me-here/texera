from collections import defaultdict

from dataclasses import dataclass
from typing import Set, Iterable, Union

from core.models.payload import DataFrame, EndOfUpstream
from core.models.tuple import ITuple
from edu.uci.ics.amber.engine.common import ActorVirtualIdentity, LinkIdentity

DataPayload = list[ITuple]


@dataclass
class Marker:
    pass


@dataclass
class SenderChangeMarker(Marker):
    link: LinkIdentity
    _priority: int = 1


@dataclass
class EndMarker(Marker):
    _priority: int = 1


@dataclass
class EndOfAllMarker(Marker):
    _priority: int = 1


class BatchToTupleConverter:
    def __init__(self):
        self._input_map: dict[ActorVirtualIdentity, LinkIdentity] = dict()
        self._upstream_map: dict[LinkIdentity, Set[ActorVirtualIdentity]] = defaultdict(set)
        self._current_link: LinkIdentity = None

    def register_input(self, identifier: ActorVirtualIdentity, input: LinkIdentity) -> None:
        self._upstream_map[input].add(identifier)
        self._input_map[identifier] = input

    def process_data_payload(self, from_: ActorVirtualIdentity, data_payload: DataPayload) \
            -> Iterable[Union[ITuple, Marker]]:
        link = self._input_map[from_]
        if self._current_link is None or self._current_link != link:
            self._current_link = link
            yield SenderChangeMarker(link)

        if isinstance(data_payload, DataFrame):
            for tuple_ in data_payload.frame:
                # logger.info(f" yielding a tuple {tuple_}")
                yield tuple_

        elif isinstance(data_payload, EndOfUpstream):
            self._upstream_map[link].remove(from_)
            if len(self._upstream_map[link]) == 0:
                del self._upstream_map[link]
                yield EndMarker()
            if len(self._upstream_map) == 0:
                yield EndOfAllMarker()

        else:
            raise NotImplementedError()
