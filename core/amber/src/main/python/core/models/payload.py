from dataclasses import dataclass
from typing import List

from core.models.tuple import ITuple


@dataclass
class DataPayload:
    pass


@dataclass
class DataFrame(DataPayload):
    frame: list[ITuple]


@dataclass
class EndOfUpstream(DataPayload):
    pass
