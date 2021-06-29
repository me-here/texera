from core.models.tuple import ITuple
from typing import List

DataPayload = list[ITuple]


class DataFrame(DataPayload):
    def __init__(self, frame: List[ITuple]):
        super().__init__()
        self.frame = frame


class EndOfUpstream(DataPayload):
    pass
