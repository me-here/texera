from typing import List

from core.models.tuple import ITuple


class DataPayload:
    pass


class DataFrame(DataPayload):
    def __init__(self, frame: List[ITuple]):
        self.frame = frame
