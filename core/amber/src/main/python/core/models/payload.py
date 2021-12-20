from dataclasses import dataclass
from typing import Generator, List, Dict
from pyarrow.lib import Table
from core.models.tuple import ImmutableTuple


@dataclass
class DataPayload:
    pass


@dataclass
class InputDataFrame(DataPayload):
    frame: Table


@dataclass
class OutputDataFrame(DataPayload):
    frame: List[ImmutableTuple]
    schema: List[str] = None


@dataclass
class EndOfUpstream(DataPayload):
    pass
