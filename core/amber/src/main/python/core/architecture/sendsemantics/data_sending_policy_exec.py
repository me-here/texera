import typing
from abc import ABC
from typing import Iterator

from core import Tuple
from core.models.payload import DataFrame, DataPayload
from core.util.proto.proto_helper import get_one_of
from edu.uci.ics.amber.engine.architecture.sendsemantics import DataSendingPolicy
from edu.uci.ics.amber.engine.common import ActorVirtualIdentity


class DataSendingPolicyExec(ABC):

    def __init__(self, policy: DataSendingPolicy):
        self.policy: DataSendingPolicy = get_one_of(policy)

    def add_tuple_to_batch(self, tuple_: Tuple) -> Iterator[typing.Tuple[ActorVirtualIdentity, DataFrame]]:
        pass

    def no_more(self) -> Iterator[typing.Tuple[ActorVirtualIdentity, DataPayload]]:
        pass

    def reset(self) -> None:
        pass

    def __repr__(self):
        return f"PolicyExec[policy={self.policy}]"
