from abc import ABC
from typing import Optional

from core import Tuple
from core.models.payload import DataFrame, DataPayload
from edu.uci.ics.amber.engine.architecture.sendsemantics import DataSendingPolicy
from edu.uci.ics.amber.engine.common import ActorVirtualIdentity


class DataSendingPolicyExec(ABC):

    def __init__(self, policy: DataSendingPolicy):
        self.policy = policy

    def add_tuple_to_batch(self, tuple_: Tuple) -> Optional[tuple[ActorVirtualIdentity, DataFrame]]:
        pass

    def no_more(self) -> tuple[ActorVirtualIdentity, DataPayload]:
        pass

    def reset(self) -> None:
        pass

    def __repr__(self):
        return f"PolicyExec[policy={self.policy}]"
