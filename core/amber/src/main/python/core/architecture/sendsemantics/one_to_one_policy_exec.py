from typing import Optional, Iterable

from core import Tuple
from core.architecture.sendsemantics.data_sending_policy_exec import DataSendingPolicyExec
from core.models.payload import DataFrame, EndOfUpstream
from edu.uci.ics.amber.engine.architecture.sendsemantics import DataSendingPolicy
from edu.uci.ics.amber.engine.common import LinkIdentity, ActorVirtualIdentity, DataPayload


class OneToOnePolicyExec(DataSendingPolicyExec):
    def __init__(self, policy: DataSendingPolicy):
        super().__init__(policy)
        self.batch_size = 100
        self.batch: list[Tuple] = list()


    def add_tuple_to_batch(self, tuple_: Tuple) -> Optional[tuple[ActorVirtualIdentity, DataFrame]]:
        self.batch.append(tuple_)
        if len(self.batch) == self.batch_size:
            ret_batch = self.batch
            self.reset()
            return self.policy.receivers[0], DataFrame(ret_batch)
        else:
            return None

    def no_more(self) -> Iterable[tuple[ActorVirtualIdentity, DataPayload]]:
        if len(self.batch) > 0:
            yield self.policy.receivers[0], DataFrame(self.batch)
        yield self.policy.receivers[0], EndOfUpstream()
        self.reset()

    def reset(self) -> None:
        self.batch = list()
