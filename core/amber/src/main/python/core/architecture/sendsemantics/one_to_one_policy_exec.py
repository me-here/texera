import typing
from overrides import overrides
from typing import Iterator

from core import Tuple
from core.architecture.sendsemantics.data_sending_policy_exec import DataSendingPolicyExec
from core.models.payload import DataFrame, DataPayload, EndOfUpstream
from core.util.proto.proto_helper import set_one_of
from edu.uci.ics.amber.engine.architecture.sendsemantics import DataSendingPolicy, OneToOnePolicy
from edu.uci.ics.amber.engine.common import ActorVirtualIdentity


class OneToOnePolicyExec(DataSendingPolicyExec):
    def __init__(self, policy: OneToOnePolicy):
        super().__init__(set_one_of(DataSendingPolicy, policy))
        self.batch_size = policy.batch_size
        self.batch: list[Tuple] = list()
        self.receiver = policy.receivers[0]  # one to one will have only one receiver.

    @overrides
    def add_tuple_to_batch(self, tuple_: Tuple) -> Iterator[typing.Tuple[ActorVirtualIdentity, DataFrame]]:
        self.batch.append(tuple_)
        if len(self.batch) == self.batch_size:
            yield self.receiver, DataFrame(frame=self.batch)
            self.reset()

    @overrides
    def no_more(self) -> Iterator[typing.Tuple[ActorVirtualIdentity, DataPayload]]:
        if len(self.batch) > 0:
            yield self.receiver, DataFrame(frame=self.batch)
        self.reset()
        yield self.receiver, EndOfUpstream()

    @overrides
    def reset(self) -> None:
        self.batch = list()
