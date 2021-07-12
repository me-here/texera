from collections import OrderedDict
from itertools import chain

from typing import Iterable, Iterator

from core import Tuple
from core.architecture.sendsemantics.data_sending_policy_exec import DataSendingPolicyExec
from core.architecture.sendsemantics.one_to_one_policy_exec import OneToOnePolicyExec
from core.models.payload import DataFrame, DataPayload
from core.util.proto.proto_helper import get_one_of
from edu.uci.ics.amber.engine.architecture.sendsemantics import DataSendingPolicy, OneToOnePolicy
from edu.uci.ics.amber.engine.common import ActorVirtualIdentity, LinkIdentity


class TupleToBatchConverter:

    def __init__(self, ):
        self._policy_execs: OrderedDict[LinkIdentity, DataSendingPolicy] = OrderedDict()
        self._policy_exec_map: dict[type(DataSendingPolicy), type(DataSendingPolicyExec)] = {
            OneToOnePolicy: OneToOnePolicyExec
        }

    def add_policy(self, policy: DataSendingPolicy) -> None:
        """
        Add down stream operator and its transfer policy
        :param policy:
        :return:
        """
        the_policy = get_one_of(policy)
        policy_exec: type = self._policy_exec_map[type(the_policy)]
        policy_exec_instance: DataSendingPolicyExec = policy_exec(the_policy)
        self._policy_execs.update({the_policy.policy_tag: policy_exec_instance})

    def tuple_to_batch(self, tuple_: Tuple) -> Iterator[tuple[ActorVirtualIdentity, DataFrame]]:
        return chain(*(policy_exec.add_tuple_to_batch(tuple_) for policy_exec in self._policy_execs.values()))

    def emit_end_of_upstream(self) -> Iterable[tuple[ActorVirtualIdentity, DataPayload]]:
        return chain(*(policy_exec.no_more() for policy_exec in self._policy_execs.values()))
