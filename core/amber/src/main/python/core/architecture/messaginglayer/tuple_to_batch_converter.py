from collections import OrderedDict
from itertools import chain

from betterproto import Casing
from loguru import logger
from typing import Iterable, Tuple

from core.architecture.sendsemantics.data_sending_policy import DataSendingPolicyExec, OneToOnePolicyExec
from core.models.payload import DataPayload
from core.models.tuple import ITuple
from core.util.proto_helper import get_oneof
from edu.uci.ics.amber.engine.architecture.sendsemantics import DataSendingPolicy, OneToOnePolicy
from edu.uci.ics.amber.engine.common import ActorVirtualIdentity


class TupleToBatchConverter:

    def __init__(self, ):
        self._policy_execs: OrderedDict[str, DataSendingPolicy] = OrderedDict()
        self._policy_exec_map: dict[type(DataSendingPolicy), type(DataSendingPolicyExec)] = {
            OneToOnePolicy: OneToOnePolicyExec
        }

    def add_policy(self, policy: DataSendingPolicy) -> None:
        """
        Add down stream operator and its transfer policy
        :param policy:
        :return:
        """
        # logger.debug(f"adding one policy {policy}")
        the_policy: OneToOnePolicy = get_oneof(policy)
        policy_exec = self._policy_exec_map[type(the_policy)]
        # logger.info(f"unpackaged policy = {the_policy.to_dict(casing=Casing.SNAKE)}")
        policy_exec_instance = policy_exec(the_policy.policy_tag, the_policy.batch_size, the_policy.receivers)
        self._policy_execs.update({the_policy.policy_tag: policy_exec_instance})

        # logger.info(f"policies now: {self._policy_execs}")

    def tuple_to_batch(self, tuple_: ITuple) -> Iterable[Tuple[ActorVirtualIdentity, DataPayload]]:
        return filter(lambda x: x is not None, map(lambda policy_exec: policy_exec.add_tuple_to_batch(tuple_),
                                                   self._policy_execs.values()))

    def emit_end_of_upstream(self) -> Iterable[Tuple[ActorVirtualIdentity, DataPayload]]:
        return chain(*map(lambda policy_exec: policy_exec.no_more(), self._policy_execs.values()))
