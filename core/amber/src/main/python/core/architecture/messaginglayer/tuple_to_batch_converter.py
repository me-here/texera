from collections import OrderedDict
from loguru import logger
from typing import Iterable, Tuple

from core.architecture.sendsemantics.data_sending_policy import DataSendingPolicy
from core.models.payload import DataPayload
from core.models.tuple import ITuple
from core.util.proto_helper import get_oneof
from edu.uci.ics.amber.engine.common import ActorVirtualIdentity


class TupleToBatchConverter:

    def __init__(self, ):
        self._policies: OrderedDict[str, DataSendingPolicy] = OrderedDict()

    def add_policy(self, policy: DataSendingPolicy) -> None:
        """
        Add down stream operator and its transfer policy
        :param policy:
        :return:
        """
        logger.debug(f"adding one policy {policy}")
        the_policy = get_oneof(policy)
        self._policies.update({the_policy.policy_tag.to_json(): the_policy})

        logger.info(f"policies now: {self._policies}")

    def tuple_to_batch(self, tuple_: ITuple) -> Iterable[Tuple[ActorVirtualIdentity, DataPayload]]:
        return filter(lambda x: x, map(lambda policy: policy.add_tuple_to_batch(tuple_), self._policies.values()))

    def emit_end_of_upstream(self) -> None:
        # TODO: finish this
        pass
