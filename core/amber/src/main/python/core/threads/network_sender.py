from loguru import logger
from pandas import DataFrame
from pyarrow import Table

from core.architecture.messaginglayer.tuple_to_batch_converter import TupleToBatchConverter
from core.models.internal_queue import InternalQueue, InputTuple, ControlElement
from core.models.payload import DataPayload
from core.models.tuple import ITuple
from core.util.proto_helper import get_oneof
from core.util.stoppable_queue_blocking_thread import StoppableQueueBlockingThread
from edu.uci.ics.amber.engine.architecture.sendsemantics import DataSendingPolicy
from edu.uci.ics.amber.engine.architecture.worker import AddOutputPolicy
from edu.uci.ics.amber.engine.common import ControlInvocation, ActorVirtualIdentity
from proxy import ProxyClient


class NetworkSender(StoppableQueueBlockingThread):
    def __init__(self, shared_queue: InternalQueue, host: str, port: int):
        super().__init__(self.__class__.__name__, queue=shared_queue)
        self._rpc_client = ProxyClient(host=host, port=port)
        self._batch = list()
        self._tuple_to_batch_converter = TupleToBatchConverter()

    def add_policy(self, policy: DataSendingPolicy):
        self._tuple_to_batch_converter.add_policy(policy)

    def main_loop(self):
        next_entry = self.interruptible_get()
        logger.debug(f"received a message")
        if isinstance(next_entry, InputTuple):
            logger.debug(f"received a InputTuple")
            self.pass_tuple_to_downstream(next_entry.tuple)
        elif isinstance(next_entry, ControlElement):
            logger.debug(f"received a ControlElement")
            payload = get_oneof(next_entry.cmd)
            if isinstance(payload, ControlInvocation):
                logger.debug("it's control invocation")
                command = get_oneof(payload.command)
                if isinstance(command, AddOutputPolicy):
                    self.add_policy(command.policy)

    def pass_tuple_to_downstream(self, tuple_: ITuple):
        for to, batch in self._tuple_to_batch_converter.tuple_to_batch(tuple_):
            self.send_batch(to, batch)

    def send_batch(self, to: ActorVirtualIdentity, batch: DataPayload) -> None:
        # TODO: add to information to the batch
        table = Table.from_pandas(DataFrame.from_records(batch))
        self._rpc_client.send_data(table)
