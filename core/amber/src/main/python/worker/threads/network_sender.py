from edu.uci.ics.amber.engine.architecture.sendsemantics.datatransferpolicy2_pb2 import DataSendingPolicy
from edu.uci.ics.amber.engine.architecture.worker.promisehandler2_pb2 import AddOutputPolicy
from edu.uci.ics.amber.engine.common.ambermessage2_pb2 import ControlInvocation
from edu.uci.ics.amber.engine.common.virtualidentity_pb2 import ActorVirtualIdentity
from loguru import logger
from pandas import DataFrame
from pyarrow import Table

from proxy import ProxyClient
from worker.architecture.messaginglayer.tuple_to_batch_converter import TupleToBatchConverter
from worker.models.internal_queue import InternalQueue, InputTuple, ControlElement
from worker.models.payload import DataPayload
from worker.models.tuple import ITuple
from worker.util.stoppable_queue_blocking_thread import StoppableQueueBlockingThread


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
            self.pass_tuple_to_downstream(next_entry.tuple)
        elif isinstance(next_entry, ControlElement):
            if isinstance(next_entry.cmd, ControlInvocation):
                if isinstance(next_entry.cmd.command, AddOutputPolicy):
                    self.add_policy(next_entry.cmd.policy)

    def pass_tuple_to_downstream(self, tuple_: ITuple):
        for to, batch in self._tuple_to_batch_converter.tuple_to_batch(tuple_):
            self.send_batch(to, batch)

    def send_batch(self, to: ActorVirtualIdentity, batch: DataPayload) -> None:
        # TODO: add to information to the batch
        table = Table.from_pandas(DataFrame.from_records(batch))
        self._rpc_client.send_data(table)
