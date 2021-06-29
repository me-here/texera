from core.models.internal_queue import InternalQueue, ControlElement, OutputDataElement
from core.models.tuple import ITuple
from core.util.stoppable_queue_blocking_thread import StoppableQueueBlockingThread
from loguru import logger
from pandas import DataFrame
from proxy import ProxyClient
from pyarrow import Table

from edu.uci.ics.amber.engine.common import ActorVirtualIdentity, ControlPayload, WorkflowControlMessage


class NetworkSender(StoppableQueueBlockingThread):
    def __init__(self, shared_queue: InternalQueue, host: str, port: int):
        super().__init__(self.__class__.__name__, queue=shared_queue)
        self._proxy_client = ProxyClient(host=host, port=port)
        self._batch = list()

    def main_loop(self):
        next_entry = self.interruptible_get()
        logger.debug(f"received a message")
        if isinstance(next_entry, OutputDataElement):
            logger.debug(f"received a InputDataElement")
            self.send_batch(next_entry.to, next_entry.batch)
        elif isinstance(next_entry, ControlElement):
            logger.debug(f"NETWORK received a ControlElement {next_entry}")
            # payload = get_oneof(next_entry.cmd)
            # if isinstance(payload, ControlInvocation):
            #
            #     logger.debug("it's control invocation")
            #     command = get_oneof(payload.command)
            #     self.send_control()
            # elif isinstance(payload, ReturnPayload):
            self.send_control(next_entry.from_, next_entry.cmd)

    def send_batch(self, to: ActorVirtualIdentity, batch: list[ITuple]) -> None:
        # TODO: add to information to the batch
        table = Table.from_pandas(DataFrame.from_records(batch))
        self._proxy_client.send_data(table)

    def send_control(self, to: ActorVirtualIdentity, cmd: ControlPayload):
        workflow_control_message = WorkflowControlMessage(from_=to, sequence_number=1, payload=cmd)
        logger.info(
            f"serialized workflow control message {workflow_control_message}, {workflow_control_message.SerializeToString()}")
        ret = self._proxy_client.call("control", workflow_control_message.SerializeToString())
        logger.info(f"return: {ret}")
