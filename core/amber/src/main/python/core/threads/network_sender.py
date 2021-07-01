import pandas
from loguru import logger
from pyarrow import Table

from core.models.internal_queue import InternalQueue, ControlElement, OutputDataElement
from core.models.payload import DataFrame
from core.util.stoppable_queue_blocking_thread import StoppableQueueBlockingThread
from edu.uci.ics.amber.engine.common import ActorVirtualIdentity, ControlPayload, WorkflowControlMessage, DataPayload
from proxy import ProxyClient


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
            self.send_control(next_entry.from_, next_entry.cmd)

    def send_batch(self, to: ActorVirtualIdentity, batch: DataPayload) -> None:
        def chunks(lst, n):
            """Yield successive n-sized chunks from lst."""
            for i in range(0, len(lst), n):
                yield lst[i:i + n]

        to.SerializeToString()
        if isinstance(batch, DataFrame):
            for chunk in chunks(batch.frame, 100):
                table = Table.from_pandas(pandas.DataFrame.from_records([t.row for t in chunk]))
                self._proxy_client.send_data(to.SerializeToString(), table)

    def send_control(self, to: ActorVirtualIdentity, cmd: ControlPayload):
        workflow_control_message = WorkflowControlMessage(from_=to, sequence_number=1, payload=cmd)
        ret = self._proxy_client.call("control", workflow_control_message.SerializeToString())
        logger.info(f"return: {ret}")
