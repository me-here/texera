import pandas
from pyarrow import Table

from core.models.internal_queue import InternalQueue, ControlElement, OutputDataElement
from core.models.payload import DataFrame, EndOfUpstream
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

        if isinstance(next_entry, OutputDataElement):
            self.send_data(next_entry.to, next_entry.payload)
        elif isinstance(next_entry, ControlElement):
            self.send_control(next_entry.from_, next_entry.cmd)

        # TODO: handle else

    def send_data(self, to: ActorVirtualIdentity, data_payload: DataPayload) -> None:

        if isinstance(data_payload, DataFrame):
            table = Table.from_pandas(pandas.DataFrame.from_records([t.row for t in data_payload.frame]))

            self._proxy_client.send_data(to.SerializeToString(), table)
        elif isinstance(data_payload, EndOfUpstream):
            self._proxy_client.send_data(to.SerializeToString(), None)

    def send_control(self, to: ActorVirtualIdentity, cmd: ControlPayload):
        workflow_control_message = WorkflowControlMessage(from_=to, sequence_number=1, payload=cmd)
        self._proxy_client.call("control", workflow_control_message.SerializeToString())
