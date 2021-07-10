import pandas
from pyarrow import Table
from pyarrow.lib import Schema, schema

from core.models.internal_queue import InternalQueue, ControlElement, OutputDataElement
from core.models.payload import DataFrame, EndOfUpstream
from core.util.stoppable_queue_blocking_thread import StoppableQueueBlockingThread
from edu.uci.ics.amber.engine.common import ActorVirtualIdentity, ControlPayload, WorkflowControlMessage, DataPayload, \
    DataHeader
from proxy import ProxyClient


class NetworkSender(StoppableQueueBlockingThread):
    def __init__(self, shared_queue: InternalQueue, host: str, port: int, schema_map: dict[str, type]):
        super().__init__(self.__class__.__name__, queue=shared_queue)
        self._proxy_client = ProxyClient(host=host, port=port)
        self._batch = list()
        self.schema_map = schema_map

    def main_loop(self):
        next_entry = self.interruptible_get()

        if isinstance(next_entry, OutputDataElement):
            self.send_data(next_entry.to, next_entry.payload)
        elif isinstance(next_entry, ControlElement):
            self.send_control(next_entry.from_, next_entry.cmd)

        # TODO: handle else

    def send_data(self, to: ActorVirtualIdentity, data_payload: DataPayload) -> None:
        if isinstance(data_payload, DataFrame):
            df = pandas.DataFrame.from_records([tuple_.as_series() for tuple_ in data_payload.frame])
            inferred_schema: Schema = Schema.from_pandas(df)
            # create a output schema, use the original input schema if possible
            output_schema = schema([self.schema_map.get(field.name, field) for field in inferred_schema])
            data_header = DataHeader(from_=to, is_end=False)
            table = Table.from_pandas(df, output_schema)
            self._proxy_client.send_data(bytes(data_header), table)
        elif isinstance(data_payload, EndOfUpstream):
            data_header = DataHeader(from_=to, is_end=True)
            self._proxy_client.send_data(bytes(data_header), None)

        # TODO: handle else

    def send_control(self, to: ActorVirtualIdentity, cmd: ControlPayload):
        workflow_control_message = WorkflowControlMessage(from_=to, sequence_number=1, payload=cmd)
        self._proxy_client.call("control", workflow_control_message.SerializeToString())
