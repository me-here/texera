from loguru import logger
from pyarrow.lib import Table

from core import Tuple
from core.models.internal_queue import InternalQueue, ControlElement, InputDataElement
from core.models.payload import DataFrame, EndOfUpstream
from core.util import StoppableThread
from edu.uci.ics.amber.engine.common import WorkflowControlMessage, ActorVirtualIdentity
from proxy import ProxyServer


class NetworkReceiver(StoppableThread):
    def __init__(self, shared_queue: InternalQueue, host: str, port: int):
        super().__init__(self.__class__.__name__)
        self._proxy_server = ProxyServer(host=host, port=port)

        def data_handler(from_: ActorVirtualIdentity, table: Table):

            if table is not None:
                # logger.info(f"getting a data payload {table} from {from_}")

                shared_queue.put(InputDataElement(batch=DataFrame([Tuple(row) for i, row in table.to_pandas().iterrows()])
                                                  , from_=from_))
            else:
                # logger.info(f"getting an end of stream from {from_}")

                shared_queue.put(
                    InputDataElement(batch=EndOfUpstream(), from_=from_))

        self._proxy_server.register_data_handler(data_handler)
        self._proxy_server.register("health_check", ProxyServer.ack()(lambda: None))

        def control_handler(workflow_control_message: WorkflowControlMessage):
            shared_queue.put(
                ControlElement(workflow_control_message.payload, workflow_control_message.from_))

        def control_deserializer(message: bytes) -> WorkflowControlMessage:
            workflow_control_message = WorkflowControlMessage().parse(message)
            # logger.info(f"serialized to \n{workflow_control_message}")
            return workflow_control_message

        # def control_serializer(workflow_control_message: WorkflowControlMessage) -> bytes:
        #     return workflow_control_message.SerializeToString()·

        self._proxy_server.register_control_handler(control_handler, control_deserializer)

    def register_shutdown(self, shutdown: callable) -> None:
        self._proxy_server.register("shutdown", shutdown)

    @logger.catch
    def run(self) -> None:
        self._proxy_server.serve()

    def stop(self):
        self._proxy_server.shutdown()
        self._proxy_server.wait()
        super(NetworkReceiver, self).stop()
