from edu.uci.ics.amber.engine.common.ambermessage2_pb2 import WorkflowControlMessage
from loguru import logger
from pyarrow import Table

from proxy import ProxyServer
from worker.models.internal_queue import InternalQueue, InputTuple, ControlElement
from worker.util import StoppableThread


class NetworkReceiver(StoppableThread):
    def __init__(self, shared_queue: InternalQueue, host: str, port: int):
        super().__init__(self.__class__.__name__)
        self._rpc_server = ProxyServer(host=host, port=port)

        def data_handler(batch: Table):
            for index, row in batch.to_pandas().iterrows():
                shared_queue.put(InputTuple(tuple=row))

        self._rpc_server.register_data_handler(data_handler)
        self._rpc_server.register("health_check", ProxyServer.ack()(lambda: None))

        def control_handler(workflow_control_message: WorkflowControlMessage):
            shared_queue.put(
                ControlElement(workflow_control_message.payload, getattr(workflow_control_message, "from")))

        def control_deserializer(message: bytes) -> WorkflowControlMessage:
            workflow_control_message = WorkflowControlMessage()
            workflow_control_message.ParseFromString(message)
            return workflow_control_message

        # def control_serializer(workflow_control_message: WorkflowControlMessage) -> bytes:
        #     return workflow_control_message.SerializeToString()

        self._rpc_server.register_control_handler(control_handler, control_deserializer)

    def register_shutdown(self, shutdown: callable) -> None:
        self._rpc_server.register("shutdown", shutdown)

    @logger.catch
    def run(self) -> None:
        self._rpc_server.serve()

    def stop(self):
        self._rpc_server.shutdown()
        self._rpc_server.wait()
        super(NetworkReceiver, self).stop()
