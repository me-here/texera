from typing import Dict

from loguru import logger
from overrides import overrides
from pyarrow.lib import Table

from core.models import ControlElement, DataElement, DataFrame, EndOfUpstream, InternalQueue, Tuple
from core.proxy import ProxyServer
from core.util import Stoppable
from core.util.runnable.runnable import Runnable
from proto.edu.uci.ics.amber.engine.common import PythonControlMessage, PythonDataHeader


class NetworkReceiver(Runnable, Stoppable):
    """
    Receive and deserialize messages.
    """

    def __init__(self, shared_queue: InternalQueue, host: str, port: int, schema_map: Dict[str, type]):
        self._proxy_server = ProxyServer(host=host, port=port)

        # register the data handler to deserialize data messages.
        @logger.catch(reraise=True)
        def data_handler(command: bytes, table: Table):
            # TODO: OPTIMIZE:
            #   change the API to use pandas.DataFrame instead of pyarrow.Table
            data_header = PythonDataHeader().parse(command)
            if not data_header.is_end:
                input_schema = table.schema
                # record input schema
                for field in input_schema:
                    schema_map[field.name] = field
                shared_queue.put(DataElement(
                    tag=data_header.tag,
                    payload=DataFrame([Tuple(row) for _, row in table.to_pandas().iterrows()])
                ))
            else:
                shared_queue.put(DataElement(tag=data_header.tag, payload=EndOfUpstream()))

        self._proxy_server.register_data_handler(data_handler)

        # register the control handler to deserialize control messages.
        @logger.catch(reraise=True)
        def control_handler(message: bytes):
            python_control_message = PythonControlMessage().parse(message)
            shared_queue.put(ControlElement(tag=python_control_message.tag, payload=python_control_message.payload))

        self._proxy_server.register_control_handler(control_handler)

    def register_shutdown(self, shutdown: callable) -> None:
        self._proxy_server.register("shutdown", ProxyServer.ack(msg="Bye bye!")(shutdown))

    @logger.catch(reraise=True)
    @overrides
    def run(self) -> None:
        self._proxy_server.serve()

    @logger.catch(reraise=True)
    @overrides
    def stop(self):
        self._proxy_server.shutdown()
        self._proxy_server.wait()
