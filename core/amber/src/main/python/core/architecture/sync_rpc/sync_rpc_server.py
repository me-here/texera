from loguru import logger

from core.architecture.handlers.add_output_policy_handler import AddOutputPolicyHandler
from core.architecture.handlers.handler import Handler
from core.architecture.handlers.query_statistics_handler import QueryStatisticsHandler
from core.architecture.handlers.update_input_linking_handler import UpdateInputLinkingHandler
from core.architecture.manager.context import Context
from core.models.internal_queue import InternalQueue, ControlElement
from core.util.proto_helper import get_oneof, set_oneof
from edu.uci.ics.amber.engine.architecture.worker import ControlCommand, AddOutputPolicy, UpdateInputLinking, \
    QueryStatistics
from edu.uci.ics.amber.engine.common import ActorVirtualIdentity, ReturnPayload, ControlInvocation, ControlPayload


class SyncRPCServer:
    def __init__(self, output_queue: InternalQueue, context: Context):
        self._output_queue = output_queue
        self._handlers: dict[type(ControlCommand), Handler] = dict()
        self.register(AddOutputPolicyHandler(AddOutputPolicy))
        self.register(UpdateInputLinkingHandler(UpdateInputLinking))
        self.register(QueryStatisticsHandler(QueryStatistics))
        self._context = context

    def receive(self, control_invocation: ControlInvocation, from_: ActorVirtualIdentity):
        command = get_oneof(control_invocation.command)
        logger.info(f"PYTHON receive a CONTROL: {control_invocation}")
        handler = self._handlers[type(command)]
        result: ControlCommand = set_oneof(ControlCommand, handler(self._context, command))

        payload = set_oneof(ControlPayload,
                            ReturnPayload(original_command_id=control_invocation.command_id,
                                          return_value=result))

        logger.info(f"returning control {payload}")
        self._output_queue.put(ControlElement(from_=from_, cmd=payload))

    def register(self, handler: Handler):
        self._handlers[handler.cmd_type] = handler
