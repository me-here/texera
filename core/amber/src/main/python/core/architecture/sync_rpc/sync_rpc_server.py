from core.architecture.handlers.add_output_policy_handler import AddOutputPolicyHandler
from core.architecture.handlers.handler import Handler
from core.architecture.handlers.update_input_linking_handler import UpdateInputLinkingHandler
from core.models.internal_queue import InternalQueue, ControlElement
from core.util.proto_helper import get_oneof
from loguru import logger

from edu.uci.ics.amber.engine.architecture.worker import ControlCommand, AddOutputPolicy, UpdateInputLinking
from edu.uci.ics.amber.engine.common import ActorVirtualIdentity, ReturnPayload, ControlInvocation


class SyncRPCServer:
    def __init__(self, output_queue: InternalQueue):
        self._output_queue = output_queue
        self._handlers: dict[type(ControlCommand), Handler] = dict()
        self.register(AddOutputPolicyHandler(AddOutputPolicy))
        self.register(UpdateInputLinkingHandler(UpdateInputLinking))

    def receive(self, control_invocation: ControlInvocation, from_: ActorVirtualIdentity):
        command = get_oneof(control_invocation.command)
        logger.info(f"type: {type(command)}")
        handler = self._handlers[type(command)]
        result: ControlCommand = handler()
        self._output_queue.put(ControlElement(from_=from_,
                                              cmd=ReturnPayload(original_command_id=control_invocation.command_id,
                                                                return_value=result)))

    def register(self, handler: Handler):
        self._handlers[handler.cmd_type] = handler
