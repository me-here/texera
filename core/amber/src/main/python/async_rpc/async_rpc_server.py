from asyncio import Future

import asyncio
from loguru import logger
from typing import Any, Dict

from edu.uci.ics.amber.engine.architecture.worker.promisehandler2_pb2 import ControlCommand
from edu.uci.ics.amber.engine.common.ambermessage2_pb2 import ControlInvocation
from edu.uci.ics.amber.engine.common.virtualidentity_pb2 import ActorVirtualIdentity
from edu.uci.ics.amber.engine.common.ambermessage2_pb2 import ReturnPayload
from worker.architecture.handlers.handler import Handler


class AsyncRPCServer:
    def __init__(self, control_output_port):
        self.control_output_port = control_output_port
        self.handlers: Dict[type, Handler] = dict()

    def register_handler(self, newHandler: Handler):
        self.handlers[newHandler.cmd_type] = newHandler

        logger.debug(f"registered a handler with control message {newHandler.cmd_type}")
    def receive(self, control: ControlInvocation, sender_id: ActorVirtualIdentity):
        try:
            future: Future = self.execute(control.command, sender_id)
            future.add_done_callback(lambda ret: self.return_result(sender_id, control.commandID, ret))
        except:
            pass

    def execute(self, command: ControlCommand, sender_id: ActorVirtualIdentity) -> Future:
        handler = self.find_handler(command)
        return asyncio.create_task(handler(command, sender_id))

    def return_result(self, sender: ActorVirtualIdentity, id: int, ret: Any) -> None:
        print("getting a result!")
        if self.no_reply_needed(id):
            return
        else:
            self.control_output_port.send(sender, ReturnPayload(id, ret))

    @staticmethod
    def no_reply_needed(id: int) -> bool:
        return id < 0

    def find_handler(self, command: ControlCommand) -> Handler:
        handler = self.handlers.get(type(command))
        if not handler:
            raise NotImplementedError
        return handler
