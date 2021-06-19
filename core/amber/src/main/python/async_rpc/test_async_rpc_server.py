from asyncio import sleep

import pytest

from async_rpc.async_rpc_server import AsyncRPCServer
from edu.uci.ics.amber.engine.architecture.worker.promisehandler2_pb2 import ControlCommand, Pause
from edu.uci.ics.amber.engine.common.virtualidentity_pb2 import ActorVirtualIdentity
from worker.architecture.handlers.handler import Handler


class TestAsyncRPCServer:
    @pytest.fixture
    def server(self):
        control_output_port = None
        return AsyncRPCServer(control_output_port)

    @pytest.mark.asyncio
    async def test_a(self, server):
        class EchoHandler(Handler):
            def __call__(self, cmd: ControlCommand, sender_id: ActorVirtualIdentity):
                return cmd

        server.register_handler(EchoHandler(cmd_type=Pause))
        server.receive(Pause(), ActorVirtualIdentity())


        await sleep(1)
