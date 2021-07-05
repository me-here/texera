from abc import ABC

from core.architecture.manager.context import Context
from edu.uci.ics.amber.engine.architecture.worker import ControlCommand


class Handler(ABC):
    def __init__(self, cmd_type: type):
        self.cmd_type = cmd_type

    def __call__(self, context: Context, command: ControlCommand, *args, **kwargs):
        pass
