from abc import ABC

from core.architecture.managers.context import Context
from edu.uci.ics.amber.engine.architecture.worker import ControlCommand


class Handler(ABC):
    cmd: ControlCommand = None

    def __call__(self, context: Context, command: ControlCommand, *args, **kwargs):
        pass
