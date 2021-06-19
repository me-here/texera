from abc import ABC


class Handler(ABC):
    def __init__(self, cmd_type: type):
        self.cmd_type = cmd_type

    def __call__(self, *args, **kwargs):
        pass
