from dataclasses import dataclass
from typing_extensions import Protocol, T


@dataclass
class QueueElement:
    pass


@dataclass
class QueueControl(QueueElement):
    msg: str


class Queue(Protocol):
    def get(self) -> T:
        pass

    def put(self, item: T) -> None:
        pass

    def empty(self) -> bool:
        pass
