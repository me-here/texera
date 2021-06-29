from core.models.tuple import ITuple
from core.util.stable_priority_queue import StablePriorityQueue, QueueElement
from dataclasses import dataclass

from edu.uci.ics.amber.engine.common import ControlPayload, ActorVirtualIdentity


class InternalQueue(StablePriorityQueue):
    pass


@dataclass
class InternalQueueElement(QueueElement):
    pass


@dataclass
class InputDataElement(InternalQueueElement):
    batch: list[ITuple]
    from_: ActorVirtualIdentity
    _priority: int = 1


@dataclass
class OutputDataElement(InternalQueueElement):
    batch: list[ITuple]
    to: ActorVirtualIdentity
    _priority: int = 1


@dataclass
class ControlElement(InternalQueueElement):
    cmd: ControlPayload
    from_: ActorVirtualIdentity
    _priority: int = 0
