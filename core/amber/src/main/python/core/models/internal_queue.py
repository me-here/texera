from dataclasses import dataclass

from core.models.payload import DataPayload
from core.util.stable_priority_queue import StablePriorityQueue, QueueElement
from edu.uci.ics.amber.engine.common import ControlPayload, ActorVirtualIdentity


class InternalQueue(StablePriorityQueue):
    pass


@dataclass
class InternalQueueElement(QueueElement):
    pass


@dataclass
class InputDataElement(InternalQueueElement):
    batch: DataPayload
    from_: ActorVirtualIdentity
    _priority: int = 1


@dataclass
class OutputDataElement(InternalQueueElement):
    batch: DataPayload
    to: ActorVirtualIdentity
    _priority: int = 1


@dataclass
class ControlElement(InternalQueueElement):
    cmd: ControlPayload
    from_: ActorVirtualIdentity
    _priority: int = 0
