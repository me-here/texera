from dataclasses import dataclass

from core.models.marker import Marker
from core.models.payload import DataPayload
from core.util.queue.double_blocking_queue import DoubleBlockingQueue
from core.util.queue.queue_base import IQueue
from edu.uci.ics.amber.engine.common import ActorVirtualIdentity, ControlPayload


class InternalQueue(DoubleBlockingQueue):
    def __init__(self):
        super().__init__(InputDataElement, OutputDataElement, Marker)


@dataclass
class InternalQueueElement(IQueue.QueueElement):
    pass


@dataclass
class InputDataElement(InternalQueueElement):
    payload: DataPayload
    from_: ActorVirtualIdentity


@dataclass
class OutputDataElement(InternalQueueElement):
    payload: DataPayload
    to: ActorVirtualIdentity


@dataclass
class ControlElement(InternalQueueElement):
    cmd: ControlPayload
    from_: ActorVirtualIdentity
