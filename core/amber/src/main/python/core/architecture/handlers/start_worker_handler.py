from loguru import logger

from edu.uci.ics.amber.engine.architecture.worker import StartWorker, WorkerStateInfo
from edu.uci.ics.amber.engine.common import Running
from .handler_base import Handler
from ..managers.context import Context
from ...models.marker import EndMarker, EndOfAllMarker


class StartWorkerHandler(Handler):
    cmd = StartWorker

    def __call__(self, context: Context, command: StartWorker, *args, **kwargs):
        if context.dp._udf_is_source:
            context.state_manager.transit_to(Running())
            context.dp._input_queue.put(EndMarker())
            context.dp._input_queue.put(EndOfAllMarker())

        state = context.state_manager.get_current_state()
        return WorkerStateInfo(state)
