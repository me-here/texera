"""
 if (stateManager.confirmState(Running(), Ready())) {
      pauseManager.pause()
      dataProcessor.disableDataQueue()
      stateManager.transitTo(Paused())
    }
    stateManager.getCurrentState
"""
from core.architecture.handlers.handler_base import Handler
from core.architecture.managers.context import Context
from edu.uci.ics.amber.engine.architecture.worker import PauseWorker, WorkerStateInfo
from edu.uci.ics.amber.engine.common import Running, Ready, Paused


class PauseWorkerHandler(Handler):
    cmd = PauseWorker

    def __call__(self, context: Context, command: PauseWorker, *args, **kwargs):
        if context.state_manager.confirm_states([Running(), Ready()]):
            context.pause_manager.pause()
            context.dp._input_queue.disable_slave()
            context.state_manager.transit_to(Paused())
        state = context.state_manager.get_current_state()
        return WorkerStateInfo(state)
