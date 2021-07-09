"""
if (stateManager.getCurrentState == Paused()) {
      if (pauseManager.isPaused) {
        pauseManager.resume()
      }
      dataProcessor.enableDataQueue()
      stateManager.transitTo(Running())
    }
    stateManager.getCurrentState
"""
from core.architecture.handlers.handler import Handler
from core.architecture.manager.context import Context
from edu.uci.ics.amber.engine.architecture.worker import ResumeWorker, WorkerStateInfo
from edu.uci.ics.amber.engine.common import Running, Paused


class ResumeWorkerHandler(Handler):
    def __call__(self, context: Context, command: ResumeWorker, *args, **kwargs):
        if context.state_manager.confirm_states(Paused()):
            if context.pause_manager.is_paused():
                context.pause_manager.resume()
            # dataProcessor.enableDataQueue()
            context.state_manager.transit_to(Running())
        state = context.state_manager.get_current_state()
        return WorkerStateInfo(state)
