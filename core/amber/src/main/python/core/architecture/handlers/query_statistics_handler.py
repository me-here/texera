from edu.uci.ics.amber.engine.architecture.worker import QueryStatistics, WorkerStatistics
from .handler_base import Handler
from ..managers.context import Context


class QueryStatisticsHandler(Handler):
    cmd = QueryStatistics

    def __call__(self, context: Context, command: QueryStatistics, *args, **kwargs):
        input_count, output_count = context.statistics_manager.get_statistics()
        state = context.state_manager.get_current_state()

        return WorkerStatistics(worker_state=state, input_row_count=input_count, output_row_count=output_count)
