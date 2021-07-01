from edu.uci.ics.amber.engine.architecture.worker import WorkerStatistics, QueryStatistics
from .handler import Handler
from ..manager.context import Context


class QueryStatisticsHandler(Handler):
    def __call__(self, context: Context, command: QueryStatistics, *args, **kwargs):
        input_count, output_count = context.statistics_manager.get_statistics()
        state = context.state_manager.get_current_state()

        return WorkerStatistics(worker_state=state, input_row_count=input_count, output_row_count=output_count)
