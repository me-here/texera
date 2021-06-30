from edu.uci.ics.amber.engine.architecture.worker import WorkerStatistics
from edu.uci.ics.amber.engine.common import WorkerState, Running
from .handler import Handler
from ...util.proto_helper import set_oneof


class QueryStatisticsHandler(Handler):
    def __call__(self, *args, **kwargs):
        worker_state = set_oneof(WorkerState, Running())
        return WorkerStatistics(worker_state=worker_state, input_row_count=0, output_row_count=1)
