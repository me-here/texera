package edu.uci.ics.amber.engine.architecture.worker
import edu.uci.ics.amber.engine.common.tuple.ITuple

case class WorkerStatistics(
    workerState: WorkerState.Value,
    inputRowCount: Long,
    outputRowCount: Long,
    outputResults: Option[List[ITuple]] // in case of a sink operator
)
