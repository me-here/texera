package edu.uci.ics.amber.engine.architecture.worker

import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager.WorkerState
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.texera.workflow.common.IncrementalOutputMode

case class WorkerStatistics(
    workerId: ActorVirtualIdentity,
    workerState: WorkerState,
    inputRowCount: Long,
    outputRowCount: Long
)

case class WorkerResult(
    workerId: ActorVirtualIdentity,
    outputMode: IncrementalOutputMode,
    result: List[ITuple]
)
