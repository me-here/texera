package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.{FusedWorkerStatistics, WorkerAsyncRPCHandlerInitializer, WorkerStatistics}
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.QueryStatisticsHandler.QueryStatistics
import edu.uci.ics.amber.engine.common.ITupleSinkOperatorExecutor
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}

object QueryStatisticsHandler {
  final case class QueryStatistics() extends ControlCommand[Either[WorkerStatistics,FusedWorkerStatistics]]
}

trait QueryStatisticsHandler {
  this: WorkerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: QueryStatistics, sender) =>
    val state = stateManager.getCurrentState
    val arr = dataProcessor.collectStatistics()
    if(arr.length == 1){
      val result = operator match {
        case sink: ITupleSinkOperatorExecutor =>
          Option(sink.getResultTuples())
        case _ =>
          Option.empty
      }
      Left(WorkerStatistics(state, arr.head._1, arr.head._2, result))
    }else{
      Right(FusedWorkerStatistics(state, arr))
    }
  }
}
