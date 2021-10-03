package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.worker.WorkerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.QueryNextOpLoadMetricsHandler.{
  FutureLoadMetrics,
  QueryFutureLoadMetrics,
  QueryNextOpLoadMetrics,
  WorkloadHistory
}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

// join-skew research related.
object QueryNextOpLoadMetricsHandler {
  final case class FutureLoadMetrics(
      dataToSend: Map[ActorVirtualIdentity, Long]
  ) // join-skew research related.
  final case class WorkloadHistory(
      history: mutable.HashMap[ActorVirtualIdentity, ArrayBuffer[Long]]
  )
  final case class QueryNextOpLoadMetrics() extends ControlCommand[(FutureLoadMetrics, WorkloadHistory)]
  final case class QueryFutureLoadMetrics() extends ControlCommand[FutureLoadMetrics]
}

trait QueryNextOpLoadMetricsHandler {
  this: WorkerAsyncRPCHandlerInitializer =>

  registerHandler { (query: QueryNextOpLoadMetrics, sender) =>
    // workerStateManager.shouldBe(Running, Ready, Pausing, Paused, Completed)
    tupleToBatchConverter.recordHistory()
    val f1: Future[FutureLoadMetrics] = sendToNetworkCommActor(QueryFutureLoadMetrics())
    val f2: Future[WorkloadHistory] =
      Future(WorkloadHistory(tupleToBatchConverter.getWorkloadHistory()))
    Future.join(f1, f2)
  }
}
