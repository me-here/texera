package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.WorkerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.QueryNextOpLoadMetricsHandler.QueryNextOpLoadMetrics
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

// join-skew research related.
object QueryNextOpLoadMetricsHandler {
  final case class FutureLoadMetrics(
      dataToSend: Map[ActorVirtualIdentity, Long]
  ) // join-skew research related.
  final case class QueryNextOpLoadMetrics() extends ControlCommand[FutureLoadMetrics]
}

trait QueryNextOpLoadMetricsHandler {
  this: WorkerAsyncRPCHandlerInitializer =>

  registerHandler { (query: QueryNextOpLoadMetrics, sender) =>
    // workerStateManager.shouldBe(Running, Ready, Pausing, Paused, Completed)
    sendToNetworkCommActor(QueryNextOpLoadMetrics())
  }
}
