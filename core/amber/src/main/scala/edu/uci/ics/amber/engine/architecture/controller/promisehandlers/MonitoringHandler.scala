package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.MonitoringHandler.{
  ControllerInitiateMonitoring,
  previousCallFinished
}
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.MonitoringHandler.QuerySelfWorkloadMetrics
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

import scala.collection.mutable

object MonitoringHandler {
  var previousCallFinished = true
  var startTimeForMetricColl: Long = _
  var endTimeForMetricColl: Long = _

  final case class ControllerInitiateMonitoring(
      filterByWorkers: Option[List[ActorVirtualIdentity]] = None
  ) extends ControlCommand[Unit]
}

trait MonitoringHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  registerHandler((msg: ControllerInitiateMonitoring, sender) => {
    if (!previousCallFinished) {
      Future.Done
    } else {
      previousCallFinished = false
      // send to specified workers (or all workers by default)
      val workers = msg.filterByWorkers.getOrElse(workflow.getAllWorkers).toList

      // send Monitoring message
      val requests = workers.map(worker =>
        send(QuerySelfWorkloadMetrics(), worker).map(metric => {
          workflow.getOperator(worker).getWorkerWorkloadInfo(worker).dataInputWorkload =
            metric.unprocessedDataInputQueueSize + metric.stashedDataInputQueueSize
          workflow.getOperator(worker).getWorkerWorkloadInfo(worker).controlInputWorkload =
            metric.unprocessedControlInputQueueSize + metric.stashedControlInputQueueSize
          println(s"${worker.toString()} - - - - ${workflow.getOperator(worker).getWorkerWorkloadInfo(worker).dataInputWorkload} ---- ${workflow.getOperator(worker).getWorkerWorkloadInfo(worker).controlInputWorkload}")
        })
      )

      Future.collect(requests).onSuccess(seq => previousCallFinished = true).unit
    }
  })
}
