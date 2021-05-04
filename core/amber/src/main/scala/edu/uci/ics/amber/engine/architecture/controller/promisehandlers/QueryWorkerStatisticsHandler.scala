package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.WorkflowResultUpdate
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.QueryWorkerStatisticsHandler.{
  ControllerInitiateQueryResults,
  ControllerInitiateQueryStatistics
}
import edu.uci.ics.amber.engine.architecture.principal.OperatorResult
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.QueryStatisticsHandler.{
  QueryWorkerResult,
  QueryWorkerStatistics
}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

import scala.collection.mutable

object QueryWorkerStatisticsHandler {
  // ask the controller to initiate querying worker statistics
  // optionally specify the workers to query, None indicates querying all workers
  final case class ControllerInitiateQueryStatistics(workers: Option[List[ActorVirtualIdentity]])
      extends ControlCommand[Unit]

  // ask the controller to initiate querying worker results
  // optionally specify the workers to query, None indicates querying all sink workers
  final case class ControllerInitiateQueryResults(workers: Option[List[ActorVirtualIdentity]])
      extends ControlCommand[Unit]
}

/** Get statistics from all the workers
  *
  * possible sender: controller(by statusUpdateAskHandle)
  */
trait QueryWorkerStatisticsHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  registerHandler((msg: ControllerInitiateQueryStatistics, sender) => {

    val workers = msg.workers.getOrElse(workflow.getAllWorkers.toList)

    // send all worker QueryStatistics message
    val requests = workers.map(worker => {
      send(QueryWorkerStatistics(), worker)
        .onSuccess(stats => {
          workflow.getWorkerInfo(stats.workerId).state = stats.workerState
          workflow.getWorkerInfo(stats.workerId).stats = stats
          // update the frontend statistics quickly after a worker responded
          // since sending statistics is a cheap operation
          updateFrontendWorkflowStatus()
        })
    })

    // request is considered complete after waiting for all workers to reply
    Future.collect(requests).map(_ => {})
  })

  registerHandler((msg: ControllerInitiateQueryResults, sender) => {
    val sinkWorkers = workflow.getSinkLayers.flatMap(l => l.workers.keys).toList

    val workers = msg.workers.getOrElse(sinkWorkers)

    // send all sink worker QueryResult message
    val requests = workers.map(worker => {
      send(QueryWorkerResult(), worker)
    })

    // wait for all reply and collect all response, then send update result to frontend
    // since sending result is heavy, we want to accumulate response from all workers
    Future
      .collect(requests)
      .map(res => {
        val operatorResultUpdate = new mutable.HashMap[String, OperatorResult]()
        res.flatten
          .groupBy(r => workflow.getOperator(r.workerId).id)
          .foreach(e => {
            val opId = e._1.operator
            val resultsPerWorker = e._2
            val outputMode = resultsPerWorker.head.outputMode
            val results = resultsPerWorker.flatMap(r => r.result).toList
            operatorResultUpdate(opId) = OperatorResult(outputMode, results)
          })
        updateFrontendWorkflowResult(WorkflowResultUpdate(operatorResultUpdate.toMap))
      })
  })
}
