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
  QueryStatistics,
  QueryWorkerResult
}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.texera.web.model.event.WebOperatorResult

import scala.collection.mutable

object QueryWorkerStatisticsHandler {

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

  registerHandler { (msg: ControllerInitiateQueryStatistics, sender) =>
    {
      // send to specified workers (or all workers by default)
      val workers = msg.workers.getOrElse(workflow.getAllWorkers).toList

      // send QueryStatistics message
      val requests =
        workers.map(worker => send(QueryStatistics(), worker).map(res => (worker, res)))

      // wait for all workers to reply
      val allResponses = Future.collect(requests)

      // update statistics and notify frontend
      allResponses.map(responses => {
        responses.foreach(res => {
          val (worker, stats) = res
          workflow.getOperator(worker).getWorker(worker).stats = stats
        })
        updateFrontendWorkflowStatus()
      })
    }
  }

  registerHandler((msg: ControllerInitiateQueryResults, sender) => {
    val sinkWorkers = workflow.getSinkLayers.flatMap(l => l.workers.keys).toList
    val workers = msg.workers.getOrElse(sinkWorkers)

    // send all sink worker QueryResult message
    val requests = workers.map(worker => {
      send(QueryWorkerResult(), worker).map(res => (worker, res))
    })

    // wait for all workers to reply, accumulate response from all workers
    val allResponses = Future.collect(requests)

    allResponses.map(responses => {
      // combine results of all workers to a single result list of this operator
      val operatorResultUpdate = new mutable.HashMap[String, OperatorResult]()
      responses
        .groupBy(workerResult => workflow.getOperator(workerResult._1).id)
        .foreach(operatorResult => {
          val workerResultList = operatorResult._2.flatMap(r => r._2)
          if (workerResultList.nonEmpty) {
            val operatorID = operatorResult._1.operator
            val outputMode = workerResultList.head.outputMode
            operatorResultUpdate(operatorID) =
              OperatorResult(outputMode, workerResultList.flatMap(r => r.result).toList)
          }
        })
      // send update result to frontend
      if (operatorResultUpdate.nonEmpty) {
        updateFrontendWorkflowResult(WorkflowResultUpdate(operatorResultUpdate.toMap))
      }
    })
  })
}
