package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.WorkflowCompleted
import edu.uci.ics.amber.engine.architecture.controller.{
  ControllerAsyncRPCHandlerInitializer,
  ControllerState
}
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.WorkerExecutionCompletedHandler.WorkerExecutionCompleted
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.KillWorkflowHandler.KillWorkflow
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.QueryWorkerStatisticsHandler.{
  ControllerInitiateQueryResults,
  ControllerInitiateQueryStatistics
}
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.CollectSinkResultsHandler.CollectSinkResults
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity.WorkerActorVirtualIdentity
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.operators.SinkOpExecConfig

import scala.collection.mutable

object WorkerExecutionCompletedHandler {
  final case class WorkerExecutionCompleted() extends ControlCommand[CommandCompleted]
}

/** indicate a worker has completed its job
  * i.e. received and processed all data from upstreams
  * note that this doesn't mean all the output of this worker
  * has been received by the downstream workers.
  *
  * possible sender: worker
  */
trait WorkerExecutionCompletedHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: WorkerExecutionCompleted, sender) =>
    {
      assert(sender.isInstanceOf[WorkerActorVirtualIdentity])
      val operator = workflow.getOperator(sender)

      // after worker execution is completed, query statistics immediately one last time
      // because the workflow might be completed and killed at the next query statistics interval
      // and prevent the user only seeing the last update before completion
      val requests = new mutable.MutableList[Future[Unit]]()
      requests += execute(
        ControllerInitiateQueryStatistics(Option(List(sender))),
        ActorVirtualIdentity.Controller
      )

      // if operator is sink, additionally query result immediately one last time
      if (operator.isInstanceOf[SinkOpExecConfig]) {
        // TODO: unify collect sink result (for final completion) and query results (for incremental update)
        requests += send(CollectSinkResults(), sender).map(results =>
          operator.acceptResultTuples(results)
        )
        requests += execute(
          ControllerInitiateQueryResults(Option(List(sender))),
          ActorVirtualIdentity.Controller
        )
      }

      val future = Future.collect(requests.toList)

      future.flatMap(_ => {
        updateFrontendWorkflowStatus()
        if (workflow.isCompleted) {
          //send result to frontend
          if (eventListener.workflowCompletedListener != null) {
            eventListener.workflowCompletedListener.apply(
              WorkflowCompleted(
                workflow.getEndOperators.map(op => op.id.operator -> op.results).toMap
              )
            )
          }
          disableStatusUpdate()
          actorContext.parent ! ControllerState.Completed // for testing
          // clean up all workers and terminate self
          execute(KillWorkflow(), ActorVirtualIdentity.Controller)
        } else {
          Future.Done.map(_ => CommandCompleted())
        }
      })
    }
  }
}
