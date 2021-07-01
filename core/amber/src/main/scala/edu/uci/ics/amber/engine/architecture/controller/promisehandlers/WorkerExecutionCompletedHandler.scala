package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.{
  WorkflowCompleted,
  WorkflowStatusUpdate
}
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
import edu.uci.ics.amber.engine.architecture.principal.OperatorResult
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity.WorkerActorVirtualIdentity
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, VirtualIdentity}
import edu.uci.ics.amber.engine.operators.SinkOpExecConfig

import scala.collection.mutable

object WorkerExecutionCompletedHandler {
  final case class WorkerExecutionCompleted() extends ControlCommand[Unit]
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
      // get the corresponding operator of this worker
      val operator = workflow.getOperator(sender)

      // after worker execution is completed, query statistics immediately one last time
      // because the worker might be killed before the next query statistics interval
      // and the user sees the last update before completion
      val queryStats = new mutable.MutableList[Future[Unit]]()
      queryStats += execute(
        ControllerInitiateQueryStatistics(Option(List(sender))),
        ActorVirtualIdentity.Controller
      )

      // if operator is sink, additionally query result immediately one last time
      val queryResults = new mutable.MutableList[Future[Map[String, OperatorResult]]]()
      if (operator.isInstanceOf[SinkOpExecConfig]) {
        queryResults += execute(
          ControllerInitiateQueryResults(Option(List(sender))),
          ActorVirtualIdentity.Controller
        )
      }

      Future
        .collect(queryStats ++ queryResults)
        .flatMap(_ => {
          if (workflow.isCompleted) {
            execute(ControllerInitiateQueryResults(), ActorVirtualIdentity.Controller)
              .flatMap(ret => {
                if (eventListener.workflowCompletedListener != null) {
                  eventListener.workflowCompletedListener.apply(WorkflowCompleted(ret))
                }
                disableStatusUpdate()
                actorContext.parent ! ControllerState.Completed // for testing
                // clean up all workers and terminate self
                execute(KillWorkflow(), ActorVirtualIdentity.Controller)
                Future.Done
              })
          } else {
            Future.Done
          }
        })
    }
  }
}
