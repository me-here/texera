package edu.uci.ics.amber.engine.architecture.controller

import akka.actor.{ActorContext, ActorRef, Cancellable}
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.WorkflowStatusUpdate
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.LinkWorkflowHandler.LinkWorkflow
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.QueryWorkerStatisticsHandler.QueryWorkerStatistics
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.{
  AssignBreakpointHandler,
  FatalErrorHandler,
  KillWorkflowHandler,
  LinkCompletedHandler,
  LinkWorkflowHandler,
  LocalBreakpointTriggeredHandler,
  LocalOperatorExceptionHandler,
  PauseHandler,
  QueryWorkerStatisticsHandler,
  ResumeHandler,
  StartWorkflowHandler,
  WorkerExecutionCompletedHandler,
  WorkerExecutionStartedHandler
}
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.architecture.messaginglayer.ControlOutputPort
import edu.uci.ics.amber.engine.common.WorkflowLogger
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import edu.uci.ics.amber.engine.common.rpc.{
  AsyncRPCClient,
  AsyncRPCHandlerInitializer,
  AsyncRPCServer
}
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

import scala.collection.mutable
import scala.concurrent.duration.{DurationInt, FiniteDuration, MILLISECONDS}

class ControllerAsyncRPCHandlerInitializer(
    val controller: Controller
) extends AsyncRPCHandlerInitializer(controller.asyncRPCClient, controller.asyncRPCServer)
    with AssignBreakpointHandler
    with WorkerExecutionCompletedHandler
    with WorkerExecutionStartedHandler
    with LocalBreakpointTriggeredHandler
    with LocalOperatorExceptionHandler
    with PauseHandler
    with QueryWorkerStatisticsHandler
    with ResumeHandler
    with StartWorkflowHandler
    with KillWorkflowHandler
    with LinkCompletedHandler
    with FatalErrorHandler
    with LinkWorkflowHandler {

  def enableStatusUpdate(): Unit = {
    if (
      controller.statisticsUpdateIntervalMs.isDefined && controller.statusUpdateAskHandle == null
    ) {
      controller.statusUpdateAskHandle = controller.context.system.scheduler.schedule(
        0.milliseconds,
        FiniteDuration.apply(controller.statisticsUpdateIntervalMs.get, MILLISECONDS),
        controller.context.self,
        ControlInvocation(AsyncRPCClient.IgnoreReplyAndDoNotLog, QueryWorkerStatistics())
      )(controller.context.dispatcher)
    }
  }

  def disableStatusUpdate(): Unit = {
    if (controller.statusUpdateAskHandle != null) {
      controller.statusUpdateAskHandle.cancel()
      controller.statusUpdateAskHandle = null
    }
  }

  def updateFrontendWorkflowStatus(): Unit = {
    if (controller.eventListener.workflowStatusUpdateListener != null) {
      sendToOWP(() =>
        controller.eventListener.workflowStatusUpdateListener
          .apply(WorkflowStatusUpdate(controller.workflow.getWorkflowStatus))
      )
    }
  }

}
