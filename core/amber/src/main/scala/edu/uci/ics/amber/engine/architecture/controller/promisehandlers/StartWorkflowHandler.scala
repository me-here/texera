package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.LinkWorkflowHandler.LinkWorkflow
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.QueryWorkerStatisticsHandler.QueryWorkerStatistics
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.StartWorkflowHandler.StartWorkflow
import edu.uci.ics.amber.engine.architecture.controller.{
  Controller,
  ControllerAsyncRPCHandlerInitializer,
  ControllerState
}
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.StartHandler.StartWorker
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager.Running
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  LayerIdentity,
  OperatorIdentity
}
import edu.uci.ics.amber.engine.operators.OpExecConfig

import scala.collection.mutable
import scala.concurrent.duration.{DurationInt, FiniteDuration, MILLISECONDS}

object StartWorkflowHandler {
  final case class StartWorkflow() extends ControlCommand[CommandCompleted]
}

/** start the workflow by starting the source workers
  * note that this SHOULD only be called once per workflow
  *
  * possible sender: client
  */
trait StartWorkflowHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: StartWorkflow, sender) =>
    {
      execute(LinkWorkflow(), ActorVirtualIdentity.Controller).flatMap { ret =>
        val startedLayers = mutable.HashSet[WorkerLayer]()
        Future
          .collect(
            controller.workflow.getSourceLayers
              // get all startable layers
              .filter(layer => layer.canStart)
              .flatMap { layer =>
                startedLayers.add(layer)
                layer.identifiers.map { worker =>
                  send(StartWorker(), worker).map { ret =>
                    // update worker state
                    controller.workflow.getWorkerInfo(worker).state = ret
                  }
                }
              }
              .toSeq
          )
          .map { ret =>
            controller.context.parent ! ControllerState.Running // for testing
            enableStatusUpdate()
            CommandCompleted()
          }
      }
    }
  }
}
