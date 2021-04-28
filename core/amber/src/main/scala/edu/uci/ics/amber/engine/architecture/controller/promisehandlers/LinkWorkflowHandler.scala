package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.{
  ControllerAsyncRPCHandlerInitializer,
  ControllerState
}
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.WorkflowStatusUpdate
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.LinkWorkflowHandler.LinkWorkflow
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.AddOutputPolicyHandler.AddOutputPolicy
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.DisableDataLogHandler.DisableDataLog
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager.Ready
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

import scala.collection.mutable

object LinkWorkflowHandler {
  final case class LinkWorkflow() extends ControlCommand[CommandCompleted]
}

trait LinkWorkflowHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: LinkWorkflow, sender) =>
    {
      val numUpstreamSender = new mutable.HashMap[ActorVirtualIdentity, Int]().withDefaultValue(0)
      Future
        .collect(controller.workflow.getAllLinks.flatMap { link =>
          link.getPolicies.flatMap {
            case (from, policy, tos) =>
              // send messages to sender worker
              link.getMappingFromDownstreamToUpstream.foreach {
                case (id, iterable) => numUpstreamSender(id) += iterable.size
              }
              Seq(send(AddOutputPolicy(policy), from))
          }
        }.toSeq)
        .flatMap { ret1 =>
          Future
            .collect(
              numUpstreamSender.filter(_._2 == 1).keys.map(x => send(DisableDataLog(), x)).toSeq
            )
            .map { ret2 =>
              controller.workflow.getAllOperators.foreach(_.setAllWorkerState(Ready))
              updateFrontendWorkflowStatus()
              // for testing, report ready state to parent
              controller.context.parent ! ControllerState.Ready
              controller.context.become(controller.running)
              controller.unstashAll()
              CommandCompleted()
            }
        }
    }
  }
}
