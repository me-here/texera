package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import akka.actor.PoisonPill
import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.KillWorkflowHandler.KillWorkflow
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}

object KillWorkflowHandler {
  final case class KillWorkflow() extends ControlCommand[CommandCompleted]
}

/** Kill the workflow and release all resources
  *
  * possible sender: controller, client
  */
trait KillWorkflowHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: KillWorkflow, sender) =>
    {
      controller.logger.logInfo("start to kill workflow")
      disableStatusUpdate()
      updateFrontendWorkflowStatus()
      // kill the controller by sending poison pill
      // the workers and network communication actors will also be killed
      // the dp thread will be shut down when the workers kill themselves
      Thread.sleep(500) //hack: wait for network communication actor send the result to frontend
      controller.context.self ! PoisonPill
      CommandCompleted()
    }
  }
}
