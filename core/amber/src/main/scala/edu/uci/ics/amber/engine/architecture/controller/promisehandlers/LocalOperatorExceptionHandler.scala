package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.ErrorOccurred
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.KillWorkflowHandler.KillWorkflow
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.LocalOperatorExceptionHandler.LocalOperatorException
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.util.CONTROLLER
import edu.uci.ics.amber.error.WorkflowRuntimeError

object LocalOperatorExceptionHandler {
  final case class LocalOperatorException(triggeredTuple: ITuple, e: Throwable)
      extends ControlCommand[Unit]
}

/** indicate an exception thrown from the operator logic on a worker
  * we catch exception when calling:
  * 1. operator.processTuple
  * 2. operator.hasNext
  * 3. operator.Next
  * //  * The triggeredTuple of this message will always be the current input tuple
  * //  * note that this message will be sent for each faulted input tuple, so the frontend
  * //  * need to update incrementally since there can be multiple faulted tuple
  * //  * from different workers at the same time.
  *
  * possible sender: worker
  */
trait LocalOperatorExceptionHandler {
  this: ControllerAsyncRPCHandlerInitializer =>
  registerHandler { (msg: LocalOperatorException, sender) =>
    {
      // report the exception to the frontend
      if (eventListener.workflowExecutionErrorListener != null) {
        eventListener.workflowExecutionErrorListener.apply(
          ErrorOccurred(
            new WorkflowRuntimeError(msg.e.toString, "", Map())
          )
        )
      }
      // then kill the workflow
      execute(KillWorkflow(), CONTROLLER)
    }
  }
}
