package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.PythonPrintTriggered
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.PrintPythonHandler.PrintPython
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}

object PrintPythonHandler {
  final case class PrintPython(content: String) extends ControlCommand[CommandCompleted]
}

trait PrintPythonHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: PrintPython, sender) =>
    {

      val targetOp = workflow.getOperator(sender)
      val opID = targetOp.id.operator
      // print to frontend
      if (eventListener.pythonPrintTriggeredListener != null) {
        eventListener.pythonPrintTriggeredListener.apply(
          PythonPrintTriggered(msg.content, opID)
        )
      }
      CommandCompleted()
    }
  }
}
