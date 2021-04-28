package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.WorkerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.DisableDataLogHandler.DisableDataLog
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand

object DisableDataLogHandler {
  final case class DisableDataLog() extends ControlCommand[Unit]
}

trait DisableDataLogHandler {
  this: WorkerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: DisableDataLog, sender) =>
    {
      dataLogManager.disable()
      inputCounter.disableDataCount()
    }
  }
}
