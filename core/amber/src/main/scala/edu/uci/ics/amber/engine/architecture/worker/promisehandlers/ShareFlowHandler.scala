package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.WorkerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.ShareFlowHandler.ShareFlow
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

// join-skew research related.
object ShareFlowHandler {
  final case class ShareFlow(
      skewedReceiverId: ActorVirtualIdentity,
      freeReceiverId: ActorVirtualIdentity
  ) extends ControlCommand[Map[ActorVirtualIdentity, Long]]
}

trait ShareFlowHandler {
  this: WorkerAsyncRPCHandlerInitializer =>

  registerHandler { (cmd: ShareFlow, sender) =>
    // workerStateManager.shouldBe(Running, Ready)
    try {
      tupleToBatchConverter.changeFlow(cmd.skewedReceiverId, cmd.freeReceiverId)
    } catch {
      case exception: Exception =>
        println(
          "Exception happened" + exception
            .getMessage() + " stacktrace " + exception.getStackTrace().mkString("\n\t")
        )
        Map[ActorVirtualIdentity, Long]()
    }
  }
}
