package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.WorkerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.RollbackFlowHandler.RollbackFlow
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

// join-skew research related.
object RollbackFlowHandler {
  final case class RollbackFlow(
      skewedReceiverId: ActorVirtualIdentity,
      freeReceiverId: ActorVirtualIdentity
  ) extends ControlCommand[Unit]
}

trait RollbackFlowHandler {
  this: WorkerAsyncRPCHandlerInitializer =>

  registerHandler { (cmd: RollbackFlow, sender) =>
    // workerStateManager.shouldBe(Running, Ready)
    tupleToBatchConverter.rollbackFlow(cmd.skewedReceiverId, cmd.freeReceiverId)
  }
}
