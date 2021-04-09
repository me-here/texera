package edu.uci.ics.amber.engine.recovery

import akka.actor.{ActorContext, Address}
import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.architecture.messaginglayer.ControlInputPort.WorkflowControlMessage
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.{
  GetActorRef,
  NetworkSenderActorRef,
  SendRequest
}
import edu.uci.ics.amber.engine.common.ambermessage.WorkflowMessage
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.recovery.DataLogManager.DataLogElement

import scala.collection.mutable

object RecoveryManager {
  def defaultControlLogStorage(
      id: ActorVirtualIdentity
  ) = new EmptyLogStorage[WorkflowControlMessage]()

  def defaultDataLogStorage(id: ActorVirtualIdentity) = new EmptyLogStorage[DataLogElement]()

  def defaultDPLogStorage(id: ActorVirtualIdentity) = new EmptyLogStorage[Long]()

  sealed trait RecoveryMessage extends WorkflowMessage
  final case class TriggerRecovery(nodeAddr: Address) extends RecoveryMessage
  final case class RecoveryCompleted(id: ActorVirtualIdentity) extends RecoveryMessage
}

class RecoveryManager(
    workflow: Workflow,
    context: ActorContext,
    communicationActor: NetworkSenderActorRef
) {

  private val isRecovering = mutable.HashSet[ActorVirtualIdentity]()

  def recoverWorkerOnNode(crashedNode: Address, replaceNode: Address): Unit = {
    workflow.getAllWorkersOnNode(crashedNode).foreach {
      recoverWorkerChain(_, replaceNode)
    }
  }

  def recoverWorkerChain(id: ActorVirtualIdentity, newNode: Address): Unit = {
    if (!isRecovering.contains(id)) {
      recoverWorker(id, newNode)
      val upstreamWorkersToReplay =
        workflow.getUpstreamWorkers(id).filter(x => !isRecovering.contains(x))
      upstreamWorkersToReplay.foreach(recoverWorker(_))
    }
  }

  def setRecoverCompleted(id: ActorVirtualIdentity): Unit = {
    if (isRecovering.contains(id)) {
      isRecovering.remove(id)
    }
  }

  private def recoverWorker(id: ActorVirtualIdentity, onNode: Address = null): Unit = {
    workflow
      .getWorkerLayer(id)
      .killAndReBuild(
        id,
        onNode,
        context,
        communicationActor.ref,
        RecoveryManager.defaultControlLogStorage(id),
        RecoveryManager.defaultDataLogStorage(id),
        RecoveryManager.defaultDPLogStorage(id)
      )
    isRecovering.add(id)
  }

}
