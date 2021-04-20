package edu.uci.ics.amber.engine.recovery

import akka.actor.{ActorContext, Address}
import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.{GetActorRef, NetworkSenderActorRef, SendRequest}
import edu.uci.ics.amber.engine.common.ambermessage.{WorkflowControlMessage, WorkflowMessage}
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.recovery.DataLogManager.DataLogElement

import scala.collection.mutable

object RecoveryManager {
  def defaultControlLogStorage(
      id: ActorVirtualIdentity
  ) = new HDFSLogStorage[WorkflowControlMessage](id.toString+"control")

  def defaultDataLogStorage(id: ActorVirtualIdentity) = new HDFSLogStorage[DataLogElement](id.toString+"data")

  def defaultDPLogStorage(id: ActorVirtualIdentity) = new HDFSLogStorage[Long](id.toString+"dp")
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
