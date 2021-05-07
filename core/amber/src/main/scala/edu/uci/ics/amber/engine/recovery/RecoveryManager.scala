package edu.uci.ics.amber.engine.recovery

import akka.actor.{ActorContext, Address}
import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.{
  GetActorRef,
  NetworkSenderActorRef,
  SendRequest
}
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

import scala.collection.mutable

object RecoveryManager {
  def defaultLogStorage(
      id: ActorVirtualIdentity
  ) = new LocalDiskLogStorage(id.toString)
}

class RecoveryManager(
    workflow: Workflow,
    context: ActorContext,
    communicationActor: NetworkSenderActorRef
) {

  private val isRecovering = mutable.HashSet[ActorVirtualIdentity]()

  def recoverWorkerOnNode(crashedNode: Address, replaceNode: Address): Unit = {
    val allWorkerOnCrashedNode = workflow.getAllWorkersOnNode(crashedNode)
    allWorkerOnCrashedNode.foreach(
      recoverWorker(_, replaceNode)
    ) // migrate all worker to another node
    allWorkerOnCrashedNode.foreach(
      rollbackUpstreamWorkers
    ) // rollback all upstream worker on their node
  }

  def rollbackUpstreamWorkers(id: ActorVirtualIdentity): Unit = {
    val upstreamWorkersToReplay =
      workflow.getUpstreamWorkers(id).filter(x => !isRecovering.contains(x))
    upstreamWorkersToReplay.foreach(recoverWorker(_))
  }

  def setRecoverCompleted(id: ActorVirtualIdentity): Unit = {
    if (isRecovering.contains(id)) {
      isRecovering.remove(id)
    }
  }

  def recoverWorker(id: ActorVirtualIdentity, onNode: Address = null): Unit = {
    workflow
      .getWorkerLayer(id)
      .killAndReBuild(
        id,
        onNode,
        context,
        communicationActor.ref,
        RecoveryManager.defaultLogStorage(id)
      )
    isRecovering.add(id)
  }

}
