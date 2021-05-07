package edu.uci.ics.amber.clustering

import akka.actor.ActorRef
import edu.uci.ics.amber.engine.architecture.controller.Controller
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor
import edu.uci.ics.amber.engine.architecture.worker.WorkflowWorker
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

import scala.collection.mutable

object ClusterRuntimeInfo {
  val controllers: mutable.HashSet[ActorRef] = mutable.HashSet[ActorRef]()
  var controllerState: Controller = _
  val workerStates: mutable.HashMap[ActorVirtualIdentity, WorkflowWorker] =
    mutable.HashMap[ActorVirtualIdentity, WorkflowWorker]()
  val senderStates: mutable.HashMap[ActorVirtualIdentity, NetworkCommunicationActor] =
    mutable.HashMap[ActorVirtualIdentity, NetworkCommunicationActor]()
}
