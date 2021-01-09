package edu.uci.ics.amber.engine.architecture.common

import akka.actor.{Actor, ActorLogging, ActorRef, Stash}
import com.softwaremill.macwire.wire
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.{
  NetworkCommunicationActorRef,
  GetActorRef,
  RegisterActorRef
}
import edu.uci.ics.amber.engine.architecture.messaginglayer.{
  ControlInputPort,
  ControlOutputPort,
  NetworkCommunicationActor
}
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.ActorVirtualIdentity
import edu.uci.ics.amber.error.WorkflowRuntimeError

abstract class WorkflowActor(identifier: ActorVirtualIdentity, parentSenderActorRef: ActorRef)
    extends Actor
    with ActorLogging
    with Stash {

  val networkSenderActor: NetworkCommunicationActorRef = NetworkCommunicationActorRef(
    context.actorOf(NetworkCommunicationActor.props(parentSenderActorRef))
  )
  lazy val controlInputPort: ControlInputPort = wire[ControlInputPort]
  lazy val controlOutputPort: ControlOutputPort = wire[ControlOutputPort]

  def disallowActorRefRelatedMessages: Receive = {
    case GetActorRef(id, replyTo) =>
      throw WorkflowRuntimeException(
        WorkflowRuntimeError(
          "workflow actor should never receive get actor ref message",
          identifier.toString,
          Map.empty
        )
      )
    case RegisterActorRef(id, ref) =>
      throw WorkflowRuntimeException(
        WorkflowRuntimeError(
          "workflow actor should never receive register actor ref message",
          identifier.toString,
          Map.empty
        )
      )
  }
}
