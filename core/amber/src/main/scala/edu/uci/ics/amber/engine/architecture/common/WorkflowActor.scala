package edu.uci.ics.amber.engine.architecture.common

import akka.actor.{Actor, ActorRef, Stash}
import com.softwaremill.macwire.wire
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.FatalErrorHandler.FatalError
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.{
  GetActorRef,
  NetworkAck,
  NetworkMessage,
  NetworkSenderActorRef,
  RegisterActorRef
}
import edu.uci.ics.amber.engine.architecture.messaginglayer.{
  ControlOutputPort,
  NetworkCommunicationActor
}
import edu.uci.ics.amber.engine.common.WorkflowLogger
import edu.uci.ics.amber.engine.common.ambermessage.{UpdateCountForInput, WorkflowControlMessage}
import edu.uci.ics.amber.engine.common.rpc.{
  AsyncRPCClient,
  AsyncRPCHandlerInitializer,
  AsyncRPCServer
}
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.recovery.{
  ControlLogManager,
  EmptyLogStorage,
  InputCounter,
  LogStorage
}
import edu.uci.ics.amber.error.WorkflowRuntimeError
import edu.uci.ics.amber.error.ErrorUtils.safely

import scala.collection.mutable

abstract class WorkflowActor(
    val identifier: ActorVirtualIdentity,
    val countCheckEnabled: Boolean,
    parentNetworkCommunicationActorRef: ActorRef
) extends Actor
    with Stash {

  val logger: WorkflowLogger = WorkflowLogger(s"$identifier")

  logger.setErrorLogAction(err => {
    asyncRPCClient.send(
      FatalError(err),
      ActorVirtualIdentity.Controller
    )
  })

  val networkCommunicationActor: NetworkSenderActorRef = NetworkSenderActorRef(
    context.actorOf(
      NetworkCommunicationActor.props(parentNetworkCommunicationActorRef, countCheckEnabled)
    )
  )

  lazy val inputCounter: InputCounter = wire[InputCounter]
  lazy val controlOutputPort: ControlOutputPort = wire[ControlOutputPort]
  lazy val asyncRPCClient: AsyncRPCClient = wire[AsyncRPCClient]
  lazy val asyncRPCServer: AsyncRPCServer = wire[AsyncRPCServer]
  // this variable cannot be lazy
  // because it should be initialized with the actor itself
  val rpcHandlerInitializer: AsyncRPCHandlerInitializer
  val controlLogManager: ControlLogManager

  var controlCount = 0L
  val stashedControlAck = new mutable.Queue[(ActorRef, Long)]()

  def disallowActorRefRelatedMessages: Receive = {
    case GetActorRef(id, replyTo) =>
      logger.logError(
        WorkflowRuntimeError(
          "workflow actor should never receive get actor ref message",
          identifier.toString,
          Map.empty
        )
      )
    case RegisterActorRef(id, ref) =>
      logger.logError(
        WorkflowRuntimeError(
          "workflow actor should never receive register actor ref message",
          identifier.toString,
          Map.empty
        )
      )
  }

  def stashControlMessages: Receive = {
    case msg @ NetworkMessage(id, cmd: WorkflowControlMessage) =>
      stash()
  }

  def logUnhandledMessages: Receive = {
    case other =>
      logger.logError(
        WorkflowRuntimeError(s"unhandled message: $other", identifier.toString, Map.empty)
      )
  }

  def stashUnhandledMessages: Receive = {
    case other =>
      stash()
  }

  override def postStop(): Unit = {
    logger.logInfo("workflow actor stopped!")
  }

  final def replyAcks(queue: mutable.Queue[(ActorRef, Long)], count: Long): Unit = {
    for (_ <- 0L until count) {
      val (senderRef, messageID) = queue.dequeue()
      senderRef ! NetworkAck(messageID)
    }
  }

  final def enqueueDelayedAck(
      queue: mutable.Queue[(ActorRef, Long)],
      elem: (ActorRef, Long)
  ): Unit = {
    if (countCheckEnabled) {
      queue.enqueue(elem)
    } else {
      elem._1 ! NetworkAck(elem._2)
    }
  }

}
