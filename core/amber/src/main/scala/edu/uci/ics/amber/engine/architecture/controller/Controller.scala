package edu.uci.ics.amber.engine.architecture.controller

import akka.actor.{ActorRef, Address, Cancellable, Props}
import akka.pattern.ask
import akka.util.Timeout
import com.softwaremill.macwire.wire
import com.twitter.util.Future
import edu.uci.ics.amber.clustering.ClusterListener.GetAvailableNodeAddresses
import edu.uci.ics.amber.clustering.ClusterRuntimeInfo
import edu.uci.ics.amber.engine.architecture.common.WorkflowActor
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.{
  ErrorOccurred,
  WorkflowStatusUpdate
}
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.LinkWorkflowHandler.LinkWorkflow
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.{
  NetworkAck,
  NetworkMessage,
  RegisterActorRef
}
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkInputPort
import edu.uci.ics.amber.engine.common.ambermessage.{
  ControlPayload,
  RecoveryCompleted,
  RecoveryMessage,
  TriggerRecovery,
  UpdateCountForInput,
  WorkflowControlMessage
}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.{ControlInvocation, ReturnPayload}
import edu.uci.ics.amber.engine.recovery.{
  ControlLogManager,
  EmptyLogStorage,
  InputCounter,
  LogStorage,
  ParallelLogWriter,
  RecoveryManager
}
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  VirtualIdentity,
  WorkflowIdentity
}
import edu.uci.ics.amber.error.ErrorUtils.safely
import edu.uci.ics.amber.error.WorkflowRuntimeError

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

object Controller {

  def props(
      id: WorkflowIdentity,
      workflow: Workflow,
      eventListener: ControllerEventListener,
      statusUpdateInterval: Long,
      controlLogStorage: LogStorage =
        RecoveryManager.defaultLogStorage(ActorVirtualIdentity.Controller),
      parentNetworkCommunicationActorRef: ActorRef = null
  ): Props =
    Props(
      new Controller(
        id,
        workflow,
        eventListener,
        Option.apply(statusUpdateInterval),
        controlLogStorage,
        parentNetworkCommunicationActorRef
      )
    )
}

class Controller(
    val id: WorkflowIdentity,
    val workflow: Workflow,
    val eventListener: ControllerEventListener = ControllerEventListener(),
    val statisticsUpdateIntervalMs: Option[Long],
    logStorage: LogStorage,
    parentNetworkCommunicationActorRef: ActorRef
) extends WorkflowActor(
      ActorVirtualIdentity.Controller,
      !logStorage.isInstanceOf[EmptyLogStorage],
      parentNetworkCommunicationActorRef
    ) {
  implicit val ec: ExecutionContext = context.dispatcher
  implicit val timeout: Timeout = 5.seconds

  // register controller itself
  networkCommunicationActor ! RegisterActorRef(ActorVirtualIdentity.Controller, self)

  // build whole workflow
  workflow.build(availableNodes, networkCommunicationActor, context)

  ClusterRuntimeInfo.controllers.add(self)
  val startTime = System.nanoTime()

  val rpcHandlerInitializer = new ControllerAsyncRPCHandlerInitializer(this)
  val controlLogManager: ControlLogManager = wire[ControlLogManager]
  val recoveryManager = wire[RecoveryManager]

  lazy val logWriter: ParallelLogWriter =
    new ParallelLogWriter(logStorage, self, networkCommunicationActor, true)

  lazy val controlInputPort: NetworkInputPort[ControlPayload] =
    new NetworkInputPort[ControlPayload](this.logger, this.handleControlPayloadWithTryCatch)

  private def errorLogAction(err: WorkflowRuntimeError): Unit = {
    eventListener.workflowExecutionErrorListener.apply(ErrorOccurred(err))
  }

  logger.setErrorLogAction(errorLogAction)

  var statusUpdateAskHandle: Cancellable = _

  controlLogManager.onComplete(() => {
    inputCounter.enable()
    // activate all links
    controlOutputPort.sendTo(ActorVirtualIdentity.Self, ControlInvocation(-1, LinkWorkflow()))
  })

  def availableNodes: Array[Address] =
    Await
      .result(context.actorSelection("/user/cluster-info") ? GetAvailableNodeAddresses, 5.seconds)
      .asInstanceOf[Array[Address]]

  override def receive: Receive = initializing

  def initializing: Receive = {
    processRecoveryMessages orElse
      receiveCountUpdate orElse {
      case NetworkMessage(
            id,
            cmd @ WorkflowControlMessage(from, seqNum, payload: ReturnPayload)
          ) =>
        //process reply messages
        controlLogManager.persistControlMessage(cmd)
        enqueueDelayedAck(stashedControlAck, (sender, id))
        controlInputPort.handleMessage(from, seqNum, payload)
      case NetworkMessage(
            id,
            cmd @ WorkflowControlMessage(ActorVirtualIdentity.Controller, seqNum, payload)
          ) =>
        //process control messages from self
        controlLogManager.persistControlMessage(cmd)
        enqueueDelayedAck(stashedControlAck, (sender, id))
        controlInputPort.handleMessage(ActorVirtualIdentity.Controller, seqNum, payload)
      case _ =>
        stash() //prevent other messages to be executed until initialized
    }
  }

  def running: Receive = {
    acceptDirectInvocations orElse
      receiveCountUpdate orElse
      processRecoveryMessages orElse {
      case NetworkMessage(id, cmd @ WorkflowControlMessage(from, seqNum, payload)) =>
        //logger.logInfo(s"received $cmd")
        controlLogManager.persistControlMessage(cmd)
        enqueueDelayedAck(stashedControlAck, (sender, id))
        controlInputPort.handleMessage(from, seqNum, payload)
      case other =>
        logger.logInfo(s"unhandled message: $other")
    }
  }

  def acceptDirectInvocations: Receive = {
    case invocation: ControlInvocation =>
      controlLogManager.persistControlMessage(
        WorkflowControlMessage(ActorVirtualIdentity.Client, -1, invocation)
      )
      controlCount += 1 //we have to advance the count here since we don't ack
      asyncRPCServer.receive(invocation, ActorVirtualIdentity.Client)
  }

  override def postStop(): Unit = {
    if (statusUpdateAskHandle != null) {
      statusUpdateAskHandle.cancel()
    }
    workflow.cleanupResults()
    ClusterRuntimeInfo.controllers.remove(self)
    val timeSpent = (System.nanoTime() - startTime).asInstanceOf[Double] / 1000000000
    logger.logInfo("workflow finished in " + timeSpent + " seconds")
    logWriter.shutdown()
    super.postStop()
  }

  def handleControlPayloadWithTryCatch(
      from: VirtualIdentity,
      controlPayload: ControlPayload
  ): Unit = {
    inputCounter.advanceControlInputCount()
    try {
      controlPayload match {
        // use control input port to pass control messages
        case invocation: ControlInvocation =>
          assert(from.isInstanceOf[ActorVirtualIdentity])
          asyncRPCServer.logControlInvocation(invocation, from)
          asyncRPCServer.receive(invocation, from.asInstanceOf[ActorVirtualIdentity])
        case ret: ReturnPayload =>
          asyncRPCClient.logControlReply(ret, from)
          asyncRPCClient.fulfillPromise(ret)
        case other =>
          logger.logError(
            WorkflowRuntimeError(
              s"unhandled control message: $other",
              "ControlInputPort",
              Map.empty
            )
          )
      }
    } catch safely {
      case e =>
        logger.logError(WorkflowRuntimeError(e, identifier.toString))
    }
  }

  def processRecoveryMessages: Receive = {
    case NetworkMessage(id, msg: RecoveryMessage) =>
      sender ! NetworkAck(id)
      msg match {
        case TriggerRecovery(addr) =>
          val targetNode = availableNodes.head
          recoveryManager.recoverWorkerOnNode(addr, targetNode)
        case RecoveryCompleted(id) =>
          recoveryManager.setRecoverCompleted(id)
      }
  }

  final def receiveCountUpdate: Receive = {
    case UpdateCountForInput(dc, cc) =>
      if(controlCount < cc){
        replyAcks(stashedControlAck, cc - controlCount)
        controlCount = cc
      }
  }

}
