package edu.uci.ics.amber.engine.architecture.worker

import akka.actor.{ActorRef, Props}
import akka.util.Timeout
import com.softwaremill.macwire.wire
import edu.uci.ics.amber.clustering.ClusterRuntimeInfo
import edu.uci.ics.amber.engine.architecture.common.WorkflowActor
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.WorkerExecutionStartedHandler.WorkerStateUpdated
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.{
  NetworkAck,
  NetworkMessage,
  RegisterActorRef,
  SendRequest
}
import edu.uci.ics.amber.engine.architecture.messaginglayer.{
  BatchToTupleConverter,
  DataOutputPort,
  NetworkAckManager,
  NetworkInputPort,
  TupleToBatchConverter
}
import edu.uci.ics.amber.engine.common.IOperatorExecutor
import edu.uci.ics.amber.engine.common.ambermessage.{
  ControlPayload,
  DataPayload,
  RecoveryCompleted,
  ShutdownWriter,
  WorkflowControlMessage,
  WorkflowDataMessage
}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.{ControlInvocation, ReturnPayload}
import edu.uci.ics.amber.engine.common.rpc.{AsyncRPCClient, AsyncRPCHandlerInitializer}
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager._
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, VirtualIdentity}
import edu.uci.ics.amber.engine.recovery.{
  ControlLogManager,
  DPLogManager,
  DataLogManager,
  EmptyLogStorage,
  ExecutionStepCursor,
  LogStorage,
  ParallelLogWriter
}
import edu.uci.ics.amber.error.ErrorUtils.safely
import edu.uci.ics.amber.error.WorkflowRuntimeError

import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object WorkflowWorker {
  def props(
      id: ActorVirtualIdentity,
      op: IOperatorExecutor,
      parentNetworkCommunicationActorRef: ActorRef,
      logStorage: LogStorage = null
  ): Props = {
    Props(
      new WorkflowWorker(
        id,
        op,
        parentNetworkCommunicationActorRef,
        if (logStorage == null) {
          new EmptyLogStorage(id.toString)
        } else {
          logStorage
        }
      )
    )
  }
}

class WorkflowWorker(
    identifier: ActorVirtualIdentity,
    operator: IOperatorExecutor,
    parentNetworkCommunicationActorRef: ActorRef,
    logStorage: LogStorage
) extends WorkflowActor(
      identifier,
      logStorage,
      parentNetworkCommunicationActorRef
    ) {
  implicit val ec: ExecutionContext = context.dispatcher
  implicit val timeout: Timeout = 5.seconds

  val rpcHandlerInitializer: AsyncRPCHandlerInitializer =
    wire[WorkerAsyncRPCHandlerInitializer]

  lazy val workerStateManager: WorkerStateManager = new WorkerStateManager()

  workerStateManager.assertState(Uninitialized)
  workerStateManager.transitTo(Ready)

  ClusterRuntimeInfo.workerStates(identifier) = this

  lazy val logWriter: ParallelLogWriter =
    new ParallelLogWriter(
      logStorage,
      networkCommunicationActor,
      networkControlAckManager,
      networkDataAckManager
    )

  lazy val dataLogManager: DataLogManager =
    new DataLogManager(logStorage, logWriter, networkDataAckManager, this.handleDataPayloadAfterLog)
  lazy val dpLogManager: DPLogManager = wire[DPLogManager]
  val controlLogManager: ControlLogManager = new ControlLogManager(
    logStorage,
    logWriter,
    controlInputPort,
    networkControlAckManager,
    this.handleControlPayloadAfterLog
  )

  lazy val pauseManager: PauseManager = wire[PauseManager]
  lazy val dataProcessor: DataProcessor = wire[DataProcessor]
  lazy val dataInputPort: NetworkInputPort[DataPayload] =
    new NetworkInputPort[DataPayload](
      this.logger,
      networkDataAckManager,
      this.handleDataPayloadAfterFIFO
    )
  lazy val controlInputPort: NetworkInputPort[ControlPayload] =
    new NetworkInputPort[ControlPayload](
      this.logger,
      networkControlAckManager,
      this.handleControlPayloadAfterFIFO
    )
  lazy val dataOutputPort: DataOutputPort = wire[DataOutputPort]
  lazy val batchProducer: TupleToBatchConverter = wire[TupleToBatchConverter]
  lazy val tupleProducer: BatchToTupleConverter = wire[BatchToTupleConverter]
  lazy val breakpointManager: BreakpointManager = wire[BreakpointManager]
  lazy val networkDataAckManager: NetworkAckManager = wire[NetworkAckManager]
  var processingTime = 0L

  if (parentNetworkCommunicationActorRef != null) {
    parentNetworkCommunicationActorRef ! RegisterActorRef(identifier, self)
  }

  dataLogManager.onComplete(() => {
    networkCommunicationActor ! SendRequest(
      ActorVirtualIdentity.Controller,
      RecoveryCompleted(identifier),
      0L
    )
    context.become(receiveAndProcessMessages)
    unstashAll()
  })

  def recovering: Receive = {
    disallowActorRefRelatedMessages orElse
      receiveDataMessages orElse
      stashControlMessages orElse
      logUnhandledMessages
  }

  override def receive: Receive = recovering

  def receiveAndProcessMessages: Receive = {
    disallowActorRefRelatedMessages orElse
      receiveDataMessages orElse
      receiveControlMessages orElse {
      case other =>
        logger.logError(
          WorkflowRuntimeError(s"unhandled message: $other", identifier.toString, Map.empty)
        )
    }
  }

  final def receiveDataMessages: Receive = {
    case NetworkMessage(id, WorkflowDataMessage(from, seqNum, payload)) =>
      dataInputPort.handleMessage(sender, id, from, seqNum, payload)
  }

  def receiveControlMessages: Receive = {
    case NetworkMessage(id, cmd @ WorkflowControlMessage(from, seqNum, payload)) =>
      try {
        // use control input port to pass control messages
        controlInputPort.handleMessage(sender, id, from, seqNum, payload)
      } catch safely {
        case e =>
          logger.logError(WorkflowRuntimeError(e, identifier.toString))
      }
  }

  final def handleDataPayloadAfterFIFO(
      currentSender: ActorRef,
      currentID: Long,
      from: VirtualIdentity,
      dataPayload: DataPayload
  ): Unit = {
    dataLogManager.handleMessage(currentSender, currentID, from, dataPayload)
  }

  final def handleDataPayloadAfterLog(from: VirtualIdentity, dataPayload: DataPayload): Unit = {
    tupleProducer.processDataPayload(from, dataPayload)
  }

  final def handleControlPayloadAfterFIFO(
      sender: ActorRef,
      id: Long,
      from: VirtualIdentity,
      controlPayload: ControlPayload
  ): Unit = {
    networkControlAckManager.enqueueDelayedAck(sender, id)
    controlLogManager.persistControlMessage(from, controlPayload)
    //logger.logInfo(s"received control $controlPayload")
    handleControlPayloadAfterLog(from, controlPayload)
  }

  final def handleControlPayloadAfterLog(
      from: VirtualIdentity,
      controlPayload: ControlPayload
  ): Unit = {
    // let dp thread process it
    assert(from.isInstanceOf[ActorVirtualIdentity])
    controlPayload match {
      case controlCommand @ (ControlInvocation(_, _) | ReturnPayload(_, _)) =>
        dataProcessor.enqueueCommand(controlCommand, from)
      case _ =>
        logger.logError(
          WorkflowRuntimeError(
            s"unhandled control payload: $controlPayload",
            identifier.toString,
            Map.empty
          )
        )
    }
  }

  override def postStop(): Unit = {
    logger.logInfo(
      s"${identifier.toString} main thread processing time: ${processingTime / 1000f}s"
    )
    // shutdown dp thread by sending a command
    dataProcessor.enqueueCommand(ShutdownDPThread(), ActorVirtualIdentity.Self)
    // release the resource
    logWriter.shutdown()
    super.postStop()
  }

}
