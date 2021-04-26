package edu.uci.ics.amber.engine.architecture.worker

import akka.actor.{ActorRef, Props}
import akka.util.Timeout
import com.softwaremill.macwire.wire
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
  NetworkInputPort,
  TupleToBatchConverter
}
import edu.uci.ics.amber.engine.architecture.worker.WorkerInternalQueue.EnableInputCounter
import edu.uci.ics.amber.engine.common.IOperatorExecutor
import edu.uci.ics.amber.engine.common.ambermessage.{
  ControlPayload,
  DataPayload,
  RecoveryCompleted,
  ShutdownWriter,
  UpdateCountForInput,
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
  InputCounter,
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
      !logStorage.isInstanceOf[EmptyLogStorage],
      parentNetworkCommunicationActorRef
    ) {
  implicit val ec: ExecutionContext = context.dispatcher
  implicit val timeout: Timeout = 5.seconds

  val rpcHandlerInitializer: AsyncRPCHandlerInitializer =
    wire[WorkerAsyncRPCHandlerInitializer]

  lazy val workerStateManager: WorkerStateManager = new WorkerStateManager()

  workerStateManager.assertState(Uninitialized)
  workerStateManager.transitTo(Ready)

  lazy val logWriter: ParallelLogWriter =
    new ParallelLogWriter(logStorage, self, networkCommunicationActor)

  lazy val dataLogManager: DataLogManager = wire[DataLogManager]
  lazy val dpLogManager: DPLogManager = wire[DPLogManager]
  val controlLogManager: ControlLogManager = wire[ControlLogManager]

  lazy val pauseManager: PauseManager = wire[PauseManager]
  lazy val dataProcessor: DataProcessor = wire[DataProcessor]
  lazy val dataInputPort: NetworkInputPort[DataPayload] =
    new NetworkInputPort[DataPayload](this.logger, this.handleDataPayload)
  lazy val controlInputPort: NetworkInputPort[ControlPayload] =
    new NetworkInputPort[ControlPayload](this.logger, this.handleControlPayload)
  lazy val dataOutputPort: DataOutputPort = wire[DataOutputPort]
  lazy val batchProducer: TupleToBatchConverter = wire[TupleToBatchConverter]
  lazy val tupleProducer: BatchToTupleConverter = wire[BatchToTupleConverter]
  lazy val breakpointManager: BreakpointManager = wire[BreakpointManager]

  var processingTime = 0L
  var dataCount = 0L
  val stashedDataAck = new mutable.Queue[(ActorRef, Long)]()

  if (parentNetworkCommunicationActorRef != null) {
    parentNetworkCommunicationActorRef ! RegisterActorRef(identifier, self)
  }

  dataLogManager.onComplete(() => {
    dataProcessor.appendElement(EnableInputCounter)
    networkCommunicationActor ! SendRequest(
      ActorVirtualIdentity.Controller,
      RecoveryCompleted(identifier),
      0,
      0
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
      receiveCountUpdate orElse
      receiveControlMessages orElse {
      case other =>
        logger.logError(
          WorkflowRuntimeError(s"unhandled message: $other", identifier.toString, Map.empty)
        )
    }
  }

  final def receiveCountUpdate: Receive = {
    case UpdateCountForInput(dc, cc) =>
      replyAcks(stashedControlAck, cc - controlCount)
      replyAcks(stashedDataAck, dc - dataCount)
      dataCount = dc
      controlCount = cc
  }

  final def receiveDataMessages: Receive = {
    case NetworkMessage(id, dataMsg @ WorkflowDataMessage(from, seqNum, payload)) =>
      val start = System.currentTimeMillis()
      if (dataLogManager.isRecovering) {
        dataLogManager.feedInMessage(dataMsg)
        while (dataLogManager.hasNext) {
          val msg = dataLogManager.next()
          if (!dataLogManager.isRecovering) {
            dataLogManager.persistDataSender(from, payload.size, seqNum)
            enqueueDelayedAck(stashedDataAck, (sender, id))
          } else {
            sender ! NetworkAck(id)
          }
          dataInputPort.handleMessage(msg.from, msg.sequenceNumber, msg.payload)
        }
      } else {
        dataLogManager.persistDataSender(from, payload.size, seqNum)
        enqueueDelayedAck(stashedDataAck, (sender, id))
        dataInputPort.handleMessage(from, seqNum, payload)
      }
      processingTime += System.currentTimeMillis() - start
  }

  def receiveControlMessages: Receive = {
    case NetworkMessage(id, cmd @ WorkflowControlMessage(from, seqNum, payload)) =>
      val start = System.currentTimeMillis()
      controlLogManager.persistControlMessage(cmd)
      enqueueDelayedAck(stashedControlAck, (sender, id))
      try {
        // use control input port to pass control messages
        controlInputPort.handleMessage(from, seqNum, payload)
      } catch safely {
        case e =>
          logger.logError(WorkflowRuntimeError(e, identifier.toString))
      }
      processingTime += System.currentTimeMillis() - start
  }

  final def handleDataPayload(from: VirtualIdentity, dataPayload: DataPayload): Unit = {
    tupleProducer.processDataPayload(from, dataPayload)
  }

  final def handleControlPayload(from: VirtualIdentity, controlPayload: ControlPayload): Unit = {
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
