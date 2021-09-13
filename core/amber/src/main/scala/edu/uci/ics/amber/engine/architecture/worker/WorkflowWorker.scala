package edu.uci.ics.amber.engine.architecture.worker

import akka.actor.{ActorRef, Props}
import akka.util.Timeout
import com.softwaremill.macwire.wire
import com.typesafe.config.ConfigFactory
import edu.uci.ics.amber.engine.architecture.common.WorkflowActor
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.FatalErrorHandler.FatalError
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.WorkerExecutionStartedHandler.WorkerStateUpdated
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.{
  NetworkMessage,
  RegisterActorRef
}
import edu.uci.ics.amber.engine.architecture.messaginglayer.{
  BatchToTupleConverter,
  DataOutputPort,
  NetworkInputPort,
  TupleToBatchConverter
}
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.ShutdownDPThreadHandler.ShutdownDPThread
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState.{
  READY,
  RUNNING,
  UNINITIALIZED
}
import edu.uci.ics.amber.engine.common.IOperatorExecutor
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.ambermessage.{
  ControlPayload,
  DataPayload,
  WorkflowControlMessage,
  WorkflowDataMessage
}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.{ControlInvocation, ReturnInvocation}
import edu.uci.ics.amber.engine.common.rpc.{AsyncRPCClient, AsyncRPCHandlerInitializer}
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.virtualidentity.util.{CONTROLLER, SELF}
import edu.uci.ics.texera.reporter.workFlowReporter
import kamon.Kamon
import kamon.module.MetricReporter

import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object WorkflowWorker {
  def props(
      id: ActorVirtualIdentity,
      op: IOperatorExecutor,
      parentNetworkCommunicationActorRef: ActorRef
  ): Props =
    Props(new WorkflowWorker(id, op, parentNetworkCommunicationActorRef))
}

class WorkflowWorker(
    actorId: ActorVirtualIdentity,
    operator: IOperatorExecutor,
    parentNetworkCommunicationActorRef: ActorRef
) extends WorkflowActor(actorId, parentNetworkCommunicationActorRef) {
  lazy val pauseManager: PauseManager = wire[PauseManager]
  lazy val dataProcessor: DataProcessor = wire[DataProcessor]
  lazy val dataInputPort: NetworkInputPort[DataPayload] =
    new NetworkInputPort[DataPayload](this.actorId, this.handleDataPayload)
  lazy val controlInputPort: NetworkInputPort[ControlPayload] =
    new NetworkInputPort[ControlPayload](this.actorId, this.handleControlPayload)
  lazy val dataOutputPort: DataOutputPort = wire[DataOutputPort]
  lazy val batchProducer: TupleToBatchConverter = wire[TupleToBatchConverter]
  lazy val tupleProducer: BatchToTupleConverter = wire[BatchToTupleConverter]
  lazy val breakpointManager: BreakpointManager = wire[BreakpointManager]
  implicit val ec: ExecutionContext = context.dispatcher
  implicit val timeout: Timeout = 5.seconds
  val workerStateManager: WorkerStateManager = new WorkerStateManager()
  val rpcHandlerInitializer: AsyncRPCHandlerInitializer =
    wire[WorkerAsyncRPCHandlerInitializer]

  val receivedFaultedTupleIds: mutable.HashSet[Long] = new mutable.HashSet[Long]()
  var isCompleted = false
  var reporter: workFlowReporter = null
  if (parentNetworkCommunicationActorRef != null) {
    parentNetworkCommunicationActorRef ! RegisterActorRef(this.actorId, self)
  }

  workerStateManager.assertState(UNINITIALIZED)
  workerStateManager.transitTo(READY)

  override def receive: Receive = receiveAndProcessMessages

  def receiveAndProcessMessages: Receive =
    try {
      disallowActorRefRelatedMessages orElse {
        case NetworkMessage(id, WorkflowDataMessage(from, seqNum, payload)) =>
          dataInputPort.handleMessage(this.sender(), id, from, seqNum, payload)
        case NetworkMessage(id, WorkflowControlMessage(from, seqNum, payload)) =>
          controlInputPort.handleMessage(this.sender(), id, from, seqNum, payload)
        case other =>
          throw new WorkflowRuntimeException(s"unhandled message: $other")
      }
    } catch {
      case err: WorkflowRuntimeException =>
        logger.error(s"Encountered fatal error, worker is shutting done.", err)
        asyncRPCClient.send(
          FatalError(err),
          CONTROLLER
        )
        throw err;
    }

  def handleDataPayload(from: ActorVirtualIdentity, dataPayload: DataPayload): Unit = {
    if (workerStateManager.getCurrentState == READY) {
      workerStateManager.transitTo(RUNNING)
      asyncRPCClient.send(
        WorkerStateUpdated(workerStateManager.getCurrentState),
        CONTROLLER
      )
    }
    tupleProducer.processDataPayload(from, dataPayload)
  }

  def handleControlPayload(
      from: ActorVirtualIdentity,
      controlPayload: ControlPayload
  ): Unit = {
    // let dp thread process it
    controlPayload match {
      case controlCommand @ (ControlInvocation(_, _) | ReturnInvocation(_, _)) =>
        dataProcessor.enqueueCommand(controlCommand, from)
      case _ =>
        throw new WorkflowRuntimeException(s"unhandled control payload: $controlPayload")
    }
  }

  override def preStart(): Unit = {
    val config = workFlowReporter.readConfiguration(Kamon.config())
    logger.debug(s"Start workflow worker reporter:${self.path.toString}")
    reporter = new workFlowReporter(
      config,
      self.path.address.system + self.path.toStringWithoutAddress,
      actorId
    )
    Kamon.registerModule(self.path.toString, reporter)
  }
  override def postStop(): Unit = {
    // shutdown dp thread by sending a command
    dataProcessor.enqueueCommand(
      ControlInvocation(AsyncRPCClient.IgnoreReply, ShutdownDPThread()),
      SELF
    )
    stopReporter()

    logger.info("stopped!")

  }
  private def stopReporter(): Unit = {
    val clazz = Class.forName("kamon.module.ModuleRegistry$Entry")
    val field = Kamon.getClass.getDeclaredField("_moduleRegistry")
    field.setAccessible(true)
    val register = field.get(Kamon)
    val reporters = register.getClass.getDeclaredField("_metricReporterModules")
    reporters.setAccessible(true)
    val moduleMap = reporters.get(register).asInstanceOf[Map[String, AnyRef]]
    val stopModuleMethod =
      register.getClass.getDeclaredMethod("kamon$module$ModuleRegistry$$stopModule", clazz)
    stopModuleMethod.setAccessible(true)
    stopModuleMethod.invoke(register, moduleMap(self.path.toString))
  }

}
