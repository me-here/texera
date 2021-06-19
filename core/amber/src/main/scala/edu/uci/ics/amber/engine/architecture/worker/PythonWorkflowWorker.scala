package edu.uci.ics.amber.engine.architecture.worker
import akka.actor.ActorRef
import edu.uci.ics.amber.engine.architecture.common.WorkflowActor
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.NetworkMessage
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkInputPort
import edu.uci.ics.amber.engine.common.IOperatorExecutor
import edu.uci.ics.amber.engine.common.ambermessage.{
  ControlPayload,
  DataPayload,
  WorkflowControlMessage,
  WorkflowDataMessage
}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.{ControlInvocation, ReturnPayload}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.error.WorkflowRuntimeError
import edu.uci.ics.texera.web.WebUtils

import java.io.IOException
import java.net.ServerSocket
import java.util.concurrent.{Executors, ExecutorService, Future}

class PythonWorkflowWorker(
    identifier: ActorVirtualIdentity,
    operator: IOperatorExecutor,
    parentNetworkCommunicationActorRef: ActorRef
) extends WorkflowActor(identifier, parentNetworkCommunicationActorRef) {
  lazy val dataInputPort: NetworkInputPort[DataPayload] =
    new NetworkInputPort[DataPayload](this.logger, this.handleDataPayload)
  lazy val controlInputPort: NetworkInputPort[ControlPayload] =
    new NetworkInputPort[ControlPayload](this.logger, this.handleControlPayload)
  override val rpcHandlerInitializer: AsyncRPCHandlerInitializer = null
  val config = WebUtils.config
  val pythonPath = config.getString("python.path").trim
  private val serverThreadExecutor: ExecutorService = Executors.newSingleThreadExecutor
  private val clientThreadExecutor: ExecutorService = Executors.newSingleThreadExecutor
  private var clientThread: Future[_] = null
  private var pythonRPCClient: PythonRPCClient = null
  private var pythonServerProcess: Process = null

  override def receive: Receive = receiveAndProcessMessages

  start

  def receiveAndProcessMessages: Receive = {

    disallowActorRefRelatedMessages orElse {
      case NetworkMessage(id, WorkflowDataMessage(from, seqNum, payload)) =>
        dataInputPort.handleMessage(this.sender(), id, from, seqNum, payload)
        logger.logInfo("getting a data message")
      case NetworkMessage(id, WorkflowControlMessage(from, seqNum, payload)) =>
        controlInputPort.handleMessage(this.sender(), id, from, seqNum, payload)
        logger.logInfo("getting a control message")
      case other =>
        logger.logError(
          WorkflowRuntimeError(s"unhandled message: $other", identifier.toString, Map.empty)
        )
    }
  }

  final def handleDataPayload(from: ActorVirtualIdentity, dataPayload: DataPayload): Unit = {
//    tupleProducer.processDataPayload(from, dataPayload)
    println("handling a data payload!!!!")
    pythonRPCClient.sendBatch(dataPayload, from)
  }

  final def handleControlPayload(
      from: ActorVirtualIdentity,
      controlPayload: ControlPayload
  ): Unit = {
    logger.logInfo("handling a controlPayload" + controlPayload.toString)
    controlPayload match {
      case controlCommand @ (ControlInvocation(_, _) | ReturnPayload(_, _)) =>
        pythonRPCClient.enqueueCommand(controlCommand, from)
      case _ =>
        logger.logError(
          WorkflowRuntimeError(
            s"unhandled control payload: $controlPayload",
            identifier.toString,
            Map.empty
          )
        )
    }
//    // let dp thread process it
//    assert(from.isInstanceOf[ActorVirtualIdentity])
//    controlPayload match {
//      case controlCommand @ (ControlInvocation(_, _) | ReturnPayload(_, _)) =>
//        dataProcessor.enqueueCommand(controlCommand, from)
//      case _ =>
//        logger.logError(
//          WorkflowRuntimeError(
//            s"unhandled control payload: $controlPayload",
//            identifier.toString,
//            Map.empty
//          )
//        )
//    }
  }

  def startRPCServer(outputPortNumber: Int): Unit = {

    val serverThread = serverThreadExecutor.submit(new PythonRPCServer(outputPortNumber))
  }

  def startRPCClient(inputPortNumber: Int): Unit = {

    pythonRPCClient = new PythonRPCClient(inputPortNumber)
    clientThread = clientThreadExecutor.submit(pythonRPCClient)
  }

  override def postStop(): Unit = {
    // shutdown dp thread by sending a command
    pythonServerProcess.destroyForcibly()
    logger.logInfo("python worker stopped!")
  }

  /**
    * Get a random free port.
    *
    * @return The port number.
    * @throws IOException,RuntimeException Might happen when getting a free port.
    */ @throws[IOException]
  @throws[RuntimeException]
  private def getFreeLocalPort: Int = {
    var s: ServerSocket = null
    try { // ServerSocket(0) results in availability of a free random port
      s = new ServerSocket(0)
      s.getLocalPort
    } catch {
      case e: Exception =>
        throw new RuntimeException(e)
    } finally {
      assert(s != null)
      s.close()
    }
  }

  @throws[IOException]
  private def start: Unit = {
    val outputPortNumber = getFreeLocalPort
    val inputPortNumber = getFreeLocalPort
    // Start Flight server (Python process)
    val udfMainScriptPath =
      "/Users/yicong-huang/IdeaProjects/texera/core/amber/src/main/python/main.py"
    // TODO: find a better way to do default conf values.

    startRPCServer(inputPortNumber)

    pythonServerProcess = new ProcessBuilder(
      if (pythonPath.isEmpty) "python3"
      else pythonPath, // add fall back in case of empty
      "-u",
      udfMainScriptPath,
      Integer.toString(outputPortNumber),
      Integer.toString(inputPortNumber)
    ).inheritIO.start
    startRPCClient(outputPortNumber)

  }
}
