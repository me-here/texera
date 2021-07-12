package edu.uci.ics.amber.engine.architecture.worker
import akka.actor.ActorRef
import com.softwaremill.macwire.wire
import com.typesafe.config.Config
import edu.uci.ics.amber.engine.architecture.common.WorkflowActor
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.NetworkMessage
import edu.uci.ics.amber.engine.architecture.messaginglayer.{DataOutputPort, NetworkInputPort}
import edu.uci.ics.amber.engine.architecture.worker.promisehandler2.SendPythonUDF
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.QueryCurrentInputTupleHandler.QueryCurrentInputTuple
import edu.uci.ics.amber.engine.common.ambermessage.{
  ControlPayload,
  DataPayload,
  WorkflowControlMessage,
  WorkflowDataMessage
}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.{ControlInvocation, ReturnPayload}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.virtualidentity.util.SELF
import edu.uci.ics.amber.engine.common.{IOperatorExecutor, ISourceOperatorExecutor, ambermessage2}
import edu.uci.ics.amber.error.WorkflowRuntimeError
import edu.uci.ics.texera.web.WebUtils
import edu.uci.ics.texera.workflow.udf.python.PythonUDFOpExec

import java.io.IOException
import java.net.ServerSocket
import java.util.concurrent.{ExecutorService, Executors}
class PythonWorkflowWorker(
    identifier: ActorVirtualIdentity,
    operator: IOperatorExecutor,
    parentNetworkCommunicationActorRef: ActorRef
) extends WorkflowActor(identifier, parentNetworkCommunicationActorRef) {
  lazy val dataInputPort: NetworkInputPort[DataPayload] =
    new NetworkInputPort[DataPayload](this.logger, this.handleDataPayload)
  lazy val controlInputPort: NetworkInputPort[ControlPayload] =
    new NetworkInputPort[ControlPayload](this.logger, this.handleControlPayload)
  lazy val dataOutputPort: DataOutputPort = wire[DataOutputPort]
  override val rpcHandlerInitializer: AsyncRPCHandlerInitializer = null
  val config: Config = WebUtils.config
  val pythonPath: String = config.getString("python.path").trim
  private val serverThreadExecutor: ExecutorService = Executors.newSingleThreadExecutor
  private val clientThreadExecutor: ExecutorService = Executors.newSingleThreadExecutor

  private var pythonProxyClient: PythonProxyClient = null
  private var pythonServerProcess: Process = null

  override def receive: Receive = {
    disallowActorRefRelatedMessages orElse {
      case NetworkMessage(id, WorkflowDataMessage(from, seqNum, payload)) =>
        dataInputPort.handleMessage(this.sender(), id, from, seqNum, payload)
      case NetworkMessage(id, WorkflowControlMessage(from, seqNum, payload)) =>
        controlInputPort.handleMessage(this.sender(), id, from, seqNum, payload)
      case other =>
        logger.logError(
          WorkflowRuntimeError(s"unhandled message: $other", identifier.toString, Map.empty)
        )
    }
  }

  final def handleDataPayload(from: ActorVirtualIdentity, dataPayload: DataPayload): Unit = {
//    println("JAVA handing a DATA payload " + dataPayload)
    pythonProxyClient.sendData(dataPayload, from)
  }

  final def handleControlPayload(
      from: ActorVirtualIdentity,
      controlPayload: ControlPayload
  ): Unit = {
    controlPayload match {
      case ControlInvocation(commandID, command) =>
        if (command.isInstanceOf[QueryCurrentInputTuple]) {
          controlOutputPort.sendTo(from, ReturnPayload(commandID, null))
        } else {
          pythonProxyClient.enqueueCommand(controlPayload, from)
        }
      case ReturnPayload(_, _) =>
        pythonProxyClient.enqueueCommand(controlPayload, from)
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

    // try to send shutdown command so that it can gracefully shutdown
    pythonProxyClient.close()

    // wait for gracefully shutdown
    Thread.sleep(100)

    // destroy python process
    pythonServerProcess.destroyForcibly()
    println("PYTHON process stopped!")
  }

  override def preStart(): Unit = {
    start()
    sendUDF()
  }

  def sendUDF(): Unit = {
    pythonProxyClient.enqueueCommand(
      ambermessage2
        .ControlInvocation(
          999999L,
          SendPythonUDF(
            udf = operator.asInstanceOf[PythonUDFOpExec].getCode,
            isSource = operator.isInstanceOf[ISourceOperatorExecutor]
          )
        ),
      SELF
    )
  }

  @throws[IOException]
  private def start(): Unit = {
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

  def startRPCServer(outputPortNumber: Int): Unit = {

    serverThreadExecutor.submit(
      new PythonProxyServer(outputPortNumber, controlOutputPort, dataOutputPort)
    )
  }

  /**
    * Get a random free port.
    *
    * @return The port number.
    * @throws IOException ,RuntimeException Might happen when getting a free port.
    */
  @throws[IOException]
  private def getFreeLocalPort: Int = {
    var s: ServerSocket = null
    try {
      // ServerSocket(0) results in availability of a free random port
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

  def startRPCClient(inputPortNumber: Int): Unit = {

    pythonProxyClient = new PythonProxyClient(inputPortNumber, operator)
    clientThreadExecutor.submit(pythonProxyClient)
  }
}
